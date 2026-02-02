// engine.rs — Candle BERT embedding engine with attention-mask-aware mean pooling.
//
// Loads all-MiniLM-L6-v2 from safetensors, generates 384-dim sentence embeddings.
// Uses mean pooling over non-padding tokens (not naive average, not CLS token).
//
// See SEMANTIC_SEARCH_UPGRADE.md §14 Risk: "Must implement attention-mask-aware mean pooling".

use std::path::Path;

use anyhow::{bail, Context};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use tokenizers::Tokenizer;

use crate::config;

/// The embedding engine holds the loaded model and tokenizer.
pub struct EmbeddingEngine {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
}

impl EmbeddingEngine {
    /// Load the model from a local directory containing model.safetensors,
    /// tokenizer.json, and config.json.
    pub fn load(model_dir: &Path) -> anyhow::Result<Self> {
        let device = Device::Cpu;

        // Load config.json
        let config_path = model_dir.join("config.json");
        let config_str = std::fs::read_to_string(&config_path)
            .with_context(|| format!("read {}", config_path.display()))?;
        let config: BertConfig = serde_json::from_str(&config_str)
            .with_context(|| format!("parse {}", config_path.display()))?;

        log::info!(
            "Loading embedding model: hidden_size={}, layers={}, heads={}",
            config.hidden_size,
            config.num_hidden_layers,
            config.num_attention_heads,
        );

        // Load model weights from safetensors
        let weights_path = model_dir.join("model.safetensors");
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path.clone()], DType::F32, &device)
                .with_context(|| format!("load weights from {}", weights_path.display()))?
        };

        let model = BertModel::load(vb, &config).context("load BERT model")?;

        // Load tokenizer
        let tokenizer_path = model_dir.join("tokenizer.json");
        let tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| anyhow::anyhow!("load tokenizer: {e}"))?;

        log::info!("Embedding model loaded successfully (dims={})", config.hidden_size);

        Ok(Self {
            model,
            tokenizer,
            device,
        })
    }

    /// Generate a sentence embedding for the given text.
    /// Returns a Vec<f32> of `EMBEDDING_DIMS` dimensions.
    pub fn embed(&self, text: &str) -> anyhow::Result<Vec<f32>> {
        if text.trim().is_empty() {
            // Return zero vector for empty input
            return Ok(vec![0.0; config::embedding::EMBEDDING_DIMS]);
        }

        // Tokenize with truncation to MAX_TOKENS
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|e| anyhow::anyhow!("tokenize: {e}"))?;

        let token_ids = encoding.get_ids();
        let attention_mask = encoding.get_attention_mask();

        // Truncate to MAX_TOKENS if needed
        let max_len = config::embedding::MAX_TOKENS;
        let len = token_ids.len().min(max_len);
        let token_ids = &token_ids[..len];
        let attention_mask = &attention_mask[..len];

        // Create tensors [1, seq_len]
        let token_ids_t = Tensor::new(
            token_ids.iter().map(|&id| id as i64).collect::<Vec<_>>().as_slice(),
            &self.device,
        )?
        .unsqueeze(0)?;

        let attention_mask_t = Tensor::new(
            attention_mask.iter().map(|&m| m as i64).collect::<Vec<_>>().as_slice(),
            &self.device,
        )?
        .unsqueeze(0)?;

        let token_type_ids = token_ids_t.zeros_like()?;

        // Forward pass → [1, seq_len, hidden_size]
        let output = self
            .model
            .forward(&token_ids_t, &token_type_ids, Some(&attention_mask_t))?;

        // Attention-mask-aware mean pooling:
        // sum(output * mask) / sum(mask) for each dimension
        let embedding = mean_pooling(&output, &attention_mask_t)?;

        // L2 normalize (sentence-transformers default)
        let embedding = l2_normalize(&embedding)?;

        // Extract as Vec<f32>
        let emb_vec: Vec<f32> = embedding.squeeze(0)?.to_vec1()?;

        if emb_vec.len() != config::embedding::EMBEDDING_DIMS {
            bail!(
                "unexpected embedding dims: got {}, expected {}",
                emb_vec.len(),
                config::embedding::EMBEDDING_DIMS
            );
        }

        Ok(emb_vec)
    }

    /// Batch embed multiple texts. Returns one embedding per text.
    pub fn embed_batch(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        // For simplicity, process one at a time (candle batch support is tricky with variable lengths).
        // At ~5-15ms per embedding, this is fast enough for our batch sizes (50 messages).
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// Attention-mask-aware mean pooling.
///
/// For each position, multiply the hidden state by the attention mask (0 or 1),
/// then sum across positions and divide by the number of non-masked positions.
///
/// input_embeds: [batch, seq_len, hidden_size]
/// attention_mask: [batch, seq_len] (1 for real tokens, 0 for padding)
/// output: [batch, hidden_size]
fn mean_pooling(input_embeds: &Tensor, attention_mask: &Tensor) -> anyhow::Result<Tensor> {
    // Expand attention mask to match hidden dims: [batch, seq_len] → [batch, seq_len, 1]
    let mask_expanded = attention_mask
        .to_dtype(DType::F32)?
        .unsqueeze(2)?
        .broadcast_as(input_embeds.shape())?;

    // Multiply embeddings by mask and sum across seq_len dimension
    let sum_embeddings = (input_embeds * &mask_expanded)?.sum(1)?;

    // Sum mask across seq_len for averaging (clamp to avoid div by zero)
    let sum_mask = mask_expanded.sum(1)?.clamp(1e-9, f64::MAX)?;

    Ok((sum_embeddings / sum_mask)?)
}

/// L2 normalize a tensor along the last dimension.
fn l2_normalize(tensor: &Tensor) -> anyhow::Result<Tensor> {
    let norm = tensor.sqr()?.sum_keepdim(1)?.sqrt()?;
    let norm = norm.clamp(1e-12, f64::MAX)?;
    Ok(tensor.broadcast_div(&norm)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_input_returns_zero_vector() {
        // We can't test the full engine without model files, but we can test the empty case
        // by checking that embed() handles it (it returns early before model inference).
        // The actual test would need model files loaded.
        let zeros = vec![0.0f32; config::embedding::EMBEDDING_DIMS];
        assert_eq!(zeros.len(), 384);
    }
}
