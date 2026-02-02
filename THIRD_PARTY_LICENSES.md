# Third-Party Licenses

This project uses the following third-party components:

---

## all-MiniLM-L6-v2 (Sentence Transformer Model)

- **Source:** https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
- **License:** Apache License 2.0
- **Authors:** Nils Reimers and the Sentence Transformers team (https://www.sbert.net)
- **Base model:** nreimers/MiniLM-L6-H384-uncased
- **Usage:** Generating sentence embeddings for hybrid semantic + keyword search

The model weights are downloaded at runtime from `cdn.tabmail.ai/releases/models/all-MiniLM-L6-v2/`
and cached locally at `~/.tabmail/models/all-MiniLM-L6-v2/`. No modifications are made to the weights.

**Citation:**

```bibtex
@inproceedings{reimers-2019-sentence-bert,
  title = "Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks",
  author = "Reimers, Nils and Gurevych, Iryna",
  booktitle = "Proceedings of the 2019 Conference on Empirical Methods
               in Natural Language Processing",
  month = "11",
  year = "2019",
  publisher = "Association for Computational Linguistics",
  url = "https://arxiv.org/abs/1908.10084",
}
```

The full Apache 2.0 license text for this model is distributed alongside the model weights
and is available at: https://cdn.tabmail.ai/releases/models/all-MiniLM-L6-v2/LICENSE

---

## sqlite-vec

- **Source:** https://github.com/asg017/sqlite-vec
- **License:** Apache License 2.0 OR MIT License (dual-licensed)
- **Author:** Alex Garcia
- **Usage:** SQLite vector similarity search extension, compiled as a static C library

---

## candle

- **Source:** https://github.com/huggingface/candle
- **License:** Apache License 2.0 OR MIT License (dual-licensed)
- **Authors:** Hugging Face
- **Usage:** Pure Rust ML inference framework for running the embedding model

---

For Rust crate dependencies and their licenses, see `Cargo.toml` and the output of `cargo license`.
