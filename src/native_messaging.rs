use std::io::{Read, Write};

use anyhow::{bail, Context};

use crate::{config, protocol::Request};

pub fn read_message(stdin: &mut dyn Read) -> anyhow::Result<Option<Request>> {
    let mut len_buf = [0u8; 4];
    let n = stdin.read(&mut len_buf).context("failed reading native message length")?;
    if n == 0 {
        return Ok(None);
    }
    if n < 4 {
        bail!("incomplete length prefix (expected 4 bytes, got {n})");
    }

    // Native messaging uses 32-bit little-endian length.
    let msg_len = u32::from_le_bytes(len_buf);
    if msg_len > config::native_messaging::MAX_MESSAGE_SIZE_BYTES {
        bail!("message too large: {msg_len} bytes");
    }

    let mut payload = vec![0u8; msg_len as usize];
    stdin
        .read_exact(&mut payload)
        .with_context(|| format!("failed reading native message payload ({msg_len} bytes)"))?;

    let req: Request = serde_json::from_slice(&payload).context("invalid JSON request")?;
    Ok(Some(req))
}

pub fn write_json(stdout: &mut dyn Write, v: &serde_json::Value) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(v).context("failed serializing JSON response")?;
    let len = bytes
        .len()
        .try_into()
        .context("response too large for u32 length")?;
    stdout.write_all(&u32::to_le_bytes(len))?;
    stdout.write_all(&bytes)?;
    stdout.flush().context("failed flushing stdout")?;
    Ok(())
}


