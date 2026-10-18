/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::io::{Read, Write};

use anyhow::{bail, Context};

use crate::{config, protocol::Request};

pub fn read_message(stdin: &mut dyn Read) -> anyhow::Result<Option<Request>> {
    // The prefix can arrive split across reads (a pipe's or stdin buffer's refill can land inside
    // it), so it is read whole; only end of input before its first byte is the end of the stream.
    let mut len_buf = [0u8; 4];
    match stdin.read_exact(&mut len_buf[..1]) {
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        result => result.context("failed reading native message length")?,
    }
    stdin
        .read_exact(&mut len_buf[1..])
        .context("incomplete length prefix")?;

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



#[cfg(test)]
mod tests {
    use super::*;

    /// Gives at most one byte per read, as a pipe can when its writer is slow.
    struct OneByteAtATime<'a>(&'a [u8]);

    impl Read for OneByteAtATime<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let Some((&first, rest)) = self.0.split_first() else { return Ok(0) };
            if buf.is_empty() {
                return Ok(0);
            }
            buf[0] = first;
            self.0 = rest;
            Ok(1)
        }
    }

    fn framed(json: &str) -> Vec<u8> {
        let mut bytes = (json.len() as u32).to_le_bytes().to_vec();
        bytes.extend_from_slice(json.as_bytes());
        bytes
    }

    #[test]
    fn test_messages_whose_length_prefix_arrives_in_pieces_are_read_whole() {
        let mut input = framed(r#"{"id":"1","method":"memoryStats","params":{}}"#);
        input.extend(framed(r#"{"id":"2","method":"memoryStats","params":{}}"#));
        let mut stdin = OneByteAtATime(&input);

        let first = read_message(&mut stdin).unwrap().expect("first message");
        let second = read_message(&mut stdin).unwrap().expect("second message");

        assert_eq!((first.id.as_str(), first.method.as_str()), ("1", "memoryStats"));
        assert_eq!((second.id.as_str(), second.method.as_str()), ("2", "memoryStats"));
        assert!(read_message(&mut stdin).unwrap().is_none(), "end of input after the last message");
    }

    #[test]
    fn test_end_of_input_inside_a_length_prefix_is_an_error() {
        let mut stdin: &[u8] = &[7, 0];
        assert!(read_message(&mut stdin).is_err());
        let mut empty: &[u8] = &[];
        assert!(read_message(&mut empty).unwrap().is_none());
    }
}
