use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::Result;

/// Reads new bytes from `reader` starting at byte offset `pos`, writes them to `writer`,
/// and returns the number of bytes read (updating `pos`).
pub fn read_new_bytes<R: Read + Seek, W: Write>(
    reader: &mut R,
    writer: &mut W,
    pos: &mut u64,
) -> Result<usize> {
    let len = reader.seek(SeekFrom::End(0))?;
    if len <= *pos {
        return Ok(0);
    }
    reader.seek(SeekFrom::Start(*pos))?;
    let mut total_read = 0;
    let mut buffer = [0u8; 8192];
    let mut remaining = len - *pos;
    while remaining > 0 {
        let to_read = buffer.len().min(remaining as usize);
        let bytes_read = reader.read(&mut buffer[..to_read])?;
        if bytes_read == 0 {
            break;
        }
        writer.write_all(&buffer[..bytes_read])?;
        *pos += bytes_read as u64;
        total_read += bytes_read;
        remaining -= bytes_read as u64;
    }
    writer.flush()?;
    Ok(total_read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_new_bytes_incremental() {
        let mut data = Cursor::new(Vec::new());
        let mut pos = 0u64;
        let mut output = Vec::new();

        // 1. Initial read on empty buffer
        let n = read_new_bytes(&mut data, &mut output, &mut pos).unwrap();
        assert_eq!(n, 0);
        assert_eq!(pos, 0);
        assert!(output.is_empty());

        // 2. Append first chunk
        data.get_mut().extend_from_slice(b"hello world\n");
        let n1 = read_new_bytes(&mut data, &mut output, &mut pos).unwrap();
        assert_eq!(n1, 12);
        assert_eq!(pos, 12);
        assert_eq!(String::from_utf8_lossy(&output), "hello world\n");

        // 3. Poll again with no new content
        let n2 = read_new_bytes(&mut data, &mut output, &mut pos).unwrap();
        assert_eq!(n2, 0);
        assert_eq!(pos, 12);
        assert_eq!(String::from_utf8_lossy(&output), "hello world\n");

        // 4. Append second chunk
        data.get_mut().extend_from_slice(b"second chunk\n");
        let n3 = read_new_bytes(&mut data, &mut output, &mut pos).unwrap();
        assert_eq!(n3, 13);
        assert_eq!(pos, 25);
        assert_eq!(
            String::from_utf8_lossy(&output),
            "hello world\nsecond chunk\n"
        );
    }

    #[test]
    fn test_read_new_bytes_large_buffer() {
        let mut data = Cursor::new(vec![b'A'; 20000]);
        let mut pos = 0u64;
        let mut output = Vec::new();

        let n = read_new_bytes(&mut data, &mut output, &mut pos).unwrap();
        assert_eq!(n, 20000);
        assert_eq!(pos, 20000);
        assert_eq!(output.len(), 20000);
    }
}
