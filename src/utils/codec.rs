use bytes::BufMut;

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum CodecError {
    #[error("invalid varint: {0}")]
    InvalidVarint(String),
}

pub fn varintu32_length(v: u32) -> u32 {
    let mut v = v;
    let b: u32 = 128;
    let mut len = 0;
    while v >= b {
        v >>= 7;
        len += 1;
    }
    len + 1
}

pub fn encode_varintu32(buf: &mut Vec<u8>, v: u32) -> u32 {
    let mut v = v;
    let b: u32 = 128;
    let mut len = 0;
    while v >= b {
        buf.put_u8((v | b) as u8);
        v >>= 7;
        len += 1;
    }
    buf.put_u8(v as u8);
    len + 1
}

pub fn decode_varintu32(buf: &[u8]) -> Result<u32, CodecError> {
    let b: u8 = 128;
    let mut v = 0;
    let mut i = 0;
    loop {
        let Some(byte) = buf.get(i) else {
            return Err(CodecError::InvalidVarint(String::from_utf8(buf.to_vec()).unwrap()));
        };
        v += ((byte & 0x7F) as u32) << (i * 7);
        i += 1;
        if (byte & b) == 0 {
            break;
        }
    }
    Ok(v)
}

#[cfg(test)]
mod tests {

    use crate::utils::codec::{decode_varintu32, encode_varintu32};

    #[test]
    fn codec_u32() {
        let mut buf = vec![];
        let x: u32 = 127;
        let len = encode_varintu32(&mut buf, x);
        assert_eq!(len, 1);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);

        let mut buf = vec![];
        let x: u32 = 1 << 7;
        let len = encode_varintu32(&mut buf, x);
        println!("{:?}", buf);
        assert_eq!(len, 2);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);

        let mut buf = vec![];
        let x: u32 = (1 << 14) - 1;
        let len = encode_varintu32(&mut buf, x);
        assert_eq!(len, 2);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);

        let mut buf = vec![];
        let x: u32 = 1 << 14;
        let len = encode_varintu32(&mut buf, x);
        assert_eq!(len, 3);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);

        let mut buf = vec![];
        let x: u32 = (1 << 21) - 1;
        let len = encode_varintu32(&mut buf, x);
        assert_eq!(len, 3);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);

        let mut buf = vec![];
        let x: u32 = 1 << 21;
        let len = encode_varintu32(&mut buf, x);
        assert_eq!(len, 4);
        let y = decode_varintu32(&buf[..]);
        assert_eq!(y.unwrap(), x);
    }
}
