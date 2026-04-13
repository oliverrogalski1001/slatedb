use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rand::Rng;

/// YCSB-style printable ASCII alphabet used to fill field values.
const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Field name for the Nth field in a record: "field0", "field1", ...
pub(crate) fn field_name(i: usize) -> String {
    format!("field{i}")
}

/// Encode a full record of `field_count` fields of `field_length` printable bytes each.
///
/// Wire format (little-endian, self-describing so partial updates can round-trip):
///   u32  field_count
///   per field:
///     u16  name_len
///     [u8] name
///     u32  value_len
///     [u8] value
pub(crate) fn encode_record<R: Rng + ?Sized>(
    rng: &mut R,
    field_count: usize,
    field_length: usize,
) -> Bytes {
    let mut buf = BytesMut::with_capacity(4 + field_count * (2 + 8 + 4 + field_length));
    buf.put_u32_le(field_count as u32);
    for i in 0..field_count {
        let name = field_name(i);
        buf.put_u16_le(name.len() as u16);
        buf.put_slice(name.as_bytes());
        buf.put_u32_le(field_length as u32);
        append_random_bytes(rng, field_length, &mut buf);
    }
    buf.freeze()
}

/// Decode a record back into a map of field name -> bytes.
pub(crate) fn decode_record(mut bytes: &[u8]) -> Option<HashMap<String, Bytes>> {
    if bytes.remaining() < 4 {
        return None;
    }
    let count = bytes.get_u32_le() as usize;
    let mut out = HashMap::with_capacity(count);
    for _ in 0..count {
        if bytes.remaining() < 2 {
            return None;
        }
        let name_len = bytes.get_u16_le() as usize;
        if bytes.remaining() < name_len + 4 {
            return None;
        }
        let name = std::str::from_utf8(&bytes[..name_len]).ok()?.to_string();
        bytes.advance(name_len);
        let val_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < val_len {
            return None;
        }
        let value = Bytes::copy_from_slice(&bytes[..val_len]);
        bytes.advance(val_len);
        out.insert(name, value);
    }
    Some(out)
}

/// Re-encode a record, replacing the value of `field_idx` with fresh random bytes
/// of `field_length`. If `existing` cannot be decoded, a full fresh record is written.
pub(crate) fn update_one_field<R: Rng + ?Sized>(
    rng: &mut R,
    existing: Option<&[u8]>,
    field_count: usize,
    field_length: usize,
    field_idx: usize,
) -> Bytes {
    let Some(bytes) = existing else {
        return encode_record(rng, field_count, field_length);
    };
    let Some(mut fields) = decode_record(bytes) else {
        return encode_record(rng, field_count, field_length);
    };
    let mut new_val = BytesMut::with_capacity(field_length);
    append_random_bytes(rng, field_length, &mut new_val);
    fields.insert(field_name(field_idx), new_val.freeze());
    reencode(fields, field_count, field_length)
}

fn reencode(mut fields: HashMap<String, Bytes>, field_count: usize, field_length: usize) -> Bytes {
    let mut buf = BytesMut::with_capacity(4 + field_count * (2 + 8 + 4 + field_length));
    buf.put_u32_le(field_count as u32);
    for i in 0..field_count {
        let name = field_name(i);
        let value = fields.remove(&name).unwrap_or_default();
        buf.put_u16_le(name.len() as u16);
        buf.put_slice(name.as_bytes());
        buf.put_u32_le(value.len() as u32);
        buf.put_slice(&value);
    }
    buf.freeze()
}

fn append_random_bytes<R: Rng + ?Sized>(rng: &mut R, n: usize, buf: &mut BytesMut) {
    buf.reserve(n);
    for _ in 0..n {
        let idx = rng.random_range(0..ALPHABET.len());
        buf.put_u8(ALPHABET[idx]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_xoshiro::Xoshiro256PlusPlus;

    #[test]
    fn roundtrip_full_record() {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(1);
        let b = encode_record(&mut rng, 10, 100);
        let decoded = decode_record(&b).expect("decode");
        assert_eq!(decoded.len(), 10);
        for i in 0..10 {
            let v = decoded.get(&field_name(i)).expect("field");
            assert_eq!(v.len(), 100);
            assert!(v.iter().all(|&b| ALPHABET.contains(&b)));
        }
    }

    #[test]
    fn update_one_field_preserves_others() {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(2);
        let original = encode_record(&mut rng, 4, 20);
        let original_fields = decode_record(&original).unwrap();
        let updated = update_one_field(&mut rng, Some(&original), 4, 20, 2);
        let updated_fields = decode_record(&updated).unwrap();
        assert_eq!(updated_fields.len(), 4);
        for i in 0..4 {
            if i == 2 {
                assert_ne!(
                    original_fields[&field_name(i)],
                    updated_fields[&field_name(i)]
                );
            } else {
                assert_eq!(
                    original_fields[&field_name(i)],
                    updated_fields[&field_name(i)]
                );
            }
        }
    }

    #[test]
    fn update_with_missing_existing_rewrites_full() {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(3);
        let b = update_one_field(&mut rng, None, 3, 10, 0);
        let fields = decode_record(&b).unwrap();
        assert_eq!(fields.len(), 3);
    }
}
