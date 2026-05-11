use bytes::Bytes;
use ulid::Ulid;

/// Reference to a value stored inside an externally-packed blob object ("pack").
///
/// The encoded form is:
/// - 16 bytes: pack ULID in big-endian order
/// - 4 bytes: byte offset within the pack in big-endian order
/// - 4 bytes: value length in bytes in big-endian order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobRef {
    pub pack_id: Ulid,
    pub offset: u32,
    pub length: u32,
}

impl BlobRef {
    pub const ENCODED_LEN: usize = 16 + 2 * std::mem::size_of::<u32>();

    pub const fn new(pack_id: Ulid, offset: u32, length: u32) -> Self {
        Self {
            pack_id,
            offset,
            length,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[..16].copy_from_slice(&self.pack_id.to_bytes());
        bytes[16..20].copy_from_slice(&self.offset.to_be_bytes());
        bytes[20..].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: [u8; Self::ENCODED_LEN]) -> Self {
        let mut pack_id = [0; 16];
        pack_id.copy_from_slice(&bytes[..16]);

        let mut offset = [0; 4];
        offset.copy_from_slice(&bytes[16..20]);

        let mut length = [0; 4];
        length.copy_from_slice(&bytes[20..]);

        Self {
            pack_id: Ulid::from_bytes(pack_id),
            offset: u32::from_be_bytes(offset),
            length: u32::from_be_bytes(length),
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let bytes: [u8; Self::ENCODED_LEN] = bytes.try_into().ok()?;
        Some(Self::from_bytes(bytes))
    }
}

/// Represents a key-value pair known not to be a tombstone.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
    pub seq: u64,
    pub create_ts: i64,
    pub expire_ts: Option<i64>,
}

/// Represents a key-value pair that may be a tombstone.
///
/// This is the entry type passed to compaction for each key value pair.
#[derive(Debug, Clone, PartialEq)]
pub struct RowEntry {
    /// The key bytes.
    pub key: Bytes,
    /// The value, which may be a regular value, merge operand, or tombstone.
    pub value: ValueDeletable,
    /// The sequence number of this entry.
    pub seq: u64,
    /// The creation timestamp (if set).
    pub create_ts: Option<i64>,
    /// The expiration timestamp (if set).
    pub expire_ts: Option<i64>,
}

impl RowEntry {
    pub(crate) fn new(
        key: Bytes,
        value: ValueDeletable,
        seq: u64,
        create_ts: Option<i64>,
        expire_ts: Option<i64>,
    ) -> Self {
        Self {
            key,
            value,
            seq,
            create_ts,
            expire_ts,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        let mut size = self.key.len() + self.value.len();
        // Add size for sequence number
        size += std::mem::size_of::<u64>();
        // Add size for timestamps
        if self.create_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        if self.expire_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        size
    }

    /// Returns the encoded size of this entry when written to an SST block.
    /// The `key_prefix_len` is the number of bytes shared with the block's first key.
    pub(crate) fn encoded_size(&self, key_prefix_len: usize) -> usize {
        let key_suffix_len = self.key.len() - key_prefix_len;
        let mut size = std::mem::size_of::<u16>() // key_prefix_len
            + std::mem::size_of::<u16>() // key_suffix_len
            + key_suffix_len
            + std::mem::size_of::<u64>() // seq
            + std::mem::size_of::<u8>(); // flags

        if self.expire_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        if self.create_ts.is_some() {
            size += std::mem::size_of::<i64>();
        }
        if !self.value.is_tombstone() {
            size += std::mem::size_of::<u32>(); // value_len
            size += self.value.len();
        }
        size
    }

    #[cfg(test)]
    pub(crate) fn new_value(key: &[u8], value: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Value(Bytes::copy_from_slice(value)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_merge(key: &[u8], value: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Merge(Bytes::copy_from_slice(value)),
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_tombstone(key: &[u8], seq: u64) -> Self {
        Self {
            key: Bytes::copy_from_slice(key),
            value: ValueDeletable::Tombstone,
            seq,
            create_ts: None,
            expire_ts: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_create_ts(&self, create_ts: i64) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            seq: self.seq,
            create_ts: Some(create_ts),
            expire_ts: self.expire_ts,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_expire_ts(&self, expire_ts: i64) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            seq: self.seq,
            create_ts: self.create_ts,
            expire_ts: Some(expire_ts),
        }
    }
}

impl From<RowEntry> for KeyValue {
    fn from(entry: RowEntry) -> Self {
        KeyValue {
            key: entry.key,
            value: entry
                .value
                .as_bytes()
                .expect("RowEntry should have a value"),
            seq: entry.seq,
            create_ts: entry.create_ts.unwrap_or(0),
            expire_ts: entry.expire_ts,
        }
    }
}

/// Represents a value for a key.
///
/// This enum distinguishes between:
/// - `Value`: A regular value entry
/// - `BlobRef`: A reference to a value stored outside the SST
/// - `Merge`: A merge operand (used with merge operators)
/// - `Tombstone`: A deletion marker
///
/// Compaction filters receive this type to determine entry type and decide
/// how to handle each entry.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueDeletable {
    /// A regular value.
    Value(Bytes),
    /// A reference to an external blob object.
    BlobRef(BlobRef),
    /// A merge operand (used with merge operators).
    Merge(Bytes),
    /// A tombstone (deletion marker).
    Tombstone,
}

#[allow(clippy::len_without_is_empty)]
impl ValueDeletable {
    /// Returns the length of the value in bytes, or 0 for tombstones.
    pub fn len(&self) -> usize {
        match self {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => v.len(),
            ValueDeletable::BlobRef(_) => BlobRef::ENCODED_LEN,
            ValueDeletable::Tombstone => 0,
        }
    }

    /// Returns true if this is a tombstone (deletion marker).
    pub fn is_tombstone(&self) -> bool {
        matches!(self, ValueDeletable::Tombstone)
    }

    /// Returns the value bytes if this is a Value or Merge, None for Tombstone.
    ///
    /// For `BlobRef`, this returns the encoded blob reference bytes.
    pub fn as_bytes(&self) -> Option<Bytes> {
        match self {
            ValueDeletable::Value(v) | ValueDeletable::Merge(v) => Some(v.clone()),
            ValueDeletable::BlobRef(blob_ref) => Some(Bytes::copy_from_slice(&blob_ref.to_bytes())),
            ValueDeletable::Tombstone => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::row::SstRowEntry;
    use rstest::rstest;

    // Tombstone encoding:
    //   key_prefix_len (u16) + key_suffix_len (u16) + key_suffix + seq (u64) + flags (u8)
    // = 2 + 2 + key_suffix_len + 8 + 1
    // = 13 + key_suffix_len
    #[rstest]
    #[case(0, 18)] // key_suffix_len=5: 13 + 5 = 18
    #[case(2, 16)] // key_suffix_len=3: 13 + 3 = 16
    #[case(4, 14)] // key_suffix_len=1: 13 + 1 = 14
    fn encoded_size_tombstone(#[case] prefix_len: usize, #[case] expected: usize) {
        let entry = RowEntry::new_tombstone(b"hello", 1);
        assert_eq!(entry.encoded_size(prefix_len), expected);
    }

    // Value encoding:
    //   tombstone encoding + value_len (u32) + value
    // = 13 + key_suffix_len + 4 + value_len
    // = 17 + key_suffix_len + value_len
    #[rstest]
    #[case(0, 25)] // key_suffix_len=5, value_len=3: 17 + 5 + 3 = 25
    #[case(2, 23)] // key_suffix_len=3, value_len=3: 17 + 3 + 3 = 23
    #[case(4, 21)] // key_suffix_len=1, value_len=3: 17 + 1 + 3 = 21
    fn encoded_size_value(#[case] prefix_len: usize, #[case] expected: usize) {
        let entry = RowEntry::new_value(b"hello", b"val", 1);
        assert_eq!(entry.encoded_size(prefix_len), expected);
    }

    #[test]
    fn encoded_size_matches_sst_row_entry() {
        let entry = RowEntry::new_value(b"prefixkey", b"value", 1);
        let sst_entry = SstRowEntry::new(
            6,
            Bytes::from("key"),
            1,
            ValueDeletable::Value(Bytes::from("value")),
            None,
            None,
        );
        assert_eq!(entry.encoded_size(6), sst_entry.size());
    }

    #[test]
    fn blob_ref_round_trips_through_bytes() {
        let blob_ref = BlobRef::new(
            Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").expect("valid ulid"),
            128,
            4_096,
        );

        let encoded = blob_ref.to_bytes();
        let decoded = BlobRef::from_bytes(encoded);

        assert_eq!(decoded, blob_ref);
    }

    #[test]
    fn blob_ref_round_trips_from_slice() {
        let blob_ref = BlobRef::new(
            Ulid::from_string("01J79C21YKR31J2BS1EFXJZ7MR").expect("valid ulid"),
            7_777,
            123_456,
        );

        let encoded = blob_ref.to_bytes();
        let decoded = BlobRef::from_slice(&encoded).expect("valid blob ref bytes");

        assert_eq!(decoded, blob_ref);
    }

    #[test]
    fn blob_ref_rejects_wrong_size_slice() {
        let encoded = [0u8; BlobRef::ENCODED_LEN - 1];
        assert!(BlobRef::from_slice(&encoded).is_none());
    }
}
