use maplit::hashmap;
use std::collections::HashMap;

pub type BucketFlags = u8;
pub type DBI = u8;
pub type CustomComparator = &'static str;

pub type HeaderHashKey = [u8; 8 + 1];

pub enum BucketFlag {
    Default = 0x00,
    ReverseKey = 0x02,
    DupSort = 0x04,
    IntegerKey = 0x08,
    DupFixed = 0x10,
    IntegerDup = 0x20,
    ReverseDup = 0x40,
}

pub const HEADER_PREFIX: &str = "h";
pub const HEADER_HASH_SUFFIX: &[u8] = b"H";

pub const CONFIG_PREFIX: &str = "ethereum-config-";

#[derive(Clone, Copy, Default)]
pub struct BucketConfigItem {
    pub flags: BucketFlags,
    // AutoDupSortKeysConversion - enables some keys transformation - to change db layout without changing app code.
    // Use it wisely - it helps to do experiments with DB format faster, but better reduce amount of Magic in app.
    // If good DB format found, push app code to accept this format and then disable this property.
    pub auto_dup_sort_keys_conversion: bool,
    pub is_deprecated: bool,
    pub dbi: DBI,
    // DupFromLen - if user provide key of this length, then next transformation applied:
    // v = append(k[DupToLen:], v...)
    // k = k[:DupToLen]
    // And opposite at retrieval
    // Works only if AutoDupSortKeysConversion enabled
    pub dup_from_len: u8,
    pub dup_to_len: u8,
    pub dup_fixed_size: u8,
    pub custom_comparator: CustomComparator,
    pub custom_dup_comparator: CustomComparator,
}

pub fn buckets_configs() -> HashMap<&'static str, BucketConfigItem> {
    hashmap! {
        "CurrentStateBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            auto_dup_sort_keys_conversion: true,
            dup_from_len: 72,
            dup_to_len: 40,
            ..Default::default()
        },
        "PlainAccountChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "PlainStorageChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "AccountChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "StorageChangeSetBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            ..Default::default()
        },
        "PlainStateBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            auto_dup_sort_keys_conversion: true,
            dup_from_len: 60,
            dup_to_len: 28,
            ..Default::default()
        },
        "IntermediateTrieHashBucket" => BucketConfigItem {
            flags: BucketFlag::DupSort as u8,
            custom_dup_comparator: "dup_cmp_suffix32",
            ..Default::default()
        },
    }
}

pub fn header_hash_key(block: u64) -> HeaderHashKey {
    let mut header_hash_key: HeaderHashKey = Default::default();

    header_hash_key[0..8].copy_from_slice(&block.to_be_bytes());
    header_hash_key[8..9].copy_from_slice(HEADER_HASH_SUFFIX);

    header_hash_key
}
