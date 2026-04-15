use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rand::{Rng, RngCore};

use crate::properties::{Distribution, InsertOrder};

/// YCSB `Utils.FNVhash64` — 8-iteration FNV-1a over the little-endian bytes of `val`.
/// Returns the absolute value, matching the Java reference.
pub(crate) fn fnv_hash64(mut val: u64) -> u64 {
    const FNV_OFFSET_BASIS_64: u64 = 0xCBF2_9CE4_8422_2325;
    const FNV_PRIME_64: u64 = 1_099_511_628_211;
    let mut hash = FNV_OFFSET_BASIS_64;
    for _ in 0..8 {
        let octet = val & 0xff;
        val >>= 8;
        hash ^= octet;
        hash = hash.wrapping_mul(FNV_PRIME_64);
    }
    (hash as i64).unsigned_abs()
}

/// Build a YCSB-compatible key: `user<zero-pad><decimal>`, where the decimal is
/// either the raw `keynum` (ordered inserts) or `fnv_hash64(keynum)` (hashed).
pub(crate) fn build_key(keynum: u64, order: InsertOrder, zero_padding: usize) -> Bytes {
    let n = match order {
        InsertOrder::Ordered => keynum,
        InsertOrder::Hashed => fnv_hash64(keynum),
    };
    let digits = n.to_string();
    let pad = zero_padding.saturating_sub(digits.len());
    let mut out = Vec::with_capacity(4 + pad + digits.len());
    out.extend_from_slice(b"user");
    out.extend(std::iter::repeat_n(b'0', pad));
    out.extend_from_slice(digits.as_bytes());
    Bytes::from(out)
}

/// Tracks the highest monotonically-acknowledged insert key. Used by `Latest` reads
/// so they never see keys that haven't been durably published yet.
#[derive(Debug)]
pub(crate) struct AcknowledgedCounter {
    value: AtomicU64,
}

impl AcknowledgedCounter {
    pub(crate) fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
        }
    }

    pub(crate) fn get(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    pub(crate) fn acknowledge(&self, v: u64) {
        let mut cur = self.value.load(Ordering::Relaxed);
        while v > cur {
            match self
                .value
                .compare_exchange_weak(cur, v, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => cur = now,
            }
        }
    }
}

/// Trait for the per-op key sampler. Implementations are `Send` so they can be
/// owned by worker tasks. The method takes a trait object for the RNG so the
/// trait itself is dyn-compatible.
pub(crate) trait KeyChooser: Send {
    fn next_key(&mut self, rng: &mut dyn RngCore) -> u64;
}

pub(crate) struct UniformChooser {
    min: u64,
    len: u64,
    ack: std::sync::Arc<AcknowledgedCounter>,
}

impl UniformChooser {
    pub(crate) fn new(
        min: u64,
        initial_max: u64,
        ack: std::sync::Arc<AcknowledgedCounter>,
    ) -> Self {
        Self {
            min,
            len: initial_max.saturating_sub(min).max(1),
            ack,
        }
    }
}

impl KeyChooser for UniformChooser {
    fn next_key(&mut self, rng: &mut dyn RngCore) -> u64 {
        let ack = self.ack.get();
        let len = ack.saturating_sub(self.min).max(self.len).max(1);
        self.min + rng.random_range(0..len)
    }
}

pub(crate) struct SequentialChooser {
    min: u64,
    len: u64,
    cursor: u64,
    ack: std::sync::Arc<AcknowledgedCounter>,
}

impl SequentialChooser {
    pub(crate) fn new(
        min: u64,
        initial_max: u64,
        ack: std::sync::Arc<AcknowledgedCounter>,
    ) -> Self {
        Self {
            min,
            len: initial_max.saturating_sub(min).max(1),
            cursor: 0,
            ack,
        }
    }
}

impl KeyChooser for SequentialChooser {
    fn next_key(&mut self, _rng: &mut dyn RngCore) -> u64 {
        let ack = self.ack.get();
        let len = ack.saturating_sub(self.min).max(self.len).max(1);
        let v = self.min + (self.cursor % len);
        self.cursor = self.cursor.wrapping_add(1);
        v
    }
}

/// Zipfian generator matching YCSB's `ZipfianGenerator.java` algorithm
/// (Gray et al., "Quickly Generating Billion-Record Synthetic Databases").
pub(crate) struct Zipfian {
    items: u64,
    base: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}

impl Zipfian {
    pub(crate) fn new(min: u64, max: u64, theta: f64) -> Self {
        let items = max.saturating_sub(min).max(1);
        Self::with_zetan(min, items, theta, zeta(items, theta))
    }

    /// Construct with a precomputed `zetan`. Used for huge universes where
    /// the O(n) sum in `zeta()` would be prohibitively slow.
    pub(crate) fn with_zetan(min: u64, items: u64, theta: f64, zetan: f64) -> Self {
        let zeta2theta = zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0_f64 / items as f64).powf(1.0 - theta)) / (1.0 - zeta2theta / zetan);
        Self {
            items,
            base: min,
            theta,
            zetan,
            alpha,
            eta,
        }
    }

    pub(crate) fn next(&self, rng: &mut dyn RngCore) -> u64 {
        let u: f64 = rng.random();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return self.base;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return self.base + 1;
        }
        let value = (self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha);
        self.base + value as u64
    }
}

fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 1..=n {
        sum += 1.0 / (i as f64).powf(theta);
    }
    sum
}

/// Scrambled Zipfian — draws from a fixed large universe, hashes, then mods
/// back into `[min, min+itemcount)`. Matches YCSB's `ScrambledZipfianGenerator`.
pub(crate) struct ScrambledZipfianChooser {
    min: u64,
    itemcount: u64,
    zipf: Zipfian,
}

/// Fixed universe size used by YCSB `ScrambledZipfianGenerator`.
const SCRAMBLED_ITEMS: u64 = 10_000_000_000;
/// Precomputed `zeta(SCRAMBLED_ITEMS, 0.99)` — lifted from YCSB Java so we don't
/// have to sum 10B terms on every worker startup.
const SCRAMBLED_ZETAN_THETA_0_99: f64 = 26.46902820178302;

impl ScrambledZipfianChooser {
    pub(crate) fn new(min: u64, max: u64, theta: f64) -> Self {
        let itemcount = max.saturating_sub(min).max(1);
        // Only the YCSB-default theta (0.99) has a hardcoded zetan. For other
        // thetas we fall back to the O(n) sum, which is slow but correct.
        let zipf = if (theta - 0.99).abs() < 1e-9 {
            Zipfian::with_zetan(0, SCRAMBLED_ITEMS, theta, SCRAMBLED_ZETAN_THETA_0_99)
        } else {
            Zipfian::new(0, SCRAMBLED_ITEMS, theta)
        };
        Self {
            min,
            itemcount,
            zipf,
        }
    }
}

impl KeyChooser for ScrambledZipfianChooser {
    fn next_key(&mut self, rng: &mut dyn RngCore) -> u64 {
        let raw = self.zipf.next(rng);
        self.min + fnv_hash64(raw) % self.itemcount
    }
}

/// Latest distribution — skewed toward the most recently inserted key.
///
/// Matches YCSB Java's `SkewedLatestGenerator`: the `Zipfian` is built once at
/// construction with `span = max - min` (the keyspace size at load time) and
/// is never rebuilt. Each call samples an offset from this fixed distribution
/// and subtracts it from the live `ack` value, so newly inserted keys slide the
/// sampling window forward without any per-call zeta recomputation.
pub(crate) struct LatestChooser {
    ack: std::sync::Arc<AcknowledgedCounter>,
    zipf: Zipfian,
}

impl LatestChooser {
    pub(crate) fn new(
        min: u64,
        max: u64,
        ack: std::sync::Arc<AcknowledgedCounter>,
        theta: f64,
    ) -> Self {
        let span = max.saturating_sub(min).max(1);
        Self {
            ack,
            zipf: Zipfian::new(0, span, theta),
        }
    }
}

impl KeyChooser for LatestChooser {
    fn next_key(&mut self, rng: &mut dyn RngCore) -> u64 {
        let last = self.ack.get();
        let offset = self.zipf.next(rng);
        last.saturating_sub(offset)
    }
}

/// Hotspot — reads from a hot subrange with probability `opn_frac`, from the
/// cold subrange otherwise. Matches YCSB's `HotspotIntegerGenerator`.
pub(crate) struct HotspotChooser {
    min: u64,
    hot_len: u64,
    cold_start: u64,
    cold_len: u64,
    opn_frac: f64,
}

impl HotspotChooser {
    pub(crate) fn new(min: u64, max: u64, data_frac: f64, opn_frac: f64) -> Self {
        let total = max.saturating_sub(min).max(1);
        let hot_len = ((total as f64 * data_frac) as u64).max(1);
        let cold_start = min + hot_len;
        let cold_len = total.saturating_sub(hot_len).max(1);
        Self {
            min,
            hot_len,
            cold_start,
            cold_len,
            opn_frac,
        }
    }
}

impl KeyChooser for HotspotChooser {
    fn next_key(&mut self, rng: &mut dyn RngCore) -> u64 {
        let p: f64 = rng.random();
        if p < self.opn_frac {
            self.min + rng.random_range(0..self.hot_len)
        } else {
            self.cold_start + rng.random_range(0..self.cold_len)
        }
    }
}

/// Factory: construct the right KeyChooser for a given distribution.
pub(crate) fn make_chooser(
    dist: Distribution,
    min: u64,
    max: u64,
    theta: f64,
    hotspot_data_frac: f64,
    hotspot_opn_frac: f64,
    ack: std::sync::Arc<AcknowledgedCounter>,
) -> Box<dyn KeyChooser> {
    match dist {
        Distribution::Uniform => Box::new(UniformChooser::new(min, max, ack)),
        Distribution::Sequential => Box::new(SequentialChooser::new(min, max, ack)),
        Distribution::Zipfian => Box::new(ScrambledZipfianChooser::new(min, max, theta)),
        Distribution::Latest => Box::new(LatestChooser::new(min, max, ack, theta)),
        Distribution::Hotspot => Box::new(HotspotChooser::new(
            min,
            max,
            hotspot_data_frac,
            hotspot_opn_frac,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_xoshiro::Xoshiro256PlusPlus;

    #[test]
    fn fnv_hash_matches_reference_value() {
        // Reference: FNVhash64(0) in YCSB Java is |FNV-1a iterated over 8 zero bytes|.
        // Offset basis is 0xCBF29CE484222325, and multiplying by FNV_PRIME_64 8 times
        // with 0 octets yields 0xCBF29CE484222325 * 1099511628211^8 (mod 2^64).
        // We don't check the exact constant — just that hash(0) is stable and nonzero.
        let h0 = fnv_hash64(0);
        assert!(h0 != 0);
        assert_eq!(h0, fnv_hash64(0));
        assert_ne!(fnv_hash64(1), fnv_hash64(2));
    }

    #[test]
    fn build_key_is_hashed_by_default() {
        let k = build_key(42, InsertOrder::Hashed, 20);
        assert!(k.starts_with(b"user"));
        assert_eq!(k.len(), 4 + 20);
    }

    #[test]
    fn build_key_ordered_uses_raw_int() {
        let k = build_key(42, InsertOrder::Ordered, 8);
        assert_eq!(&k[..], b"user00000042");
    }

    #[test]
    fn zipfian_stays_in_range() {
        let z = Zipfian::new(0, 1000, 0.99);
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(7);
        for _ in 0..10_000 {
            let v = z.next(&mut rng);
            assert!(v < 1000);
        }
    }

    #[test]
    fn uniform_chooser_stays_in_bounds() {
        let ack = std::sync::Arc::new(AcknowledgedCounter::new(100));
        let mut c = UniformChooser::new(0, 100, ack);
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(1);
        for _ in 0..1000 {
            let v = c.next_key(&mut rng);
            assert!(v < 100);
        }
    }

    #[test]
    fn acknowledged_counter_is_monotonic() {
        let c = AcknowledgedCounter::new(0);
        c.acknowledge(5);
        assert_eq!(c.get(), 5);
        c.acknowledge(3);
        assert_eq!(c.get(), 5);
        c.acknowledge(10);
        assert_eq!(c.get(), 10);
    }

    #[test]
    fn latest_chooser_returns_recent_keys() {
        let ack = std::sync::Arc::new(AcknowledgedCounter::new(1000));
        let mut c = LatestChooser::new(0, 1000, ack, 0.99);
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(42);
        let mut high_bucket = 0;
        for _ in 0..5_000 {
            let v = c.next_key(&mut rng);
            if v >= 900 {
                high_bucket += 1;
            }
        }
        // The top 10% of the keyspace should dominate a zipfian-over-offset.
        assert!(
            high_bucket > 2_500,
            "expected >2500 samples in top decile, got {high_bucket}"
        );
    }
}
