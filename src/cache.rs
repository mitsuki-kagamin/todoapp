use std::hash::BuildHasherDefault;
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use hashbrown::HashMap;
use nohash_hasher::NoHashHasher;
use uuid::Uuid;

/// Two-tier read cache sitting in front of [`crate::store::Store`], as laid
/// out in `api_reference.md`:
///
/// ```text
/// L1 (hot, single slot) -> L2 (warm, hashmap) -> DB
/// ```
///
/// Both tiers cache the already-serialized JSON response body, so a hit
/// never touches the store or `sonic_rs` again.
pub struct Cache {
    hot: ArcSwapOption<HotEntry>,
    warm: ArcSwap<WarmMap>,
}

struct HotEntry {
    id: Uuid,
    body: Bytes,
}

// The fold below already spreads a random UUID over the full u64 range, so
// hashing it again would just waste cycles - key the map on it directly.
type WarmMap = HashMap<u64, (Uuid, Bytes), BuildHasherDefault<NoHashHasher<u64>>>;

#[inline(always)]
fn fold(id: Uuid) -> u64 {
    let n = id.as_u128();
    (n as u64) ^ ((n >> 64) as u64)
}

impl Cache {
    pub fn new() -> Self {
        Self {
            hot: ArcSwapOption::const_empty(),
            warm: ArcSwap::from_pointee(WarmMap::default()),
        }
    }

    pub fn get_hot(&self, id: Uuid) -> Option<Bytes> {
        let guard = self.hot.load();
        let entry = guard.as_deref()?;
        (entry.id == id).then(|| entry.body.clone())
    }

    pub fn get_warm(&self, id: Uuid) -> Option<Bytes> {
        let map = self.warm.load();
        let (stored_id, body) = map.get(&fold(id))?;
        (*stored_id == id).then(|| body.clone())
    }

    /// L2 hit: promote straight to L1. This is a plain atomic pointer store,
    /// not I/O, so unlike the DB path there's nothing to hand off - it just
    /// happens inline before the response goes out.
    pub fn promote(&self, id: Uuid, body: Bytes) {
        self.hot.store(Some(Arc::new(HotEntry { id, body })));
    }

    /// DB hit: fill both tiers ("FillAll" in `api_reference.md`).
    pub fn fill(&self, id: Uuid, body: Bytes) {
        self.put_warm(id, body.clone());
        self.promote(id, body);
    }

    fn put_warm(&self, id: Uuid, body: Bytes) {
        let mut next = (**self.warm.load()).clone();
        next.insert(fold(id), (id, body));
        self.warm.store(Arc::new(next));
    }

    /// Drop any cached response for `id` from both tiers. Used on PATCH/DELETE
    /// so a stale body can never outlive the write that invalidated it.
    pub fn invalidate(&self, id: Uuid) {
        if matches!(self.hot.load().as_deref(), Some(entry) if entry.id == id) {
            self.hot.store(None);
        }

        let current = self.warm.load();
        if current.get(&fold(id)).is_some_and(|(stored, _)| *stored == id) {
            let mut next = (**current).clone();
            next.remove(&fold(id));
            self.warm.store(Arc::new(next));
        }
    }
}
