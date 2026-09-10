use std::hash::BuildHasherDefault;
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use hashbrown::HashMap;
use nohash_hasher::NoHashHasher;
use uuid::Uuid;

/// Two-tier read cache sitting in front of [`crate::db::Db`], as laid out in
/// `api_reference.md`:
///
/// ```text
/// L1 (hot, single slot) -> L2 (warm, hashmap) -> DB
/// ```
///
/// Both tiers cache the already-serialized JSON response body, so a hit
/// never touches the DB or `sonic_rs` again.
pub struct Cache {
    hot: ArcSwapOption<HotEntry>,
    warm: ArcSwap<WarmMap>,
}

struct HotEntry {
    id: Uuid,
    body: Bytes,
}

/// What a task wants done - `FillL1`/`Write` from `api_reference.md`'s
/// `cache.send_task(...)` calls.
pub enum CacheTask {
    FillL1(Uuid, Bytes),
    /// DB hit: fill both tiers ("FillAll" in the diagram).
    Write(Uuid, Bytes),
    Invalidate(Uuid),
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

    /// Dispatches a cache write as its own task and calls `continuation`
    /// once it lands - `cache.send_task(Task::Write(id, value), |_| {})`
    /// from `api_reference.md`. This is a plain atomic pointer/map swap, no
    /// I/O involved, but it still gets its own task rather than running
    /// inline on the response path: the point isn't speed here, it's that
    /// filling the cache is *not* the caller's problem, exactly like the
    /// diagram draws it as a separate branch off to the side of `answer`.
    pub fn send_task<F>(self: &Arc<Self>, task: CacheTask, continuation: F)
    where
        F: FnOnce(()) + 'static,
    {
        let cache = Arc::clone(self);
        compio::runtime::spawn(async move {
            match task {
                CacheTask::FillL1(id, body) => cache.promote(id, body),
                CacheTask::Write(id, body) => cache.fill(id, body),
                CacheTask::Invalidate(id) => cache.invalidate(id),
            }
            continuation(());
        })
        .detach();
    }

    fn promote(&self, id: Uuid, body: Bytes) {
        self.hot.store(Some(Arc::new(HotEntry { id, body })));
    }

    fn fill(&self, id: Uuid, body: Bytes) {
        self.put_warm(id, body.clone());
        self.promote(id, body);
    }

    fn put_warm(&self, id: Uuid, body: Bytes) {
        let mut next = (**self.warm.load()).clone();
        next.insert(fold(id), (id, body));
        self.warm.store(Arc::new(next));
    }

    /// Drops any cached response for `id` from both tiers, so a stale body
    /// can never outlive the write that invalidated it.
    fn invalidate(&self, id: Uuid) {
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
