// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

use minibytes::Bytes;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{DeserializeOwned, Error},
};

/// Reference-counted wrapper that pairs a value with its bincode-serialized bytes.
/// Serialization is performed once at creation; subsequent serializations emit the
/// cached bytes directly.
///
/// **Important:** `serialize(T) != serialize(Data<T>)`. `Data<T>` is not a transparent
/// serde wrapper — it always writes raw bytes, regardless of the outer format.
#[derive(Clone)]
pub struct Data<T>(Arc<DataInner<T>>);

struct DataInner<T> {
    t: T,
    /// Bincode-serialized form of `t`, cached at creation.
    serialized: Bytes,
}

impl<T: Serialize + DeserializeOwned> Data<T> {
    /// Create a new `Data<T>` by serializing `t` into a cached byte buffer.
    pub fn new(t: T) -> Self {
        let serialized = bincode::serialize(&t).expect("Serialization should not fail");
        let serialized: Bytes = serialized.into();
        memory_tracking::track_alloc(serialized.len());
        Self(Arc::new(DataInner { t, serialized }))
    }

    /// Deserialize from pre-existing bytes (e.g. memory-mapped WAL). Prefer this
    /// over serde `Deserialize` — it reuses the `Bytes` buffer directly instead of
    /// allocating a copy.
    pub fn from_bytes(bytes: Bytes) -> bincode::Result<Self> {
        memory_tracking::track_alloc(bytes.len());
        let inner = DataInner {
            t: bincode::deserialize(&bytes)?,
            serialized: bytes,
        };
        Ok(Self(Arc::new(inner)))
    }

    /// Return the cached bincode-serialized bytes.
    pub fn serialized_bytes(&self) -> &Bytes {
        &self.0.serialized
    }
}

impl<T> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.t
    }
}

impl<T: Serialize> Serialize for Data<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0.serialized)
    }
}

impl<T> Drop for DataInner<T> {
    fn drop(&mut self) {
        memory_tracking::track_dealloc(self.serialized.len());
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Data<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized = Vec::<u8>::deserialize(deserializer)?;
        let Ok(t) = bincode::deserialize(&serialized) else {
            return Err(D::Error::custom("Failed to deserialize inner bytes"));
        };
        memory_tracking::track_alloc(serialized.len());
        let serialized = serialized.into();
        Ok(Self(Arc::new(DataInner { t, serialized })))
    }
}

impl<T: fmt::Debug> fmt::Debug for Data<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.t.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Data<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.t.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Data<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.t == other.0.t
    }
}

impl<T: Eq> Eq for Data<T> {}

impl<T: Hash> Hash for Data<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.t.hash(state)
    }
}

pub(crate) mod memory_tracking {
    use std::sync::OnceLock;

    use prometheus::IntGauge;

    static BLOCK_COUNT: OnceLock<IntGauge> = OnceLock::new();
    static BLOCK_BYTES: OnceLock<IntGauge> = OnceLock::new();

    /// Register prometheus gauges for tracking in-memory block count and byte usage.
    /// Only the first call takes effect; subsequent calls are silently ignored (this
    /// is expected in the simulator where multiple authorities share a process).
    pub(crate) fn init(blocks: IntGauge, bytes: IntGauge) {
        BLOCK_COUNT.set(blocks).ok();
        BLOCK_BYTES.set(bytes).ok();
    }

    pub(super) fn track_alloc(size: usize) {
        if let Some(g) = BLOCK_COUNT.get() {
            g.inc();
        }
        if let Some(g) = BLOCK_BYTES.get() {
            g.add(size as i64);
        }
    }

    pub(super) fn track_dealloc(size: usize) {
        if let Some(g) = BLOCK_COUNT.get() {
            g.dec();
        }
        if let Some(g) = BLOCK_BYTES.get() {
            g.sub(size as i64);
        }
    }
}
