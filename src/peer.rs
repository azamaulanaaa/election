use std::collections::{HashMap, HashSet};

use async_lock::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum PeerError {}

#[derive(Clone, Copy)]
pub struct Peer {
    pub last_index: u64,
}

#[async_trait::async_trait]
pub trait Peers {
    async fn len(&self) -> Result<u64, PeerError>;
    async fn get(&self, id: u64) -> Result<Option<Peer>, PeerError>;
    async fn insert(&self, id: u64, peer: Peer) -> Result<(), PeerError>;
    async fn ids(&self) -> Result<HashSet<u64>, PeerError>;
}

struct MemPeersInner {
    map: HashMap<u64, Peer>,
}

pub struct MemPeers {
    inner: RwLock<MemPeersInner>,
}

#[async_trait::async_trait]
impl Peers for MemPeers {
    async fn len(&self) -> Result<u64, PeerError> {
        let len = self.inner.read().await.map.len();

        Ok(len as u64)
    }

    async fn get(&self, id: u64) -> Result<Option<Peer>, PeerError> {
        let peer = self.inner.read().await.map.get(&id).cloned();

        Ok(peer)
    }

    async fn insert(&self, id: u64, peer: Peer) -> Result<(), PeerError> {
        let mut peers = self.inner.write().await;
        peers.map.insert(id, peer);

        Ok(())
    }

    async fn ids(&self) -> Result<HashSet<u64>, PeerError> {
        let ids = self
            .inner
            .read()
            .await
            .map
            .keys()
            .map(|k| *k as u64)
            .collect::<HashSet<_>>();

        Ok(ids)
    }
}
