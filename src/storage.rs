use std::{cmp::Ordering, fmt::Debug};

use async_lock::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Index {index} is out of bounds for storage of size {size}")]
    OutOfBound { index: u64, size: u64 },
    #[error("The index must be positive (1-based), but received a non-positive index")]
    NonPositif,
}

#[derive(Clone, PartialEq, Debug)]
pub struct StorageValue<E>
where
    E: Clone + Send + Sync + PartialEq + Debug,
{
    pub term: u64,
    pub entry: E,
}

#[async_trait::async_trait]
pub trait Storage<E>: Sync
where
    E: Clone + Send + Sync + PartialEq + Debug,
{
    async fn push(&self, value: StorageValue<E>) -> Result<(), StorageError>;
    async fn get(&self, index: u64) -> Result<StorageValue<E>, StorageError>;
    async fn truncate(&self, index: u64) -> Result<(), StorageError>;
    async fn last_index(&self) -> Result<u64, StorageError>;

    async fn get_state(&self, index: u64) -> Result<StorageState, StorageError> {
        if index == 0 {
            return Ok(StorageState::default());
        }

        let value = self.get(index).await?;

        Ok(StorageState {
            index,
            term: value.term,
        })
    }

    async fn set_commited_index<I: Into<u64> + Send>(
        &self,
        commited_index: I,
    ) -> Result<(), StorageError>;
    async fn get_commited_index(&self) -> Result<u64, StorageError>;
}

#[derive(Default)]
pub struct MemStorageInner<E>
where
    E: Clone + Send + Sync + PartialEq + Debug,
{
    vector: Vec<StorageValue<E>>,
    commited_index: u64,
}

#[derive(Default)]
pub struct MemStorage<E>
where
    E: Clone + Send + Sync + PartialEq + Debug,
{
    inner: RwLock<MemStorageInner<E>>,
}

#[async_trait::async_trait]
impl<E> Storage<E> for MemStorage<E>
where
    E: Clone + Send + Sync + PartialEq + Debug,
{
    async fn push(&self, value: StorageValue<E>) -> Result<(), StorageError> {
        self.inner.write().await.vector.push(value);

        Ok(())
    }

    async fn get(&self, index: u64) -> Result<StorageValue<E>, StorageError> {
        if index == 0 {
            return Err(StorageError::NonPositif);
        }
        let inner_index = index - 1;
        let size = self.last_index().await?;

        self.inner
            .read()
            .await
            .vector
            .get(inner_index as usize)
            .ok_or(StorageError::OutOfBound { index, size })
            .map(|value| value.clone())
    }

    async fn truncate(&self, index: u64) -> Result<(), StorageError> {
        if index == 0 {
            return Err(StorageError::NonPositif);
        }

        let new_len = index - 1;

        self.inner.write().await.vector.truncate(new_len as usize);

        Ok(())
    }

    async fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.inner.read().await.vector.len() as u64)
    }

    async fn set_commited_index<I: Into<u64> + Send>(
        &self,
        commited_index: I,
    ) -> Result<(), StorageError> {
        let mut storage = self.inner.write().await;
        storage.commited_index = commited_index.into();

        Ok(())
    }

    async fn get_commited_index(&self) -> Result<u64, StorageError> {
        let commited_index = self.inner.read().await.commited_index;

        Ok(commited_index)
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Debug)]
pub struct StorageState {
    pub term: u64,
    pub index: u64,
}

impl Ord for StorageState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match Ord::cmp(&self.term, &other.term) {
            Ordering::Equal => Ord::cmp(&self.index, &other.index),
            other => other,
        }
    }
}
