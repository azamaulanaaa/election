use std::cmp::Ordering;

use async_lock::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Index {index} is out of bounds for storage of size {size}")]
    OutOfBound { index: u64, size: u64 },
    #[error("The index must be positive (1-based), but received a non-positive index")]
    NonPositif,
}

#[derive(Clone, PartialEq)]
pub struct StorageValue<E>
where
    E: Clone + Send + Sync + PartialEq,
{
    pub term: u64,
    pub entry: E,
}

#[async_trait::async_trait]
pub trait Storage<E>: Sync
where
    E: Clone + Send + Sync + PartialEq,
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
}

#[derive(Default)]
pub struct MemStorage<E>
where
    E: Clone + Send + Sync + PartialEq,
{
    vector: RwLock<Vec<StorageValue<E>>>,
}

#[async_trait::async_trait]
impl<E> Storage<E> for MemStorage<E>
where
    E: Clone + Send + Sync + PartialEq,
{
    async fn push(&self, value: StorageValue<E>) -> Result<(), StorageError> {
        self.vector.write().await.push(value);

        Ok(())
    }

    async fn get(&self, index: u64) -> Result<StorageValue<E>, StorageError> {
        if index == 0 {
            return Err(StorageError::NonPositif);
        }
        let inner_index = index - 1;
        let size = self.last_index().await?;

        self.vector
            .read()
            .await
            .get(inner_index as usize)
            .ok_or(StorageError::OutOfBound { index, size })
            .map(|value| value.clone())
    }

    async fn truncate(&self, index: u64) -> Result<(), StorageError> {
        if index == 0 {
            return Err(StorageError::NonPositif);
        }

        let new_len = index - 1;

        self.vector.write().await.truncate(new_len as usize);

        Ok(())
    }

    async fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.vector.read().await.len() as u64)
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
