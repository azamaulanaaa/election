use async_lock::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Index {index} is out of bounds for storage of size {size}")]
    OutOfBound { index: u64, size: u64 },
    #[error("The index must be positive (1-based), but received a non-positive index")]
    NonPositif,
}

#[derive(Clone)]
pub struct StorageValue<E>
where
    E: Clone,
{
    pub term: u64,
    pub entry: E,
}

pub trait Storage<E>
where
    E: Clone,
{
    async fn push(&self, value: StorageValue<E>) -> Result<(), StorageError>;
    async fn get(&self, index: u64) -> Result<StorageValue<E>, StorageError>;
    async fn truncate(&self, index: u64) -> Result<(), StorageError>;
    async fn last_index(&self) -> Result<u64, StorageError>;
}

#[derive(Default)]
pub struct MemStorage<E>
where
    E: Clone,
{
    vector: RwLock<Vec<StorageValue<E>>>,
}

impl<E> Storage<E> for MemStorage<E>
where
    E: Clone,
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
        let inner_index = index - 1;
        let size = self.last_index().await?;

        if index > size + 1 {
            return Err(StorageError::OutOfBound { index, size });
        }

        self.vector.write().await.truncate(inner_index as usize);

        Ok(())
    }

    async fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.vector.read().await.len() as u64)
    }
}
