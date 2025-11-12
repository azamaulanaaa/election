use async_lock::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum StateError {
    #[error("Something wrong.")]
    Other,
}

#[async_trait::async_trait]
pub trait State: Sync {
    async fn set_term<I: Into<u64> + Send>(&self, term: I) -> Result<(), StateError>;
    async fn get_term(&self) -> Result<u64, StateError>;

    async fn set_leader_id<I: Into<Option<u64>> + Send>(
        &self,
        leader_id: I,
    ) -> Result<(), StateError>;
    async fn get_leader_id(&self) -> Result<Option<u64>, StateError>;

    async fn set_vote_for<I: Into<Option<u64>> + Send>(
        &self,
        vote_for: I,
    ) -> Result<(), StateError>;
    async fn get_vote_for(&self) -> Result<Option<u64>, StateError>;
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
pub struct MemStateInner {
    term: u64,
    leader_id: Option<u64>,
    vote_for: Option<u64>,
}

#[derive(Default, Debug)]
pub struct MemState {
    inner: RwLock<MemStateInner>,
}

#[async_trait::async_trait]
impl State for MemState {
    async fn set_term<I: Into<u64> + Send>(&self, term: I) -> Result<(), StateError> {
        let mut state = self.inner.write().await;
        state.term = term.into();

        Ok(())
    }

    async fn get_term(&self) -> Result<u64, StateError> {
        let term = self.inner.read().await.term;

        Ok(term)
    }

    async fn set_leader_id<I: Into<Option<u64>> + Send>(
        &self,
        leader_id: I,
    ) -> Result<(), StateError> {
        let mut state = self.inner.write().await;
        state.leader_id = leader_id.into();

        Ok(())
    }

    async fn get_leader_id(&self) -> Result<Option<u64>, StateError> {
        let leader_id = self.inner.read().await.leader_id;

        Ok(leader_id)
    }

    async fn set_vote_for<I: Into<Option<u64>> + Send>(
        &self,
        vote_for: I,
    ) -> Result<(), StateError> {
        let mut state = self.inner.write().await;
        state.vote_for = vote_for.into();

        Ok(())
    }

    async fn get_vote_for(&self) -> Result<Option<u64>, StateError> {
        let vote_for = self.inner.read().await.vote_for.clone();

        Ok(vote_for)
    }
}
