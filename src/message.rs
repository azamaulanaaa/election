use futures::channel::oneshot;

use crate::storage::StorageState;

pub struct Message<E>
where
    E: Clone,
{
    pub node_id: u64,
    pub body: MessageBody<E>,
}

pub enum MessageBody<E>
where
    E: Clone,
{
    RequestVote(MsgRequestVoteReq, oneshot::Sender<MsgRequestVoteRes>),
    AppendEntries(MsgAppendEntriesReq<E>, oneshot::Sender<MsgAppendEntriesRes>),
}

#[derive(Clone, Copy)]
pub struct MsgRequestVoteReq {
    pub term: u64,
    pub candidate_id: u64,
    pub last_storage_state: StorageState,
}

#[derive(Clone, Copy)]
pub struct MsgRequestVoteRes {
    pub term: u64,
    pub granted: bool,
}

#[derive(Clone)]
pub struct MsgAppendEntriesReq<E>
where
    E: Clone,
{
    pub term: u64,
    pub prev_storage_state: StorageState,
    pub entries: Vec<E>,
    pub commited_index: u64,
}

#[derive(Clone)]
pub struct MsgAppendEntriesRes {
    pub term: u64,
    pub success: bool,
}
