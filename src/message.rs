use futures::channel::oneshot;

use crate::storage::StorageState;

pub struct Message {
    pub node_id: usize,
    pub body: MessageBody,
}

pub enum MessageBody {
    RequestVote(MsgRequestVoteReq, oneshot::Sender<MsgRequestVoteRes>),
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
