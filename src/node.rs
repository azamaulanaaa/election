use std::{cmp::Ordering, fmt::Debug};

use futures::{
    SinkExt, StreamExt,
    channel::{mpsc, oneshot},
    lock::Mutex,
};

use crate::{
    message::{
        Message, MessageBody, MsgAppendEntriesReq, MsgAppendEntriesRes, MsgRequestVoteReq,
        MsgRequestVoteRes,
    },
    peers::{MemPeers, Peers, PeersError},
    state::{State, StateError},
    storage::{Storage, StorageError, StorageValue},
};

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("{0}")]
    State(#[from] StateError),
    #[error("{0}")]
    Peers(#[from] PeersError),
}

pub struct Node<T, S, E>
where
    T: State,
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq + Debug,
{
    id: u64,
    tx_out: mpsc::Sender<Message<E>>,
    rx_in: Mutex<mpsc::Receiver<Message<E>>>,
    storage: S,
    state: T,
    peers: MemPeers,
}

impl<T, S, E> Node<T, S, E>
where
    T: State,
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq + Debug,
{
    pub fn new(
        id: u64,
        tx_out: mpsc::Sender<Message<E>>,
        rx_in: mpsc::Receiver<Message<E>>,
        state: T,
        storage: S,
    ) -> Self {
        Self {
            id,
            tx_out,
            rx_in: Mutex::new(rx_in),
            state,
            storage,
            peers: MemPeers::default(),
        }
    }

    pub async fn run(&self) {
        let mut rx_in = self.rx_in.lock().await;

        while let Some(message) = rx_in.next().await {
            match message.body {
                MessageBody::RequestVote(msg_req, tx_res) => {
                    let msg_res = self.handle_request_vote(msg_req).await.unwrap();
                    let _ = tx_res.send(msg_res);
                }
                MessageBody::AppendEntries(msg_req, tx_res) => {
                    let msg_res = self
                        .handle_append_entries(message.node_id, msg_req)
                        .await
                        .unwrap();
                    let _ = tx_res.send(msg_res);
                }
            };
        }
    }

    pub async fn is_leader(&self) -> bool {
        self.state
            .get_leader_id()
            .await
            .unwrap()
            .is_some_and(|id| id == self.id)
    }

    async fn start_election(&self) -> Result<(), NodeError> {
        self.state
            .set_term(self.state.get_term().await? + 1)
            .await?;

        let candidate_id = self.state.get_vote_for().await?.unwrap_or(self.id);
        self.state.set_vote_for(Some(candidate_id)).await?;

        let last_storage_state = {
            let last_index = self.storage.last_index().await?;
            let last_storage_state = self.storage.get_state(last_index).await?;
            last_storage_state
        };

        let msg_req = MsgRequestVoteReq {
            term: self.state.get_term().await?,
            candidate_id,
            last_storage_state,
        };

        let receiver_ids = self.peers.ids().await?.into_iter();
        for node_id in receiver_ids {
            let (tx_res, _rx_res) = oneshot::channel();
            let msg_body = MessageBody::RequestVote(msg_req.clone(), tx_res);
            let msg = Message {
                node_id,
                body: msg_body,
            };

            self.tx_out.clone().send(msg).await.unwrap();
        }

        Ok(())
    }

    async fn handle_request_vote(
        &self,
        msg: MsgRequestVoteReq,
    ) -> Result<MsgRequestVoteRes, NodeError> {
        self.state
            .set_term(self.state.get_term().await?.max(msg.term))
            .await
            .unwrap();

        if self.state.get_term().await? == msg.term {
            self.state.set_leader_id(None).await?;
        }

        let last_storage_state = {
            let last_index = self.storage.last_index().await.unwrap();

            self.storage.get_state(last_index).await?
        };

        let granted = match Ord::cmp(&msg.term, &self.state.get_term().await?) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => self
                .state
                .get_vote_for()
                .await?
                .is_none_or(|vote_for| vote_for == msg.candidate_id),
        };
        let granted = granted && msg.last_storage_state >= last_storage_state;
        if granted {
            self.state.set_vote_for(Some(msg.candidate_id)).await?;
        }

        Ok(MsgRequestVoteRes {
            term: self.state.get_term().await?,
            granted,
        })
    }

    async fn handle_append_entries(
        &self,
        from: u64,
        msg: MsgAppendEntriesReq<E>,
    ) -> Result<MsgAppendEntriesRes, NodeError> {
        self.state
            .set_term(self.state.get_term().await?.max(msg.term))
            .await?;

        let success = self.state.get_term().await? == msg.term;
        if success {
            self.state.set_leader_id(Some(from)).await?;
            self.state.set_vote_for(None).await?;
        }

        let storage_state = self.storage.get_state(msg.prev_storage_state.index).await;
        let success = success && storage_state.is_ok_and(|v| v == msg.prev_storage_state);

        if success {
            let entries = msg.entries.into_iter().enumerate();
            let mut truncated = false;
            for (offset, entry) in entries {
                let current_index = (offset + 1) as u64 + msg.prev_storage_state.index;
                let value = StorageValue {
                    term: self.state.get_term().await?,
                    entry,
                };

                let stored_entry = self.storage.get(current_index).await;
                if stored_entry.is_ok_and(|e| e == value) {
                    continue;
                }

                if !truncated {
                    self.storage.truncate(current_index).await.unwrap();
                    truncated = true;
                }

                self.storage.push(value).await.unwrap();
            }

            self.storage
                .set_commited_index(msg.commited_index.min(self.storage.last_index().await?))
                .await?;
        }

        Ok(MsgAppendEntriesRes {
            term: self.state.get_term().await?,
            success,
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use tokio::time;

    use crate::{state::MemState, storage::MemStorage};

    use super::*;

    async fn init_node(
        tx_in: mpsc::Sender<Message<usize>>,
        rx_out: mpsc::Receiver<Message<usize>>,
    ) -> Node<MemState, MemStorage<usize>, usize> {
        let mem_state = MemState::default();
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, tx_in, rx_out, mem_state, mem_storage);

        node
    }

    #[tokio::test]
    async fn stop_run_when_no_sender() {
        let (tx_in, rx_in) = mpsc::channel(1);
        let (tx_out, _rx_out) = mpsc::channel(1);
        let node = init_node(tx_out, rx_in).await;

        let timeout = time::Duration::from_millis(100);

        {
            drop(tx_in);
        }

        tokio::select! {
            _ = time::sleep(timeout) => {
                panic!("Test timeout. Node does not stop");
            }
            _ = node.run() => {
                assert!(true, "Node stopped successfully");
            }
        }
    }

    mod handle_request_vote {
        use futures::channel::mpsc;

        use crate::storage::{StorageState, StorageValue};

        use super::*;

        #[tokio::test]
        async fn update_state_term_with_highest_term() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgRequestVoteReq {
                term: node.state.get_term().await.unwrap() + 1,
                candidate_id: node.id + 1,
                last_storage_state,
            };
            let _msg_res = node.handle_request_vote(msg_req.clone()).await.unwrap();

            let term = node.state.get_term().await.unwrap();
            assert_eq!(term, msg_req.term);
        }

        #[tokio::test]
        async fn response_with_state_term() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgRequestVoteReq {
                term: node.state.get_term().await.unwrap(),
                candidate_id: node.id + 1,
                last_storage_state,
            };
            let msg_res = node.handle_request_vote(msg_req.clone()).await.unwrap();

            let term = node.state.get_term().await.unwrap();
            assert_eq!(term, msg_res.term);
        }

        mod no_grant_if_last_storage_state_is_behind {
            use super::*;

            #[tokio::test]
            async fn term_is_older() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(4_u64).await.unwrap();
                }
                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.id + 1,
                    last_storage_state: StorageState {
                        term: last_storage_state.term - 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }

            #[tokio::test]
            async fn index_is_older() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(4_u64).await.unwrap();
                }
                {
                    for _ in 0..=2 {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.id + 1,
                    last_storage_state: StorageState {
                        index: last_storage_state.index - 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }
        }

        mod higher_term {
            use super::*;

            #[tokio::test]
            async fn step_down() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_leader_id(Some(node.id)).await.unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    candidate_id: node.id + 1,
                    last_storage_state: last_storage_state.clone(),
                };
                let _msg_res = node.handle_request_vote(msg_req).await;

                let leader_id = node.state.get_leader_id().await.unwrap();
                assert_ne!(leader_id, Some(node.id));
            }

            #[tokio::test]
            async fn grant() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(None).await.unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn update_vote_for() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(None).await.unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                assert_eq!(vote_for, Some(msg_req.candidate_id));
            }
        }

        mod lower_term {
            use super::*;

            #[tokio::test]
            async fn no_grant() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(None).await.unwrap();
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap() - 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }

            #[tokio::test]
            async fn no_change() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(None).await.unwrap();
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                }

                let init_state = (
                    node.state.get_term().await.unwrap(),
                    node.state.get_leader_id().await.unwrap(),
                    node.state.get_vote_for().await.unwrap(),
                );

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap() - 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                let state = (
                    node.state.get_term().await.unwrap(),
                    node.state.get_leader_id().await.unwrap(),
                    node.state.get_vote_for().await.unwrap(),
                );
                assert_eq!(state, init_state);
            }
        }

        mod equal_term {
            use super::*;

            #[tokio::test]
            async fn grant_for_empty_vote_for() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(None).await.unwrap();
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn grant_for_voting_same_candidate() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(Some(node.id + 1)).await.unwrap();
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.state.get_vote_for().await.unwrap().unwrap(),
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn no_grant_for_voting_different_candidate() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(Some(node.id + 1)).await.unwrap();
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.state.get_vote_for().await.unwrap().unwrap() + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }

            #[tokio::test]
            async fn update_vote_for_if_empty_vote_for() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                    node.state.set_vote_for(None).await.unwrap();
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: node.state.get_term().await.unwrap(),
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                assert_eq!(vote_for, Some(msg_req.candidate_id));
            }
        }
    }

    mod handle_append_entries {
        use futures::channel::mpsc;

        use crate::storage::{StorageState, StorageValue};

        use super::*;

        #[tokio::test]
        async fn update_term_to_highest_term() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgAppendEntriesReq {
                term: node.state.get_term().await.unwrap() + 1,
                prev_storage_state: last_storage_state,
                commited_index: node.storage.get_commited_index().await.unwrap(),
                entries: Vec::new(),
            };
            let _msg_res = node
                .handle_append_entries(node.id + 1, msg_req.clone())
                .await
                .unwrap();

            let term = node.state.get_term().await.unwrap();
            assert_eq!(term, msg_req.term);
        }

        #[tokio::test]
        async fn response_with_state_term() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgAppendEntriesReq {
                term: node.state.get_term().await.unwrap(),
                prev_storage_state: last_storage_state,
                commited_index: node.storage.get_commited_index().await.unwrap(),
                entries: Vec::new(),
            };
            let msg_res = node
                .handle_append_entries(node.id + 1, msg_req.clone())
                .await
                .unwrap();

            let term = node.state.get_term().await.unwrap();
            assert_eq!(msg_res.term, term);
        }

        mod higher_term {
            use super::*;

            #[tokio::test]
            async fn update_leader_id() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_leader_id(node.id + 1).await.unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let new_leader_id = node.state.get_leader_id().await.unwrap().unwrap() + 1;
                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                };
                let _msg_res = node.handle_append_entries(new_leader_id, msg_req).await;

                let leader_id = node.state.get_leader_id().await.unwrap();
                assert_eq!(leader_id, Some(new_leader_id))
            }

            #[tokio::test]
            async fn set_vote_for_to_empty() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(Some(node.id)).await.unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                assert_eq!(vote_for, None);
            }

            #[tokio::test]
            async fn update_commited_index() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(3_u64).await.unwrap();
                }

                {
                    node.storage.set_commited_index(0_u64).await.unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() + 1,
                    commited_index: 1,
                    entries: vec![1, 2, 3],
                    prev_storage_state: last_storage_state,
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let commited_index = node.storage.get_commited_index().await.unwrap();
                assert_eq!(commited_index, 1);
            }
        }

        mod lower_term {
            use super::*;

            #[tokio::test]
            async fn response_success_is_false() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() - 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                };
                let msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                assert_eq!(msg_res.success, false);
            }

            #[tokio::test]
            async fn no_change_state() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                }

                let init_state = (
                    node.state.get_term().await.unwrap(),
                    node.state.get_leader_id().await.unwrap(),
                    node.state.get_vote_for().await.unwrap(),
                );

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() - 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let state = (
                    node.state.get_term().await.unwrap(),
                    node.state.get_leader_id().await.unwrap(),
                    node.state.get_vote_for().await.unwrap(),
                );
                assert_eq!(state, init_state);
            }

            #[tokio::test]
            async fn no_change_storage() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap() - 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let last_index = node.storage.last_index().await.unwrap();
                assert_eq!(last_index, 0);
            }
        }

        mod reject_on_no_match_storage_state {
            use super::*;

            #[tokio::test]
            async fn higher_term() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                    node.state.set_leader_id(Some(node.id + 1)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 2,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let leader_id = node.state.get_leader_id().await.unwrap().unwrap();

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: StorageState {
                        term: last_storage_state.term + 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node
                    .handle_append_entries(leader_id, msg_req)
                    .await
                    .unwrap();

                assert_eq!(msg_res.success, false);
            }

            #[tokio::test]
            async fn lower_term() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                    node.state.set_leader_id(Some(node.id + 1)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 2,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let leader_id = node.state.get_leader_id().await.unwrap().unwrap();

                let msg_req = MsgAppendEntriesReq {
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    term: node.state.get_term().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: StorageState {
                        term: last_storage_state.term - 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node
                    .handle_append_entries(leader_id, msg_req)
                    .await
                    .unwrap();

                assert_eq!(msg_res.success, false);
            }

            #[tokio::test]
            async fn higher_index() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                    node.state.set_leader_id(Some(node.id + 1)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 2,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let leader_id = node.state.get_leader_id().await.unwrap().unwrap();

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: StorageState {
                        index: last_storage_state.index + 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node
                    .handle_append_entries(leader_id, msg_req)
                    .await
                    .unwrap();

                assert_eq!(msg_res.success, false);
            }

            #[tokio::test]
            async fn lower_index() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state
                        .set_term(node.state.get_term().await.unwrap() + 1)
                        .await
                        .unwrap();
                    node.state.set_leader_id(Some(node.id + 1)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 2,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let leader_id = node.state.get_leader_id().await.unwrap().unwrap();

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: StorageState {
                        index: last_storage_state.index - 1,
                        ..last_storage_state
                    },
                };
                let msg_res = node
                    .handle_append_entries(leader_id, msg_req)
                    .await
                    .unwrap();

                assert_eq!(msg_res.success, false);
            }
        }

        mod equal_term {
            use super::*;

            mod push_no_truncate {
                use super::*;

                const ENTRIES: [usize; 2] = [1, 2];

                #[tokio::test]
                async fn response_success_is_true() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_storage_state = {
                        let last_index = node.storage.last_index().await.unwrap();
                        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                        last_storage_state
                    };

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    assert_eq!(msg_res.success, true);
                }

                #[tokio::test]
                async fn match_last_storage_index() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();

                    let expected_last_index = last_index + ENTRIES.len() as u64;

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let _msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    let last_index = node.storage.last_index().await.unwrap();

                    assert_eq!(last_index, expected_last_index);
                }

                #[tokio::test]
                async fn match_storage_entity() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let _msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    let entries = ENTRIES.iter().enumerate();
                    for (offset, &expected_entry) in entries {
                        let index = last_index + 1 + offset as u64;
                        let entry = node.storage.get(index).await.unwrap().entry;

                        assert_eq!(entry, expected_entry);
                    }
                }
            }

            mod push_with_truncate {
                use super::*;

                const ENTRIES: [usize; 2] = [1, 2];

                #[tokio::test]
                async fn response_success_is_true() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_storage_state = {
                        let last_index = node.storage.last_index().await.unwrap();
                        let last_storage_state =
                            node.storage.get_state(last_index - 1).await.unwrap();
                        last_storage_state
                    };

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    assert_eq!(msg_res.success, true);
                }

                #[tokio::test]
                async fn match_last_storage_index() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();

                    let expected_last_index = last_index - 1 + ENTRIES.len() as u64;

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let _msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    let last_index = node.storage.last_index().await.unwrap();

                    assert_eq!(last_index, expected_last_index);
                }

                #[tokio::test]
                async fn match_storage_entity() {
                    let (_tx_in, rx_in) = mpsc::channel(1);
                    let (tx_out, _rx_out) = mpsc::channel(1);
                    let node = init_node(tx_out, rx_in).await;

                    {
                        node.state.set_term(2_u64).await.unwrap();
                    }

                    {
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap() - 1,
                                entry: 0,
                            })
                            .await
                            .unwrap();
                        node.storage
                            .push(StorageValue {
                                term: node.state.get_term().await.unwrap(),
                                entry: 0,
                            })
                            .await
                            .unwrap();
                    }

                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();

                    let msg_req = MsgAppendEntriesReq {
                        term: node.state.get_term().await.unwrap(),
                        commited_index: node.storage.get_commited_index().await.unwrap(),
                        entries: Vec::from(ENTRIES),
                        prev_storage_state: last_storage_state,
                    };
                    let _msg_res = node
                        .handle_append_entries(node.id + 1, msg_req)
                        .await
                        .unwrap();

                    let entries = ENTRIES.iter().enumerate();
                    for (offset, &entry) in entries {
                        let index = last_index + offset as u64;
                        assert_eq!(node.storage.get(index).await.unwrap().entry, entry);
                    }
                }
            }

            #[tokio::test]
            async fn update_leader_id() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(2_u64).await.unwrap();
                    node.state.set_leader_id(Some(node.id)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 0,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();
                    last_storage_state
                };

                let new_leader_id = node.state.get_leader_id().await.unwrap().unwrap() + 1;

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: last_storage_state,
                };
                let _msg_res = node
                    .handle_append_entries(new_leader_id, msg_req)
                    .await
                    .unwrap();

                let leader_id = node.state.get_leader_id().await.unwrap();
                assert_eq!(leader_id, Some(new_leader_id));
            }

            #[tokio::test]
            async fn set_vote_for_to_empty() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(2_u64).await.unwrap();
                    node.state.set_vote_for(Some(node.id)).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 0,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: node.storage.get_commited_index().await.unwrap(),
                    entries: Vec::new(),
                    prev_storage_state: last_storage_state,
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                assert_eq!(vote_for, None);
            }

            #[tokio::test]
            async fn update_commited_index() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_term(2_u64).await.unwrap();
                    node.storage.set_commited_index(0_u64).await.unwrap();
                }

                {
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap() - 1,
                            entry: 0,
                        })
                        .await
                        .unwrap();
                    node.storage
                        .push(StorageValue {
                            term: node.state.get_term().await.unwrap(),
                            entry: 0,
                        })
                        .await
                        .unwrap();
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index - 1).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgAppendEntriesReq {
                    term: node.state.get_term().await.unwrap(),
                    commited_index: 1,
                    entries: vec![1, 2, 3],
                    prev_storage_state: last_storage_state,
                };
                let _msg_res = node
                    .handle_append_entries(node.id + 1, msg_req)
                    .await
                    .unwrap();

                let commited_index = node.storage.get_commited_index().await.unwrap();
                assert_eq!(commited_index, 1);
            }
        }
    }

    mod start_election {
        use super::*;

        #[tokio::test]
        async fn increase_current_term() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            {
                node.state.set_term(2_u64).await.unwrap();
            }

            {
                node.storage
                    .push(StorageValue {
                        term: node.state.get_term().await.unwrap() - 1,
                        entry: 0,
                    })
                    .await
                    .unwrap();
                node.storage
                    .push(StorageValue {
                        term: node.state.get_term().await.unwrap(),
                        entry: 0,
                    })
                    .await
                    .unwrap();
            }

            let init_term = node.state.get_term().await.unwrap();

            node.start_election().await.unwrap();

            let term = node.state.get_term().await.unwrap();
            assert_eq!(term, init_term + 1);
        }

        #[tokio::test]
        async fn update_vote_for_self_if_empty() {
            let (_tx_in, rx_in) = mpsc::channel(1);
            let (tx_out, _rx_out) = mpsc::channel(1);
            let node = init_node(tx_out, rx_in).await;

            {
                node.state.set_term(2_u64).await.unwrap();
                node.state.set_vote_for(None).await.unwrap();
            }

            {
                node.storage
                    .push(StorageValue {
                        term: node.state.get_term().await.unwrap() - 1,
                        entry: 0,
                    })
                    .await
                    .unwrap();
                node.storage
                    .push(StorageValue {
                        term: node.state.get_term().await.unwrap(),
                        entry: 0,
                    })
                    .await
                    .unwrap();
            }

            node.start_election().await.unwrap();

            let vote_for = node.state.get_vote_for().await.unwrap();
            assert_eq!(vote_for, Some(node.id));
        }

        mod broadcast_message {
            use crate::peers::Peer;

            use super::*;

            #[tokio::test]
            async fn term_is_state_term() {
                let n_msgs = 2;

                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, mut rx_out) = mpsc::channel(n_msgs);
                let node = init_node(tx_out, rx_in).await;

                let last_index = node.storage.last_index().await.unwrap();

                {
                    for offset in 0..n_msgs {
                        let id = node.id + offset as u64;
                        node.peers.insert(id, Peer { last_index }).await.unwrap();
                    }
                }

                node.start_election().await.unwrap();

                let timeout = time::Duration::from_millis(100);

                for i in 0..n_msgs {
                    tokio::select! {
                        _ = tokio::time::sleep(timeout) => {
                            panic!("Test timeout. Received {} of {} messages", i, n_msgs);
                        }
                        Some(msg) = rx_out.next() => {
                            match msg.body {
                                MessageBody::RequestVote(msg_req, _tx_res) => {
                                    let term = node.state.get_term().await.unwrap();
                                    assert_eq!(msg_req.term, term);

                                },
                                _ => {
                                    panic!("message at {} is not RequestVote", i + 1);
                                }
                            }
                        }

                    }
                }
            }
        }
    }
}
