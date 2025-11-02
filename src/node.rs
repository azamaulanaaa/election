use std::cmp::Ordering;

use futures::{StreamExt, channel::mpsc, lock::Mutex};

use crate::{
    message::{
        Message, MessageBody, MsgAppendEntriesReq, MsgAppendEntriesRes, MsgRequestVoteReq,
        MsgRequestVoteRes,
    },
    storage::{Storage, StorageError, StorageValue},
};

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
    #[error("{0}")]
    Storage(#[from] StorageError),
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
pub struct NodeState {
    pub vote_for: Option<u64>,
    pub term: u64,
    pub commited_index: u64,
    pub leader_id: Option<u64>,
}

pub struct Node<S, E>
where
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq,
{
    id: u64,
    rx: Mutex<mpsc::Receiver<Message<E>>>,
    storage: S,
    state: Mutex<NodeState>,
}

impl<S, E> Node<S, E>
where
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq,
{
    pub fn new(id: u64, rx: mpsc::Receiver<Message<E>>, storage: S) -> Self {
        Self {
            id,
            rx: Mutex::new(rx),
            state: Default::default(),
            storage,
        }
    }

    pub async fn run(&self) {
        let mut rx_guard = self.rx.lock().await;

        while let Some(message) = rx_guard.next().await {
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
            .lock()
            .await
            .leader_id
            .is_some_and(|id| id == self.id)
    }

    async fn handle_request_vote(
        &self,
        msg: MsgRequestVoteReq,
    ) -> Result<MsgRequestVoteRes, NodeError> {
        let mut node_state = self.state.lock().await;
        let last_index = self.storage.last_index().await.unwrap();
        let last_storage_state = self.storage.get_state(last_index).await?;

        let granted = match Ord::cmp(&msg.term, &node_state.term) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => node_state
                .vote_for
                .is_none_or(|vote_for| vote_for == msg.candidate_id),
        };
        let granted = granted && msg.last_storage_state >= last_storage_state;
        if granted {
            node_state.vote_for = Some(msg.candidate_id);
        }

        if node_state.term < msg.term {
            node_state.term = msg.term;
            node_state.leader_id = None;
        }

        Ok(MsgRequestVoteRes {
            term: node_state.term,
            granted,
        })
    }

    async fn handle_append_entries(
        &self,
        from: u64,
        msg: MsgAppendEntriesReq<E>,
    ) -> Result<MsgAppendEntriesRes, NodeError> {
        let mut node_state = self.state.lock().await;

        if node_state.term < msg.term {
            node_state.leader_id = Some(from);
        }
        node_state.term = node_state.term.max(msg.term);

        let success = node_state.term == msg.term;

        let storage_state = self.storage.get_state(msg.prev_storage_state.index).await;
        let success = success && storage_state.is_ok_and(|v| v == msg.prev_storage_state);

        if success {
            node_state.vote_for = None;

            let mut entries = msg.entries.into_iter().enumerate();
            let mut truncated = false;
            while let Some((offset, entry)) = entries.next() {
                let current_index = (offset + 1) as u64 + msg.prev_storage_state.index;
                let value = StorageValue {
                    term: node_state.term,
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
        }

        Ok(MsgAppendEntriesRes {
            term: node_state.term,
            success,
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use tokio::time;

    use crate::storage::MemStorage;

    use super::*;

    #[tokio::test]
    async fn stop_run_when_no_sender() {
        let (tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);
        let timeout = time::Duration::from_millis(100);

        {
            drop(tx);
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

        use crate::storage::{MemStorage, StorageState, StorageValue};

        use super::*;

        #[tokio::test]
        async fn update_state_term_with_highest_term() {
            let (_tx_req, rx_req) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx_req, mem_storage);

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let msg_req = MsgRequestVoteReq {
                term: { node.state.lock().await.term } + 1,
                candidate_id: node.id + 1,
                last_storage_state,
            };
            let _msg_res = node.handle_request_vote(msg_req.clone()).await.unwrap();

            assert_eq!(node.state.lock().await.term, msg_req.term);
        }

        #[tokio::test]
        async fn response_with_state_term() {
            let (_tx_req, rx_req) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx_req, mem_storage);

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let msg_req = MsgRequestVoteReq {
                term: { node.state.lock().await.term },
                candidate_id: node.id + 1,
                last_storage_state,
            };
            let _msg_res = node.handle_request_vote(msg_req.clone()).await.unwrap();

            assert_eq!(node.state.lock().await.term, msg_req.term);
        }

        mod no_grant_if_last_storage_state_is_behind {
            use super::*;

            #[tokio::test]
            async fn term_is_older() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.term = 4;
                }
                {
                    node.storage
                        .push(StorageValue {
                            term: { node.state.lock().await.term },
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
                    term: { node.state.lock().await.term },
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
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.term = 4;
                }
                {
                    for _ in 0..=2 {
                        node.storage
                            .push(StorageValue {
                                term: { node.state.lock().await.term },
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
                    term: { node.state.lock().await.term },
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
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.leader_id = Some(node.id);
                }

                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term } + 1,
                    candidate_id: node.id + 1,
                    last_storage_state: last_storage_state.clone(),
                };
                let _msg_res = node.handle_request_vote(msg_req).await;

                assert_ne!(node.state.lock().await.leader_id, Some(node.id));
            }

            #[tokio::test]
            async fn grant() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term } + 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn update_vote_for() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term } + 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(node.state.lock().await.vote_for, Some(msg_req.candidate_id));
            }
        }

        mod lower_term {
            use super::*;

            #[tokio::test]
            async fn no_grant() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                    node_state.term = node_state.term + 1;
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term } - 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }

            #[tokio::test]
            async fn no_change() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                let init_node_state = {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                    node_state.term = node_state.term + 1;
                    node_state.clone()
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term } - 1,
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(node.state.lock().await.clone(), init_node_state);
            }
        }

        mod equal_term {
            use super::*;

            #[tokio::test]
            async fn grant_for_empty_vote_for() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                    node_state.term = node_state.term + 1;
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term },
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn grant_for_voting_same_candidate() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = Some(node.id + 1);
                    node_state.term = node_state.term + 1;
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term },
                    candidate_id: { node.state.lock().await.vote_for.unwrap() },
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn no_grant_for_voting_different_candidate() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = Some(node.id + 1);
                    node_state.term = node_state.term + 1;
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term },
                    candidate_id: { node.state.lock().await.vote_for.unwrap() } + 1,
                    last_storage_state,
                };
                let msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(msg_res.granted, false);
            }

            #[tokio::test]
            async fn update_vote_for_if_empty_vote_for() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.vote_for = None;
                    node_state.term = node_state.term + 1;
                };

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let msg_req = MsgRequestVoteReq {
                    term: { node.state.lock().await.term },
                    candidate_id: node.id + 1,
                    last_storage_state,
                };
                let _msg_res = node.handle_request_vote(msg_req).await.unwrap();

                assert_eq!(node.state.lock().await.vote_for, Some(msg_req.candidate_id));
            }
        }
    }

    mod handle_append_entries {
        use futures::channel::mpsc;

        use crate::storage::{MemStorage, StorageState, StorageValue};

        use super::*;

        #[tokio::test]
        async fn update_term_to_highest_term() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgAppendEntriesReq {
                term: { node.state.lock().await.term } + 1,
                prev_storage_state: last_storage_state,
                commited_index: { node.state.lock().await.commited_index },
                entries: Vec::new(),
            };
            let _msg_res = node
                .handle_append_entries(node.id + 1, msg_req.clone())
                .await
                .unwrap();

            assert_eq!(node.state.lock().await.term, msg_req.term);
        }

        #[tokio::test]
        async fn response_with_state_term() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let last_storage_state = {
                let last_index = node.storage.last_index().await.unwrap();
                let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                last_storage_state
            };

            let msg_req = MsgAppendEntriesReq {
                term: { node.state.lock().await.term },
                prev_storage_state: last_storage_state,
                commited_index: { node.state.lock().await.commited_index },
                entries: Vec::new(),
            };
            let msg_res = node
                .handle_append_entries(node.id + 1, msg_req.clone())
                .await
                .unwrap();

            assert_eq!(msg_res.term, node.state.lock().await.term);
        }

        mod higher_term {
            use super::*;

            #[tokio::test]
            async fn update_leader_id() {
                let (_tx, rx) = mpsc::channel(1);
                let mem_storage = MemStorage::<usize>::default();
                let node = Node::new(1, rx, mem_storage);

                {
                    let mut node_state = node.state.lock().await;
                    node_state.leader_id = Some(node.id + 1);
                }

                let last_storage_state = {
                    let last_index = node.storage.last_index().await.unwrap();
                    let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                    last_storage_state
                };

                let new_leader_id = { node.state.lock().await.leader_id.unwrap() } + 1;
                let msg_req = MsgAppendEntriesReq {
                    term: { node.state.lock().await.term } + 1,
                    prev_storage_state: last_storage_state,
                    entries: Vec::new(),
                    commited_index: { node.state.lock().await.term },
                };
                let _msg_res = node.handle_append_entries(new_leader_id, msg_req).await;

                assert_eq!(node.state.lock().await.leader_id, Some(new_leader_id))
            }
        }

        #[tokio::test]
        async fn reject_on_lower_term() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let term = 2;
            node.storage
                .push(StorageValue { term, entry: 0 })
                .await
                .unwrap();
            let node_state = {
                let mut node_state = node.state.lock().await;
                node_state.term = term;
                node_state.clone()
            };

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let msg_req = MsgAppendEntriesReq {
                term: node_state.term - 1,
                entries: vec![1],
                commited_index: node_state.commited_index,
                prev_storage_state: last_storage_state.clone(),
            };

            let msg_res = node.handle_append_entries(1, msg_req).await.unwrap();
            assert!(
                !msg_res.success,
                "Message response's success must be false when the term is lower"
            );
            assert_eq!(
                msg_res.term, node_state.term,
                "Message response's term must be equal to node state's term",
            );

            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(
                new_node_state, node_state,
                "Node state must not change due to lower term",
            );

            let new_last_index = node.storage.last_index().await.unwrap();
            let new_last_stroage_state = node.storage.get_state(new_last_index).await.unwrap();
            assert_eq!(
                new_last_stroage_state, last_storage_state,
                "Last storage state must not change due to lower term"
            );
        }

        #[tokio::test]
        async fn reject_on_no_match_storage_state() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let node_state = {
                let mut node_state = node.state.lock().await;
                node_state.term = 2;
                node_state.clone()
            };
            node.storage
                .push(StorageValue {
                    term: node_state.term,
                    entry: 2,
                })
                .await
                .unwrap();

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let storage_states = [
                StorageState {
                    index: last_storage_state.index - 1,
                    ..last_storage_state
                },
                StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
                StorageState {
                    term: last_storage_state.term - 1,
                    ..last_storage_state
                },
                StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            ];
            let mut storage_states = storage_states.into_iter();

            while let Some(storage_state) = storage_states.next() {
                let msg_res = node
                    .handle_append_entries(
                        1,
                        MsgAppendEntriesReq {
                            term: node_state.term,
                            commited_index: node_state.commited_index,
                            entries: vec![1],
                            prev_storage_state: storage_state,
                        },
                    )
                    .await
                    .unwrap();
                assert!(
                    !msg_res.success,
                    "Message response's success must be false if no previous storage state match found"
                );

                let new_last_index = node.storage.last_index().await.unwrap();
                let new_last_stroage_state = node.storage.get_state(new_last_index).await.unwrap();
                assert_eq!(
                    new_last_stroage_state, last_storage_state,
                    "Last storage state must not change due to lower term"
                );
            }
        }

        #[tokio::test]
        async fn match_storage_state_no_truncate() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let node_state = {
                let mut node_state = node.state.lock().await;
                node_state.term = 2;
                node_state.clone()
            };

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let entry = 1;

            let msg_req = MsgAppendEntriesReq {
                term: node_state.term,
                commited_index: node_state.commited_index,
                entries: vec![entry],
                prev_storage_state: last_storage_state,
            };
            let msg_res = node.handle_append_entries(1, msg_req).await.unwrap();
            assert!(msg_res.success, "Message response's success must be true");

            let new_last_index = node.storage.last_index().await.unwrap();

            let new_entry = node.storage.get(new_last_index).await.unwrap().entry;
            assert!(new_entry == entry, "Entry must be stored to the storage");

            let new_last_stroage_state = node.storage.get_state(new_last_index).await.unwrap();
            assert_eq!(
                new_last_stroage_state,
                StorageState {
                    term: node_state.term,
                    index: last_storage_state.index + 1,
                },
                "Last storage state must updated"
            );
        }

        #[tokio::test]
        async fn match_storage_state_with_truncate() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let node_state = {
                let mut node_state = node.state.lock().await;
                node_state.term = 2;
                node_state.clone()
            };
            node.storage
                .push(StorageValue {
                    term: node_state.term,
                    entry: 0,
                })
                .await
                .unwrap();

            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();

            let entry = 1;

            let msg_req = MsgAppendEntriesReq {
                term: node_state.term,
                commited_index: node_state.commited_index,
                entries: vec![entry],
                prev_storage_state: StorageState::default(),
            };
            let msg_res = node.handle_append_entries(1, msg_req).await.unwrap();
            assert!(msg_res.success, "Message response's success must be true");

            let new_last_index = node.storage.last_index().await.unwrap();

            let new_entry = node.storage.get(new_last_index).await.unwrap().entry;
            assert!(new_entry == entry, "Entry must be stored to the storage");

            let new_last_stroage_state = node.storage.get_state(new_last_index).await.unwrap();
            assert_eq!(
                new_last_stroage_state,
                StorageState {
                    term: node_state.term,
                    index: last_storage_state.index,
                },
                "Last storage state must updated"
            );
        }

        #[tokio::test]
        async fn reset_vote_for() {
            let (_tx, rx) = mpsc::channel(1);
            let mem_storage = MemStorage::<usize>::default();
            let node = Node::new(1, rx, mem_storage);

            let node_state = {
                let mut node_state = node.state.lock().await;
                node_state.term = 2;
                node_state.vote_for = Some(2);
                node_state.clone()
            };

            let msg_req = MsgAppendEntriesReq {
                term: node_state.term,
                commited_index: node_state.commited_index,
                entries: Vec::new(),
                prev_storage_state: StorageState::default(),
            };
            let msg_res = node.handle_append_entries(2, msg_req).await.unwrap();
            assert!(msg_res.success, "Message response must be success");

            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(
                new_node_state.vote_for, None,
                "Node state's vote for must be reset after receiving success append entries"
            );
        }
    }
}
