pub mod message;
pub mod storage;

use std::{cmp::Ordering, marker::PhantomData};

use futures::{StreamExt, channel::mpsc, lock::Mutex};

use crate::{
    message::{Message, MessageBody, MsgRequestVoteReq, MsgRequestVoteRes},
    storage::{Storage, StorageState},
};

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub enum NodeKind {
    #[default]
    Leader,
    Follower,
}

#[derive(Default, Clone, Copy)]
pub struct NodeState {
    pub vote_for: Option<u64>,
    pub term: u64,
    pub kind: NodeKind,
}

pub struct Node<S, E>
where
    S: Storage<E>,
    E: Clone,
{
    id: usize,
    rx: Mutex<mpsc::Receiver<Message>>,
    storage: S,
    node_state: Mutex<NodeState>,
    _entity: PhantomData<E>,
}

impl<S, E> Node<S, E>
where
    S: Storage<E>,
    E: Clone,
{
    pub fn new(id: usize, rx: mpsc::Receiver<Message>, storage: S) -> Self {
        Self {
            id,
            rx: Mutex::new(rx),
            node_state: Default::default(),
            storage,
            _entity: Default::default(),
        }
    }

    pub async fn run(&self) {
        let mut rx_guard = self.rx.lock().await;

        while let Some(message) = rx_guard.next().await {
            match message.body {
                MessageBody::RequestVote(msg_req, tx_res) => {
                    let msg_res = self.handle_request_vote(msg_req).await;
                    let _ = tx_res.send(msg_res);
                }
            };
        }
    }

    async fn handle_request_vote(&self, msg: MsgRequestVoteReq) -> MsgRequestVoteRes {
        let mut node_state = self.node_state.lock().await;
        let last_storage_state = self.storage.last_state().await.unwrap();

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
            node_state.kind = NodeKind::Follower;
        }

        MsgRequestVoteRes {
            term: node_state.term,
            granted,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use tokio::time;

    use crate::storage::{MemStorage, StorageValue};

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

    #[tokio::test]
    async fn handle_request_vote_always_update_to_highest_term() {
        let (_tx_req, rx_req) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx_req, mem_storage);

        let node_state = { node.node_state.lock().await.clone() };
        let last_storage_state = node.storage.last_state().await.unwrap();
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: last_storage_state.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let msg_res = node.handle_request_vote(msg_req).await;
            assert_eq!(msg_res.term, new_term);

            let new_node_state = { node.node_state.lock().await.clone() };
            assert_eq!(new_node_state.term, new_term)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_reject_if_last_storage_state_is_behind() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = {
            let mut node_state = node.node_state.lock().await;
            node_state.term = node_state.term + 1;
            node_state.clone()
        };
        let last_storage_state = {
            node.storage
                .push(StorageValue {
                    term: node_state.term,
                    entry: 0,
                })
                .await
                .unwrap();
            let last_storage_state = node.storage.last_state().await.unwrap();
            last_storage_state
        };

        let msg_reqs = [
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: last_storage_state.term - 1,
                    ..last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: last_storage_state.index - 1,
                    ..last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: last_storage_state.term - 1,
                    ..last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: last_storage_state.index - 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let msg_res = node.handle_request_vote(msg_req).await;
            assert_eq!(msg_res.granted, false);

            let new_node_state = { node.node_state.lock().await.clone() };
            assert_eq!(new_node_state.vote_for, node_state.vote_for)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_always_step_down_for_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let mut node_state = { node.node_state.lock().await.clone() };
        node_state = NodeState {
            kind: NodeKind::Leader,
            ..node_state
        };

        let last_storage_state = node.storage.last_state().await.unwrap();
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: last_storage_state.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let _msg_res = node.handle_request_vote(msg_req).await;
            let new_node_state = { node.node_state.lock().await.clone() };
            assert_eq!(new_node_state.kind, NodeKind::Follower)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_for_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.node_state.lock().await.clone() };
        let init_node_states = [
            NodeState {
                vote_for: None,
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                vote_for: None,
                kind: NodeKind::Leader,
                ..node_state
            },
            NodeState {
                vote_for: Some(1),
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                vote_for: Some(1),
                kind: NodeKind::Leader,
                ..node_state
            },
        ];
        let mut init_node_states = init_node_states.into_iter();

        let last_storage_state = node.storage.last_state().await.unwrap();

        while let Some(init_node_state) = init_node_states.next() {
            {
                let mut node_state = node.node_state.lock().await;
                *node_state = init_node_state;
            }
            let new_term = init_node_state.term + 1;
            let new_candidate = init_node_state.vote_for.map(|v| v + 1).unwrap_or(32);

            let msg_res = node
                .handle_request_vote(MsgRequestVoteReq {
                    term: new_term,
                    candidate_id: new_candidate,
                    last_storage_state,
                })
                .await;
            assert!(
                msg_res.granted,
                "RequestVote must granted vote for higher term"
            );

            let node_state = { node.node_state.lock().await };
            assert_eq!(
                node_state.vote_for,
                Some(new_candidate),
                "Node should vote for candidate with higher term"
            );
        }
    }

    #[tokio::test]
    async fn handle_request_vote_for_lower_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.node_state.lock().await.clone() };
        let init_node_states = [
            NodeState {
                term: 10,
                vote_for: None,
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                term: 10,
                vote_for: None,
                kind: NodeKind::Leader,
                ..node_state
            },
            NodeState {
                term: 10,
                vote_for: Some(1),
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                term: 10,
                vote_for: Some(1),
                kind: NodeKind::Leader,
                ..node_state
            },
        ];
        let mut init_node_states = init_node_states.into_iter();

        let last_storage_state = node.storage.last_state().await.unwrap();

        while let Some(init_node_state) = init_node_states.next() {
            {
                let mut node_state = node.node_state.lock().await;
                *node_state = init_node_state;
            }
            let new_term = init_node_state.term - 1;
            let new_candidate = init_node_state.vote_for.map(|v| v + 1).unwrap_or(32);

            let msg_res = node
                .handle_request_vote(MsgRequestVoteReq {
                    term: new_term,
                    candidate_id: new_candidate,
                    last_storage_state,
                })
                .await;
            assert!(
                !msg_res.granted,
                "RequestVote must not granted vote for lower term"
            );

            let node_state = { node.node_state.lock().await };
            assert_eq!(
                init_node_state.vote_for, node_state.vote_for,
                "Node should change what it vote for when see lower term"
            );
        }
    }

    #[tokio::test]
    async fn handle_request_vote_for_equal_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.node_state.lock().await.clone() };
        let node_states = [
            NodeState {
                vote_for: None,
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                vote_for: None,
                kind: NodeKind::Leader,
                ..node_state
            },
            NodeState {
                vote_for: Some(1),
                kind: NodeKind::Follower,
                ..node_state
            },
            NodeState {
                vote_for: Some(1),
                kind: NodeKind::Leader,
                ..node_state
            },
        ];
        let mut node_states = node_states.into_iter();

        let last_storage_state = node.storage.last_state().await.unwrap();

        while let Some(init_node_state) = node_states.next() {
            {
                let mut node_state = node.node_state.lock().await;
                *node_state = init_node_state;
            }

            let msg_reqs = [
                MsgRequestVoteReq {
                    term: init_node_state.term,
                    candidate_id: init_node_state.vote_for.unwrap_or(1),
                    last_storage_state,
                },
                MsgRequestVoteReq {
                    term: init_node_state.term,
                    candidate_id: init_node_state.vote_for.map_or(1, |vote_for| vote_for + 1),
                    last_storage_state,
                },
            ];
            let mut msg_reqs = msg_reqs.into_iter();

            while let Some(msg_req) = msg_reqs.next() {
                let msg_res = node.handle_request_vote(msg_req.clone()).await;

                match init_node_state.vote_for {
                    Some(candidate) => {
                        if candidate == msg_req.candidate_id {
                            assert!(
                                msg_res.granted,
                                "RequestVote must granted vote when voting for same candidate"
                            );
                            let node_state = { node.node_state.lock().await };
                            assert_eq!(
                                node_state.vote_for, init_node_state.vote_for,
                                "Node should not change vote for when voting for same candidate"
                            );
                        } else {
                            assert!(
                                !msg_res.granted,
                                "RequestVote must not grant vote when not voting for different candidate"
                            );

                            let node_state = { node.node_state.lock().await };
                            assert_eq!(
                                node_state.vote_for, init_node_state.vote_for,
                                "Node should not change vote for when not voting for diffent candidate"
                            );
                        }
                    }
                    None => {
                        assert!(
                            msg_res.granted,
                            "RequestVote must granted vote when not voting for any candidate"
                        );

                        let node_state = { node.node_state.lock().await };
                        assert_eq!(
                            node_state.vote_for,
                            Some(msg_req.candidate_id),
                            "Node should vote for given candidate when not voting for any candidate"
                        );
                    }
                }
            }
        }
    }
}
