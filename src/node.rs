use std::cmp::Ordering;

use futures::{StreamExt, channel::mpsc, lock::Mutex};

use crate::{
    message::{
        Message, MessageBody, MsgAppendEntriesReq, MsgAppendEntriesRes, MsgRequestVoteReq,
        MsgRequestVoteRes,
    },
    storage::{Storage, StorageValue},
};

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub enum NodeKind {
    #[default]
    Leader,
    Follower,
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
pub struct NodeState {
    pub vote_for: Option<u64>,
    pub term: u64,
    pub kind: NodeKind,
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
                    let msg_res = self.handle_request_vote(msg_req).await;
                    let _ = tx_res.send(msg_res);
                }
                MessageBody::AppendEntries(msg_req, tx_res) => {
                    let msg_res = self.handle_append_entries(message.node_id, msg_req).await;
                    let _ = tx_res.send(msg_res);
                }
            };
        }
    }

    async fn handle_request_vote(&self, msg: MsgRequestVoteReq) -> MsgRequestVoteRes {
        let mut node_state = self.state.lock().await;
        let last_index = self.storage.last_index().await.unwrap();
        let last_storage_state = self.storage.get_state(last_index).await.unwrap();

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

    async fn handle_append_entries(
        &self,
        from: u64,
        msg: MsgAppendEntriesReq<E>,
    ) -> MsgAppendEntriesRes {
        let mut node_state = self.state.lock().await;

        if node_state.term < msg.term {
            node_state.kind = NodeKind::Follower;
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

        MsgAppendEntriesRes {
            term: node_state.term,
            success,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;
    use tokio::time;

    use crate::storage::{MemStorage, StorageState, StorageValue};

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
    async fn handle_request_vote_update_to_highest_term() {
        let (_tx_req, rx_req) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx_req, mem_storage);

        let node_state = { node.state.lock().await.clone() };
        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
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

            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.term, new_term)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_reject_if_last_storage_state_is_behind() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = {
            let mut node_state = node.state.lock().await;
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
            let last_index = node.storage.last_index().await.unwrap();
            let last_storage_state = node.storage.get_state(last_index).await.unwrap();
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

            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.vote_for, node_state.vote_for)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_step_down_for_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = {
            let mut node_state = node.state.lock().await;
            *node_state = NodeState {
                kind: NodeKind::Leader,
                ..*node_state
            };
            node_state.clone()
        };

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
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
            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.kind, NodeKind::Follower)
        }
    }

    #[tokio::test]
    async fn handle_request_vote_for_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.state.lock().await.clone() };
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

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();

        while let Some(init_node_state) = init_node_states.next() {
            {
                let mut node_state = node.state.lock().await;
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

            let node_state = { node.state.lock().await };
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

        let node_state = { node.state.lock().await.clone() };
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

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();

        while let Some(init_node_state) = init_node_states.next() {
            {
                let mut node_state = node.state.lock().await;
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

            let node_state = { node.state.lock().await };
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

        let node_state = { node.state.lock().await.clone() };
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

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();

        while let Some(init_node_state) = node_states.next() {
            {
                let mut node_state = node.state.lock().await;
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
                            let node_state = { node.state.lock().await };
                            assert_eq!(
                                node_state.vote_for, init_node_state.vote_for,
                                "Node should not change vote for when voting for same candidate"
                            );
                        } else {
                            assert!(
                                !msg_res.granted,
                                "RequestVote must not grant vote when not voting for different candidate"
                            );

                            let node_state = { node.state.lock().await };
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

                        let node_state = { node.state.lock().await };
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

    #[tokio::test]
    async fn handle_append_entries_update_term_to_highest_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.state.lock().await.clone() };
        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: last_storage_state.clone(),
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let msg_res = node.handle_append_entries(1, msg_req).await;
            assert_eq!(msg_res.term, new_term);

            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.term, new_term)
        }
    }

    #[tokio::test]
    async fn handle_append_entries_step_down_for_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = {
            let mut node_state = node.state.lock().await;
            *node_state = NodeState {
                kind: NodeKind::Leader,
                ..*node_state
            };
            node_state.clone()
        };

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: last_storage_state.clone(),
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let _msg_res = node.handle_append_entries(1, msg_req).await;
            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.kind, NodeKind::Follower)
        }
    }

    #[tokio::test]
    async fn handle_append_entries_update_leader_id_with_higher_term() {
        let (_tx, rx) = mpsc::channel(1);
        let mem_storage = MemStorage::<usize>::default();
        let node = Node::new(1, rx, mem_storage);

        let node_state = { node.state.lock().await.clone() };

        let last_index = node.storage.last_index().await.unwrap();
        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
        let new_term = node_state.term + 1;
        let new_leader_id = 1;

        let msg_reqs = [
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: last_storage_state.clone(),
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    term: last_storage_state.term + 1,
                    ..last_storage_state
                },
            },
            MsgAppendEntriesReq {
                term: new_term,
                entries: Vec::new(),
                commited_index: 0,
                prev_storage_state: StorageState {
                    index: last_storage_state.index + 1,
                    ..last_storage_state
                },
            },
        ];
        let mut msg_reqs = msg_reqs.into_iter();

        while let Some(msg_req) = msg_reqs.next() {
            let _msg_res = node.handle_append_entries(new_leader_id, msg_req).await;
            let new_node_state = { node.state.lock().await.clone() };
            assert_eq!(new_node_state.leader_id, Some(new_leader_id))
        }
    }

    #[tokio::test]
    async fn handle_append_entries_reject_on_lower_term() {
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

        let msg_res = node.handle_append_entries(1, msg_req).await;
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
    async fn handle_append_entries_reject_on_no_match_storage_state() {
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
                .await;
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
    async fn handle_append_entries_match_storage_state_no_truncate() {
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
        let msg_res = node.handle_append_entries(1, msg_req).await;
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
    async fn handle_append_entries_match_storage_state_with_truncate() {
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
        let msg_res = node.handle_append_entries(1, msg_req).await;
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
    async fn handle_append_entries_reset_vote_for() {
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
        let msg_res = node.handle_append_entries(2, msg_req).await;
        assert!(msg_res.success, "Message response must be success");

        let new_node_state = { node.state.lock().await.clone() };
        assert_eq!(
            new_node_state.vote_for, None,
            "Node state's vote for must be reset after receiving success append entries"
        );
    }
}
