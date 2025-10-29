mod storage;

use std::cmp::Ordering;

use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
    lock::Mutex,
};

use crate::storage::StorageState;

pub struct Message {
    node_id: usize,
    body: MessageBody,
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

pub struct Node {
    id: usize,
    rx: Mutex<mpsc::Receiver<Message>>,
    storage_state: Mutex<StorageState>,
    node_state: Mutex<NodeState>,
}

impl Node {
    pub fn new(id: usize, rx: mpsc::Receiver<Message>) -> Self {
        Self {
            id,
            rx: Mutex::new(rx),
            node_state: Default::default(),
            storage_state: Default::default(),
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
        let storage_state = self.storage_state.lock().await;

        let granted = match Ord::cmp(&msg.term, &node_state.term) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => node_state
                .vote_for
                .is_none_or(|vote_for| vote_for == msg.candidate_id),
        };
        let granted = granted && msg.last_storage_state >= *storage_state;
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

    use super::*;

    #[tokio::test]
    async fn stop_run_when_no_sender() {
        let (tx, rx) = mpsc::channel(1);
        let node = Node::new(1, rx);
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
        let node = Node::new(1, rx_req);

        let node_state = { node.node_state.lock().await.clone() };
        let node_last_storage_state = { node.storage_state.lock().await.clone() };
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: node_last_storage_state.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: node_last_storage_state.term + 1,
                    ..node_last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: node_last_storage_state.index + 1,
                    ..node_last_storage_state
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(1, rx_req);

        let node_state = { node.node_state.lock().await.clone() };
        let node_last_storage_state = {
            let mut node_last_storage_state = node.storage_state.lock().await;
            *node_last_storage_state = StorageState {
                term: 23,
                index: 3289,
            };
            node_last_storage_state.clone()
        };

        let msg_reqs = [
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: node_last_storage_state.term - 1,
                    ..node_last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: node_last_storage_state.index - 1,
                    ..node_last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: node_last_storage_state.term - 1,
                    ..node_last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: node_last_storage_state.index - 1,
                    ..node_last_storage_state
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(1, rx_req);

        let mut node_state = { node.node_state.lock().await.clone() };
        node_state = NodeState {
            kind: NodeKind::Leader,
            ..node_state
        };

        let node_last_storage_state = { node.storage_state.lock().await.clone() };
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: node_last_storage_state.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    term: node_last_storage_state.term + 1,
                    ..node_last_storage_state
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_storage_state: StorageState {
                    index: node_last_storage_state.index + 1,
                    ..node_last_storage_state
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(1, rx_req);

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
                    last_storage_state: { node.storage_state.lock().await.clone() },
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(1, rx_req);

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
                    last_storage_state: { node.storage_state.lock().await.clone() },
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(1, rx_req);

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

        while let Some(init_node_state) = node_states.next() {
            {
                let mut node_state = node.node_state.lock().await;
                *node_state = init_node_state;
            }

            let msg_reqs = [
                MsgRequestVoteReq {
                    term: init_node_state.term,
                    candidate_id: init_node_state.vote_for.unwrap_or(1),
                    last_storage_state: { node.storage_state.lock().await.clone() },
                },
                MsgRequestVoteReq {
                    term: init_node_state.term,
                    candidate_id: init_node_state.vote_for.map_or(1, |vote_for| vote_for + 1),
                    last_storage_state: { node.storage_state.lock().await.clone() },
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
