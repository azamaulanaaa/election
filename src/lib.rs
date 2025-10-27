use std::cmp::Ordering;

use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
    lock::Mutex,
};

pub enum Message {
    RequestVote(MsgRequestVoteReq, oneshot::Sender<MsgRequestVoteRes>),
}

pub struct MsgRequestVoteReq {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_state: LogState,
}

pub struct MsgRequestVoteRes {
    pub term: u64,
    pub granted: bool,
}

#[derive(Clone, Default, PartialEq, Eq, PartialOrd)]
pub struct LogState {
    pub term: u64,
    pub index: u64,
}

impl Ord for LogState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match Ord::cmp(&self.term, &other.term) {
            Ordering::Equal => Ord::cmp(&self.index, &other.index),
            other => other,
        }
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub enum NodeKind {
    #[default]
    Leader,
    Follower,
}

#[derive(Default, Clone, Copy)]
pub struct State {
    pub vote_for: Option<u64>,
    pub term: u64,
    pub kind: NodeKind,
}

pub struct Node {
    rx: Mutex<mpsc::Receiver<Message>>,
    log_state: Mutex<LogState>,
    state: Mutex<State>,
}

impl Node {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self {
            rx: Mutex::new(rx),
            state: Default::default(),
            log_state: Default::default(),
        }
    }

    pub async fn run(&self) {
        let mut rx_guard = self.rx.lock().await;

        while let Some(message) = rx_guard.next().await {
            match message {
                Message::RequestVote(msg_req, tx_res) => {
                    let msg_res = self.handle_request_vote(msg_req).await;
                    let _ = tx_res.send(msg_res);
                }
            };
        }
    }

    async fn handle_request_vote(&self, msg: MsgRequestVoteReq) -> MsgRequestVoteRes {
        let mut state = self.state.lock().await;
        let log_state = self.log_state.lock().await;

        let granted = match Ord::cmp(&msg.term, &state.term) {
            Ordering::Greater => true,
            _ => false,
        };
        let granted = granted && msg.last_log_state >= *log_state;

        if state.term < msg.term {
            state.term = msg.term;
            state.kind = NodeKind::Follower;
        }

        MsgRequestVoteRes {
            term: state.term,
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
        let node = Node::new(rx);
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
        let node = Node::new(rx_req);

        let node_state = { node.state.lock().await.clone() };
        let node_last_log = { node.log_state.lock().await.clone() };
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: node_last_log.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: LogState {
                    term: node_last_log.term + 1,
                    ..node_last_log
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: LogState {
                    index: node_last_log.index + 1,
                    ..node_last_log
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
    async fn handle_request_vote_reject_if_last_log_state_is_behind() {
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(rx_req);

        let node_state = { node.state.lock().await.clone() };
        let mut node_last_log = node.log_state.lock().await;
        *node_last_log = LogState {
            term: 23,
            index: 3289,
        };

        let msg_reqs = [
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_log_state: LogState {
                    term: node_last_log.term - 1,
                    ..*node_last_log
                },
            },
            MsgRequestVoteReq {
                term: node_state.term,
                candidate_id: 32,
                last_log_state: LogState {
                    index: node_last_log.index - 1,
                    ..*node_last_log
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_log_state: LogState {
                    term: node_last_log.term - 1,
                    ..*node_last_log
                },
            },
            MsgRequestVoteReq {
                term: node_state.term + 1,
                candidate_id: 32,
                last_log_state: LogState {
                    index: node_last_log.index - 1,
                    ..*node_last_log
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
    async fn handle_request_vote_always_step_down_for_higher_term() {
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(rx_req);

        let mut node_state = { node.state.lock().await.clone() };
        node_state = State {
            kind: NodeKind::Leader,
            ..node_state
        };

        let node_last_log = { node.log_state.lock().await.clone() };
        let new_term = node_state.term + 1;

        let msg_reqs = [
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: node_last_log.clone(),
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: LogState {
                    term: node_last_log.term + 1,
                    ..node_last_log
                },
            },
            MsgRequestVoteReq {
                term: new_term,
                candidate_id: 32,
                last_log_state: LogState {
                    index: node_last_log.index + 1,
                    ..node_last_log
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
        let (_tx_req, rx_req) = mpsc::channel(1);
        let node = Node::new(rx_req);

        let node_state = { node.state.lock().await.clone() };
        let new_node_states = [
            State {
                vote_for: None,
                kind: NodeKind::Follower,
                ..node_state
            },
            State {
                vote_for: None,
                kind: NodeKind::Leader,
                ..node_state
            },
            State {
                vote_for: Some(1),
                kind: NodeKind::Follower,
                ..node_state
            },
            State {
                vote_for: Some(1),
                kind: NodeKind::Leader,
                ..node_state
            },
        ];
        let mut new_node_state = new_node_states.into_iter();

        while let Some(new_node_state) = new_node_state.next() {
            {
                let mut node_state = node.state.lock().await;
                *node_state = new_node_state;
            }
            let new_term = new_node_state.term + 1;
            let new_candidate = new_node_state.vote_for.map(|v| v + 1).unwrap_or(32);

            let msg_res = node
                .handle_request_vote(MsgRequestVoteReq {
                    term: new_term,
                    candidate_id: new_candidate,
                    last_log_state: { node.log_state.lock().await.clone() },
                })
                .await;
            assert!(
                msg_res.granted,
                "RequestVote must granted vote for higher term"
            );

            let state = { node.state.lock().await };
            assert_eq!(
                state.vote_for,
                Some(new_candidate),
                "Node should vote for candidate with higher term"
            );
        }
    }
}
