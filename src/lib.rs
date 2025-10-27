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

#[derive(Default, Clone, Copy)]
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
        state.term = state.term.max(msg.term);

        MsgRequestVoteRes {
            term: state.term,
            granted: false,
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
}
