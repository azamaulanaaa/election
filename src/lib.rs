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

#[derive(Default)]
pub enum NodeKind {
    #[default]
    Leader,
    Follower,
}

#[derive(Default)]
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
        let state = self.state.lock().await;

        MsgRequestVoteRes {
            term: state.term.max(msg.term),
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
}
