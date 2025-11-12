mod append_entries;
mod request_vote;

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
    n_quorum: u64,
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
        n_quorum: u64,
        state: T,
        storage: S,
    ) -> Self {
        Self {
            id,
            tx_out,
            rx_in: Mutex::new(rx_in),
            n_quorum,
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
                    let msg_res = request_vote::handle_req(&self, msg_req).await.unwrap();
                    let _ = tx_res.send(msg_res);
                }
                MessageBody::AppendEntries(msg_req, tx_res) => {
                    let msg_res = append_entries::handle_req(&self, message.node_id, msg_req)
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
        self.state.set_vote_for(Some(candidate_id.clone())).await?;

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
        let n_quorum = 2;
        let node = Node::new(1, tx_in, rx_out, n_quorum, mem_state, mem_storage);

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
            let expected_vote_for = Some(node.id);
            assert_eq!(vote_for, expected_vote_for);
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
                        node.peers
                            .insert(
                                id,
                                Peer {
                                    last_index,
                                    vote_granted: None,
                                },
                            )
                            .await
                            .unwrap();
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

            #[tokio::test]
            async fn last_storage_state_is_node_last_storage_state() {
                let n_msgs = 2;

                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, mut rx_out) = mpsc::channel(n_msgs);
                let node = init_node(tx_out, rx_in).await;

                let last_index = node.storage.last_index().await.unwrap();

                {
                    for offset in 0..n_msgs {
                        let id = node.id + offset as u64;
                        node.peers
                            .insert(
                                id,
                                Peer {
                                    last_index,
                                    vote_granted: None,
                                },
                            )
                            .await
                            .unwrap();
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
                                    let last_storage_state = {
                                        let last_index = node.storage.last_index().await.unwrap();
                                        let last_storage_state = node.storage.get_state(last_index).await.unwrap();
                                        last_storage_state
                                    };
                                    assert_eq!(msg_req.last_storage_state, last_storage_state);

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
