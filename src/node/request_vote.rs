use super::*;

pub async fn handle_req<T, S, E>(
    node: &Node<T, S, E>,
    msg: MsgRequestVoteReq,
) -> Result<MsgRequestVoteRes, NodeError>
where
    T: State,
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq + Debug,
{
    node.state
        .set_term(node.state.get_term().await?.max(msg.term))
        .await
        .unwrap();

    if node.state.get_term().await? == msg.term {
        node.state.set_leader_id(None).await?;
    }

    let last_storage_state = {
        let last_index = node.storage.last_index().await.unwrap();

        node.storage.get_state(last_index).await?
    };

    let granted = match Ord::cmp(&msg.term, &node.state.get_term().await?) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => node
            .state
            .get_vote_for()
            .await?
            .is_none_or(|candidate_id| candidate_id == msg.candidate_id),
    };
    let granted = granted && msg.last_storage_state >= last_storage_state;
    if granted {
        node.state.set_vote_for(Some(msg.candidate_id)).await?;
    }

    Ok(MsgRequestVoteRes {
        term: node.state.get_term().await?,
        granted,
    })
}

pub async fn handle_res<T, S, E>(
    node: &Node<T, S, E>,
    from: u64,
    msg: MsgRequestVoteRes,
) -> Result<(), NodeError>
where
    T: State,
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq + Debug,
{
    let candidate_id = node.state.get_vote_for().await?;
    if candidate_id.is_none() {
        return Ok(());
    }

    let peer = node.peers.get(from).await?;
    if let Some(peer) = peer {
        let mut peer = peer;
        peer.vote_granted = Some(msg.granted);

        node.peers.insert(from, peer).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;

    use crate::{
        state::MemState,
        storage::{MemStorage, StorageState, StorageValue},
    };

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

    mod handle_req {
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
            let _msg_res = handle_req(&node, msg_req.clone()).await.unwrap();

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
            let msg_res = handle_req(&node, msg_req.clone()).await.unwrap();

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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, msg_req).await;

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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, msg_req).await.unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                let expected_vote_for = Some(msg_req.candidate_id);
                assert_eq!(vote_for, expected_vote_for);
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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

                assert_eq!(msg_res.granted, true);
            }

            #[tokio::test]
            async fn grant_for_voting_same_candidate() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(node.id + 1).await.unwrap();
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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, msg_req).await.unwrap();

                let vote_for = node.state.get_vote_for().await.unwrap();
                let expected_vote_for = Some(msg_req.candidate_id);
                assert_eq!(vote_for, expected_vote_for);
            }
        }
    }

    mod handle_res {
        use crate::peers::Peer;

        use super::*;

        mod equal_term {
            use super::*;

            #[tokio::test]
            async fn update_peer_vote_granted() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(Some(node.id + 1)).await.unwrap();
                }

                let from = node.id + 1;
                {
                    node.peers
                        .insert(
                            from,
                            Peer {
                                last_index: node.storage.last_index().await.unwrap(),
                                vote_granted: None,
                            },
                        )
                        .await
                        .unwrap();
                }

                let msg_res = MsgRequestVoteRes {
                    term: node.state.get_term().await.unwrap(),
                    granted: true,
                };

                handle_res(&node, from, msg_res).await.unwrap();

                let vote_granted = node.peers.get(from).await.unwrap().unwrap().vote_granted;
                let expect_vote_granted = Some(msg_res.granted);

                assert_eq!(vote_granted, expect_vote_granted);
            }
        }

        mod higher_term {
            use super::*;

            #[tokio::test]
            async fn update_term_to_higher_term() {
                let (_tx_in, rx_in) = mpsc::channel(1);
                let (tx_out, _rx_out) = mpsc::channel(1);
                let node = init_node(tx_out, rx_in).await;

                {
                    node.state.set_vote_for(Some(node.id + 1)).await.unwrap();
                }

                let from = node.id + 1;
                {
                    node.peers
                        .insert(
                            from,
                            Peer {
                                last_index: node.storage.last_index().await.unwrap(),
                                vote_granted: None,
                            },
                        )
                        .await
                        .unwrap();
                }

                let new_term = node.state.get_term().await.unwrap() + 1;
                let msg_res = MsgRequestVoteRes {
                    term: new_term,
                    granted: false,
                };

                handle_res(&node, from, msg_res).await.unwrap();

                let term = node.state.get_term().await.unwrap();
                assert_eq!(term, new_term);
            }
        }
    }
}
