use super::*;

pub async fn handle_req<T, S, E>(
    node: &Node<T, S, E>,
    from: u64,
    msg: MsgAppendEntriesReq<E>,
) -> Result<MsgAppendEntriesRes, NodeError>
where
    T: State,
    S: Storage<E>,
    E: Clone + Send + Sync + PartialEq + Debug,
{
    node.state
        .set_term(node.state.get_term().await?.max(msg.term))
        .await?;

    let success = node.state.get_term().await? == msg.term;
    if success {
        node.state.set_leader_id(Some(from)).await?;
        node.state.set_vote_for(None).await?;
    }

    let storage_state = node.storage.get_state(msg.prev_storage_state.index).await;
    let success = success && storage_state.is_ok_and(|v| v == msg.prev_storage_state);

    if success {
        let entries = msg.entries.into_iter().enumerate();
        let mut truncated = false;
        for (offset, entry) in entries {
            let current_index = (offset + 1) as u64 + msg.prev_storage_state.index;
            let value = StorageValue {
                term: node.state.get_term().await?,
                entry,
            };

            let stored_entry = node.storage.get(current_index).await;
            if stored_entry.is_ok_and(|e| e == value) {
                continue;
            }

            if !truncated {
                node.storage.truncate(current_index).await.unwrap();
                truncated = true;
            }

            node.storage.push(value).await.unwrap();
        }

        node.storage
            .set_commited_index(msg.commited_index.min(node.storage.last_index().await?))
            .await?;
    }

    Ok(MsgAppendEntriesRes {
        term: node.state.get_term().await?,
        success,
    })
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc;

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

    mod handle_req {
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
            let _msg_res = handle_req(&node, node.id + 1, msg_req.clone())
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
            let msg_res = handle_req(&node, node.id + 1, msg_req.clone())
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
                let _msg_res = handle_req(&node, new_leader_id, msg_req).await;

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, leader_id, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, leader_id, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, leader_id, msg_req).await.unwrap();

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
                let msg_res = handle_req(&node, leader_id, msg_req).await.unwrap();

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
                    let msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                    let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                    let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                    let msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                    let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                    let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, new_leader_id, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

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
                let _msg_res = handle_req(&node, node.id + 1, msg_req).await.unwrap();

                let commited_index = node.storage.get_commited_index().await.unwrap();
                assert_eq!(commited_index, 1);
            }
        }
    }
}
