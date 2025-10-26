use futures::{
    StreamExt,
    channel::{mpsc, oneshot},
    lock::Mutex,
};

pub struct Message {
    request: MessageRequest,
    response: oneshot::Sender<MessageResponse>,
}

pub enum MessageRequest {}

pub enum MessageResponse {}

pub struct Node {
    rx: Mutex<mpsc::Receiver<Message>>,
}

impl Node {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self { rx: Mutex::new(rx) }
    }

    pub async fn run(&self) {
        let mut rx_guard = self.rx.lock().await;

        while let Some(_message) = rx_guard.next().await {}
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
