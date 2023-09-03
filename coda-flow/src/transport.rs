use std::collections::HashMap;
use std::net::SocketAddr;
use anyhow::Error;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use crate::concurrency::{MainLoopTx};

#[derive(Debug, Copy, Clone, Hash, Ord, Eq, PartialEq, PartialOrd)]
pub enum Recipient {
    Supervisor(SocketAddr),
    Client(SocketAddr),
}

pub struct FlowTransport {
    listener: TcpListener,
    clients: HashMap<SocketAddr, OwnedWriteHalf>,
    mainloop_tx: MainLoopTx,
}

impl FlowTransport {
    pub async fn connect(
        addr: SocketAddr,
        mainloop_tx: MainLoopTx,
    ) -> Result<FlowTransport, Error> {
        Ok(FlowTransport {
            listener: TcpListener::bind(addr).await?,
            clients: HashMap::new(),
            mainloop_tx,
        })
    }

    pub async fn accept(&mut self) -> Result<(), Error> {
        let (stream, addr) = self.listener.accept().await?;
        let (mut read, write) = stream.into_split();
        self.clients.insert(addr, write);

        let mainloop_tx = self.mainloop_tx.clone();
        tokio::spawn(async move {
            loop {
                // let msg = read_msg(&mut read).await;
                // let failed = msg.is_err();
                // if mainloop_tx
                //     .send((Recipient::Client(addr), msg))
                //     .await
                //     .is_err()
                //     || failed
                // {
                //     break;
                // }
            }
        });

        Ok(())
    }
}