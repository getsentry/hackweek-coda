use std::net::SocketAddr;

use anyhow::Error;
use futures::future::Either;
use tokio::signal;
use tokio::sync::mpsc;

use coda_ipc::Message;

use crate::transport::{FlowTransport, Recipient};

pub type MainLoopTx = mpsc::Sender<(Recipient, Result<Message, Error>)>;
pub type MainLoopRx = mpsc::Receiver<(Recipient, Result<Message, Error>)>;

pub struct FlowMainLoop {
    mainloop_tx: MainLoopTx,
    mainloop_rx: MainLoopRx,
    shutting_down: bool,
    flow_transport: Option<FlowTransport>,
}

impl FlowMainLoop {
    pub async fn new(listen_addr: Option<SocketAddr>) -> Result<Self, Error> {
        let (mainloop_tx, mainloop_rx) = mpsc::channel(100);
        let instance = FlowMainLoop {
            mainloop_tx: mainloop_tx.clone(),
            mainloop_rx,
            shutting_down: false,
            flow_transport: if let Some(addr) = listen_addr {
                Some(FlowTransport::connect(addr, mainloop_tx).await?)
            } else {
                None
            },
        };
        Ok(instance)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        self.event_loop().await?;
        Ok(())
    }

    async fn event_loop(&mut self) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                _ = signal::ctrl_c() => {
                    self.shutting_down = true;
                    break;
                }
                _ = self.event_loop_iterate() => {}
            }
        }
        Ok(())
    }

    async fn event_loop_iterate(&mut self) -> Result<(), Error> {
        tokio::select! {
            // We receive a message on the main loop channel.
            Some((recipient, rv)) = self.mainloop_rx.recv() => {
                println!("RECEIVED MESSAGE ON MAIN LOOP")
            },
            // We receive a message via the flow transport layer.
            _ = self.flow_transport
                .as_mut()
                .map(|x| Either::Left(x.accept()))
                .unwrap_or(Either::Right(futures::future::pending())) => {}
        }
        Ok(())
    }
}