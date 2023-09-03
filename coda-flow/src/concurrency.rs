use std::net::SocketAddr;

use anyhow::Error;
use futures::future::Either;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{event, Level};

use coda_ipc::Message;

use crate::transport::{FlowTransport, Recipient};

pub type MainLoopTx = mpsc::Sender<(Recipient, Result<Message, Error>)>;
pub type MainLoopRx = mpsc::Receiver<(Recipient, Result<Message, Error>)>;

pub struct FlowMainLoop {
    main_loop_tx: MainLoopTx,
    main_loop_rx: MainLoopRx,
    shutting_down: bool,
    flow_transport: Option<FlowTransport>,
}

impl FlowMainLoop {
    pub async fn new(listen_addr: Option<SocketAddr>) -> Result<Self, Error> {
        let (mainloop_tx, mainloop_rx) = mpsc::channel(100);
        let instance = FlowMainLoop {
            main_loop_tx: mainloop_tx.clone(),
            main_loop_rx: mainloop_rx,
            shutting_down: false,
            flow_transport: if let Some(addr) = listen_addr {
                Some(FlowTransport::connect(addr).await?)
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
            Some((recipient, rv)) = self.main_loop_rx.recv() => {
                event!(Level::DEBUG, "received message on main loop");
            },
            // We receive a message via the flow transport layer which will be re-routed into
            // the main loop channel.
            _ = self.flow_transport
                .as_mut()
                .map(|x| Either::Left(x.accept_and_register(self.main_loop_tx.clone())))
                .unwrap_or(Either::Right(futures::future::pending())) => {
                event!(Level::DEBUG, "client is connected to the flow transport");
            }
        }
        Ok(())
    }
}