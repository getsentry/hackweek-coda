use std::net::SocketAddr;

use anyhow::Error;
use futures::future::Either;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{event, Level};

use coda_ipc::{Cmd, Message, Req};

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
        // This main loop will contain all the possible actions that flow can perform at each loop
        // iteration, which include:
        // - Consuming all incoming messages
        // - Accepting an incoming flow transport connection
        tokio::select! {
            // We receive a message on the main loop channel.
            Some((recipient, rv)) = self.main_loop_rx.recv() => {
                match rv {
                    Ok(msg) => {
                        match self.handle_message(recipient, msg).await {
                            Ok(()) => {}
                            Err(err) => {
                                event!(Level::ERROR, "error while handling message: {}", err);
                            }
                        }
                    }
                    Err(err) => {
                        event!(Level::ERROR, "error when receiving message: {}", err);
                    }
                }
            },
            // We accept an incoming connection on the transport which will be registered on the
            // main loop.
            _ = self.flow_transport
                .as_mut()
                .map(|x| Either::Left(x.accept_and_register(self.main_loop_tx.clone())))
                .unwrap_or(Either::Right(futures::future::pending())) => {
                event!(Level::DEBUG, "client is connected to the flow transport");
            }
        }
        Ok(())
    }

    async fn handle_message(&self, recipient: Recipient, msg: Message) -> Result<(), Error> {
        match msg {
            Message::Req(req) => self.handle_request(recipient, req).await?,
            _ => {}
        }
        Ok(())
    }

    async fn handle_request(&self, recipient: Recipient, req: Req) -> Result<(), Error> {
        match req.cmd {
            Cmd::SpawnWorkflow(workflow) => {}
            _ => {}
        }
        Ok(())
    }
}
