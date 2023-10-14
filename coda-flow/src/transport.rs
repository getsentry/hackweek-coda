use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;

use anyhow::{anyhow, Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use tracing::{event, Level};

use coda_ipc::{Message, Sender};

use crate::concurrency::MainLoopTx;

#[derive(Debug, Copy, Clone, Hash, Ord, Eq, PartialEq, PartialOrd)]
pub enum Recipient {
    Supervisor(SocketAddr),
    Client(SocketAddr),
    Unknown
}

impl Recipient {

    fn from_sender(sender: Sender, addr: SocketAddr) -> Option<Recipient> {
        match sender {
            Sender::Flow => None,
            Sender::Supervisor => Some(Recipient::Supervisor(addr)),
            Sender::Client => Some(Recipient::Client(addr))
        }
    }
}

pub struct FlowTransport {
    listener: TcpListener,
    clients: HashMap<SocketAddr, OwnedWriteHalf>,
}

impl FlowTransport {
    pub async fn connect(addr: SocketAddr) -> Result<FlowTransport, Error> {
        Ok(FlowTransport {
            listener: TcpListener::bind(addr).await?,
            clients: HashMap::new(),
        })
    }

    pub async fn accept_and_listen(&mut self, main_loop_tx: MainLoopTx) -> Result<(), Error> {
        let (stream, addr) = self.listener.accept().await?;
        let (mut read, write) = stream.into_split();
        self.clients.insert(addr, write);

        let main_loop_tx = main_loop_tx.clone();
        tokio::spawn(async move {
            loop {
                event!(Level::DEBUG, "waiting for message from recipient");

                let msg = read_msg(&mut read).await;

                let is_msg_failed = msg.is_err();
                if !is_msg_failed {
                    event!(Level::DEBUG, "received message from recipient");
                }

                let send_result = main_loop_tx
                    .send((Recipient::Client(addr), msg))
                    .await;

                if send_result.is_err() || is_msg_failed {
                    break;
                }


            }
        });

        Ok(())
    }

    pub async fn send(&mut self, msg: Message, recipient: &Recipient) -> Result<(), Error> {
        let addr = match recipient {
            Recipient::Supervisor(addr) => addr,
            Recipient::Client(addr) => addr,
            Recipient::Unknown => {
                return Err(anyhow!("Cannot send to an unknown recipient"))
            }
        };

        let write = match self.clients.get_mut(addr) {
            Some(write) => write,
            None => {
                return Err(anyhow!("The recipient was not found in the internal clients map"))
            }
        };

        event!(Level::DEBUG, "sending response to recipient");

        let mut bytes = pack_msg(msg)?;
        write.write_all_buf(&mut bytes).await?;

        event!(Level::DEBUG, "response sent to recipient");

        Ok(())
    }
}

async fn read_msg<R: AsyncRead + Unpin>(r: &mut R) -> Result<Message, Error> {
    let mut bytes = [0u8; 4];
    r.read_exact(&mut bytes).await?;
    let len = (&bytes[..]).get_u32();
    let mut buf = vec![0; len as usize];
    r.read_exact(&mut buf).await?;

    Ok(ciborium::from_reader(&buf[..])?)
}

fn pack_msg(msg: Message) -> Result<Cursor<Bytes>, Error> {
    let mut buf = Vec::<u8>::new();
    ciborium::into_writer(&msg, &mut buf).unwrap();
    let mut rv = BytesMut::with_capacity(4);
    rv.put_u32(buf.len() as u32);
    rv.extend_from_slice(&buf);

    Ok(Cursor::new(rv.freeze()))
}
