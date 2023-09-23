use std::collections::HashMap;
use std::io::Cursor;
use std::net::SocketAddr;

use anyhow::Error;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;

use coda_ipc::Message;

use crate::concurrency::MainLoopTx;

#[derive(Debug, Copy, Clone, Hash, Ord, Eq, PartialEq, PartialOrd)]
pub enum Recipient {
    Supervisor(SocketAddr),
    Client(SocketAddr),
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

    pub async fn accept_and_register(&mut self, main_loop_tx: MainLoopTx) -> Result<(), Error> {
        let (stream, addr) = self.listener.accept().await?;
        let (mut read, write) = stream.into_split();
        self.clients.insert(addr, write);

        let main_loop_tx = main_loop_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = read_msg(&mut read).await;
                let failed = msg.is_err();
                if main_loop_tx
                    .send((Recipient::Client(addr), msg))
                    .await
                    .is_err()
                    || failed
                {
                    break;
                }
            }
        });
        Ok(())
    }
}

// TODO: share implementation of cbor message transport across supervisor and flow.
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
