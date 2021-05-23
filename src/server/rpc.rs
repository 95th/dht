use crate::id::NodeId;
use crate::msg::recv::Msg;
use crate::msg::TxnId;
use crate::table::RoutingTable;
use ben::{Encode, Parser};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

pub struct RpcMgr {
    socket: UdpSocket,
    buf: Vec<u8>,
    recv_buf: Box<[u8]>,
    parser: Parser,
    txn_id: TxnId,
}

impl RpcMgr {
    pub fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            buf: Vec::new(),
            recv_buf: vec![0; 2048].into_boxed_slice(),
            parser: Parser::new(),
            txn_id: TxnId(0),
        }
    }

    pub fn next_id(&mut self) -> TxnId {
        self.txn_id.next_id()
    }

    pub async fn send<E: Encode>(&mut self, msg: E, addr: &SocketAddr) -> anyhow::Result<()> {
        self.buf.clear();
        msg.encode(&mut self.buf);

        let n = self.socket.send_to(&self.buf, addr).await?;
        log::trace!("Sent: {} bytes to {}", n, addr);

        anyhow::ensure!(n == self.buf.len(), "Failed to send complete message");
        Ok(())
    }

    pub async fn recv(&mut self) -> anyhow::Result<(Msg<'_, '_>, SocketAddr)> {
        let (n, addr) = self.socket.recv_from(&mut self.recv_buf).await?;
        log::trace!("Received: {} bytes from {}", n, addr);

        let msg = self.parser.parse(&self.recv_buf[..n])?;
        log::trace!("{:#?}", msg);
        Ok((msg, addr))
    }

    pub async fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> anyhow::Result<Option<(Msg<'_, '_>, SocketAddr)>> {
        match tokio::time::timeout(timeout, self.recv()).await {
            Ok(x) => x.map(Some),
            Err(_) => Ok(None),
        }
    }
}

pub struct Request {
    pub id: NodeId,
    pub sent: Instant,
    pub has_id: bool,
}

impl Request {
    pub fn new(id: &NodeId) -> Self {
        Self {
            id: if id.is_zero() { NodeId::gen() } else { *id },
            sent: Instant::now(),
            has_id: !id.is_zero(),
        }
    }
}

pub struct Transactions {
    pending: HashMap<TxnId, Request>,
    timeout: Duration,
}

impl Transactions {
    pub fn new() -> Self {
        Self::with_timeout(Duration::from_secs(5))
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            timeout,
        }
    }

    pub fn insert(&mut self, txn_id: TxnId, id: &NodeId) {
        self.pending.insert(txn_id, Request::new(id));
    }

    pub fn remove(&mut self, txn_id: &TxnId) -> Option<Request> {
        self.pending.remove(txn_id)
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Remove transactions that are timed out or not in Routing table
    /// anymore.
    pub fn prune_with<F>(&mut self, table: &mut RoutingTable, mut f: F)
    where
        F: FnMut(&NodeId),
    {
        let timeout = self.timeout;

        self.pending.retain(|txn_id, request| {
            if Instant::now() - request.sent < timeout {
                // Not timed out. Keep it.
                return true;
            }

            if request.has_id {
                table.failed(&request.id);
            }

            f(&request.id);

            log::trace!("Txn {:?} expired", txn_id);
            false
        });
    }
}
