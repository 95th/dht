use slab::Slab;
use tokio::net::UdpSocket;

use crate::{
    id::NodeId,
    msg::{recv::Msg, TxnId},
    table::RoutingTable,
};
use hashbrown::HashMap;
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::request::DhtTraversal;

pub struct RpcMgr<'a> {
    txn_id: TxnId,
    pub own_id: NodeId,
    pub tokens: HashMap<SocketAddr, Vec<u8>>,
    pub txns: Transactions,
    pub udp: &'a UdpSocket,
    pub buf: Vec<u8>,
}

impl<'a> RpcMgr<'a> {
    pub fn new(own_id: NodeId, udp: &'a UdpSocket) -> Self {
        Self {
            txn_id: TxnId(0),
            own_id,
            tokens: HashMap::new(),
            txns: Transactions::new(),
            udp,
            buf: Vec::with_capacity(1024),
        }
    }

    pub fn new_txn(&mut self) -> TxnId {
        self.txn_id.next_id()
    }

    pub async fn send(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let n = self.udp.send_to(&self.buf, addr).await?;
        anyhow::ensure!(n == self.buf.len(), "Couldn't write the whole message");
        Ok(())
    }

    pub async fn handle_response(
        &mut self,
        msg: Msg<'_, '_>,
        addr: SocketAddr,
        table: &mut RoutingTable,
        running: &mut Slab<DhtTraversal>,
    ) {
        log::debug!("Received msg: {:?}", msg);

        let resp = match msg {
            Msg::Response(x) => x,
            Msg::Error(e) => {
                match self.txns.remove(e.txn_id) {
                    Some(req) => {
                        log::warn!("Error response from {}: {:?}", addr, e);
                        if req.has_id {
                            table.failed(&req.id);
                        }

                        let t = &mut running[req.traversal_id];
                        t.set_failed(&req.id, &addr);
                        let done = t.add_requests(self).await;
                        if done {
                            running.remove(req.traversal_id).done();
                        }
                    }
                    None => {
                        log::warn!("Response for unrecognized txn: {:?}", e.txn_id);
                    }
                }
                return;
            }
            x => {
                log::warn!("Unhandled msg: {:?}", x);
                return;
            }
        };

        let req = match self.txns.remove(resp.txn_id) {
            Some(req) => {
                if req.has_id && &req.id == resp.id {
                    table.heard_from(&req.id);
                } else if req.has_id {
                    log::warn!(
                        "ID mismatch from {}, Expected: {:?}, Actual: {:?}",
                        addr,
                        &req.id,
                        &resp.id
                    );
                    table.failed(&req.id);

                    let t = &mut running[req.traversal_id];
                    t.set_failed(&req.id, &addr);
                    let done = t.add_requests(self).await;
                    if done {
                        running.remove(req.traversal_id).done();
                    }
                    return;
                }
                req
            }
            None => {
                log::warn!("Response for unrecognized txn: {:?}", resp.txn_id);
                return;
            }
        };

        let t = &mut running[req.traversal_id];
        t.handle_response(&resp, &addr, table, self, req.has_id);
        let done = t.add_requests(self).await;
        if done {
            running.remove(req.traversal_id).done();
        }
    }

    pub async fn check_timeouts(
        &mut self,
        table: &mut RoutingTable,
        running: &mut Slab<DhtTraversal>,
        timed_out: &mut Vec<(TxnId, Request)>,
    ) {
        let before = self.txns.pending.len();
        let cutoff = Instant::now() - self.txns.timeout;

        log::debug!(
            "{} pending txns in {} traversals",
            self.txns.pending.len(),
            running.len()
        );

        if self.txns.pending.is_empty() {
            assert!(running.is_empty());
        }

        timed_out.extend(self.txns.pending.drain_filter(|_, req| req.sent < cutoff));

        for (txn_id, req) in timed_out.drain(..) {
            log::trace!("Txn {:?} expired", txn_id);
            if req.has_id {
                table.failed(&req.id);
            }

            let t = &mut running[req.traversal_id];
            t.set_failed(&req.id, &req.addr);
            let done = t.add_requests(self).await;
            if done {
                running.remove(req.traversal_id).done();
            }
        }

        log::trace!(
            "Check timed out txns, before: {}, after: {}",
            before,
            self.txns.pending.len()
        );
    }
}

pub struct Request {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub sent: Instant,
    pub has_id: bool,
    pub traversal_id: usize,
}

impl Request {
    pub fn new(id: &NodeId, addr: &SocketAddr, traversal_id: usize) -> Self {
        Self {
            id: if id.is_zero() { NodeId::gen() } else { *id },
            addr: *addr,
            sent: Instant::now(),
            has_id: !id.is_zero(),
            traversal_id,
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

    pub fn insert(&mut self, txn_id: TxnId, id: &NodeId, addr: &SocketAddr, traversal_id: usize) {
        self.pending
            .insert(txn_id, Request::new(id, addr, traversal_id));
    }

    pub fn remove(&mut self, txn_id: TxnId) -> Option<Request> {
        self.pending.remove(&txn_id)
    }
}
