use slab::Slab;
use tokio::net::UdpSocket;

use crate::{
    id::NodeId,
    msg::{recv::Msg, TxnId},
    table::RoutingTable,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::request::DhtTraversal;

pub struct RpcMgr {
    txn_id: TxnId,
    pub(crate) own_id: NodeId,
    pub(crate) tokens: HashMap<SocketAddr, Vec<u8>>,
    pub(crate) txns: Transactions,
}

impl RpcMgr {
    pub fn new(own_id: NodeId) -> Self {
        Self {
            txn_id: TxnId(0),
            own_id,
            tokens: HashMap::new(),
            txns: Transactions::new(),
        }
    }

    pub fn new_txn(&mut self) -> TxnId {
        self.txn_id.next_id()
    }

    #[allow(unused)]
    pub fn get_token(&self, addr: &SocketAddr) -> Option<&[u8]> {
        self.tokens.get(addr).map(|v| &v[..])
    }

    pub async fn handle_response(
        &mut self,
        msg: Msg<'_, '_>,
        addr: SocketAddr,
        table: &mut RoutingTable,
        udp: &UdpSocket,
        running: &mut Slab<DhtTraversal>,
        buf: &mut Vec<u8>,
    ) {
        log::debug!("Received msg: {:?}", msg);

        let resp = match msg {
            Msg::Response(x) => x,
            x => {
                log::warn!("Unhandled msg: {:?}", x);
                return;
            }
        };

        let request = match self.txns.remove(resp.txn_id) {
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
                    running[req.traversal_id].failed(&req.id);
                    table.failed(&req.id);
                    return;
                }
                req
            }
            None => {
                log::warn!("Response for unrecognized txn: {:?}", resp.txn_id);
                return;
            }
        };

        let traversal = &mut running[request.traversal_id];
        traversal.handle_response(&resp, &addr, table, self, request.has_id);

        let done = traversal
            .add_requests(self, udp, buf, request.traversal_id)
            .await;

        if done {
            running.remove(request.traversal_id);
        }
    }

    pub fn prune(&mut self, table: &mut RoutingTable, running: &mut Slab<DhtTraversal>) {
        self.txns
            .prune_with(table, |id, traversal_id| running[traversal_id].failed(id))
    }
}

pub struct Request {
    pub id: NodeId,
    pub sent: Instant,
    pub has_id: bool,
    pub traversal_id: usize,
}

impl Request {
    pub fn new(id: &NodeId, traversal_id: usize) -> Self {
        Self {
            id: if id.is_zero() { NodeId::gen() } else { *id },
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

    pub fn insert(&mut self, txn_id: TxnId, id: &NodeId, traversal_id: usize) {
        self.pending.insert(txn_id, Request::new(id, traversal_id));
    }

    pub fn remove(&mut self, txn_id: TxnId) -> Option<Request> {
        self.pending.remove(&txn_id)
    }

    /// Remove transactions that are timed out or not in Routing table
    /// anymore.
    pub fn prune_with<F>(&mut self, table: &mut RoutingTable, mut f: F)
    where
        F: FnMut(&NodeId, usize),
    {
        let before = self.pending.len();
        let timeout = self.timeout;

        self.pending.retain(|txn_id, request| {
            if Instant::now() - request.sent < timeout {
                // Not timed out. Keep it.
                return true;
            }

            if request.has_id {
                table.failed(&request.id);
            }

            f(&request.id, request.traversal_id);

            log::trace!("Txn {:?} expired", txn_id);
            false
        });

        log::trace!(
            "Prune txns, before: {}, after: {}",
            before,
            self.pending.len()
        );
    }
}
