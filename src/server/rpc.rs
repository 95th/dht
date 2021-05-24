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
    pub(crate) invoked: Vec<(TxnId, NodeId)>,
}

impl RpcMgr {
    pub fn new(own_id: NodeId) -> Self {
        Self {
            txn_id: TxnId(0),
            own_id,
            tokens: HashMap::new(),
            txns: Transactions::new(),
            invoked: Vec::new(),
        }
    }

    pub fn new_txn(&mut self) -> TxnId {
        self.txn_id.next_id()
    }

    pub fn get_token(&self, addr: &SocketAddr) -> Option<&[u8]> {
        self.tokens.get(addr).map(|v| &v[..])
    }

    pub async fn handle_response(
        &mut self,
        msg: Msg<'_, '_>,
        addr: SocketAddr,
        table: &mut RoutingTable,
        udp: &UdpSocket,
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

        match &request.traversal {
            DhtTraversal::GetPeers(gp) => {
                let gp = &mut *gp.borrow_mut();
                gp.handle_response(&resp, &addr, table, self);
                gp.invoke(self, udp, buf).await;

                while let Some((txn_id, node_id)) = self.invoked.pop() {
                    self.txns
                        .insert(txn_id, &node_id, request.traversal.clone());
                }
            }
            DhtTraversal::Bootstrap(b) => {
                let b = &mut *b.borrow_mut();
                b.handle_response(&resp, &addr, table);
                b.invoke(self, udp, buf).await;

                while let Some((txn_id, node_id)) = self.invoked.pop() {
                    self.txns
                        .insert(txn_id, &node_id, request.traversal.clone());
                }
            }
        }
    }

    pub fn prune(&mut self, table: &mut RoutingTable) {
        log::trace!("RPC Prune");
        self.txns
            .prune_with(table, |id, traversal| match traversal {
                DhtTraversal::GetPeers(gp) => gp.borrow_mut().failed(id),
                DhtTraversal::Bootstrap(b) => b.borrow_mut().failed(id),
            })
    }
}

pub struct Request {
    pub id: NodeId,
    pub sent: Instant,
    pub has_id: bool,
    pub traversal: DhtTraversal,
}

impl Request {
    pub fn new(id: &NodeId, traversal: DhtTraversal) -> Self {
        Self {
            id: if id.is_zero() { NodeId::gen() } else { *id },
            sent: Instant::now(),
            has_id: !id.is_zero(),
            traversal,
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

    pub fn insert(&mut self, txn_id: TxnId, id: &NodeId, traversal: DhtTraversal) {
        self.pending.insert(txn_id, Request::new(id, traversal));
    }

    pub fn remove(&mut self, txn_id: TxnId) -> Option<Request> {
        self.pending.remove(&txn_id)
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Remove transactions that are timed out or not in Routing table
    /// anymore.
    pub fn prune_with<F>(&mut self, table: &mut RoutingTable, mut f: F)
    where
        F: FnMut(&NodeId, &DhtTraversal),
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

            f(&request.id, &request.traversal);

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
