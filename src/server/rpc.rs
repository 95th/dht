use crate::{
    id::NodeId,
    msg::{recv::Msg, TxnId},
    server::ClientRequest,
    table::RoutingTable,
};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::request::{DhtNode, Status};

pub struct RpcMgr {
    pub(crate) own_id: NodeId,
    txn_id: TxnId,
    tokens: HashMap<SocketAddr, Vec<u8>>,
    pub(crate) txns: Transactions,
    peers: HashMap<NodeId, HashSet<SocketAddr>>,
    nodes: Vec<DhtNode>,
}

impl RpcMgr {
    pub fn new(own_id: NodeId) -> Self {
        Self {
            own_id,
            txn_id: TxnId(0),
            tokens: HashMap::new(),
            txns: Transactions::new(),
            peers: HashMap::new(),
            nodes: Vec::new(),
        }
    }

    pub fn new_txn(&mut self) -> TxnId {
        self.txn_id.next_id()
    }

    pub fn get_token(&self, addr: &SocketAddr) -> Option<&[u8]> {
        self.tokens.get(addr).map(|v| &v[..])
    }

    pub fn handle_response(
        &mut self,
        msg: Msg<'_, '_>,
        addr: SocketAddr,
        table: &mut RoutingTable,
    ) -> Option<ClientRequest> {
        log::debug!("Received msg: {:?}", msg);

        let resp = match msg {
            Msg::Response(x) => x,
            x => {
                log::warn!("Unhandled msg: {:?}", x);
                return None;
            }
        };

        let request = match self.txns.remove(resp.txn_id) {
            Some(req) => {
                if req.has_id && &req.id == resp.id {
                    table.heard_from(&req.id);
                } else if req.has_id {
                    log::warn!("ID mismatch from {}", addr);
                    table.failed(&req.id);
                    return None;
                }
                req
            }
            None => {
                log::warn!("Response for unrecognized txn: {:?}", resp.txn_id);
                return None;
            }
        };

        if let Some(node) = self.nodes.iter_mut().find(|node| node.addr == addr) {
            node.status.insert(Status::ALIVE);
        } else {
            return None;
        }

        let result = table.read_nodes_with(&resp, |c| {
            if !self.nodes.iter().any(|n| &n.id == c.id) {
                self.nodes.push(DhtNode::new(c));
            }
        });

        if let Err(e) = result {
            log::warn!("Error in reading nodes: {}", e);
            return None;
        }

        use ClientRequest as Req;
        match &request.client_request {
            Req::Announce { .. } => return None,
            Req::GetPeers { info_hash } => {
                if let Some(token) = resp.body.get_bytes("token") {
                    self.tokens.insert(addr, token.to_vec());
                }

                if let Some(peers) = resp.body.get_list("values") {
                    let peers = peers.into_iter().flat_map(decode_peer);
                    self.peers
                        .entry(*info_hash)
                        .or_insert_with(HashSet::new)
                        .extend(peers);
                }
            }
            Req::Ping { .. } => return None,
            Req::BootStrap { .. } => {}
        }

        Some(request.client_request)
    }
}

fn decode_peer(d: ben::Decoder) -> Option<SocketAddr> {
    if let Some(b) = d.as_bytes() {
        if b.len() == 6 {
            unsafe {
                let ip = *(b.as_ptr() as *const [u8; 4]);
                let port = *(b.as_ptr().add(4) as *const [u8; 2]);
                let port = u16::from_be_bytes(port);
                return Some((ip, port).into());
            }
        } else {
            log::warn!("Incorrect Peer length. Expected: 6, Actual: {}", b.len());
        }
    } else {
        log::warn!("Unexpected Peer format: {:?}", d);
    }

    None
}

pub struct Request {
    pub id: NodeId,
    pub sent: Instant,
    pub has_id: bool,
    pub client_request: ClientRequest,
}

impl Request {
    pub fn new(id: &NodeId, client_request: ClientRequest) -> Self {
        Self {
            id: if id.is_zero() { NodeId::gen() } else { *id },
            sent: Instant::now(),
            has_id: !id.is_zero(),
            client_request,
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

    pub fn insert(&mut self, txn_id: TxnId, id: &NodeId, client_request: ClientRequest) {
        self.pending
            .insert(txn_id, Request::new(id, client_request));
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

    /// Remove transactions that are timed out or not in Routing table
    /// anymore.
    pub fn prune(&mut self, table: &mut RoutingTable) {
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
