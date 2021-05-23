use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::GetPeers;
use crate::server::request::{DhtNode, Status};
use crate::server::{RpcMgr, Transactions};
use crate::table::RoutingTable;
use ben::Decoder;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

pub struct GetPeersRequest {
    pub info_hash: NodeId,
    pub own_id: NodeId,
    pub nodes: Vec<DhtNode>,
    pub tokens: HashMap<SocketAddr, Vec<u8>>,
    txns: Transactions,
    peers: HashSet<SocketAddr>,
    branch_factor: u8,
}

impl GetPeersRequest {
    pub fn new(info_hash: &NodeId, own_id: &NodeId, table: &mut RoutingTable) -> Self {
        let mut closest = Vec::with_capacity(Bucket::MAX_LEN);
        table.find_closest(info_hash, &mut closest, Bucket::MAX_LEN);

        let mut nodes = vec![];
        for c in closest {
            nodes.push(DhtNode::new(&c));
        }

        if nodes.len() < 3 {
            for node in &table.router_nodes {
                nodes.push(DhtNode {
                    id: NodeId::new(),
                    addr: *node,
                    status: Status::INITIAL | Status::NO_ID,
                });
            }
        }

        Self {
            info_hash: *info_hash,
            own_id: *own_id,
            nodes,
            tokens: HashMap::new(),
            txns: Transactions::new(),
            peers: HashSet::new(),
            branch_factor: 3,
        }
    }

    pub fn prune(&mut self, table: &mut RoutingTable) {
        log::trace!("Prune GET_PEERS request");
        let nodes = &mut self.nodes;
        self.txns.prune_with(table, |id| {
            if let Some(node) = nodes.iter_mut().find(|node| &node.id == id) {
                node.status.insert(Status::FAILED);
            }
        })
    }

    pub fn handle_reply(
        &mut self,
        resp: &Response<'_, '_>,
        addr: &SocketAddr,
        table: &mut RoutingTable,
    ) -> bool {
        if let Some(req) = self.txns.remove(&resp.txn_id) {
            if req.has_id {
                if &req.id == resp.id {
                    table.heard_from(&req.id);
                } else {
                    log::warn!("ID mismatch from {}", addr);
                    table.failed(&req.id);
                    return true;
                }
            }
        } else {
            return false;
        }

        if let Some(node) = self.nodes.iter_mut().find(|node| &node.addr == addr) {
            node.status.insert(Status::ALIVE);
        } else {
            return false;
        }

        log::trace!("Handle GET_PEERS response");

        let result = table.read_nodes_with(resp, |c| {
            if !self.nodes.iter().any(|n| &n.id == c.id) {
                self.nodes.push(DhtNode::new(c));
            }
        });

        if let Err(e) = result {
            log::warn!("{}", e);
        }

        if let Some(token) = resp.body.get_bytes("token") {
            self.tokens.insert(*addr, token.to_vec());
        }

        fn decode_peer(d: Decoder) -> Option<SocketAddr> {
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

        if let Some(peers) = resp.body.get_list("values") {
            let peers = peers.into_iter().flat_map(decode_peer);
            self.peers.extend(peers);
        }

        let target = &self.info_hash;
        self.nodes.sort_by_key(|n| n.id ^ target);
        self.nodes.truncate(100);

        true
    }

    pub async fn invoke(&mut self, rpc: &mut RpcMgr) -> bool {
        log::trace!("Invoke GET_PEERS request");
        let mut outstanding = 0;
        let mut alive = 0;

        for n in &mut self.nodes {
            if alive == Bucket::MAX_LEN {
                break;
            }

            if outstanding == self.branch_factor {
                break;
            }

            if n.status.contains(Status::ALIVE) {
                alive += 1;
                continue;
            }

            if n.status.contains(Status::QUERIED) {
                if !n.status.contains(Status::FAILED) {
                    outstanding += 1;
                }
                continue;
            };

            let msg = GetPeers {
                info_hash: &self.info_hash,
                id: &self.own_id,
                txn_id: rpc.next_id(),
            };

            match rpc.send(&msg, &n.addr).await {
                Ok(_) => {
                    n.status.insert(Status::QUERIED);
                    self.txns.insert(msg.txn_id, &n.id);
                    outstanding += 1;
                }
                Err(e) => {
                    log::warn!("{}", e);
                    n.status.insert(Status::FAILED);
                }
            }
        }

        outstanding == 0 && alive == Bucket::MAX_LEN || self.txns.is_empty()
    }

    pub fn get_peers(self) -> Vec<SocketAddr> {
        self.peers.into_iter().collect()
    }
}
