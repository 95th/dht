use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::FindNode;
use crate::server::request::{DhtNode, Status};
use crate::server::{RpcMgr, Transactions};
use crate::table::RoutingTable;
use std::net::SocketAddr;

pub struct BootstrapRequest {
    target: NodeId,
    own_id: NodeId,
    nodes: Vec<DhtNode>,
    txns: Transactions,
    branch_factor: u8,
}

impl BootstrapRequest {
    pub(super) fn new(target: &NodeId, own_id: &NodeId, table: &mut RoutingTable) -> Self {
        let mut closest = Vec::with_capacity(Bucket::MAX_LEN);
        table.find_closest(target, &mut closest, Bucket::MAX_LEN);

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
            target: *target,
            own_id: *own_id,
            nodes,
            txns: Transactions::new(),
            branch_factor: 3,
        }
    }

    pub fn prune(&mut self, table: &mut RoutingTable) {
        log::trace!("Prune BOOTSTRAP request");
        let nodes = &mut self.nodes;
        self.txns.prune_with(table, |id| {
            if let Some(node) = nodes.iter_mut().find(|node| &node.id == id) {
                node.status.insert(Status::FAILED);
            }
        })
    }

    pub fn handle_reply(
        &mut self,
        resp: &Response,
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

        log::trace!("Handle BOOTSTRAP response");

        let result = table.read_nodes_with(resp, |c| {
            if !self.nodes.iter().any(|n| &n.id == c.id) {
                self.nodes.push(DhtNode::new(c));
            }
        });

        if let Err(e) = result {
            log::warn!("{}", e);
        }

        let target = &self.target;
        self.nodes.sort_by_key(|n| n.id ^ target);
        self.nodes.truncate(100);

        true
    }

    pub async fn invoke(&mut self, rpc: &mut RpcMgr) -> bool {
        log::trace!("Invoke BOOTSTRAP request");
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

            let msg = FindNode {
                target: &self.target,
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
}
