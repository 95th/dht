use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::FindNode;
use crate::server::request::{DhtNode, Status};
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::Encode;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct DhtBootstrap {
    target: NodeId,
    branch_factor: u8,
    nodes: Vec<DhtNode>,
}

impl DhtBootstrap {
    pub fn new(target: &NodeId, table: &mut RoutingTable) -> Self {
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
            nodes,
            branch_factor: 3,
        }
    }

    pub fn is_done(&self) -> bool {
        let mut outstanding = 0;
        let mut alive = 0;

        for n in &self.nodes {
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
        }

        outstanding == 0 && alive == Bucket::MAX_LEN
    }

    pub fn handle_response(
        &mut self,
        resp: &Response,
        addr: &SocketAddr,
        table: &mut RoutingTable,
    ) {
        if let Some(node) = self.nodes.iter_mut().find(|node| &node.addr == addr) {
            node.status.insert(Status::ALIVE);
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
    }

    pub fn failed(&mut self, id: &NodeId) {
        if let Some(node) = self.nodes.iter_mut().find(|node| &node.id == id) {
            node.status.insert(Status::FAILED);
        }
    }

    pub async fn invoke(
        &mut self,
        rpc: &mut RpcMgr,
        udp: &UdpSocket,
        buf: &mut Vec<u8>,
        traversal_id: usize,
    ) {
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
                txn_id: rpc.new_txn(),
                target: &self.target,
                id: &rpc.own_id,
            };

            buf.clear();
            msg.encode(buf);

            log::trace!("Send to {}: {:?}", n.addr, msg);

            match udp.send_to(buf, n.addr).await {
                Ok(count) if count == buf.len() => {
                    n.status.insert(Status::QUERIED);
                    rpc.txns.insert(msg.txn_id, &n.id, traversal_id);
                    outstanding += 1;
                }
                Ok(count) => {
                    log::warn!(
                        "Expected to write {} bytes, actual written: {}",
                        buf.len(),
                        count
                    );
                    n.status.insert(Status::FAILED);
                }
                Err(e) => {
                    log::warn!("{}", e);
                    n.status.insert(Status::FAILED);
                }
            }
        }
    }
}
