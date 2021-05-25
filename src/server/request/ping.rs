use crate::contact::ContactRef;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::Ping;
use crate::server::request::{DhtNode, Status};
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::Encode;
use std::net::SocketAddr;

pub struct DhtPing {
    node: DhtNode,
    done: bool,
    traversal_id: usize,
}

impl DhtPing {
    pub fn new(id: &NodeId, addr: &SocketAddr, traversal_id: usize) -> Self {
        Self {
            node: DhtNode {
                id: *id,
                addr: *addr,
                status: Status::INITIAL,
            },
            done: false,
            traversal_id,
        }
    }

    pub fn set_failed(&mut self, id: &NodeId) {
        if &self.node.id == id {
            self.node.status.insert(Status::FAILED);
        }
        self.done = true;
    }

    pub fn handle_response(
        &mut self,
        resp: &Response,
        addr: &SocketAddr,
        table: &mut RoutingTable,
    ) {
        log::trace!("Handle PING response");

        if self.node.id == *resp.id && self.node.addr == *addr {
            table.add_contact(&ContactRef {
                id: resp.id,
                addr: *addr,
            });
        } else {
            table.failed(resp.id);
        }

        self.done = true;
    }

    pub async fn add_requests(&mut self, rpc: &mut RpcMgr<'_>) -> bool {
        log::trace!("Invoke PING request");
        if self.done {
            return true;
        }

        let txn_id = rpc.new_txn();
        let msg = Ping {
            txn_id,
            id: &rpc.own_id,
        };

        rpc.buf.clear();
        msg.encode(&mut rpc.buf);

        match rpc.send(self.node.addr).await {
            Ok(_) => {
                self.node.status.insert(Status::QUERIED);
                rpc.txns
                    .insert(txn_id, &self.node.id, &self.node.addr, self.traversal_id);
                false
            }
            Err(e) => {
                log::warn!("{}", e);
                true
            }
        }
    }

    pub fn done(self) {}
}
