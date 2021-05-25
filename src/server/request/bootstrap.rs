use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::FindNode;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::Encode;
use std::net::SocketAddr;

use super::traversal::Traversal;

pub struct DhtBootstrap {
    traversal: Traversal,
}

impl DhtBootstrap {
    pub fn new(target: &NodeId, table: &mut RoutingTable, traversal_id: usize) -> Self {
        Self {
            traversal: Traversal::new(target, table, traversal_id),
        }
    }

    pub fn handle_response(
        &mut self,
        resp: &Response,
        addr: &SocketAddr,
        table: &mut RoutingTable,
        has_id: bool,
    ) {
        log::trace!("Handle BOOTSTRAP response");
        self.traversal.handle_response(resp, addr, table, has_id);
    }

    pub fn set_failed(&mut self, id: &NodeId, addr: &SocketAddr) {
        self.traversal.set_failed(id, addr);
    }

    pub async fn add_requests(&mut self, rpc: &mut RpcMgr<'_>) -> bool {
        log::trace!("Add BOOTSTRAP requests");

        let target = self.traversal.target;
        self.traversal
            .add_requests(rpc, |rpc| {
                let msg = FindNode {
                    txn_id: rpc.new_txn(),
                    target: &target,
                    id: &rpc.own_id,
                };
                log::trace!("Send {:?}", msg);

                msg.encode(&mut rpc.buf);
                msg.txn_id
            })
            .await
    }

    pub fn done(self) {}
}
