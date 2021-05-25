use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::FindNode;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::Encode;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use super::traversal::Traversal;

pub struct DhtBootstrap<'a> {
    traversal: Traversal<'a>,
}

impl<'a> DhtBootstrap<'a> {
    pub fn new(
        target: &NodeId,
        table: &mut RoutingTable,
        udp: &'a UdpSocket,
        traversal_id: usize,
    ) -> Self {
        Self {
            traversal: Traversal::new(target, table, udp, traversal_id),
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

    pub async fn add_requests(&mut self, rpc: &mut RpcMgr, buf: &mut Vec<u8>) -> bool {
        log::trace!("Add BOOTSTRAP requests");

        let target = self.traversal.target;
        self.traversal
            .add_requests(rpc, buf, |txn_id, own_id, buf| {
                let msg = FindNode {
                    txn_id,
                    target: &target,
                    id: own_id,
                };
                msg.encode(buf);
                log::trace!("Send {:?}", msg);
            })
            .await
    }

    pub fn done(self) {}
}
