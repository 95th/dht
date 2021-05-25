use crate::contact::ContactRef;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::Ping;
use crate::msg::TxnId;
use crate::server::request::{DhtNode, Status};
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::Encode;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct DhtPing<'a> {
    node: DhtNode,
    txn_id: TxnId,
    done: bool,
    udp: &'a UdpSocket,
}

impl<'a> DhtPing<'a> {
    pub fn new(id: &NodeId, addr: &SocketAddr, udp: &'a UdpSocket) -> Self {
        Self {
            node: DhtNode {
                id: *id,
                addr: *addr,
                status: Status::INITIAL,
            },
            txn_id: TxnId(0),
            done: false,
            udp,
        }
    }

    pub fn set_failed(&mut self, id: &NodeId) {
        if &self.node.id == id {
            self.node.status.insert(Status::FAILED);
        }
    }

    pub fn handle_response(
        &mut self,
        resp: &Response,
        addr: &SocketAddr,
        table: &mut RoutingTable,
    ) {
        if self.txn_id != resp.txn_id {
            return;
        }

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

    pub async fn add_requests(&mut self, rpc: &mut RpcMgr, buf: &mut Vec<u8>) -> bool {
        log::trace!("Invoke PING request");
        if self.done {
            return true;
        }

        let msg = Ping {
            txn_id: rpc.new_txn(),
            id: &rpc.own_id,
        };

        buf.clear();
        msg.encode(buf);

        match self.udp.send_to(&buf, &self.node.addr).await {
            Ok(_) => {
                self.node.status.insert(Status::QUERIED);
                self.txn_id = msg.txn_id;
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
