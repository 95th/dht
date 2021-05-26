use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::GetPeers;
use crate::server::PeerSender;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::{Decoder, Encode};
use std::collections::HashSet;
use std::net::SocketAddr;

use super::traversal::Traversal;

pub struct DhtGetPeers {
    pub traversal: Traversal,
    peers: HashSet<SocketAddr>,
    sender: PeerSender,
}

impl DhtGetPeers {
    pub fn new(
        info_hash: &NodeId,
        table: &RoutingTable,
        sender: PeerSender,
        traversal_id: usize,
    ) -> Self {
        Self {
            traversal: Traversal::new(info_hash, table, traversal_id),
            peers: HashSet::new(),
            sender,
        }
    }

    pub fn handle_response(
        &mut self,
        resp: &Response<'_, '_>,
        addr: &SocketAddr,
        table: &mut RoutingTable,
        rpc: &mut RpcMgr,
        has_id: bool,
    ) {
        log::trace!("Handle GET_PEERS response");
        self.traversal.handle_response(resp, addr, table, has_id);

        if let Some(token) = resp.body.get_bytes("token") {
            rpc.tokens.insert(*addr, token.to_vec());
        }

        if let Some(peers) = resp.body.get_list("values") {
            let peers = peers.into_iter().flat_map(decode_peer);
            self.peers.extend(peers);
        }
    }

    pub fn set_failed(&mut self, id: &NodeId, addr: &SocketAddr) {
        self.traversal.set_failed(id, addr);
    }

    pub async fn add_requests(&mut self, rpc: &mut RpcMgr<'_>) -> bool {
        log::trace!("Add GET_PEERS requests");

        let info_hash = self.traversal.target;
        self.traversal
            .add_requests(rpc, |rpc| {
                let msg = GetPeers {
                    txn_id: rpc.new_txn(),
                    id: &rpc.own_id,
                    info_hash: &info_hash,
                };

                log::trace!("Send {:?}", msg);

                msg.encode(&mut rpc.buf);
                msg.txn_id
            })
            .await
    }

    pub fn done(self) {
        self.sender.send(self.peers).unwrap()
    }
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
