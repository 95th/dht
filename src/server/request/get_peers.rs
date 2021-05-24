use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::GetPeers;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::{Decoder, Encode};
use futures::channel::oneshot;
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use super::traversal::Traversal;

pub struct DhtGetPeers {
    traversal: Traversal,
    peers: HashSet<SocketAddr>,
    peer_tx: Option<oneshot::Sender<Vec<SocketAddr>>>,
}

impl DhtGetPeers {
    pub fn new(
        info_hash: &NodeId,
        table: &RoutingTable,
        peer_tx: oneshot::Sender<Vec<SocketAddr>>,
    ) -> Self {
        Self {
            traversal: Traversal::new(info_hash, table),
            peers: HashSet::new(),
            peer_tx: Some(peer_tx),
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

    pub fn failed(&mut self, id: &NodeId) {
        self.traversal.failed(id);
    }

    pub async fn add_requests(
        &mut self,
        rpc: &mut RpcMgr,
        udp: &UdpSocket,
        buf: &mut Vec<u8>,
        traversal_id: usize,
    ) -> bool {
        log::trace!("Add GET_PEERS requests");

        let info_hash = self.traversal.target;
        self.traversal
            .add_requests(rpc, udp, buf, traversal_id, |txn_id, own_id, buf| {
                let msg = GetPeers {
                    txn_id,
                    id: own_id,
                    info_hash: &info_hash,
                };
                buf.clear();
                msg.encode(buf);
                log::trace!("Send {:?}", msg);
            })
            .await
    }
}

impl Drop for DhtGetPeers {
    fn drop(&mut self) {
        self.peer_tx
            .take()
            .unwrap()
            .send(self.peers.drain().collect())
            .unwrap()
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
