use ben::Encode;
use futures::channel::oneshot;

use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::AnnouncePeer;
use crate::server::request::Status;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use super::DhtGetPeers;

pub struct DhtAnnounce<'a> {
    inner: DhtGetPeers<'a>,
}

impl<'a> DhtAnnounce<'a> {
    pub fn new(
        info_hash: &NodeId,
        table: &mut RoutingTable,
        peer_tx: oneshot::Sender<Vec<SocketAddr>>,
        udp: &'a UdpSocket,
    ) -> Self {
        Self {
            inner: DhtGetPeers::new(info_hash, table, peer_tx, udp),
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
        log::trace!("Handle ANNOUNCE response");
        self.inner.handle_response(resp, addr, table, rpc, has_id);
    }

    pub fn failed(&mut self, id: &NodeId, addr: &SocketAddr) {
        self.inner.failed(id, addr);
    }

    pub async fn add_requests(
        &mut self,
        rpc: &mut RpcMgr,
        buf: &mut Vec<u8>,
        traversal_id: usize,
    ) -> bool {
        log::trace!("Add ANNOUNCE's GET_PEERS requests");

        let done = self.inner.add_requests(rpc, buf, traversal_id).await;
        if !done {
            return false;
        }

        log::trace!("Finished ANNOUNCE's GET_PEERS. Time to announce");

        let mut announce_count = 0;
        for n in &self.inner.traversal.nodes {
            if announce_count == Bucket::MAX_LEN {
                break;
            }

            if !n.status.contains(Status::ALIVE) {
                continue;
            }

            let txn_id = rpc.new_txn();
            let token = match rpc.tokens.get(&n.addr) {
                Some(t) => t,
                None => {
                    log::warn!("Token not found for {}", n.addr);
                    continue;
                }
            };

            let msg = AnnouncePeer {
                txn_id,
                id: &rpc.own_id,
                info_hash: &self.inner.traversal.target,
                port: 0,
                implied_port: true,
                token,
            };

            buf.clear();
            msg.encode(buf);

            match self.inner.traversal.udp.send_to(&buf, &n.addr).await {
                Ok(_) => {
                    log::debug!("Announced to {}", n.addr);
                    announce_count += 1;
                }
                Err(e) => {
                    log::warn!("Failed to announce to {}: {}", n.addr, e);
                }
            }
        }

        if announce_count == 0 {
            log::warn!("Couldn't announce to anyone");
        }

        true
    }
}
