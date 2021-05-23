use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::AnnouncePeer;
use crate::server::request::GetPeersRequest;
use crate::server::request::Status;
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use std::net::SocketAddr;

pub struct AnnounceRequest {
    inner: GetPeersRequest,
}

impl AnnounceRequest {
    pub fn new(info_hash: &NodeId, own_id: &NodeId, table: &mut RoutingTable) -> Self {
        Self {
            inner: GetPeersRequest::new(info_hash, own_id, table),
        }
    }

    pub fn prune(&mut self, table: &mut RoutingTable) {
        self.inner.prune(table);
    }

    pub fn handle_reply(
        &mut self,
        resp: &Response<'_, '_>,
        addr: &SocketAddr,
        table: &mut RoutingTable,
    ) -> bool {
        self.inner.handle_reply(resp, addr, table)
    }

    pub async fn invoke(&mut self, rpc: &mut RpcMgr) -> anyhow::Result<bool> {
        if !self.inner.invoke(rpc).await {
            return Ok(false);
        }

        let mut announce_count = 0;
        for n in &self.inner.nodes {
            if announce_count == Bucket::MAX_LEN {
                break;
            }

            if !n.status.contains(Status::ALIVE) {
                continue;
            }

            let token = match self.inner.tokens.get(&n.addr) {
                Some(t) => t,
                None => continue,
            };

            let msg = AnnouncePeer {
                id: &self.inner.own_id,
                info_hash: &self.inner.info_hash,
                port: 0,
                implied_port: true,
                txn_id: rpc.next_id(),
                token,
            };

            rpc.send(&msg, &n.addr).await?;
            announce_count += 1;
            log::debug!("Announced to {}", n.addr);
        }

        Ok(true)
    }

    pub fn get_peers(self) -> Vec<SocketAddr> {
        self.inner.get_peers()
    }
}
