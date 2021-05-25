use crate::id::NodeId;
use crate::{contact::ContactRef, msg::recv::Response, table::RoutingTable};
use std::net::SocketAddr;

mod announce;
mod bootstrap;
mod get_peers;
mod ping;
mod traversal;

pub use announce::DhtAnnounce;
pub use bootstrap::DhtBootstrap;
pub use get_peers::DhtGetPeers;
pub use ping::DhtPing;

use super::rpc::RpcMgr;

pub enum DhtTraversal<'a> {
    GetPeers(DhtGetPeers<'a>),
    Bootstrap(DhtBootstrap<'a>),
    Announce(DhtAnnounce<'a>),
    Ping(DhtPing<'a>),
}

impl<'a> DhtTraversal<'a> {
    pub async fn add_requests(
        &mut self,
        rpc: &mut RpcMgr,
        buf: &mut Vec<u8>,
        traversal_id: usize,
    ) -> bool {
        match self {
            DhtTraversal::GetPeers(x) => x.add_requests(rpc, buf, traversal_id).await,
            DhtTraversal::Bootstrap(x) => x.add_requests(rpc, buf, traversal_id).await,
            DhtTraversal::Announce(x) => x.add_requests(rpc, buf, traversal_id).await,
            DhtTraversal::Ping(x) => x.add_requests(rpc, buf).await,
        }
    }

    pub fn failed(&mut self, id: &NodeId, addr: &SocketAddr) {
        match self {
            DhtTraversal::GetPeers(x) => x.failed(id, addr),
            DhtTraversal::Bootstrap(x) => x.failed(id, addr),
            DhtTraversal::Announce(x) => x.failed(id, addr),
            DhtTraversal::Ping(x) => x.failed(id),
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
        match self {
            DhtTraversal::GetPeers(x) => x.handle_response(resp, addr, table, rpc, has_id),
            DhtTraversal::Bootstrap(x) => x.handle_response(resp, addr, table, has_id),
            DhtTraversal::Announce(x) => x.handle_response(resp, addr, table, rpc, has_id),
            DhtTraversal::Ping(x) => x.handle_response(resp, addr, table),
        }
    }
}

pub struct DhtNode {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub status: Status,
}

impl DhtNode {
    pub fn new(c: &ContactRef) -> Self {
        Self {
            id: *c.id,
            addr: c.addr,
            status: Status::INITIAL,
        }
    }
}

bitflags::bitflags! {
    pub struct Status: u8 {
        const INITIAL   = 0x01;
        const ALIVE     = 0x02;
        const FAILED    = 0x04;
        const NO_ID     = 0x08;
        const QUERIED   = 0x10;
    }
}
