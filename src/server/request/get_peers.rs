use crate::bucket::Bucket;
use crate::id::NodeId;
use crate::msg::recv::Response;
use crate::msg::send::GetPeers;
use crate::server::request::{DhtNode, Status};
use crate::server::RpcMgr;
use crate::table::RoutingTable;
use ben::{Decoder, Encode};
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct DhtGetPeers {
    info_hash: NodeId,
    branch_factor: u8,
    nodes: Vec<DhtNode>,
    peers: HashSet<SocketAddr>,
}

impl DhtGetPeers {
    pub fn new(info_hash: &NodeId, table: &RoutingTable) -> Self {
        let mut closest = Vec::with_capacity(Bucket::MAX_LEN);
        table.find_closest(info_hash, &mut closest, Bucket::MAX_LEN);

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
            info_hash: *info_hash,
            branch_factor: 3,
            nodes,
            peers: HashSet::new(),
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
        resp: &Response<'_, '_>,
        addr: &SocketAddr,
        table: &mut RoutingTable,
        rpc: &mut RpcMgr,
    ) {
        log::trace!("Handle GET_PEERS response");

        if let Some(node) = self.nodes.iter_mut().find(|node| &node.addr == addr) {
            node.status.insert(Status::ALIVE);
        }

        let result = table.read_nodes_with(resp, |c| {
            if !self.nodes.iter().any(|n| &n.id == c.id) {
                self.nodes.push(DhtNode::new(c));
            }
        });

        if let Err(e) = result {
            log::warn!("{}", e);
        }

        if let Some(token) = resp.body.get_bytes("token") {
            rpc.tokens.insert(*addr, token.to_vec());
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

        if let Some(peers) = resp.body.get_list("values") {
            let peers = peers.into_iter().flat_map(decode_peer);
            self.peers.extend(peers);
        }

        let target = &self.info_hash;
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
        log::trace!("Invoke GET_PEERS request");
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

            let msg = GetPeers {
                txn_id: rpc.new_txn(),
                info_hash: &self.info_hash,
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
