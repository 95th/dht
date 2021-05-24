use self::request::DhtGetPeers;
use crate::{
    contact::{CompactNodes, CompactNodesV6, ContactRef},
    id::NodeId,
    msg::recv::{Msg, Response},
    server::request::{DhtBootstrap, DhtTraversal},
    table::RoutingTable,
};
use ben::Parser;
use futures::{channel::mpsc, select, FutureExt, SinkExt, StreamExt};
use rpc::RpcMgr;
use slab::Slab;
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::{net::UdpSocket, time};

mod request;
mod rpc;

pub struct Dht {
    port: u16,
    router_nodes: Vec<SocketAddr>,
}

pub enum ClientRequest {
    Announce { info_hash: NodeId },
    GetPeers { info_hash: NodeId },
    Ping { id: NodeId, addr: SocketAddr },
    BootStrap { target: NodeId },
}

impl Dht {
    pub fn new(port: u16, router_nodes: Vec<SocketAddr>) -> Self {
        Self { port, router_nodes }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let udp = &UdpSocket::bind((Ipv4Addr::UNSPECIFIED, self.port)).await?;

        let id = NodeId::gen();
        let table = &mut RoutingTable::new(id, self.router_nodes);
        let rpc = &mut RpcMgr::new(id);
        let parser = &mut Parser::new();
        let running = &mut Slab::new();

        let recv_buf: &mut [u8] = &mut [0; 1024];
        let send_buf = &mut Vec::with_capacity(1024);

        let mut prune_txn = time::interval(Duration::from_secs(1));
        let mut table_refresh = time::interval(Duration::from_secs(60));
        let mut check_running = time::interval(Duration::from_secs(5));

        let (mut tx, mut rx) = mpsc::channel::<ClientRequest>(200);

        // Bootstrap on ourselves
        tx.send(ClientRequest::BootStrap { target: id })
            .await
            .unwrap();

        let info_hash = NodeId::from_hex(b"d04480dfa670f72f591439b51a9f82dcc58711b5").unwrap();
        tx.send(ClientRequest::GetPeers { info_hash })
            .await
            .unwrap();

        loop {
            select! {
                // Clear timed-out transactions
                _ = prune_txn.tick().fuse() => rpc.prune(table, running),

                // Refresh table buckets
                _ = table_refresh.tick().fuse() => {
                    if let Some(refresh) = table.next_refresh2() {
                        log::trace!("Time to refresh the routing table");
                        tx.send(refresh).await.unwrap();
                    }
                }

                // Check running traversal and remove completed ones.
                _ = check_running.tick().fuse() => {
                    for (t_id, t) in running.iter_mut() {
                        t.invoke(rpc, udp, send_buf, t_id).await;
                    }

                    let before = running.len();
                    if before == 0 {
                        continue;
                    }

                    running.retain(|_id, t| !t.is_done());
                    log::trace!("Prune traversals, before: {}, after: {}", before, running.len());
                }

                // Listen for response
                resp = udp.recv_from(recv_buf).fuse() => {
                    let (n, addr) = match resp {
                        Ok(x) => x,
                        Err(e) => {
                            log::warn!("Error: {}", e);
                            continue;
                        },
                    };

                    log::debug!("Got {} bytes from {}", n, addr);

                    let msg = match parser.parse::<Msg>(&recv_buf[..n]) {
                        Ok(x) => x,
                        Err(e) => {
                            log::warn!("Error parsing message from {}: {}", addr, e);
                            continue;
                        }
                    };

                   rpc.handle_response(msg, addr, table, udp, running, send_buf).await;
                },

                // Send requests
                request = rx.next() => {
                    let request = match request {
                        Some(x) => x,

                        // The channel is closed
                        None => break,
                    };

                    let traversal_id = match request {
                        ClientRequest::GetPeers { info_hash } => {
                            let gp = DhtGetPeers::new(&info_hash, table);
                            running.insert(DhtTraversal::GetPeers(gp))
                        },
                        ClientRequest::BootStrap { target } => {
                            let b = DhtBootstrap::new(&target, table);
                            running.insert(DhtTraversal::Bootstrap(b))
                        },
                        _ => {
                            continue;
                        }
                    };

                    let t = running.get_mut(traversal_id).unwrap();
                    t.invoke(rpc, udp, send_buf, traversal_id).await;
                },
                complete => break,
            }
        }
        Ok(())
    }
}

impl RoutingTable {
    fn read_nodes_with<F>(&mut self, response: &Response, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(&ContactRef),
    {
        if let Some(nodes) = response.body.get_bytes("nodes") {
            for c in CompactNodes::new(nodes)? {
                self.add_contact(&c);
                f(&c);
            }
        }

        if let Some(nodes6) = response.body.get_bytes("nodes6") {
            for c in CompactNodesV6::new(nodes6)? {
                self.add_contact(&c);
                f(&c);
            }
        }

        Ok(())
    }
}
