use crate::{
    contact::{CompactNodes, CompactNodesV6, ContactRef},
    id::NodeId,
    msg::recv::{Msg, Response},
    server::request::{DhtAnnounce, DhtBootstrap, DhtGetPeers, DhtPing, DhtTraversal},
    table::RoutingTable,
};
use ben::Parser;
use futures::{
    channel::{mpsc, oneshot},
    select, FutureExt, SinkExt, StreamExt,
};
use rpc::RpcMgr;
use slab::Slab;
use std::{
    net::{Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::{net::UdpSocket, time};

mod request;
mod rpc;

pub enum ClientRequest {
    Announce {
        info_hash: NodeId,
        peer_tx: oneshot::Sender<Vec<SocketAddr>>,
    },
    GetPeers {
        info_hash: NodeId,
        peer_tx: oneshot::Sender<Vec<SocketAddr>>,
    },
    Ping {
        id: NodeId,
        addr: SocketAddr,
    },
    BootStrap {
        target: NodeId,
    },
}

#[derive(Clone)]
pub struct Dht {
    tx: mpsc::Sender<ClientRequest>,
}

impl Dht {
    pub fn new(port: u16, router_nodes: Vec<SocketAddr>) -> Self {
        let (tx, rx) = mpsc::channel::<ClientRequest>(200);
        let dht = DhtServer {
            port,
            router_nodes,
            tx: tx.clone(),
            rx,
        };

        tokio::spawn(dht.run());
        Self { tx }
    }

    pub async fn get_peers(&mut self, info_hash: NodeId) -> anyhow::Result<Vec<SocketAddr>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ClientRequest::GetPeers {
                info_hash,
                peer_tx: tx,
            })
            .await?;

        Ok(rx.await?)
    }

    pub async fn announce(&mut self, info_hash: NodeId) -> anyhow::Result<Vec<SocketAddr>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ClientRequest::Announce {
                info_hash,
                peer_tx: tx,
            })
            .await?;

        Ok(rx.await?)
    }
}

struct DhtServer {
    port: u16,
    router_nodes: Vec<SocketAddr>,
    tx: mpsc::Sender<ClientRequest>,
    rx: mpsc::Receiver<ClientRequest>,
}

impl DhtServer {
    async fn run(self) {
        let udp = &match UdpSocket::bind((Ipv6Addr::UNSPECIFIED, self.port)).await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("Cannot open UDP socket: {}", e);
                return;
            }
        };

        let id = NodeId::gen();
        let table = &mut RoutingTable::new(id, self.router_nodes);
        let rpc = &mut RpcMgr::new(id);
        let parser = &mut Parser::new();
        let running = &mut Slab::new();

        let recv_buf: &mut [u8] = &mut [0; 1024];
        let send_buf = &mut Vec::with_capacity(1024);

        let mut prune_txn = time::interval(Duration::from_secs(1));
        let mut table_refresh = time::interval(Duration::from_secs(60));

        let mut tx = self.tx;
        let mut rx = self.rx;

        // Bootstrap on ourselves
        tx.send(ClientRequest::BootStrap { target: id })
            .await
            .unwrap();

        loop {
            select! {
                // Clear timed-out transactions
                _ = prune_txn.tick().fuse() => {
                    rpc.check_timed_out_txns(table, running, send_buf).await;
                }

                // Refresh table buckets
                _ = table_refresh.tick().fuse() => {
                    if let Some(refresh) = table.next_refresh() {
                        log::trace!("Time to refresh the routing table");
                        tx.send(refresh).await.unwrap();
                    }
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

                   rpc.handle_response(msg, addr, table, running, send_buf).await;
                },

                // Send requests
                request = rx.next() => {
                    let request = match request {
                        Some(x) => x,

                        // The channel is closed
                        None => break,
                    };

                    let traversal_id = match request {
                        ClientRequest::GetPeers { info_hash, peer_tx } => {
                            let x = DhtGetPeers::new(&info_hash, table, peer_tx, udp);
                            running.insert(DhtTraversal::GetPeers(x))
                        },
                        ClientRequest::BootStrap { target } => {
                            let x = DhtBootstrap::new(&target, table, udp);
                            running.insert(DhtTraversal::Bootstrap(x))
                        },
                        ClientRequest::Announce { info_hash, peer_tx } => {
                            let x = DhtAnnounce::new(&info_hash, table, peer_tx, udp);
                            running.insert(DhtTraversal::Announce(x))
                        }
                        ClientRequest::Ping { id, addr } => {
                            let x = DhtPing::new(&id, &addr, udp);
                            running.insert(DhtTraversal::Ping(x))
                        }
                    };

                    let t = &mut running[traversal_id];
                    let done = t.add_requests(rpc, send_buf, traversal_id).await;
                    if done {
                        running.remove(traversal_id);
                    }
                },
                complete => break,
            }
        }
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
