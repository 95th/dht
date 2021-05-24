use crate::{
    contact::{CompactNodes, CompactNodesV6, ContactRef},
    id::NodeId,
    msg::{
        recv::{ErrorResponse, Msg, Query, Response},
        send::{AnnouncePeer, FindNode, GetPeers, Ping},
        TxnId,
    },
    table::RoutingTable,
};
use ben::{Encode, Parser};
use futures::{channel::mpsc, select, FutureExt, SinkExt, StreamExt};
use rpc::RpcMgr;
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

impl ClientRequest {
    fn write(
        &self,
        table: &mut RoutingTable,
        rpc: &mut RpcMgr,
        buf: &mut Vec<u8>,
    ) -> Option<(TxnId, SocketAddr)> {
        use ClientRequest as Req;

        let addr;
        let txn_id = rpc.new_txn();
        buf.clear();

        match self {
            Req::Announce { info_hash } => {
                addr = table.pick_closest(info_hash)?;
                let token = rpc.get_token(&addr)?;

                let msg = AnnouncePeer {
                    id: &rpc.own_id,
                    info_hash,
                    port: 0,
                    implied_port: true,
                    txn_id,
                    token,
                };

                msg.encode(buf);
                log::debug!("Sending {:?}", msg);
            }

            Req::GetPeers { info_hash } => {
                addr = table.pick_closest(info_hash)?;

                let msg = GetPeers {
                    txn_id,
                    id: &rpc.own_id,
                    info_hash,
                };

                msg.encode(buf);
                log::debug!("Sending {:?}", msg);
            }

            Req::Ping {
                id,
                addr: target_addr,
            } => {
                addr = *target_addr;
                let msg = Ping { txn_id, id };
                msg.encode(buf);
                log::debug!("Sending {:?}", msg);
            }

            Req::BootStrap { target } => {
                addr = table.pick_closest(target)?;

                let msg = FindNode {
                    target,
                    id: &rpc.own_id,
                    txn_id,
                };
                msg.encode(buf);
                log::debug!("Sending {:?}", msg);
            }
        }

        Some((txn_id, addr))
    }
}

impl Dht {
    pub fn new(port: u16, router_nodes: Vec<SocketAddr>) -> Self {
        Self { port, router_nodes }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, self.port)).await?;

        let id = NodeId::gen();
        let table = &mut RoutingTable::new(id, self.router_nodes);
        let rpc = &mut RpcMgr::new(id);
        let parser = &mut Parser::new();

        let recv_buf: &mut [u8] = &mut [0; 1024];
        let send_buf = &mut Vec::with_capacity(1024);

        let mut txn_prune = time::interval(Duration::from_secs(1));
        let mut table_refresh = time::interval(Duration::from_secs(60));

        let (mut tx, mut rx) = mpsc::channel::<ClientRequest>(200);

        // Bootstrap on ourselves
        tx.send(ClientRequest::BootStrap { target: id }).await.unwrap();

        loop {
            select! {
                // Clear timed-out transactions
                _ = txn_prune.tick().fuse() => rpc.txns.prune(table),

                // Refresh table buckets
                _ = table_refresh.tick().fuse() => {
                    if let Some(refresh) = table.next_refresh2() {
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

                    if let Some(request) = rpc.handle_response(msg, addr, table) {
                        tx.send(request).await.unwrap();
                    }
                },

                // Send requests
                request = rx.next() => {
                    let request = match request {
                        Some(x) => x,

                        // The channel is closed
                        None => break,
                    };

                    let (txn_id, addr) = match request.write(table, rpc, send_buf) {
                        Some(x) => x,
                        None => continue,
                    };

                    match udp.send_to(&send_buf, addr).await {
                        Ok(_) => {
                            rpc.txns.insert(txn_id, &id, request);
                        },
                        Err(e) => {
                            log::warn!("Got error while sending request: {}", e);
                        }
                    }
                },
                complete => break,
            }
        }
        Ok(())
    }

    // pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
    //     let target = self.own_id;
    //     self.refresh(&target).await?;
    //     Ok(())
    // }

    // pub async fn announce(&mut self, info_hash: &NodeId) -> anyhow::Result<Vec<SocketAddr>> {
    //     log::debug!("Start announce for {:?}", info_hash);
    //     let mut req = AnnounceRequest::new(info_hash, &self.own_id, &mut self.table);

    //     loop {
    //         // refresh the table
    //         if let Some(refresh) = self.table.next_refresh() {
    //             match &refresh {
    //                 Refresh::Single(id, addr) => self.submit_ping(id, addr),
    //                 Refresh::Full(id) => self.submit_refresh(id),
    //             }
    //         }

    //         // Housekeep running requests
    //         self.check_running().await?;

    //         req.prune(&mut self.table);

    //         if req.invoke(&mut self.rpc).await? {
    //             return Ok(req.get_peers());
    //         }

    //         // Wait for socket response
    //         self.recv_response(Duration::from_secs(1), &mut req).await?;
    //     }
    // }

    // async fn check_running(&mut self) -> anyhow::Result<()> {
    //     let mut i = 0;
    //     while let Some(t) = self.running.get_mut(i) {
    //         t.prune(&mut self.table);

    //         if t.invoke(&mut self.rpc).await? {
    //             self.running.swap_remove(i);
    //         } else {
    //             i += 1;
    //         }
    //     }

    //     Ok(())
    // }

    // fn submit_refresh(&mut self, target: &NodeId) {
    //     let request = DhtRequest::new_bootstrap(target, &self.own_id, &mut self.table);
    //     self.running.push(request);
    // }

    // fn submit_ping(&mut self, id: &NodeId, addr: &SocketAddr) {
    //     let request = DhtRequest::new_ping(&self.own_id, id, addr);
    //     self.running.push(request);
    // }

    // async fn refresh(&mut self, target: &NodeId) -> anyhow::Result<()> {
    //     let mut request = DhtRequest::new_bootstrap(target, &self.own_id, &mut self.table);

    //     loop {
    //         if request.invoke(&mut self.rpc).await? {
    //             break;
    //         }

    //         request.prune(&mut self.table);

    //         let (msg, addr) = match self.rpc.recv_timeout(Duration::from_secs(1)).await? {
    //             Some(x) => x,
    //             None => continue,
    //         };

    //         if let Msg::Response(resp) = msg {
    //             request.handle_reply(&resp, &addr, &mut self.table).await;
    //         }
    //     }

    //     log::debug!(
    //         "Table size:: live: {}, extra: {}",
    //         self.table.len(),
    //         self.table.len_extra()
    //     );

    //     Ok(())
    // }

    // pub fn submit_get_peers(&mut self, info_hash: &NodeId) {
    //     let request = DhtRequest::new_get_peers(info_hash, &self.own_id, &mut self.table);
    //     self.running.push(request);
    // }

    // pub fn submit_announce(&mut self, info_hash: &NodeId) {
    //     let request = DhtRequest::new_announce(info_hash, &self.own_id, &mut self.table);
    //     self.running.push(request);
    // }

    // async fn recv_response(
    //     &mut self,
    //     timeout: Duration,
    //     req: &mut AnnounceRequest,
    // ) -> anyhow::Result<()> {
    //     let (msg, addr) = match self.rpc.recv_timeout(timeout).await? {
    //         Some(x) => x,
    //         None => return Ok(()),
    //     };

    //     match msg {
    //         Msg::Response(resp) => {
    //             if req.handle_reply(&resp, &addr, &mut self.table) {
    //                 return Ok(());
    //             }

    //             for t in &mut self.running {
    //                 if t.handle_reply(&resp, &addr, &mut self.table).await {
    //                     break;
    //                 }
    //             }
    //         }
    //         Msg::Query(query) => self.table.handle_query(&query),
    //         Msg::Error(err) => self.table.handle_error(&err),
    //     }

    //     Ok(())
    // }
}

impl RoutingTable {
    fn handle_query(&mut self, query: &Query) {
        log::debug!("Got query request: {:#?}", query);
    }

    fn handle_error(&mut self, err: &ErrorResponse) {
        log::debug!("Got query request: {:#?}", err);
    }

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
