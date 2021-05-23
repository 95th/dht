use crate::id::NodeId;
use crate::msg::recv::{ErrorResponse, Msg, Query, Response};
use crate::table::{Refresh, RoutingTable};
use crate::{
    contact::{CompactNodes, CompactNodesV6, ContactRef},
    server::request::AnnounceRequest,
};
use request::DhtRequest;
use rpc::{RpcMgr, Transactions};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

mod request;
mod rpc;

pub struct Dht {
    rpc: RpcMgr,
    table: RoutingTable,
    own_id: NodeId,
    running: Vec<DhtRequest>,
}

impl Dht {
    pub async fn new(port: u16, router_nodes: Vec<SocketAddr>) -> anyhow::Result<Dht> {
        let addr = SocketAddr::from(([0u8; 4], port));
        let socket = UdpSocket::bind(addr).await?;
        let id = NodeId::gen();

        let server = Dht {
            rpc: RpcMgr::new(socket),
            table: RoutingTable::new(id, router_nodes),
            own_id: id,
            running: vec![],
        };

        Ok(server)
    }

    pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
        let target = self.own_id;
        self.refresh(&target).await?;
        Ok(())
    }

    pub async fn announce(&mut self, info_hash: &NodeId) -> anyhow::Result<Vec<SocketAddr>> {
        log::debug!("Start announce for {:?}", info_hash);
        let mut req = AnnounceRequest::new(info_hash, &self.own_id, &mut self.table);

        loop {
            // refresh the table
            if let Some(refresh) = self.table.next_refresh() {
                match &refresh {
                    Refresh::Single(id, addr) => self.submit_ping(id, addr),
                    Refresh::Full(id) => self.submit_refresh(id),
                }
            }

            // Housekeep running requests
            self.check_running().await?;

            req.prune(&mut self.table);

            if req.invoke(&mut self.rpc).await? {
                return Ok(req.get_peers());
            }

            // Wait for socket response
            self.recv_response(Duration::from_secs(1), &mut req).await?;
        }
    }

    async fn check_running(&mut self) -> anyhow::Result<()> {
        let mut i = 0;
        while let Some(t) = self.running.get_mut(i) {
            t.prune(&mut self.table);

            if t.invoke(&mut self.rpc).await? {
                self.running.swap_remove(i);
            } else {
                i += 1;
            }
        }

        Ok(())
    }

    fn submit_refresh(&mut self, target: &NodeId) {
        let request = DhtRequest::new_bootstrap(target, &self.own_id, &mut self.table);
        self.running.push(request);
    }

    fn submit_ping(&mut self, id: &NodeId, addr: &SocketAddr) {
        let request = DhtRequest::new_ping(&self.own_id, id, addr);
        self.running.push(request);
    }

    async fn refresh(&mut self, target: &NodeId) -> anyhow::Result<()> {
        let mut request = DhtRequest::new_bootstrap(target, &self.own_id, &mut self.table);

        loop {
            if request.invoke(&mut self.rpc).await? {
                break;
            }

            request.prune(&mut self.table);

            let (msg, addr) = match self.rpc.recv_timeout(Duration::from_secs(1)).await? {
                Some(x) => x,
                None => continue,
            };

            if let Msg::Response(resp) = msg {
                request.handle_reply(&resp, &addr, &mut self.table).await;
            }
        }

        log::debug!(
            "Table size:: live: {}, extra: {}",
            self.table.len(),
            self.table.len_extra()
        );

        Ok(())
    }

    pub fn submit_get_peers(&mut self, info_hash: &NodeId) {
        let request = DhtRequest::new_get_peers(info_hash, &self.own_id, &mut self.table);
        self.running.push(request);
    }

    pub fn submit_announce(&mut self, info_hash: &NodeId) {
        let request = DhtRequest::new_announce(info_hash, &self.own_id, &mut self.table);
        self.running.push(request);
    }

    async fn recv_response(
        &mut self,
        timeout: Duration,
        req: &mut AnnounceRequest,
    ) -> anyhow::Result<()> {
        let (msg, addr) = match self.rpc.recv_timeout(timeout).await? {
            Some(x) => x,
            None => return Ok(()),
        };

        match msg {
            Msg::Response(resp) => {
                if req.handle_reply(&resp, &addr, &mut self.table) {
                    return Ok(());
                }

                for t in &mut self.running {
                    if t.handle_reply(&resp, &addr, &mut self.table).await {
                        break;
                    }
                }
            }
            Msg::Query(query) => self.table.handle_query(&query),
            Msg::Error(err) => self.table.handle_error(&err),
        }

        Ok(())
    }
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
