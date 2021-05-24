use dht::Dht;
use std::net::ToSocketAddrs;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut dht_routers = vec![];
    dht_routers.extend("dht.libtorrent.org:25401".to_socket_addrs()?);

    let dht = Dht::new(6881, dht_routers.clone());
    dht.run().await?;
    // dht.bootstrap().await?;

    // let info_hash = NodeId::from_hex(b"d04480dfa670f72f591439b51a9f82dcc58711b5").unwrap();

    // let peers = dht.announce(&info_hash).await.unwrap();
    // println!("Got peers: {:?}", peers);

    Ok(())
}
