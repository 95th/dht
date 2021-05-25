use dht::{id::NodeId, Dht};
use futures::join;
use std::net::ToSocketAddrs;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut dht_routers = vec![];
    dht_routers.extend("dht.libtorrent.org:25401".to_socket_addrs()?);

    let info_hash = NodeId::from_hex(b"d04480dfa670f72f591439b51a9f82dcc58711b5").unwrap();

    let mut dht = Dht::new(6881, dht_routers);
    let mut dht2 = dht.clone();
    let mut dht3 = dht.clone();

    join! {
        async {
            let p = dht.announce(info_hash).await.unwrap();
            log::info!("Got peers: {:?}", p);
        },
        async {
            let p = dht2.announce(info_hash).await.unwrap();
            log::info!("Got peers: {:?}", p);
        },
        async {
            let p = dht3.announce(info_hash).await.unwrap();
            log::info!("Got peers: {:?}", p);
        }
    };

    Ok(())
}
