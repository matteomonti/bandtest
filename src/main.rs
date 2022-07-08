use atomic_counter::{AtomicCounter, RelaxedCounter};

use doomstack::{here, Doom, ResultExt, Top};

use rand::prelude::*;

use std::{sync::Arc, time::Duration};

use talk::{
    crypto::{Identity, KeyCard, KeyChain},
    link::rendezvous::{Client, Connector, Listener, Server, ServerSettings},
    net::{Connector as NetConnector, Listener as NetListener, SecureConnection},
};

use tokio::time;

const RENDEZVOUS: &str = "172.31.36.16:9000";

const NODES: usize = 2;
const WORKERS: usize = 1;

#[derive(Doom)]
enum BandError {
    #[doom(description("Connect failed"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

#[tokio::main]
async fn main() {
    node().await;
}

#[allow(dead_code)]
async fn rendezvous() {
    let _rendezvous_server = Server::new(
        RENDEZVOUS,
        ServerSettings {
            shard_sizes: vec![NODES],
        },
    )
    .await
    .unwrap();

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}

#[allow(dead_code)]
async fn node() {
    let keychain = KeyChain::random();
    let rendezvous_client = Client::new(RENDEZVOUS, Default::default());

    println!("Publishing card..");

    rendezvous_client
        .publish_card(keychain.keycard(), Some(0))
        .await
        .unwrap();

    println!("Waiting for nodes..");

    let mut nodes = loop {
        if let Ok(nodes) = rendezvous_client.get_shard(0).await {
            break nodes;
        }
        time::sleep(Duration::from_millis(100)).await;
    };

    nodes.sort();

    let first = nodes.first().unwrap().clone();

    if first == keychain.keycard() {
        server(keychain).await;
    } else {
        client(keychain, first).await;
    }
}

async fn server(keychain: KeyChain) {
    println!("Running as server..");

    let mut listener = Listener::new(RENDEZVOUS, keychain, Default::default()).await;
    let counter = Arc::new(RelaxedCounter::new(0));

    {
        let counter = counter.clone();
        let mut last = 0;

        tokio::spawn(async move {
            loop {
                let counter = counter.get();
                println!("Received {} batches ({} batches / s)", counter, (counter - last));
                last = counter;

                time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    loop {
        let (_, connection) = listener.accept().await.unwrap();
        let counter = counter.clone();

        tokio::spawn(async move {
            if serve(connection).await.is_ok() {
                counter.inc();
            }
        });
    }
}

async fn serve(mut connection: SecureConnection) -> Result<(), Top<BandError>> {
    let buffer = connection
        .receive::<Vec<u8>>()
        .await
        .pot(BandError::ConnectionError, here!())?;

    connection
        .send(&(buffer.len() as u64))
        .await
        .pot(BandError::ConnectionError, here!())?;

    Ok(())
}

async fn client(keychain: KeyChain, server: KeyCard) {
    println!("Running as client..");

    time::sleep(Duration::from_secs(1)).await;

    let connector = Connector::new(RENDEZVOUS, keychain, Default::default());
    let connector = Arc::new(connector);

    for _ in 0..WORKERS {
        let connector = connector.clone();
        let server = server.identity();

        tokio::spawn(async move {
            loop {
                if let Err(error) = ping(connector.as_ref(), server).await {
                    println!("{:?}", error);
                }
            }
        });
    }

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn ping(connector: &Connector, server: Identity) -> Result<(), Top<BandError>> {
    let mut connection = connector
        .connect(server)
        .await
        .pot(BandError::ConnectFailed, here!())?;

    let buffer = (0..1048576).map(|_| random()).collect::<Vec<u8>>();

    connection
        .send(&buffer)
        .await
        .pot(BandError::ConnectionError, here!())?;

    let len = connection
        .receive::<u64>()
        .await
        .pot(BandError::ConnectionError, here!())?;

    assert_eq!(len as usize, buffer.len());

    Ok(())
}
