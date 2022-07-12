use atomic_counter::{AtomicCounter, RelaxedCounter};

use doomstack::{here, Doom, ResultExt, Top};

use rand::prelude::*;

use std::{sync::Arc, time::Duration};

use talk::{
    crypto::{Identity, KeyCard, KeyChain},
    link::rendezvous::{Client, Connector, Listener, Server, ServerSettings},
    net::{Session, SessionConnector, SessionListener},
};

use tokio::time;

const RENDEZVOUS: &str = "172.31.5.210:9000";

const NODES: usize = 2;
const WORKERS: usize = 1;

const BATCH_SIZE: usize = 1048576;
const BATCHES_PER_SESSION: usize = 1;

const GRACE: usize = 5;

type Message = u32;

#[derive(Doom)]
enum BandError {
    #[doom(description("Connect failed"))]
    ConnectFailed,
    #[doom(description("Connection error"))]
    ConnectionError,
}

#[tokio::main]
async fn main() {
    if std::env::var("RENDEZVOUS").unwrap_or_default() != "" {
        println!("Running as rendezvous..");
        rendezvous().await
    } else {
        println!("Running as node..");
        node().await;
    }
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

    let listener = Listener::new(RENDEZVOUS, keychain, Default::default()).await;
    let mut listener = SessionListener::new(listener);

    let counter = Arc::new(RelaxedCounter::new(0));

    {
        let counter = counter.clone();

        tokio::spawn(async move {
            let mut grace = GRACE;

            let mut last = 0;
            let mut speeds = Vec::new();

            loop {
                let counter = counter.get();

                if counter > 0 {
                    let speed = counter - last;

                    if grace == 0 {
                        speeds.push(speed as f64);

                        let average = statistical::mean(speeds.as_slice());

                        let standard_deviation = if speeds.len() > 1 {
                            statistical::standard_deviation(speeds.as_slice(), None)
                        } else {
                            0.
                        };

                        println!(
                            "Received {} batches (instant: {} B / s) (average: {} ± {} B / s)",
                            counter, speed, average, standard_deviation
                        );
                    } else {
                        println!("Received {} batches (instant: {} B / s)", counter, speed,);
                        grace -= 1;
                    }

                    last = counter;
                }

                time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    loop {
        let (_, session) = listener.accept().await;
        let counter = counter.clone();

        tokio::spawn(async move {
            if let Err(error) = serve(session, counter.as_ref()).await {
                println!("{:?}", error);
            }
        });
    }
}

async fn serve(mut session: Session, counter: &RelaxedCounter) -> Result<(), Top<BandError>> {
    for _ in 0..BATCHES_PER_SESSION {
        let buffer = session
            .receive_raw::<Vec<Message>>()
            .await
            .pot(BandError::ConnectionError, here!())?;

        session
            .send_raw(&(buffer.len() as u64))
            .await
            .pot(BandError::ConnectionError, here!())?;

        counter.inc();
    }

    session.end();

    Ok(())
}

async fn client(keychain: KeyChain, server: KeyCard) {
    println!("Running as client..");

    time::sleep(Duration::from_secs(1)).await;

    let connector = Connector::new(RENDEZVOUS, keychain, Default::default());
    let connector = SessionConnector::new(connector);
    let connector = Arc::new(connector);

    let buffer = (0..BATCH_SIZE).map(|_| random()).collect::<Vec<Message>>();
    let buffer = Arc::new(buffer);

    for _ in 0..WORKERS {
        let connector = connector.clone();
        let server = server.identity();
        let buffer = buffer.clone();

        tokio::spawn(async move {
            loop {
                if let Err(error) = ping(connector.as_ref(), server, buffer.as_ref()).await {
                    println!("{:?}", error);
                }
            }
        });
    }

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}

async fn ping(
    connector: &SessionConnector,
    server: Identity,
    buffer: &Vec<Message>,
) -> Result<(), Top<BandError>> {
    let mut session = connector
        .connect(server)
        .await
        .pot(BandError::ConnectFailed, here!())?;

    for _ in 0..BATCHES_PER_SESSION {
        session
            .send_raw(&buffer)
            .await
            .pot(BandError::ConnectionError, here!())?;

        let len = session
            .receive_raw::<u64>()
            .await
            .pot(BandError::ConnectionError, here!())?;

        assert_eq!(len as usize, buffer.len());
    }

    session.end();

    Ok(())
}
