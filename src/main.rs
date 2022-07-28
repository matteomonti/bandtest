use rand_distr::{Distribution, Poisson};

use serde::{Deserialize, Serialize};

use std::{
    cmp, env,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use talk::{
    crypto::primitives::sign::{KeyPair, PublicKey, Signature},
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramSender},
};

use tokio::time;

const MESSAGES: u64 = 128; // 33554432;
const SIGNATURE_MODULO: u64 = 1024;

const CLIENT_RATE: f64 = 1.;
const CLIENT_WORKERS: u64 = 32;
const CLIENT_POLLING: Duration = Duration::from_millis(1000);

const RATE_PER_CLIENT_WORKER: f64 = CLIENT_RATE / (CLIENT_WORKERS as f64);

#[derive(Serialize, Deserialize)]
enum Message {
    Request {
        id: u64,
        public: PublicKey,
        signature: Signature,
        padding: [[u8; 8]; 25],
        more_padding: [u8; 12],
    },
    Inclusion {
        id: u64,
        padding: [[u8; 32]; 25],
        more_padding: [u8; 20],
    },
    Reduction {
        id: u64,
        padding: [[u8; 8]; 25],
        more_padding: [u8; 20],
    },
    Completion {
        id: u64,
        padding: [[u8; 15]; 16],
        more_padding: [u8; 4],
    },
}

async fn client_load(
    _worker: u64,
    sender: Arc<DatagramSender>,
    cursor: Arc<Mutex<u64>>,
    public: PublicKey,
    signatures: Arc<Vec<Signature>>,
) {
    let destination = env::var("BROKER_ADDRESS").unwrap();
    let destination = destination.parse().unwrap();

    let mut last_wake = Instant::now();

    loop {
        time::sleep(CLIENT_POLLING).await;

        let wake = Instant::now();
        let elapsed = wake - last_wake;
        last_wake = wake;

        let range = {
            let mut cursor = cursor.lock().unwrap();

            if *cursor == MESSAGES {
                return;
            }

            let expected = RATE_PER_CLIENT_WORKER * elapsed.as_secs_f64();
            let poisson = Poisson::new(expected).unwrap();
            let progress = poisson.sample(&mut rand::thread_rng()) as u64;

            let start = *cursor;
            let stop = cmp::min(*cursor + progress, MESSAGES);

            *cursor = stop;

            start..stop
        };

        for id in range {
            let message = Message::Request {
                id,
                public,
                signature: signatures
                    .get((id % SIGNATURE_MODULO) as usize)
                    .unwrap()
                    .clone(),
                padding: Default::default(),
                more_padding: Default::default(),
            };

            let message = bincode::serialize(&message).unwrap();
            sender.send(destination, message).await;
        }
    }
}

async fn client() {
    let keypair = KeyPair::random();
    let public = keypair.public();

    let signatures = (0..SIGNATURE_MODULO)
        .map(|modulo| keypair.sign_raw(&modulo).unwrap())
        .collect::<Vec<_>>();

    let signatures = Arc::new(signatures);
    let cursor = Arc::new(Mutex::new(0));

    let mut senders = Vec::new();
    let mut receivers = Vec::new();

    for _ in 0..CLIENT_WORKERS {
        let dispatcher = DatagramDispatcher::bind(
            "0.0.0.0:0",
            DatagramDispatcherSettings {
                workers: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let (sender, receiver) = dispatcher.split();
        let sender = Arc::new(sender);

        senders.push(sender);
        receivers.push(receiver);
    }

    let tasks = senders
        .iter()
        .enumerate()
        .map(|(worker, sender)| {
            tokio::spawn(client_load(
                worker as u64,
                sender.clone(),
                cursor.clone(),
                public.clone(),
                signatures.clone(),
            ))
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.await.unwrap();
    }
}

async fn broker() {
    let bind = env::var("BANDTEST_BROKER_ADDRESS").unwrap();

    let mut dispatcher = DatagramDispatcher::bind(bind, Default::default())
        .await
        .unwrap();

    for index in 0u64.. {
        let _ = dispatcher.receive().await;
        println!("Received {} messages", index);
    }
}

#[tokio::main]
async fn main() {
    let role = env::var("BANDTEST_ROLE").unwrap();
    if role == "CLIENT" {
        client().await;
    } else {
        broker().await;
    }
}
