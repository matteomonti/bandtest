use rand_distr::{Distribution, Poisson};

use serde::{Deserialize, Serialize};

use std::{
    cmp,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use talk::crypto::primitives::sign::{KeyPair, PublicKey, Signature};

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
    worker: u64,
    cursor: Arc<Mutex<u64>>,
    public: PublicKey,
    signatures: Arc<Vec<Signature>>,
) {
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
            println!("Should send {:?}", id);
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

    let tasks = (0..CLIENT_WORKERS)
        .map(|worker| {
            tokio::spawn(client_load(
                worker,
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

#[tokio::main]
async fn main() {
    client().await;
}
