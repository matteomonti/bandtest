use crate::{
    message::Message, CLIENT_POLLING, CLIENT_WORKERS, MESSAGES, RATE_PER_CLIENT_WORKER,
    SIGNATURE_MODULO,
};

use rand_distr::{Distribution, Poisson};

use std::{
    cmp, env,
    sync::{Arc, Mutex},
    time::Instant,
};

use talk::{
    crypto::primitives::sign::{KeyPair, PublicKey, Signature},
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramSender},
};

use tokio::time;

async fn client_load(
    _worker: u64,
    sender: Arc<DatagramSender>,
    cursor: Arc<Mutex<u64>>,
    public: PublicKey,
    signatures: Arc<Vec<Signature>>,
) {
    let destination = env::var("BANDTEST_BROKER_ADDRESS").unwrap();
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

pub async fn main() {
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
