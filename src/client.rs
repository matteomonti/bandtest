use crate::{
    message::Message, CLIENT_POLLING, CLIENT_RATE, CLIENT_WORKERS, MESSAGES, SIGNATURE_MODULO,
};

use rand_distr::{Distribution, Poisson};

use std::{
    cmp, env,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use talk::{
    crypto::primitives::sign::{KeyPair, PublicKey, Signature},
    net::{DatagramDispatcher, DatagramDispatcherSettings, DatagramReceiver, DatagramSender},
};

use tokio::time;

const RATE_PER_CLIENT_WORKER: f64 = CLIENT_RATE / (CLIENT_WORKERS as f64);

async fn load(
    index: usize,
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
            println!("[{}] Sending request {}", index, id);

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

async fn react(index: usize, sender: Arc<DatagramSender>, mut receiver: DatagramReceiver) {
    loop {
        let (source, message) = receiver.receive().await;
        let message = bincode::deserialize::<Message>(message.as_slice()).unwrap();

        match message {
            Message::Inclusion { id, .. } => {
                println!("[{}] Received inclusion {}", index, id);

                let message = Message::Reduction {
                    id,
                    padding: Default::default(),
                    more_padding: Default::default(),
                };

                let message = bincode::serialize(&message).unwrap();
                sender.send(source, message).await;
            }
            Message::Completion { .. } => {
                unreachable!();
            }
            _ => unreachable!(),
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

    for (index, sender) in senders.iter().enumerate() {
        tokio::spawn(load(
            index,
            sender.clone(),
            cursor.clone(),
            public.clone(),
            signatures.clone(),
        ));
    }

    for (index, (sender, receiver)) in senders.into_iter().zip(receivers.into_iter()).enumerate() {
        tokio::spawn(react(index, sender, receiver));
    }

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
