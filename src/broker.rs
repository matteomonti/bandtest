use crate::{
    message::Message, BROKER_BROADCAST_POLLING, BROKER_FLUSH_INTERVAL, BROKER_HANDLERS,
    BROKER_INCLUSION_BROADCAST_DURATION, MESSAGES,
};

use std::{
    cmp, env, mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use talk::net::{DatagramDispatcher, DatagramReceiver, DatagramSender};

use tokio::{
    sync::mpsc::{self, Receiver as MpscReceiver},
    time,
};

async fn handle(
    mut message_outlet: MpscReceiver<(SocketAddr, Vec<u8>)>,
    pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>,
) {
    loop {
        let (source, message) = message_outlet.recv().await.unwrap();
        let message = bincode::deserialize::<Message>(message.as_slice()).unwrap();

        match message {
            Message::Request {
                id,
                public,
                signature,
                ..
            } => {
                signature.verify_raw(public, &id).unwrap();
                println!("Received request {}", id);
                pool.lock().unwrap().push((id, source));
            }
            Message::Reduction { id, .. } => {
                println!("Received reduction {}", id);
            }
            _ => todo!(),
        }
    }
}

async fn receive(
    mut receiver: DatagramReceiver,
    pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>,
    _inclusion_times: Arc<Mutex<Vec<Option<Instant>>>>,
) {
    let mut message_inlets = Vec::new();
    let mut message_outlets = Vec::new();

    for _ in 0..BROKER_HANDLERS {
        let (inlet, outlet) = mpsc::channel(1024);

        message_inlets.push(inlet);
        message_outlets.push(outlet);
    }

    for message_outlet in message_outlets {
        tokio::spawn(handle(message_outlet, pool.clone()));
    }

    for robin in 0.. {
        let datagram = receiver.receive().await;

        message_inlets
            .get((robin % BROKER_HANDLERS) as usize)
            .unwrap()
            .send(datagram)
            .await
            .unwrap();
    }
}

async fn flush(
    pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>,
    sender: Arc<DatagramSender>,
    inclusion_times: Arc<Mutex<Vec<Option<Instant>>>>,
) {
    loop {
        time::sleep(BROKER_FLUSH_INTERVAL).await;
        let flush: Vec<_> = mem::take(pool.lock().unwrap().as_mut());
        tokio::spawn(broadcast(flush, sender.clone(), inclusion_times.clone()));
    }
}

async fn broadcast(
    batch: Vec<(u64, SocketAddr)>,
    sender: Arc<DatagramSender>,
    inclusion_times: Arc<Mutex<Vec<Option<Instant>>>>,
) {
    if batch.is_empty() {
        return;
    }

    let start = Instant::now();
    let mut sent = 0;

    while sent < batch.len() {
        time::sleep(BROKER_BROADCAST_POLLING).await;

        let target = ((start.elapsed().as_secs_f64()
            / BROKER_INCLUSION_BROADCAST_DURATION.as_secs_f64())
            * (batch.len() as f64)) as usize;

        let target = cmp::min(target, batch.len());

        for (id, address) in &batch[sent..target] {
            println!("Sending inclusion {}", id);

            let message = Message::Inclusion {
                id: *id,
                padding: Default::default(),
                more_padding: Default::default(),
            };

            let message = bincode::serialize(&message).unwrap();
            sender.send(*address, message).await;
        }

        sent = target;
    }

    let inclusion_time = Instant::now();

    {
        let mut inclusion_times = inclusion_times.lock().unwrap();

        for (id, _) in batch.iter() {
            *inclusion_times.get_mut(*id as usize).unwrap() = Some(inclusion_time);
        }
    }
}

pub async fn main() {
    let bind = env::var("BANDTEST_BROKER_ADDRESS").unwrap();

    let dispatcher = DatagramDispatcher::bind(bind, Default::default())
        .await
        .unwrap();

    let (sender, receiver) = dispatcher.split();
    let sender = Arc::new(sender);

    let pool = Arc::new(Mutex::new(Vec::new()));
    let inclusion_times = Arc::new(Mutex::new(vec![None; MESSAGES as usize]));

    tokio::spawn(receive(receiver, pool.clone(), inclusion_times.clone()));
    tokio::spawn(flush(pool, sender, inclusion_times));

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
