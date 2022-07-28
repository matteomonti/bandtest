use crate::{
    message::Message, BROKER_BROADCAST_POLLING, BROKER_FLUSH_INTERVAL, BROKER_HANDLERS,
    BROKER_INCLUSION_BROADCAST_DURATION,
};

use std::{
    cmp, env, mem,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use talk::net::{DatagramDispatcher, DatagramReceiver};

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
            _ => todo!(),
        }
    }
}

async fn receive(mut receiver: DatagramReceiver, pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>) {
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

async fn flush(pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>) {
    loop {
        time::sleep(BROKER_FLUSH_INTERVAL).await;
        let flush: Vec<_> = mem::take(pool.lock().unwrap().as_mut());
        tokio::spawn(broadcast(flush));
    }
}

async fn broadcast(batch: Vec<(u64, SocketAddr)>) {
    if batch.is_empty() {
        return;
    }

    println!(
        "Broadcasting inclusions to {:?}",
        batch.iter().map(|(id, _)| id).copied().collect::<Vec<_>>()
    );

    let start = Instant::now();
    let mut sent = 0;

    while sent < batch.len() {
        time::sleep(BROKER_BROADCAST_POLLING).await;

        let target = ((start.elapsed().as_secs_f64()
            / BROKER_INCLUSION_BROADCAST_DURATION.as_secs_f64())
            * (batch.len() as f64)) as usize;

        let target = cmp::min(target, batch.len());

        for (id, address) in &batch[sent..target] {
            println!("Should send inclusion {} -> {:?}", id, address);
        }

        sent = target;
    }

    println!("Finished broadcasting inclusions in {:?}", start.elapsed());
}

pub async fn main() {
    let bind = env::var("BANDTEST_BROKER_ADDRESS").unwrap();

    let dispatcher = DatagramDispatcher::bind(bind, Default::default())
        .await
        .unwrap();

    let (_sender, receiver) = dispatcher.split();

    let pool = Arc::new(Mutex::new(Vec::new()));

    tokio::spawn(receive(receiver, pool.clone()));
    tokio::spawn(flush(pool));

    loop {
        time::sleep(Duration::from_secs(1)).await;
    }
}
