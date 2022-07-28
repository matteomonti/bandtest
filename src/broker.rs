use crate::{
    message::Message, BROKER_BROADCAST_POLLING, BROKER_COMPLETION_BROADCAST_DURATION,
    BROKER_COMPLETION_DELAY, BROKER_FLUSH_INTERVAL, BROKER_HANDLERS,
    BROKER_INCLUSION_BROADCAST_DURATION, BROKER_REDUCTION_TIMEOUT, MESSAGES, SIGNATURE_MODULO,
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pending,
    Timely,
    Late,
}

struct StatusTable {
    status: Vec<Status>,
    timely: usize,
    late: usize,
}

async fn handle(
    mut message_outlet: MpscReceiver<(SocketAddr, Vec<u8>)>,
    pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>,
    inclusion_times: Arc<Mutex<Vec<Option<Instant>>>>,
    status_table: Arc<Mutex<StatusTable>>,
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
                signature
                    .verify_raw(public, &(id % SIGNATURE_MODULO))
                    .unwrap();
                // println!("Received request {}", id);
                pool.lock().unwrap().push((id, source));
            }
            Message::Reduction { id, .. } => {
                let reduction_delay =
                    match inclusion_times.lock().unwrap().get(id as usize).unwrap() {
                        Some(inclusion_time) => inclusion_time.elapsed(),
                        None => Duration::ZERO, // Have not finished broadcasting the inclusions yet
                    };

                let mut status_table = status_table.lock().unwrap();
                let status = *status_table.status.get(id as usize).unwrap();

                if status == Status::Pending {
                    if reduction_delay < BROKER_REDUCTION_TIMEOUT {
                        // println!("Received timely reduction {}", id);
                        *status_table.status.get_mut(id as usize).unwrap() = Status::Timely;
                        status_table.timely += 1;
                    } else {
                        // println!("Received late reduction {}", id);
                        *status_table.status.get_mut(id as usize).unwrap() = Status::Late;
                        status_table.late += 1;
                    };
                }
            }
            _ => unreachable!(),
        }
    }
}

async fn receive(
    mut receiver: DatagramReceiver,
    pool: Arc<Mutex<Vec<(u64, SocketAddr)>>>,
    inclusion_times: Arc<Mutex<Vec<Option<Instant>>>>,
    status_table: Arc<Mutex<StatusTable>>,
) {
    let mut message_inlets = Vec::new();
    let mut message_outlets = Vec::new();

    for _ in 0..BROKER_HANDLERS {
        let (inlet, outlet) = mpsc::channel(1024);

        message_inlets.push(inlet);
        message_outlets.push(outlet);
    }

    for message_outlet in message_outlets {
        tokio::spawn(handle(
            message_outlet,
            pool.clone(),
            inclusion_times.clone(),
            status_table.clone(),
        ));
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
            // println!("Sending inclusion {}", id);

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

    time::sleep(BROKER_COMPLETION_DELAY).await;

    let start = Instant::now();
    let mut sent = 0;

    while sent < batch.len() {
        time::sleep(BROKER_BROADCAST_POLLING).await;

        let target = ((start.elapsed().as_secs_f64()
            / BROKER_COMPLETION_BROADCAST_DURATION.as_secs_f64())
            * (batch.len() as f64)) as usize;

        let target = cmp::min(target, batch.len());

        for (id, address) in &batch[sent..target] {
            // println!("Sending completion {}", id);

            let message = Message::Completion {
                id: *id,
                padding: Default::default(),
                more_padding: Default::default(),
            };

            let message = bincode::serialize(&message).unwrap();
            sender.send(*address, message).await;
        }

        sent = target;
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

    let status_table = StatusTable {
        status: vec![Status::Pending; MESSAGES as usize],
        timely: 0,
        late: 0,
    };

    let status_table = Arc::new(Mutex::new(status_table));

    tokio::spawn(receive(
        receiver,
        pool.clone(),
        inclusion_times.clone(),
        status_table.clone(),
    ));

    tokio::spawn(flush(pool, sender, inclusion_times));

    loop {
        {
            let status_table = status_table.lock().unwrap();

            println!(
                "Pending: {} \t Timely: {} \t Late: {}",
                MESSAGES - status_table.timely as u64 - status_table.late as u64,
                status_table.timely,
                status_table.late
            );
        }

        time::sleep(Duration::from_secs(1)).await;
    }
}
