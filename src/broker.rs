use crate::message::Message;

use talk::net::DatagramDispatcher;

use std::env;

pub async fn main() {
    let bind = env::var("BANDTEST_BROKER_ADDRESS").unwrap();

    let mut dispatcher = DatagramDispatcher::bind(bind, Default::default())
        .await
        .unwrap();

    loop {
        let (_, message) = dispatcher.receive().await;
        let message = bincode::deserialize::<Message>(message.as_slice()).unwrap();

        match message {
            Message::Request { id, .. } => {
                println!("Received request {}", id);
            }
            _ => unreachable!(),
        }
    }
}
