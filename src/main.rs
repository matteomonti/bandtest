mod broker;
mod client;
mod message;

use std::{env, time::Duration};

const MESSAGES: u64 = 128;
const SIGNATURE_MODULO: u64 = 1024;

const CLIENT_RATE: f64 = 1.;
const CLIENT_WORKERS: u64 = 32;
const CLIENT_POLLING: Duration = Duration::from_millis(1000);

const RATE_PER_CLIENT_WORKER: f64 = CLIENT_RATE / (CLIENT_WORKERS as f64);

#[tokio::main]
async fn main() {
    let role = env::var("BANDTEST_ROLE").unwrap();

    if role == "CLIENT" {
        client::main().await;
    } else if role == "BROKER" {
        broker::main().await;
    }
}
