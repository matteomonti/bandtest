mod broker;
mod client;
mod message;

use std::{env, time::Duration};

const MESSAGES: u64 = 128;
const SIGNATURE_MODULO: u64 = 1024;

const CLIENT_RATE: f64 = 1.;
const CLIENT_WORKERS: u64 = 32;
const CLIENT_POLLING: Duration = Duration::from_millis(10);

const BROKER_FLUSH_INTERVAL: Duration = Duration::from_millis(1000);
const BROKER_REDUCTION_TIMEOUT: Duration = Duration::from_millis(1000);
const BROKER_COMPLETION_DELAY: Duration = Duration::from_secs(10);
const BROKER_HANDLERS: u64 = 32;
const BROKER_INCLUSION_BROADCAST_DURATION: Duration = Duration::from_millis(500);
const BROKER_COMPLETION_BROADCAST_DURATION: Duration = Duration::from_millis(500);
const BROKER_BROADCAST_POLLING: Duration = Duration::from_millis(10);

#[tokio::main]
async fn main() {
    let role = env::var("BANDTEST_ROLE").unwrap();

    if role == "CLIENT" {
        client::main().await;
    } else if role == "BROKER" {
        broker::main().await;
    }
}
