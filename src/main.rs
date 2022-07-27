use serde::{Deserialize, Serialize};

use talk::crypto::primitives::sign::{KeyPair, PublicKey, Signature};

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

#[tokio::main]
async fn main() {
    let id = 44;
    let keypair = KeyPair::random();
    let public = keypair.public();
    let signature = keypair.sign_raw(&44u64).unwrap();

    println!(
        "Request: {}",
        bincode::serialize(&Message::Request {
            id,
            public,
            signature,
            padding: Default::default(),
            more_padding: Default::default()
        })
        .unwrap()
        .len()
    );

    println!(
        "Inclusion: {}",
        bincode::serialize(&Message::Inclusion {
            id,
            padding: Default::default(),
            more_padding: Default::default()
        })
        .unwrap()
        .len()
    );

    println!(
        "Reduction: {}",
        bincode::serialize(&Message::Reduction {
            id,
            padding: Default::default(),
            more_padding: Default::default()
        })
        .unwrap()
        .len()
    );

    println!(
        "Completion: {}",
        bincode::serialize(&Message::Completion {
            id,
            padding: Default::default(),
            more_padding: Default::default()
        })
        .unwrap()
        .len()
    );
}
