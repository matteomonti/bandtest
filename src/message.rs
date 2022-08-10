use serde::{Deserialize, Serialize};

use talk::crypto::primitives::sign::{PublicKey, Signature};

#[derive(Serialize, Deserialize)]
pub enum Message {
    Request {
        id: u64,
        public: PublicKey,
        signature: Signature,
        other_signature: Signature,
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
        public: PublicKey,
        signature: Signature,
        padding: [[u8; 8]; 25],
        more_padding: [u8; 20],
    },
    Completion {
        id: u64,
        padding: [[u8; 15]; 16],
        more_padding: [u8; 4],
    },
}
