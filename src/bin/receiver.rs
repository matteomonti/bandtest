use atomic_counter::{AtomicCounter, RelaxedCounter};
use nix::sys::socket::{recvmmsg, MsgFlags, RecvMmsgData, SockaddrStorage};
use tokio::{
    io::{Error, ErrorKind, Interest},
    net::UdpSocket,
    time,
};

use std::{io::IoSliceMut, os::unix::io::AsRawFd, sync::Arc, time::Duration};

const BATCH_SIZE: usize = 100;
const MSS: usize = 2048;

async fn listener(port: u16, counter: Arc<RelaxedCounter>) {
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", port)).await.unwrap();

    let mut buf = vec![0_u8; MSS * BATCH_SIZE];

    loop {
        let bufs = buf.chunks_exact_mut(MSS);
        let mut recv_mesg_data: Vec<RecvMmsgData<_>> = bufs
            .map(|b| RecvMmsgData {
                iov: [IoSliceMut::new(&mut b[..])],
                cmsg_buffer: None,
            })
            .collect();

        socket.readable().await.unwrap();
        let msgs: Vec<_> = socket
            .try_io(Interest::READABLE, || {
                let msgs = recvmmsg(
                    socket.as_raw_fd(),
                    &mut recv_mesg_data,
                    MsgFlags::MSG_DONTWAIT,
                    None,
                )
                .map_err(|err| {
                    if err == nix::errno::Errno::EWOULDBLOCK {
                        return Error::new(ErrorKind::WouldBlock, "recvmmsg would block");
                    }
                    Error::new(ErrorKind::Other, err)
                })?
                .iter()
                .map(|msg| {
                    let addr: SockaddrStorage = msg.address.unwrap();
                    (msg.bytes, addr)
                })
                .collect();
                Ok(msgs)
            })
            .unwrap_or_default();

        let packets: Vec<_> = msgs
            .into_iter()
            .zip(buf.chunks_exact_mut(MSS))
            .map(|((nbytes, addr), buf)| {
                let packet = buf[..nbytes].to_vec();
                Some((packet, addr))
            })
            .collect();

        counter.add(packets.len());
    }
}

#[tokio::main]
async fn main() {
    let counter = Arc::new(RelaxedCounter::new(0));

    for port in 1234..1244 {
        let counter = counter.clone();
        tokio::spawn(listener(port, counter));
    }

    loop {
        println!("Received {} packets", counter.get());
        time::sleep(Duration::from_secs(1)).await;
    }
}
