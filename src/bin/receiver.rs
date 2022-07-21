use nix::sys::socket::{recvmmsg, MsgFlags, RecvMmsgData, SockaddrStorage};
use tokio::io::{Error, ErrorKind, Interest};
use tokio::net::UdpSocket;

use std::io::IoSliceMut;
use std::os::unix::io::AsRawFd;

const BATCH_SIZE: usize = 100;
const MSS: usize = 2048;

#[tokio::main]
async fn main() {
    let socket = UdpSocket::bind("0.0.0.0:1234").await.unwrap();

    let mut buf = vec![0_u8; MSS * BATCH_SIZE];

    let mut count = 0;

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

        count += packets.len();

        println!("{}", count);
    }
}
