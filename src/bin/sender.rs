use nix::sys::socket::{sendmmsg, MsgFlags, SendMmsgData, SockaddrStorage};
use tokio::{net::UdpSocket, time};
use tokio::io::{Error, ErrorKind, Result, Interest};

use std::time::Duration;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::io::IoSlice;

const BATCH_SIZE: usize = 100;
const NB_BATCHES: usize = 100_000;


async fn send_multiple(socket: &UdpSocket, addr: SocketAddr, bufs: &[[u8; 1024]]) -> Result<usize> {
    let dest: SockaddrStorage = addr.into();
    let buffers: Vec<SendMmsgData<_, _, _>> = bufs
        .iter()
        .map(|packet| SendMmsgData {
            iov: [IoSlice::new(packet)],
            cmsgs: &[],
            addr: Some(dest),
            _lt: Default::default(),
        })
        .collect();
    socket.writable().await?;
    let sent = socket
        .try_io(Interest::WRITABLE, || {
            let sock_fd = socket.as_raw_fd();
            let sent: usize = sendmmsg(sock_fd, &buffers, MsgFlags::MSG_DONTWAIT)
                .map_err(|err| {
                    if err == nix::errno::Errno::EWOULDBLOCK {
                        return Error::new(ErrorKind::WouldBlock, "sendmmsg would block");
                    }
                    Error::new(ErrorKind::Other, err)
                })?
                .into_iter()
                .sum();
            Ok(sent)
        })
        .unwrap_or(0);
    Ok(sent)
}

#[tokio::main]
async fn main() {
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let buffers: Vec<_> = (0..BATCH_SIZE).map(|_| [0u8; 1024]).collect();
    let destination = "172.31.11.87:1234".parse().unwrap();

    for count in 0..NB_BATCHES {
        send_multiple(&socket, destination, &buffers).await.unwrap();
    }
}
