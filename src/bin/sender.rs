use nix::sys::socket::{sendmmsg, MsgFlags, SendMmsgData, SockaddrStorage};
use tokio::{
    io::{Error, ErrorKind, Interest, Result},
    net::UdpSocket,
    time,
};

use std::{io::IoSlice, net::SocketAddr, os::unix::io::AsRawFd, time::Duration};

const BATCH_SIZE: usize = 1000;
const NB_BATCHES: usize = 2000;

async fn send_multiple(socket: &UdpSocket, addr: SocketAddr, bufs: &[[u8; 288]]) -> Result<usize> {
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
                .take_while(|sent_bytes| *sent_bytes > 0)
                .count();

            assert_eq!(sent, bufs.len());

            Ok(sent)
        })
        .unwrap_or(0);
    Ok(sent)
}

async fn spam(port: u16) {
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let buffers: Vec<_> = (0..BATCH_SIZE).map(|_| [0u8; 288]).collect();
    let destination = format!("172.31.11.87:{}", port).parse().unwrap();

    for count in 0..NB_BATCHES {
        println!("Sending {}-th mmmmsg", count);
        send_multiple(&socket, destination, &buffers).await.unwrap();

        time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::main]
async fn main() {
    let tasks = (1234..1244)
        .into_iter()
        .map(|port| tokio::spawn(spam(port)))
        .collect::<Vec<_>>();

    for task in tasks {
        task.await.unwrap();
    }
}
