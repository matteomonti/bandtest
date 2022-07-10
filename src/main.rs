use rand::prelude::*;

use std::time::{Duration, Instant};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
};

const BATCH_SIZE: usize = 1048576;

#[tokio::main]
async fn main() {
    let _ = client().await;
}

async fn server() {
    let listener = TcpListener::bind("0.0.0.0:1234").await.unwrap();

    loop {
        let (connection, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            let _ = serve(connection).await;
        });
    }
}

async fn serve(mut connection: TcpStream) -> Result<()> {
    let mut buffer: Vec<u8> = Vec::new();

    loop {
        let size = connection.read_u32().await?;
        buffer.resize(size as usize, 0);
        connection.read_exact(buffer.as_mut_slice()).await?;
        connection.write_u32(size).await?;
    }
}

async fn client() -> Result<()> {
    let mut connection = TcpStream::connect("172.31.38.206:1234").await.unwrap();
    let buffer = (0..BATCH_SIZE).map(|_| random()).collect::<Vec<u8>>();

    let mut last_print = Instant::now();
    let mut last_value = 0;

    let mut speeds = Vec::new();

    for batch in 0.. {
        if last_print.elapsed() >= Duration::from_secs(1) {
            let speed = ((batch - last_value) as f64) / last_print.elapsed().as_secs_f64();
            speeds.push(speed);

            let average = statistical::mean(speeds.as_slice());

            let std = if speeds.len() > 1 {
                statistical::standard_deviation(speeds.as_slice(), None)
            } else {
                0.
            };

            println!("Speed: {} +- {} B/s", average, std);

            last_print = Instant::now();
            last_value = batch;
        }

        connection.write_u32(BATCH_SIZE as u32).await?;
        connection.write_all(buffer.as_slice()).await?;
        let size = connection.read_u32().await?;
        assert_eq!(size, BATCH_SIZE as u32);
    }

    Ok(())
}
