use rand::prelude::*;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
};

const BATCH_SIZE: usize = 1048576;

#[tokio::main]
async fn main() {
    tokio::spawn(server());
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
    let mut connection = TcpStream::connect("127.0.0.1:1234").await.unwrap();
    let buffer = (0..BATCH_SIZE).map(|_| random()).collect::<Vec<u8>>();

    for index in 0.. {
        println!("Sending batch {}", index);
        connection.write_u32(BATCH_SIZE as u32).await?;
        connection.write_all(buffer.as_slice()).await?;
        let size = connection.read_u32().await?;
        assert_eq!(size, BATCH_SIZE as u32);
    }

    Ok(())
}
