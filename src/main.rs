#![allow(unused_imports)]
use anyhow::{Error, Ok};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                let req = socket.read(&mut buf).await.unwrap();
                if req == 0 {
                    break;
                }
                socket.write_all(b"+PONG\r\n").await.unwrap();
            }
        });
    }
}
