#![allow(unused_imports)]
use anyhow::{Error, Ok};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
pub mod resp;
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(socket).await
        });
    }
}

async fn handle_connection(mut socket: TcpStream) {
    let mut buf = [0; 512];
}
