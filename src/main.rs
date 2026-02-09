#![allow(unused_imports)]
use core::panic;
use std::{any, collections::btree_map::Values, env::args_os};

use anyhow::{Error, Ok};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

use crate::{database::db, handlers::{extract_command, get_handle, lpush_handle, lrange_handle, rpush_handle, set_handle}, resp::Value};
pub mod resp;
pub mod database;
pub mod handlers;


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let redisdb = db::new();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let redisdb = redisdb.clone();
        tokio::spawn(async move {
            handle_connection(socket, redisdb).await
        });
    }
}

async fn handle_connection(mut socket: TcpStream, redisdb: db) {
    let mut handler = resp::RespHandler::new(socket);

    loop {
        let value = handler.read_value().await.unwrap();
        println!("{:?}",value);
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            println!("{:?} {:?}", command, args);
            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args[0].clone(),
                "SET" => {
                    set_handle(&args, &redisdb).await.unwrap();
                    Value::SimpleString("OK".to_string())
                }
                "GET" => {
                    if let Some(value) = get_handle(&args, &redisdb).await {
                        Value::SimpleString(value)
                    } else {
                        Value::NullBulkString
                    }
                }
                "RPUSH" => {
                    let size = rpush_handle(&args, &redisdb).await.unwrap();
                    Value::Integer(size)
                }
                "LRANGE" => {
                    let range = lrange_handle(&args, &redisdb).await.unwrap();
                    range
                }
                "LPUSH" => {
                    let size = lpush_handle(&args, &redisdb).await.unwrap();
                    Value::Integer(size)
                }
                c => panic!("Cannot handle command {:?}",c)
            }
        }
        else {
            break;
        };
        handler.write_value(response).await.unwrap();
    }
}

