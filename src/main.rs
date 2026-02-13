#![allow(unused_imports)]
use core::panic;
use std::{any, collections::btree_map::Values, env::args_os};

use anyhow::{Error, Ok};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

use crate::{database::db, handlers::{blpop_handle, extract_command, get_handle, llen_handle, lpop_handle, lpush_handle, lrange_handle, rpush_handle, set_handle, type_handle, unpack_bulk_str, xadd_handle, xrange_handle, xread_block_handle, xread_handle}, resp::Value};
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
            let (command, vec_args) = extract_command(v).unwrap();
            let args = unpack_bulk_str(&vec_args).unwrap();
            println!("{:?} {:?}", command, args);
            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => Value::BulkString(args[0].clone()),
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
                    rpush_handle(&args, &redisdb).await.unwrap()
                }
                "LRANGE" => {
                    lrange_handle(&args, &redisdb).await.unwrap()
                }
                "LPUSH" => {
                    lpush_handle(&args, &redisdb).await.unwrap()
                }
                "LLEN" => {
                    llen_handle(&args, &redisdb).await.unwrap()
                }
                "LPOP" => {
                    lpop_handle(&args, &redisdb).await.unwrap()
                }
                "BLPOP" => {
                    blpop_handle(&args, &redisdb).await.unwrap()
                }
                "TYPE" => {
                    type_handle(&args, &redisdb).await.unwrap()
                }
                "XADD" => {
                    xadd_handle(&args, &redisdb).await.unwrap()
                }
                "XRANGE" => {
                    xrange_handle(&args, &redisdb).await.unwrap()
                }
                "XREAD" => {
                    if args[0] == "STREAMS" {
                        xread_handle(&args, &redisdb).await.unwrap()
                    } else {
                        xread_block_handle(&args, &redisdb).await.unwrap()
                    }
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

