#![allow(unused_imports)]
use core::panic;
use std::{any, collections::btree_map::Values};

use anyhow::{Error, Ok};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

use crate::{database::db, resp::Value};
pub mod resp;
pub mod database;


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

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "ping" => Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
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
                c => panic!("Cannot handle command {:?}",c)
            }
        }
        else {
            break;
        };
        handler.write_value(response).await.unwrap();
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>), Error>{
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect()
        ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format"))
    }
}
fn unpack_bulk_str(value: Value) -> Result<String, Error> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Unexpected command for a bulkstring"))
    }
}
async fn get_handle(args: &Vec<Value>, db: &db) -> Option<String> {
    let key_value = args[1].clone();
    let key = match key_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a get command")
    }.unwrap();
    let value = db.get(&key).await;
    if let Some(value) = value {
        return Some(value)
    } else {
        None
    }
}
async fn set_handle(args: &Vec<Value>, db: &db) -> Result<(), Error> {
    let key_value = args[1].clone();
    let value_value = args[2].clone();
    let key = match key_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a set command")
    }.unwrap();
    let value = match value_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a set command")
    }.unwrap();
    db.set(&key, &value).await.unwrap();
    Ok(())
}