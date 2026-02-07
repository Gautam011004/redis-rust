#![allow(unused_imports)]
use core::panic;
use std::{any, collections::btree_map::Values, env::args_os};

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
    let key_value = args[0].clone();
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
    let (key_value,
        value_value) = (args[0].clone(),args[1].clone());
    let key = match key_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a set command")
    }.unwrap();
    let value = match value_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a set command")
    }.unwrap();
    let ttl = if args.len() == 3 {
        let value_ttl = args[2].clone();
        match value_ttl {
            Value::BulkString(s) => Some(s),
            _ => panic!("Wrong argument for set command"),
        }
    } else {
        None
    };

    let s = if ttl.is_some() {
        Some(ttl.unwrap().parse::<u64>().unwrap())
    } else {
        None
    };
    db.set(key, &value, s).await.unwrap();
    Ok(())
}

pub async fn rpush_handle(args: &Vec<Value>, db: &db) -> Result<u32, Error> {
    let (list_key, 
        list_value) = (args[0].clone(),args[1].clone());
    let key = match list_key {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a rpush command")
    }.unwrap();
    let value = match list_value {
        Value::BulkString(s) => Some(s),
        _ => panic!("Wrong arrguments for a rpush command")
    }.unwrap();
    let mut lock = db.state.lock().await;
    let mut v = Vec::new();
    let v = if lock.lists.get(&key).is_some() {
        let list = lock.lists.get_mut(&key).unwrap();
        list.push(value);
        list.len()
    } else {
        v.push(value);
        lock.lists.insert(key, v.clone());
        v.len()
    };
    Ok(v as u32)
}