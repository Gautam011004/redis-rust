use core::panic;
use std::vec;

use anyhow::{Error, Ok};

use crate::{database::db, resp::Value};

pub fn extract_command(value: Value) -> Result<(String, Vec<Value>), Error>{
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(&a).unwrap().first().unwrap().to_string(),
                a.into_iter().skip(1).collect()
        ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format"))
    }
}
pub fn unpack_bulk_str(value: &Vec<Value>) -> Result<Vec<String>, Error> {
    let mut bulk_strings = Vec::new();
    for i in 0..value.len() {
        let v = match value[i].clone() {
            Value::BulkString(s) => Ok(s),
            _ => Err(anyhow::anyhow!("Unexpected command for a bulkstring"))
        }.unwrap();
        bulk_strings.push(v);
    }
    Ok(bulk_strings)
}
pub async fn get_handle(vec_args: &Vec<Value>, db: &db) -> Option<String> {
    let args = unpack_bulk_str(vec_args).unwrap();
    let value = db.get(&args[0]).await;
    if let Some(value) = value {
        return Some(value)
    } else {
        None
    }
}
pub async fn set_handle(vec_args: &Vec<Value>, db: &db) -> Result<(), Error> {
    let args = unpack_bulk_str(vec_args).unwrap();
    let ttl = if args.len() == 3 {
        Some(args[2].clone())
    } else {
        None
    };
    let s = if ttl.is_some() {
        Some(ttl.unwrap().parse::<u64>().unwrap())
    } else {
        None
    };
    db.set(args[0].clone(), args[1].clone(), s).await.unwrap();
    Ok(())
}

pub async fn rpush_handle(vec_args: &Vec<Value>, db: &db) -> Result<u32, Error> {
    let args = unpack_bulk_str(vec_args).unwrap();
    let key = args[0].clone();
    let mut list_values: Vec<String> = Vec::new();
    for i in 1..args.len() {
        list_values.push(args[i].clone());
    }
    let mut lock = db.state.lock().await;
    let mut v = Vec::new();
    let v = if lock.lists.get(&key).is_some() {
        let list = lock.lists.get_mut(&key).unwrap();
        list.append(&mut list_values);
        list.len()
    } else {
        v.append(&mut list_values);
        lock.lists.insert(key, v.clone());
        v.len()
    };
    Ok(v as u32)
}

pub async fn lrange_handle(vec_args: &Vec<Value>, db: &db) -> Result<Value, Error> {
    let args = unpack_bulk_str(vec_args).unwrap();
    let start = args[1].parse::<usize>().unwrap();
    let end = args[2].parse::<usize>().unwrap();
    let lock = db.state.lock().await;
    let list = lock.lists.get(&args[0]);
    match list {
        Some(list) => {
            if start > end || start > list.len()  {
                return Ok(Value::EmptyArray)
            } else {
                let list = list;
                let mut v= Vec::new();
                for i  in start..=end {
                    v.push(Value::BulkString(list[i].clone()));
                }
                return Ok(Value::Array(v))
            }
        }
        None => {
            return Ok(Value::EmptyArray)
        }
    }
}