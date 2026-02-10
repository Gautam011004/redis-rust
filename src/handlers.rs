use core::{f64, panic, time};
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
pub async fn get_handle(args: &Vec<String>, db: &db) -> Option<String> {
    let value = db.get(&args[0]).await;
    if let Some(value) = value {
        return Some(value)
    } else {
        None
    }
}
pub async fn set_handle(args: &Vec<String>, db: &db) -> Result<(), Error> {
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

pub async fn rpush_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
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
    Ok(Value::Integer(v as u32))
}

pub async fn lrange_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let lock = db.state.lock().await;
    let list = lock.lists.get(&args[0]);
    let start = args[1].parse::<isize>().unwrap();
    let end = args[2].parse::<isize>().unwrap();
    match list {
        Some(list) => {
            let len = list.len() as isize;
            let norm = |i: isize|({ i + len }).max(0);
            let s = if start >= 0 { start } else { norm(start) };
            let e = if end > 0 { end } else { norm(end) };
            if s > e {
                return Ok(Value::EmptyArray)
            } else {
                let list = list;
                let mut v= Vec::new();
                for i  in s..=e {
                    v.push(Value::BulkString(list[i as usize].clone()));
                }
                return Ok(Value::Array(v))
            }
        }
        None => {
            return Ok(Value::EmptyArray)
        }
    }
}

pub async fn lpush_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = args[0].clone();
    let len = args.len();
    let mut list_values: Vec<String> = Vec::new();
    for i in 1..args.len() {
        list_values.push(args[len - i].clone());
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
    Ok(Value::Integer(v as u32))
}
pub async fn llen_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = args[0].clone();
    let lock = db.state.lock().await;
    let list = lock.lists.get(&key);
    let len = match list {
        Some(list) => {
            list.len()
        }
        None => 0
    };
    Ok(Value::Integer(len as u32))
}
pub async fn lpop_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let args_len = args.len();
    let key = args[0].clone();
    let element_count = if args_len > 1 { args[1].clone().parse::<usize>().unwrap() } else { 0 };
    let mut lock = db.state.lock().await;
    let list = lock.lists.get_mut(&key);
    let v: Value = match list {
        Some(list) => {
            let len = list.len();
            if len == 0 { Value::NullBulkString }
            else { 
                if element_count == 0 {
                    Value::BulkString(list.remove(0))
                } else {
                    let mut s = Vec::new();
                    for _ in 0..element_count {
                        s.push(Value::BulkString(list.remove(0)));
                    }
                    Value::Array(s)
                }
            }
        }
        None => Value::NullBulkString
    };
    Ok(v)
}
pub async fn blpop_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = args[0].clone();
    let mut time_out = args[1].clone().parse::<f64>().unwrap();
    let now = std::time::Instant::now();

    if time_out == 0.0 {
        time_out = f64::INFINITY;
    }
    
    while now.elapsed().as_secs_f64() < time_out {
        let mut lock = db.state.lock().await;
        let list = match lock.lists.get_mut(&key) {
            Some(l) => l,
            None => continue
        };
        if list.len() == 0 {
            continue;
        } else {
            let v = list.remove(0);
            return Ok(Value::Array(vec![Value::BulkString(key), Value::BulkString(v)]));
        }
    }
    Ok(Value::NullBulkString)
}