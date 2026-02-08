use core::panic;

use anyhow::Error;

use crate::{database::db, resp::Value};

pub fn extract_command(value: Value) -> Result<(String, Vec<Value>), Error>{
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
pub fn unpack_bulk_str(value: Value) -> Result<String, Error> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Unexpected command for a bulkstring"))
    }
}
pub async fn get_handle(args: &Vec<Value>, db: &db) -> Option<String> {
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
pub async fn set_handle(args: &Vec<Value>, db: &db) -> Result<(), Error> {
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
    let list_key = args[0].clone();
    let mut list_values = Vec::new();
    for i in 1..args.len() {
        let v = match args[i].clone() {
            Value::BulkString(s) => s,
            _ => panic!("Wrong argument for rpush command")
        };
        list_values.push(v);
    }
    let key = match list_key {
        Value::BulkString(s) => s,
        _ => panic!("Wrong arrguments for a rpush command")
    };
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

pub async fn lrange_handle(args: &Vec<Value>, db: &db) -> Result<Value, Error> {
    let (key, 
        start_index, 
        end_index) = (args[0].clone(), args[1].clone(), args[2].clone());
    let list_key = match key {
        Value::BulkString(s) => s,
        _ => panic!("Wrong argument for a lrange handle")
    };
    let start = match start_index {
        Value::BulkString(s) => s.parse::<usize>().unwrap(),
        _ => panic!("Wrng arguments for a lrange handle")
    };
    let end = match end_index {
        Value::BulkString(s) => s.parse::<usize>().unwrap(),
        _ => panic!("Wrng arguments for a lrange handle")
    };
    let lock = db.state.lock().await;
    let list = lock.lists.get(&list_key);
    
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