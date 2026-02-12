use core::{f64, panic, time};
use std::{collections::{BTreeMap, HashMap}, hash::Hash, time::SystemTime, vec};

use anyhow::{Error, Ok};

use crate::{database::{db, key_value}, resp::Value};

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
    let v = if lock.kv.get(&key).is_some() {
        let list = match lock.kv.get_mut(&key).unwrap() {
            key_value::List(list )=> {
                list.append(&mut list_values);
                list.len()
            }
            _ => panic!("rpush only for lists")
        };
        list
    } else {
        v.append(&mut list_values);
        lock.kv.insert(key, key_value::List(v.clone()));
        v.len()
    };
    Ok(Value::Integer(v as u32))
}

pub async fn lrange_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let lock = db.state.lock().await;
    let list = match lock.kv.get(&args[0]) {
        Some(key_value::List(l )) => Some(l),
        None => None,
        _ => panic!("lrange not supported for the given key")
    };
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
    let v = if lock.kv.get(&key).is_some() {
        let len = match lock.kv.get_mut(&key).unwrap() {
            key_value::List(list) => {
                list.append(&mut list_values);
                list.len()
            }
            _ => panic!("Only lists")
        };
        len
    } else {
        v.append(&mut list_values);
        lock.kv.insert(key, key_value::List(v.clone()));
        v.len()
    };
    Ok(Value::Integer(v as u32))
}
pub async fn llen_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = &args[0];
    let lock = db.state.lock().await;
    let list = lock.kv.get(key);
    let len = match list {
        Some(key_value::List(list)) => {
            list.len()
        }
        _ => 0
    };
    Ok(Value::Integer(len as u32))
}
pub async fn lpop_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let args_len = args.len();
    let key = args[0].clone();
    let element_count = if args_len > 1 { args[1].clone().parse::<usize>().unwrap() } else { 0 };
    let mut lock = db.state.lock().await;
    let list = lock.kv.get_mut(&key);
    let v: Value = match list {
        Some(key_value::List(list)) => {
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
        _ => Value::NullBulkString
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
        let list = match lock.kv.get_mut(&key) {
            Some(key_value::List(l)) => l,
            _ => continue
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

pub async fn type_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = &args[0];
    let lock = db.state.lock().await;
    let value = lock.kv.get(key);

    let s = match value {
        Some(l ) => match l {
            key_value::List(_) => {
                Value::SimpleString("list".to_string())
            },
            key_value::String(_) => {
                Value::SimpleString("string".to_string())
            }
            key_value::Stream(_) => {
                Value::SimpleString("stream".to_string())
            }
        }
        None => Value::SimpleString("none".to_string())
    };
    Ok(s)
}
pub async fn xadd_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = args[0].clone();
    let mut id = args[1].clone();
    let (ms, sq)  = if id == "0-0" {
        panic!("Error min valid redis ID is 0-1")
    } else if id == "*" {
        let newms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
        id = newms.to_string() + "-0";
        (newms, 0)
    } else {
        let (m, s) = id.split_once("-").unwrap();
        (m.parse::<u128>().unwrap(), s.parse::<u128>().unwrap())
    };
    let len = args.len();
    let mut lock = db.state.lock().await;
    let last = match lock
                                                                .kv
                                                                .entry(key.clone())
                                                                .or_insert_with(|| key_value::Stream(BTreeMap::<(u128, u128), HashMap<String, String>>::new())){
                                                                    key_value::Stream(s) => s,
                                                                    _ => panic!("Error only supports streams")
                                                                };
    let (last_ms, last_sq) = if last.is_empty() {
        (0,0)
    } else {
        let t = last.last_key_value().unwrap();
        (t.0.0 , t.0.1)
    };
    if &args[1] == "*" && last_ms == ms {
         id = ms.to_string() + "-" + &(last_sq + 1).to_string()
    } else if (last_ms, last_sq) >= (ms, sq) {
        panic!("Error the ID is equal to less than the previous entry")
    }
    let mut s = HashMap::new();
    for i in (2..len).step_by(2) {
        s.insert(args[i].clone(), args[i+1].clone());
    }
    let (d,q) = id.split_once("-").unwrap();
    let final_id = d.parse::<u128>().unwrap();
    let final_seq = q.parse::<u128>().unwrap();
    last.insert((final_id, final_seq), s);
    Ok(Value::BulkString(id))
}
pub async fn xrange_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = &args[0];
    fn parse_id(s: &str, default_seq: u128) -> (u128, u128) {
        println!("{:?}", s);
        match s.split_once('-') {
            Some((ms, seq)) => (ms.parse().unwrap_or(default_seq), seq.parse().unwrap_or(default_seq)),
            None => if s == "+" {
                (default_seq, default_seq)
            } else {
                (s.parse::<u128>().unwrap(), default_seq)
            }
        }
    }
    
    let (start_ms, start_sq) = parse_id(&args[1], 0);
    let (end_ms, end_sq)   = parse_id(&args[2], u128::MAX);

    let lock = db.state.lock().await;
    let stream = match lock.kv.get(key) {
        Some(s) => match s {
            key_value::Stream(l) => l,
            _ => panic!("Only streams are supported for this cmd")
        }
        None => return Ok(Value::BulkError("Key not found".to_string()))
    };
    let mut res = Vec::new();
    for (id, field) in stream.range((start_ms, start_sq)..=(end_ms, end_sq)){
        let mut values = Vec::new();
        for i in field {
            values.push(Value::BulkString(i.0.to_string()));
            values.push(Value::BulkString(i.1.to_string()));
        }
        res.push(Value::Array(vec![Value::BulkString(id.0.to_string() + "-" + &id.1.to_string()), Value::Array(values)]));
    }
    Ok(Value::Array(res))
}
pub async fn xread_handle(args: &Vec<String>, db: &db) -> Result<Value, Error> {
    let key = &args[1];
    let id = args[2].clone();
    let (start_ms, start_sq) = match id.split_once("-") {
        Some((s,q)) => (s.parse::<u128>().unwrap(), q.parse::<u128>().unwrap() + 1),
        None =>  (id.parse::<u128>().unwrap(), 1)
    };
    let (end_ms, end_sq) = (u128::MAX, u128::MAX);
    let lock = db.state.lock().await;
    let stream = match lock.kv.get(key) {
        Some(s) => match s {
            key_value::Stream(l) => l,
            _ => panic!("Only streams are supported for this cmd")
        }
        None => return Ok(Value::BulkError("Key not found".to_string()))
    };
    let mut fin = Vec::new();
    let mut res = Vec::new();
    res.push(Value::BulkString(id));
    let mut nes = Vec::new();
    for (id, field) in stream.range((start_ms, start_sq)..=(end_ms, end_sq)){
        let mut values = Vec::new();
        for i in field {
            values.push(Value::BulkString(i.0.to_string()));
            values.push(Value::BulkString(i.1.to_string()));
        }
        nes.push(Value::Array(vec![Value::BulkString(id.0.to_string() + "-" + &id.1.to_string()), Value::Array(values)]));
    }
    res.push(Value::Array(nes));
    fin.push(Value::Array(res));
    Ok(Value::Array(fin))
}
