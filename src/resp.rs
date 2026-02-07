use core::panic;
use std::{fmt::format, io::Read};

use anyhow::{Error, Ok, Result};
use bytes::{BytesMut, buf};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

#[derive(Clone,Debug)]
pub enum Value{
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<Value>),
    Integer(u32)
}

impl Value {
    pub fn serialize(&self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n",s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n",s.chars().count(),s),
            Value::NullBulkString => format!("$-1\r\n"),
            Value::Integer(s) => format!(":{}\r\n", s),
            _ => panic!("Unsupported value for serialized")
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler { stream, buffer: BytesMut::with_capacity(512) }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;

        if bytes_read == 0 {
            return Ok(None)
        }

        let (v, _) = parse_message(self.buffer.split())?;
        Ok(Some(v))
    }
    pub async fn write_value(&mut self, value: Value) -> Result<(), Error>{
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }
}

fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_arrays(buffer),
        '$' => parse_bulk_strings(buffer),
        _ =>  panic!("Not a known value type {}", buffer[0])
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = match_until_crlf(&buffer[1..]){
        let string = String::from_utf8(line.to_vec()).unwrap();

        return Ok((Value::SimpleString(string), len));
    }
    return Err(anyhow::anyhow!("Invalid simple string {:?}", buffer));
}

fn parse_bulk_strings(buffer: BytesMut) -> Result<(Value, usize)> {
    let (string_length, bytes_consumed) = if let Some((line,len)) = match_until_crlf(&buffer[1..]){
        let string_length = parse_int(line)?;
        (string_length, len + 1)
        } else {
            return Err(anyhow::anyhow!("Error invalid bulk string"));
        };
    let end_of_bulk_str = string_length as usize + bytes_consumed ;
    let total_parsed = end_of_bulk_str + 2;
    Ok((Value::BulkString(String::from_utf8(buffer[bytes_consumed..end_of_bulk_str].to_vec())?), total_parsed))
    
}

fn parse_arrays(buffer: BytesMut) -> Result<(Value, usize)> {
    let (array_length, mut bytes_consumed) = if let Some((line, len)) = match_until_crlf(&buffer[1..]){
        let array_length = parse_int(line)?;
        (array_length, len + 1)
    } else {
        return Err(anyhow::anyhow!("Error invalid array"));
    };
    let mut items = vec![];
    for _ in 0..array_length {
        let (item, length) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(item);
        bytes_consumed += length;
    }
    return Ok((Value::Array(items), bytes_consumed));
}

pub fn parse_int(buffer: &[u8]) -> Result<i64, Error>{
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn match_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i-1] == b'\r' && buffer[i] == b'\n' {
            return  Some((&buffer[..(i-1)], i+1));
        }
    }
    return None
}