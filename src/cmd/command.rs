use bytes::Bytes;

use crate::RESPType;

use super::{Echo, Get, Ping, Set};

pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Get(Get),
    Set(Set),
}

impl TryFrom<RESPType> for Command {
    type Error = crate::Error;

    fn try_from(value: RESPType) -> Result<Self, Self::Error> {
        match value {
            RESPType::Array(arr) => match &arr[0] {
                RESPType::Bulk(cmd) => match &cmd[..] {
                    b"ping" => Ok(Command::Ping(try_ping(arr)?)),
                    b"echo" => Ok(Command::Echo(try_echo(arr)?)),
                    b"get" => Ok(Command::Get(try_get(arr)?)),
                    b"set" => Ok(Command::Set(try_set(arr)?)),
                    _ => todo!(),
                },
                RESPType::String(cmd) => match &cmd[..] {
                    "ping" => Ok(Command::Ping(try_ping(arr)?)),
                    "echo" => Ok(Command::Echo(try_echo(arr)?)),
                    "get" => Ok(Command::Get(try_get(arr)?)),
                    "set" => Ok(Command::Set(try_set(arr)?)),
                    _ => todo!(),
                },
                _ => Err("invalid data type for cmd".into()),
            },
            _ => Err("Can only get command from array types".into()),
        }
    }
}

fn try_ping(arr: Vec<RESPType>) -> crate::Result<Ping> {
    match arr.len() {
        1 => Ok(Ping::new(None)),
        2 => match &arr[1] {
            RESPType::Bulk(msg) => Ok(Ping::new(Some(msg.clone()))),
            RESPType::String(msg) => Ok(Ping::new(Some(Bytes::from(msg.clone())))),
            _ => Err("invalid data type for echo message".into()),
        },
        _ => Err("Too many arguments for ping request".into()),
    }
}

fn try_echo(arr: Vec<RESPType>) -> crate::Result<Echo> {
    match arr.len() {
        1 => Ok(Echo::new(None)),
        2 => match &arr[1] {
            RESPType::Bulk(msg) => Ok(Echo::new(Some(msg.clone()))),
            RESPType::String(msg) => Ok(Echo::new(Some(Bytes::from(msg.clone())))),
            _ => Err("invalid data type for echo message".into()),
        },
        _ => Err("Too many arguments for echo request".into()),
    }
}

fn try_get(arr: Vec<RESPType>) -> crate::Result<Get> {
    match arr.len() {
        1 => Err("Array does not hold key for get request".into()),
        2 => match &arr[1] {
            RESPType::String(s) => Ok(Get::new(s.to_string())),
            RESPType::Bulk(b) => Ok(Get::new(std::str::from_utf8(&b).unwrap().to_string())),
            _ => Err("invalid data type for get key".into()),
        },
        _ => Err("Too many arguments for get request".into()),
    }
}

fn try_set(arr: Vec<RESPType>) -> crate::Result<Set> {
    match arr.len() {
        1 => Err("array does not hold key and value for set request".into()),
        2 => Err("array does not hold value for set request".into()),
        3 => match (&arr[1], &arr[2]) {
            (RESPType::Bulk(b1), RESPType::Bulk(b2)) => {
                Ok(Set::new(std::str::from_utf8(b1)?.to_string(), b2.clone()))
            }
            (RESPType::Bulk(b), RESPType::String(s)) => Ok(Set::new(
                std::str::from_utf8(b)?.to_string(),
                Bytes::from(s.clone()),
            )),
            (RESPType::String(s), RESPType::Bulk(b)) => Ok(Set::new(s.clone(), b.clone())),
            (RESPType::String(s1), RESPType::String(s2)) => {
                Ok(Set::new(s1.clone(), Bytes::from(s2.clone())))
            }
            _ => Err("invalid data type for set command arguments".into()),
        },
        _ => Err("Too many arguments for get request".into()),
    }
}
