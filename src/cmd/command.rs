use crate::RESPType;

use super::{Echo, Ping};

pub enum Command {
    Ping(Ping),
    Echo(Echo),
}

impl TryFrom<RESPType> for Command {
    type Error = crate::Error;

    fn try_from(value: RESPType) -> Result<Self, Self::Error> {
        match value {
            RESPType::Array(arr) => match &arr[0] {
                RESPType::Bulk(cmd) => match &cmd[..] {
                    b"ping" => Ok(Command::Ping(try_ping(arr)?)),
                    b"echo" => Ok(Command::Echo(try_echo(arr)?)),
                    _ => todo!(),
                },
                _ => Err("Can only get command from bulk as first element in array".into()),
            },
            _ => Err("Can only get command from array types".into()),
        }
    }
}

fn try_ping(arr: Vec<RESPType>) -> crate::Result<Ping> {
    match arr.len() {
        1 => Ok(Ping::new(None)),
        _ => match &arr[1] {
            RESPType::Bulk(msg) => Ok(Ping::new(Some(msg.clone()))),
            _ => Err("Can only get ping message from bulk as second element in array".into()),
        },
    }
}

fn try_echo(arr: Vec<RESPType>) -> crate::Result<Echo> {
    match arr.len() {
        1 => Ok(Echo::new(None)),
        _ => match &arr[1] {
            RESPType::Bulk(msg) => Ok(Echo::new(Some(msg.clone()))),
            _ => Err("Can only get ping message from bulk as second element in array".into()),
        },
    }
}
