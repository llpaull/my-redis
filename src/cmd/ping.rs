use bytes::Bytes;

use crate::RESPType;

pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub fn new(msg: Option<Bytes>) -> Self {
        Ping { msg }
    }

    pub fn response(&self) -> RESPType {
        match &self.msg {
            None => RESPType::Bulk("pong".into()),
            Some(msg) => RESPType::Bulk(msg.clone()),
        }
    }
}

impl Into<RESPType> for Ping {
    fn into(self) -> RESPType {
        let mut arr = vec![];

        arr.push(RESPType::Bulk(Bytes::from("ping")));
        if self.msg.is_some() {
            arr.push(RESPType::Bulk(self.msg.unwrap()));
        }

        RESPType::Array(arr)
    }
}
