use crate::RESPType;
use bytes::Bytes;

pub struct Echo {
    msg: Option<Bytes>,
}

impl Echo {
    pub fn new(msg: Option<Bytes>) -> Self {
        Echo { msg }
    }

    pub fn response(&self) -> RESPType {
        match &self.msg {
            None => RESPType::Bulk("".into()),
            Some(msg) => RESPType::Bulk(msg.clone()),
        }
    }
}

impl Into<RESPType> for Echo {
    fn into(self) -> RESPType {
        let mut arr = vec![];

        arr.push(RESPType::Bulk(Bytes::from("echo")));
        if self.msg.is_some() {
            arr.push(RESPType::Bulk(self.msg.unwrap()));
        } else {
            arr.push(RESPType::Bulk(Bytes::from("")));
        }

        RESPType::Array(arr)
    }
}
