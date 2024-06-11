use std::io::{Error, ErrorKind};

use crate::cmd::{Echo, Ping};
use crate::{resp::*, Connection};
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Self> {
        Ok(Client {
            connection: Connection::new(TcpStream::connect(addr).await?),
        })
    }

    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let ping = Ping::new(msg);
        let frame = ping.into();

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            RESPType::String(msg) => Ok(msg.into()),
            RESPType::Bulk(msg) => Ok(msg),
            err => Err(format!("unexpected resp data type: {:?}", err).into()),
        }
    }

    pub async fn echo(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let echo = Echo::new(msg);
        let frame = echo.into();

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            RESPType::String(msg) => Ok(msg.into()),
            RESPType::Bulk(msg) => Ok(msg),
            err => Err(format!("unexpected resp data type: {:?}", err).into()),
        }
    }

    pub async fn get(&mut self, key: String) -> crate::Result<Bytes> {
        let mut arr = vec![];

        arr.push(RESPType::Bulk(Bytes::from("get")));
        arr.push(RESPType::Bulk(Bytes::from(key)));

        let frame = RESPType::Array(arr);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            RESPType::String(msg) => Ok(msg.into()),
            RESPType::Bulk(msg) => Ok(msg),
            err => Err(format!("unexpected resp data type: {:?}", err).into()),
        }
    }

    pub async fn set(&mut self, key: String, value: Bytes) -> crate::Result<Bytes> {
        let mut arr = vec![];

        arr.push(RESPType::Bulk(Bytes::from("set")));
        arr.push(RESPType::Bulk(Bytes::from(key)));
        arr.push(RESPType::Bulk(value));

        let frame = RESPType::Array(arr);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            RESPType::String(msg) => Ok(msg.into()),
            RESPType::Bulk(msg) => Ok(msg),
            err => Err(format!("unexpected resp data type: {:?}", err).into()),
        }
    }

    async fn read_response(&mut self) -> crate::Result<RESPType> {
        let frame = self.connection.read_frame().await?;

        match frame {
            Some(RESPType::Error(err)) => Err(err.into()),
            Some(frame) => Ok(frame),
            None => {
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");
                Err(err.into())
            }
        }
    }
}
