use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::resp::*;

pub struct Connection {
    socket: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            socket: socket,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<RESPType>> {
        loop {
            let mut buf = Cursor::new(&self.buffer[..]);
            if let Some(resp) = RESPParser::parse(&mut buf)? {
                self.buffer.advance(buf.position() as usize);
                return Ok(Some(resp));
            }

            if 0 == self.socket.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    // graceful shutdown
                    return Ok(None);
                } else {
                    // connection shutdown in middle of sending frame
                    return Err("Connection reset by peer".into());
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &RESPType) -> crate::Result<()> {
        let res = match RESPSerializer::serialize(frame) {
            Ok(val) => val,
            Err(_) => return Err("incorrect input".into()),
        };
        self.socket.write_all(&res).await?;
        Ok(())
    }
}
