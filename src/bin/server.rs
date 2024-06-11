use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use my_redis::cmd::Command;
use my_redis::{self, Connection, RESPType};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = new_sharded_db(25);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = db.clone();

        tokio::spawn(async move { process(socket, db).await });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match frame.try_into() {
            Ok(cmd) => match cmd {
                Command::Ping(ping) => ping.response(),
                Command::Echo(echo) => echo.response(),
                Command::Get(get) => get.response(&db),
                Command::Set(set) => set.response(&db),
            },
            Err(e) => RESPType::Error(e.to_string()),
        };

        connection.write_frame(&response).await.unwrap();
    }
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }
    Arc::new(db)
}
