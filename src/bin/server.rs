use bytes::Bytes;
use mini_redis::Command::{self, Get, Set};
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

type ShardedDb = Arc<Vec<Mutex<HashMap<String, Bytes>>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    //let db = Arc::new(Mutex::new(HashMap::new()));
    let db = new_sharded_db(10);

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = db.clone();

        tokio::spawn(async move {
            process(stream, db).await;
        });
    }
}

async fn process(stream: TcpStream, db: ShardedDb) {
    let mut connection = Connection::new(stream);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let mut hasher = DefaultHasher::new();
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                cmd.key().hash(&mut hasher);
                let mut shard = db[hasher.finish() as usize % db.len()].lock().unwrap();
                shard.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                cmd.key().hash(&mut hasher);
                let shard = db[hasher.finish() as usize % db.len()].lock().unwrap();
                if let Some(val) = shard.get(cmd.key()) {
                    Frame::Bulk(val.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
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
