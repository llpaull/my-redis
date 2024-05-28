use mini_redis::client;

#[tokio::main]
async fn main() {
    let mut client = client::connect("127.0.0.1:6369").await.unwrap();
}
