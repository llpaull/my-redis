use my_redis::cmd::Command;
use my_redis::{self, Connection, RESPType};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move { process(socket).await });
    }
}

async fn process(socket: TcpStream) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match frame.try_into() {
            Ok(cmd) => match cmd {
                Command::Ping(ping) => ping.response(),
                Command::Echo(echo) => echo.response(),
            },
            Err(e) => RESPType::Error(e.to_string()),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
