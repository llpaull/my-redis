use bytes::Bytes;
use clap::{Parser, Subcommand};
use my_redis::Client;
use std::convert::Infallible;

#[derive(Parser, Debug)]
#[command(disable_help_flag = true)]
struct Cli {
    #[clap(subcommand)]
    cmd: Command,
    #[clap(short = 'h', long = "hostname", default_value = "127.0.0.1")]
    host: String,
    #[clap(short = 'p', long = "port", default_value_t = 6379)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    Echo {
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,
    },
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let mut client = Client::connect(addr).await.unwrap();

    match args.cmd {
        Command::Ping { msg } => match client.ping(msg).await {
            Ok(msg) => println!("{:?}", std::str::from_utf8(&msg[..]).unwrap()),
            Err(e) => eprintln!("{}", e),
        },
        Command::Echo { msg } => match client.echo(msg).await {
            Ok(msg) => println!("{:?}", std::str::from_utf8(&msg[..]).unwrap()),
            Err(e) => eprintln!("{}", e),
        },
        Command::Get { key } => match client.get(key).await {
            Ok(msg) => println!("{:?}", std::str::from_utf8(&msg[..]).unwrap()),
            Err(e) => eprintln!("{}", e),
        },
        Command::Set { key, value } => match client.set(key, value).await {
            Ok(msg) => println!("{:?}", std::str::from_utf8(&msg[..]).unwrap()),
            Err(e) => eprintln!("{}", e),
        },
    }
}

fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}
