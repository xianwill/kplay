#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate nonzero_ext;

use async_std::task;
use futures::sink::SinkExt;
use futures::StreamExt;
use governor::{Quota, RateLimiter};
use log::*;
use nonzero_ext::nonzero;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader, Lines};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use structopt::StructOpt;

fn main() {
    pretty_env_logger::init();

    let opt = Opt::from_args();

    println!("Starting player. Options {:?}", opt);

    let path: &Path = opt.message_file.as_path();

    // load the message file
    let file = File::open(path).expect(format!("Failed to open file {:?}", path).as_str());
    let reader = BufReader::new(file);

    let mut kafka_config: ClientConfig = ClientConfig::new();

    kafka_config.set("bootstrap.servers", &opt.bootstrap_servers);

    println!("Starting produce loop.");

    // block on `play` which awaits an async rate limiter.
    task::block_on(play(
        reader.lines(),
        opt.topic,
        opt.message_rate,
        opt.message_count,
        kafka_config,
    ));
}

async fn play<T>(
    lines: Lines<T>,
    topic: String,
    message_rate: u32,
    message_count: u32,
    kafka_config: ClientConfig,
) where
    T: BufRead,
{
    // setup rate limiter
    let lim = RateLimiter::direct(Quota::per_second(
        nonzero_ext::NonZero::new(message_rate).unwrap(),
    ));
    let mut send_count = 0u32;
    let start = Instant::now();

    // setup the kafka producer
    let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");

    for result in lines {
        debug!("Handling line");
        if send_count == message_count {
            println!("Sent {} messages. Terminating.", send_count);
            break;
        }
        // TODO: Add progress interval to config
        if send_count % 1000 == 0 && send_count > 0 {
            let elapsed_millis = start.elapsed().as_millis() as f64;
            let elapsed_seconds = start.elapsed().as_secs_f32();
            let per_milli = send_count as f64 / elapsed_millis;
            let per_sec = per_milli * 1000f64;
            println!(
                "Message rate is {} per millisecond ({} per second). Sent {} messages in {} seconds ({} minutes).",
                per_milli,
                per_sec,
                send_count,
                elapsed_seconds,
                elapsed_seconds / 60 as f32
            );
        }
        match result {
            Ok(line) => {
                let record: FutureRecord<String, String> = FutureRecord::to(&topic).payload(&line);
                debug!("Spawning producer send");
                spawn_and_log_error(producer.send(record, -1i64));
            }
            Err(e) => {
                eprintln!("Failed to read line {:?}", e);
            }
        }
        send_count += 1;
        debug!("Awaiting limiter");
        lim.until_ready().await
    }
}

fn spawn_and_log_error(fut: DeliveryFuture) -> task::JoinHandle<()> {
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kafka-player")]
/// Options for kafka-player to use.
struct Opt {
    /// The Kafka bootstrap servers.
    #[structopt(short, long, default_value = "localhost:9092")]
    bootstrap_servers: String,

    /// The topic to play messages onto.
    #[structopt(short, long)]
    topic: String,

    /// Location of keystore to use for TLS authentication.
    #[structopt(short = "l", long, parse(from_os_str))]
    keystore_location: Option<PathBuf>,

    /// Passphrase for the keystore.
    #[structopt(short = "s", long)]
    keystore_secret: Option<String>,

    /// The line-delimited message file containing the messages to play.
    #[structopt(short = "f", long, parse(from_os_str))]
    message_file: PathBuf,

    /// The number of messages to play in total.
    #[structopt(short = "c", long, default_value = "100000")]
    message_count: u32,

    /// The number of messages to play per second.
    #[structopt(short = "r", long, default_value = "1")]
    message_rate: u32,
}
