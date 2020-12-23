#![warn(rust_2018_idioms)]

use async_std::task;
use governor::{Quota, RateLimiter};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::FnMut;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

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

    /// The line-delimited message file containing the messages to play.
    #[structopt(short = "f", long, parse(from_os_str))]
    message_file: Option<PathBuf>,

    /// The number of messages to play in total.
    #[structopt(short = "c", long, default_value = "100000")]
    message_count: u32,

    /// The number of messages to play per second.
    #[structopt(short = "r", long, default_value = "10")]
    message_rate: u32,

    /// The number of messages to wait for between progress reports.
    #[structopt(short = "p", long, default_value = "1000")]
    progress_interval: u32,

    /// Additional Kafka properties in the format "key=value".
    /// Each property will be split on `=` and added to the Kafka config before producing.
    #[structopt(short = "K", long)]
    kafka_properties: Option<Vec<String>>,
}

/// Creates an `rdkafka::config::ClientConfig` from an `Opt`.
impl From<&Opt> for ClientConfig {
    fn from(opt: &Opt) -> Self {
        let mut kafka_config: ClientConfig = ClientConfig::new();

        kafka_config.set("bootstrap.servers", &opt.bootstrap_servers);

        if let Some(kafka_props) = &opt.kafka_properties {
            kafka_props.iter().for_each(|p| {
                let mut parts = p.splitn(2, "=");
                let (key, value) = (&parts.next(), &parts.next());

                if key.is_none() || value.is_none() {
                    panic!("Malformed Kafka property specified as option {:?}", p)
                }

                kafka_config.set(key.unwrap(), value.unwrap());
            })
        }

        kafka_config
    }
}

fn main() {
    pretty_env_logger::init();

    let opt = Opt::from_args();

    info!("Will play messages with options {:?}", opt);

    let kafka_config = ClientConfig::from(&opt);
    let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");

    // define the program to run generically over any `BufRead` trait object.
    let program = |buf: Box<dyn BufRead>| {
        // initialize a timer to track message rate
        let start = Instant::now();
        // block on `read_all` which awaits an async rate limiter.
        task::block_on(read_all(
            opt.message_rate,
            opt.message_count,
            // send to kafka and check progress on each callback from the read loop
            |i, line| {
                send_to_kafka(line, &opt.topic, &producer);
                check_progress(i, opt.progress_interval, &start);
            },
            buf,
        ));
    };

    // invoke the program with the appropriate input buffer
    match file_buf(&opt) {
        Some(b) => program(Box::new(b)),
        None => program(Box::new(std::io::stdin().lock())),
    }
}

/// Creates an Option<BufReader<File>> for the file path specified in program options. Returns
/// `None` if no path is specified.
fn file_buf(opt: &Opt) -> Option<BufReader<File>> {
    match &opt.message_file {
        Some(p) => {
            let path = p.as_path();
            let file = File::open(path).expect(format!("Failed to open file {:?}", path).as_str());
            Some(BufReader::new(file))
        }
        _ => None,
    }
}

/// Reads all messages in the buffer and invokes the given callback on each one.
async fn read_all<T, F>(message_rate: u32, message_count: u32, mut callback: F, buf: T)
where
    T: BufRead,
    F: FnMut(u32, String),
{
    // setup rate limiter to send `message_rate` messages per second
    let limiter = RateLimiter::direct(Quota::per_second(
        nonzero_ext::NonZero::new(message_rate).unwrap(),
    ));

    // initialize send count
    let mut send_count = 0u32;

    // iterate lines and invoke the callback for each one
    for result in buf.lines() {
        if send_count == message_count {
            info!("Sent {} lines. Terminating.", send_count);
            break;
        }
        match result {
            Ok(line) => {
                callback(send_count, line);
            }
            Err(e) => {
                error!("Failed to read line {:?}", e);
            }
        }
        // increment the send count
        send_count += 1;
        // wait for the rate limiter before continuing
        limiter.until_ready().await
    }
}

/// Writes the line to the specified Kafka topic.
fn send_to_kafka(line: String, topic: &str, producer: &FutureProducer) {
    let record: FutureRecord<'_, String, String> = FutureRecord::to(topic).payload(&line);
    spawn_and_log_error(producer.send(record, -1i64));
}

/// Logs current progress.
fn check_progress(send_count: u32, progress_interval: u32, start: &Instant) {
    if send_count % progress_interval == 0 && send_count > 0 {
        let elapsed_millis = start.elapsed().as_millis() as f64;
        let elapsed_seconds = start.elapsed().as_secs_f32();
        let per_milli = send_count as f64 / elapsed_millis;
        let per_sec = per_milli * 1000f64;
        info!(
            "Message rate is {} per millisecond ({} per second). Sent {} messages in {} seconds ({} minutes).",
            per_milli,
            per_sec,
            send_count,
            elapsed_seconds,
            elapsed_seconds / 60 as f32
        );
    }
}

/// Spawns a separate task on which to await the future, logging an error result if appropriate.
fn spawn_and_log_error(fut: DeliveryFuture) -> task::JoinHandle<()> {
    task::spawn(async move {
        if let Err(e) = fut.await {
            error!("{:?}", e)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_buf_returns_some_file_buf() {}

    #[test]
    fn file_buf_returns_none() {}

    #[test]
    fn read_all_invokes_callback_with_message_count_and_line() {}

    #[test]
    fn check_progress_test() {}
}
