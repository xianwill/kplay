#![warn(rust_2018_idioms)]

use async_std::task;
use futures::prelude::*;
use governor::{Quota, RateLimiter};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use std::fs::File;
use std::io;
use std::io::prelude::*;
//use async_std::io;
//use async_std::io::prelude::*;
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
                check_progress(i, opt.progress_interval, &start).map(|p| info!("{:?}", p));
            },
            buf.lines(),
        ));
    };

    // invoke the program with the appropriate input buffer
    match file_buf(&opt.message_file) {
        Some(b) => program(Box::new(b)),
        None => program(Box::new(std::io::stdin().lock())),
    }
}

/// Creates an Option<BufReader<File>> for the file path specified in program options. Returns
/// `None` if no path is specified in program options.
fn file_buf(path_buf: &Option<PathBuf>) -> Option<io::BufReader<File>> {
    match path_buf {
        Some(p) => {
            let path = p.as_path();
            let file = File::open(path).expect(format!("Failed to open file {:?}", path).as_str());
            Some(io::BufReader::new(file))
        }
        _ => None,
    }
}

fn async_lines<T>(reader: T) -> impl Stream<Item = Result<String, String>>
where
    T: BufRead,
{
    stream::iter(reader.lines().map(|r| match r {
        Ok(line) => Ok(line),
        Err(e) => Err(e.to_string()),
    }))
}

/// Reads all messages in the buffer and invokes the given callback on each one.
async fn read_all<T, F>(message_rate: u32, message_count: u32, mut callback: F, buf: io::Lines<T>)
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
    for result in buf {
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
fn check_progress(send_count: u32, progress_interval: u32, start: &Instant) -> Option<Progress> {
    if send_count % progress_interval == 0 && send_count > 0 {
        Some(Progress::new(send_count, start))
    } else {
        None
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

#[derive(Debug)]
struct Progress {
    send_count: u32,
    per_milli: f64,
    per_sec: f64,
    elapsed_seconds: f32,
    elapsed_minutes: f32,
}

impl Progress {
    fn new(send_count: u32, start: &Instant) -> Self {
        let elapsed_millis = start.elapsed().as_millis() as f64;
        let elapsed_seconds = start.elapsed().as_secs_f32();
        let elapsed_minutes = elapsed_seconds / 60 as f32;
        let per_milli = send_count as f64 / elapsed_millis;
        let per_sec = per_milli * 1000f64;

        Progress {
            send_count,
            per_milli,
            per_sec,
            elapsed_seconds,
            elapsed_minutes,
        }
    }
}

// struct FileMessageSource {
//     buf: io::BufReader<File>,
// }

// struct StdinLockMessageSource<'a> {
//     stdin: io::StdinLock<'a>,
// }

// struct VecMessageSource {
//     vec: Vec<String>,
// }

trait MessageSource {}

// enum MessageSource<'a> {
//     FileMessageSource(io::BufReader<File>),
//     StdinLockMessageSource(io::StdinLock<'a>),
//     VecMessageSource(Vec<String>),
// }

// impl<'a> Iterator for MessageSource<'a> {
//     type Item = Result<String, String>;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!();
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_buf_returns_some_file_buf() {
        let opt_path_buf = Some(PathBuf::from("src/main.rs"));
        let buf = file_buf(&opt_path_buf);
        assert!(buf.is_some());
    }

    #[test]
    fn file_buf_returns_none() {
        let opt_path_buf: Option<PathBuf> = None;
        let buf = file_buf(&opt_path_buf);
        assert!(buf.is_none());
    }

    #[test]
    fn read_all_invokes_callback_with_send_count_and_line() {
        // let mut buf = &["See spot run.", "See spot jump.", "See spot run."];
        // let mut buf = Cursor::new(vec!["See Spot run.".to_string()]);

        // let buf = String::from("See Spot run.\nSee Spot jump.\nSee Spot run.\n").lines();

        // read_all(1, 3, |send_count, line| {}, buf);
    }

    #[test]
    fn read_all_with_3_messages_and_rate_of_1_takes_3_seconds() {
        //
    }

    #[test]
    fn check_progress_returns_some_at_interval() {
        let start = Instant::now();
        let progress = check_progress(10, 10, &start);
        assert!(progress.is_some());
    }

    #[test]
    fn check_progress_returns_none_between_intervals() {
        let start = Instant::now();
        let progress = check_progress(3, 10, &start);
        assert!(progress.is_none());
    }
}
