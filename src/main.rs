#![warn(rust_2018_idioms)]
#![deny(warnings)]

use governor::clock::QuantaClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::fmt;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;
use tokio::{
    fs::File,
    io::{self, stdin, AsyncBufReadExt, AsyncSeekExt, BufReader},
    time::Duration,
};

/// Options for kplay to use.
#[derive(Debug, StructOpt)]
#[structopt(name = "kplay")]
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
                let mut parts = p.splitn(2, '=');
                let (key, value) = (&parts.next(), &parts.next());

                info!("Setting additional Kafka config for key {:?}", key);

                if key.is_none() || value.is_none() {
                    panic!("Malformed Kafka property specified as option {:?}", p)
                }

                kafka_config.set(key.unwrap(), value.unwrap());
            })
        }

        kafka_config
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let opt = Opt::from_args();

    info!("Will play messages with options {:?}", opt);

    let kafka_config = ClientConfig::from(&opt);
    let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");
    let limiter = RateLimiter::direct(Quota::per_second(
        nonzero_ext::NonZero::new(opt.message_rate).unwrap(),
    ));

    match &opt.message_file {
        // play from a message file
        Some(path_buf) => {
            play_file(&opt, &producer, limiter, path_buf.clone()).await;
        }
        // play from stdin
        None => {
            play_stdin(&opt, &producer, limiter).await;
        }
    };
}

async fn play_file(
    opt: &Opt,
    producer: &FutureProducer,
    limiter: RateLimiter<NotKeyed, InMemoryState, QuantaClock>,
    path_buf: PathBuf,
) {
    let start = Instant::now();
    let mut send_count = 0u32;

    let f = File::open(path_buf.clone()).await.unwrap();
    let mut reader = BufReader::new(f);

    while send_count < opt.message_count {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(bytes_read) if bytes_read > 0 => {
                send_to_kafka(line.trim_end().to_string(), opt.topic.as_str(), producer).await;
                check_progress(send_count, opt.progress_interval, &start);
                send_count += 1;
                limiter.until_ready().await;
            }
            Ok(_) => {
                warn!("Reached end of file before sending desired message count. Re-seeking to beginning of file. Duplicate messages will be sent.");
                reader.seek(io::SeekFrom::Start(0)).await.unwrap();
            }
            Err(e) => {
                error!("Failed to read line from file. {}", e);
                break;
            }
        }
    }
}

async fn play_stdin(
    opt: &Opt,
    producer: &FutureProducer,
    limiter: RateLimiter<NotKeyed, InMemoryState, QuantaClock>,
) {
    let start = Instant::now();
    let mut send_count = 0u32;

    let mut reader = BufReader::new(stdin());

    while send_count < opt.message_count {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(bytes_read) if bytes_read > 0 => {
                send_to_kafka(line, opt.topic.as_str(), producer).await;
                check_progress(send_count, opt.progress_interval, &start);
                send_count += 1;
                limiter.until_ready().await;
            }
            Ok(_) => {
                info!("No more input available on stdin.");
                break;
            }
            Err(e) => {
                error!("Failed to read line from stdin. {}", e);
                break;
            }
        }
    }
}

async fn send_to_kafka(line: String, topic: &str, producer: &FutureProducer) {
    let producer = producer.clone();
    let topic = topic.to_string();

    tokio::spawn(async move {
        let record: FutureRecord<'_, String, String> = FutureRecord::to(&topic).payload(&line);
        let delivery_status = producer.send(
            record,
            Timeout::After(Duration::from_secs(30)),
        );
        if let Err((kafka_error, _)) = delivery_status.await {
            error!("Error sending line to Kafka. {}", kafka_error);
        }
    });
}

/// Logs current progress.
fn check_progress(send_count: u32, progress_interval: u32, start: &Instant) {
    if send_count % progress_interval == 0 && send_count > 0 {
        let p = Progress::new(send_count, start);
        info!("{}", p);
    }
}

/// Struct containing progress information of the task.
struct Progress {
    send_count: u32,
    elapsed_secs: u64,
}

impl Progress {
    /// Creates a new progress struct from a send count and start time.
    /// Calculates message rate from these parameters.
    fn new(send_count: u32, start: &Instant) -> Self {
        let elapsed_secs = start.elapsed().as_secs();

        Progress {
            send_count,
            elapsed_secs,
        }
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Total messages sent: {}. Elapsed seconds: {}.",
            self.send_count, self.elapsed_secs
        )
    }
}
