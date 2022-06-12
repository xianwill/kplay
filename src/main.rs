#![warn(rust_2018_idioms)]
#![deny(warnings)]

use clap::Parser;
use governor::clock::QuantaClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use log::*;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::{
    fs::File,
    io::{self, stdin, AsyncBufReadExt, AsyncSeekExt, BufReader},
    time::{sleep, Duration},
};

/// Args for kplay
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The Kafka bootstrap servers.
    #[clap(short = 'b', long, default_value = "localhost:9092")]
    bootstrap_servers: String,

    /// The topic to play messages onto.
    #[clap(short = 't', long)]
    topic: String,

    /// The number of messages to play in total before terminating.
    /// If not set, kplay will play until the end of the source input and then exit.
    /// If this arg is set and greater than the number of lines in the `file` arg,
    /// kplay will seek to the beginning of the file and produce duplicates until count is met.
    #[clap(short = 'c', long)]
    count: Option<usize>,

    /// The number of messages to play per second.
    /// If this option is not provided, no rate limiter will be set.
    #[clap(short = 'r', long)]
    rate: Option<u32>,

    /// The line-delimited message file containing the messages to play.
    /// If not specified, kplay will read from stdin.
    #[clap(short = 'f', long, parse(from_os_str))]
    file: Option<PathBuf>,

    /// The number of messages to wait for between progress reports.
    #[clap(short = 'p', long, default_value = "1000")]
    progress: usize,

    /// Timeout duration for messages to be acknowledged in the *local* Kafka client queue.
    #[clap(long, default_value = "5")]
    queue_timeout_seconds: u64,

    /// Pause an extra duration (see queue_size_pause_seconds) 
    /// if queue size exceeds this value.
    #[clap(long, default_value = "100000")]
    queue_size_pause_count: usize,

    /// Milliseconds to pause if queue_size_pause_count is exceeded.
    #[clap(long, default_value = "250")]
    queue_size_pause_millis: u64,

    /// Additional Kafka properties in the format "key=value".
    /// Each property will be split on `=` and added to the Kafka config before producing.
    #[clap(short = 'K', long)]
    kafka_properties: Option<Vec<String>>,
}

/// Creates an `rdkafka::config::ClientConfig` from `Args`.
impl From<&Args> for ClientConfig {
    fn from(opt: &Args) -> Self {
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

    let args = Args::parse();

    info!("Will play messages with options {:?}", args);

    let kafka_config = ClientConfig::from(&args);
    let producer: FutureProducer = kafka_config.create().expect("Producer creation failed");
    let limiter: Option<RateLimiter<_, _, _>> = args.rate.map(|rate| {
        RateLimiter::direct(Quota::per_second(nonzero_ext::NonZero::new(rate).unwrap()))
    });
    let inflight = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let result = match &args.file {
        // play from a message file
        Some(path_buf) => {
            play_file(&args, &producer, limiter, path_buf.clone(), inflight.clone(), &start).await
        }
        // play from stdin
        None => {
            play_stdin(&args, &producer, limiter, inflight.clone(), &start).await
        }
    };

    if let Ok(send_count) = result {
        info!("Waiting for local queue.");

        while inflight.load(Ordering::SeqCst) > 0 {
            warn!("Local Kafka client queue is not empty. Waiting.");
            sleep(Duration::from_millis(args.queue_size_pause_millis)).await;
        }

        check_progress(send_count, send_count, &start, inflight);
    }
}

async fn play_file(
    args: &Args,
    producer: &FutureProducer,
    limiter: Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock>>,
    path_buf: PathBuf,
    inflight: Arc<AtomicUsize>,
    start: &Instant,
) -> Result<usize, ()> {
    let mut send_count = 0usize;
    let f = File::open(path_buf.clone()).await.unwrap();
    let mut reader = BufReader::new(f);

    loop {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(bytes_read) if bytes_read > 0 => {
                send_to_kafka(
                    line.trim_end().to_string(),
                    producer,
                    inflight.clone(),
                    args,
                )
                .await;
                check_progress(send_count, args.progress, &start, inflight.clone());
                send_count += 1;
                if let Some(l) = limiter.as_ref() {
                    l.until_ready().await;
                }
            }
            Ok(_) => {
                if args.count.is_some() {
                    warn!("Reached end of file before sending desired message count. Re-seeking to beginning of file. Duplicate messages will be sent.");
                    reader.seek(io::SeekFrom::Start(0)).await.unwrap();
                } else {
                    return Ok(send_count);
                }
            }
            Err(e) => {
                error!("Failed to read line from file. {}", e);
                return Err(());
            }
        }

        if let Some(c) = args.count {
            if send_count >= c {
                return Ok(send_count);
            }
        }
    }
}

async fn play_stdin(
    args: &Args,
    producer: &FutureProducer,
    limiter: Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock>>,
    inflight: Arc<AtomicUsize>,
    start: &Instant,
) -> Result<usize, ()> {
    let mut send_count = 0usize;
    let mut reader = BufReader::new(stdin());

    loop {
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(bytes_read) if bytes_read > 0 => {
                send_to_kafka(line, producer, inflight.clone(), args).await;
                check_progress(send_count, args.progress, &start, inflight.clone());
                send_count += 1;
                if let Some(l) = limiter.as_ref() {
                    l.until_ready().await;
                }
            }
            Ok(_) => {
                info!("No more input available on stdin.");
                return Ok(send_count);
            }
            Err(e) => {
                error!("Failed to read line from stdin. {}", e);
                return Err(());
            }
        }

        if let Some(c) = args.count {
            if send_count >= c {
                return Ok(send_count);
            }
        }
    }
}

async fn send_to_kafka(line: String, producer: &FutureProducer, inflight: Arc<AtomicUsize>, args: &Args) {
    let producer = producer.clone();
    let topic = args.topic.to_string();
    let timeout = args.queue_timeout_seconds;

    while inflight.load(Ordering::Relaxed) > args.queue_size_pause_count {
        warn!("Queue size exceeds queue_size_pause_count. Waiting for queue_size_pause_millis.");
        sleep(Duration::from_millis(args.queue_size_pause_millis)).await;
    }

    tokio::spawn(async move {
        let record: FutureRecord<'_, String, String> = FutureRecord::to(&topic).payload(&line);
        let delivery_status =
            producer.send(record, Timeout::After(Duration::from_secs(timeout)));
        inflight.fetch_add(1, Ordering::Relaxed);
        if let Err((kafka_error, _)) = delivery_status.await {
            error!("Error sending line to Kafka. {}", kafka_error);
        } 
        inflight.fetch_sub(1, Ordering::Relaxed);
    });
}

/// Logs current progress.
fn check_progress(send_count: usize, progress_interval: usize, start: &Instant, inflight: Arc<AtomicUsize>) {
    if send_count % progress_interval == 0 && send_count > 0 {
        let p = Progress::new(send_count, start, inflight.load(Ordering::Relaxed));
        info!("{}", p);
    }
}

/// Struct containing progress information of the task.
struct Progress {
    send_count: usize,
    elapsed_secs: u64,
    inflight: usize,
}

impl Progress {
    fn new(send_count: usize, start: &Instant, inflight: usize) -> Self {
        let elapsed_secs = start.elapsed().as_secs();

        Progress {
            send_count,
            elapsed_secs,
            inflight,
        }
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Total messages sent: {}. Elapsed seconds: {}. Messages in flight: {}.",
            self.send_count, self.elapsed_secs, self.inflight
        )
    }
}
