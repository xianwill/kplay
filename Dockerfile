# docker build -t kplay .

FROM rust:1.60-bullseye as builder
COPY . .
# RUN apt-get update && apt-get install -qy libssl-dev
RUN cargo build --release

FROM debian:bullseye

RUN apt-get update && \
  apt-get install -qy pkg-config libssl-dev ca-certificates && \
  apt-get autoremove -y && \
  rm -rf /var/apt/lists/* && \
  rm -rf /var/cache/apt/* && \
  rm -rf /var/lib/apt/lists/*
COPY --from=builder /target/release/kplay /usr/local/bin/kplay
COPY ./bin/run-kplay.sh .

# Set the ca certs file as expected by debian
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Run w/ Tini for proper signal forwarding
# https://github.com/krallin/tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--", "./run-kplay.sh"]

