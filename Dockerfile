FROM rust:1.40 as builder

WORKDIR /usr/src/app

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./src ./src

RUN cargo install --path .
RUN rm src/*.rs

FROM debian:buster-slim
RUN apt-get update \
    && apt-get install -y libssl1.1 \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/cbbot /usr/local/bin/cbbot

ENTRYPOINT ["cbbot"]