FROM rust:1.40 as builder

WORKDIR /usr/src/app

COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml
COPY ./src ./src

RUN cargo install --path .
RUN rm src/*.rs

FROM rust:1.40
COPY --from=builder /usr/local/cargo/bin/cbpro-rsi-bot /usr/local/bin/cbpro-rsi-bot

ENTRYPOINT ["cbpro-rsi-bot"]