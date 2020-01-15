use cbpro::websocket::{Channels, WebSocketFeed, WEBSOCKET_FEED_URL};
use futures::StreamExt;
use futures::future::FutureExt;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use futures::select;
use tokio_postgres::{types::ToSql, NoTls};
use chrono::{DateTime, Timelike};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=timescaledb user=postgres password=test123", NoTls).await?;
    
    let client = Arc::new(client);
    let ticker_client = Arc::clone(&client);
    let bucket_client = Arc::clone(&client);

    let (mut ticker_tx, mut ticker_rx) = mpsc::channel(1000);
    let (mut bucket_tx, mut bucket_rx) = mpsc::channel(1000);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client.simple_query(
            "CREATE TABLE IF NOT EXISTS ticker (
                trade_id    INT PRIMARY KEY,
                type        VARCHAR (50) NOT NULL,
                sequence    INT NOT NULL,
                time        TIMESTAMP WITH TIME ZONE NOT NULL,
                product_id  VARCHAR (50) NOT NULL,
                price       FLOAT NOT NULL,
                side        VARCHAR (50) NOT NULL,
                last_size   FLOAT NOT NULL,
                best_bid    FLOAT NOT NULL,
                best_ask    FLOAT NOT NULL,
                high_24h    FLOAT NOT NULL,
                low_24h     FLOAT NOT NULL,
                open_24h    FLOAT NOT NULL,
                volume_24h  FLOAT NOT NULL,
                volume_30d  FLOAT NOT NULL
            )"
        ).await?;

    client.simple_query(
            "CREATE TABLE IF NOT EXISTS candle (
                id      SERIAL PRIMARY KEY,
                time    TIMESTAMP WITH TIME ZONE NOT NULL,
                open    FLOAT NOT NULL,
                high    FLOAT NOT NULL,
                low     FLOAT NOT NULL,
                close   FLOAT NOT NULL,
                volume  FLOAT NOT NULL
            )"
        ).await?;

    let time = chrono::Utc::now().time();
    tokio::time::delay_for(Duration::from_secs(60 - time.second() as u64)).await;

    tokio::spawn(async move {
        let statement = ticker_client.prepare(
            "INSERT INTO ticker (
                trade_id, type, sequence, 
                time, product_id, price, 
                side, last_size, best_bid, 
                best_ask, high_24h, low_24h, open_24h, volume_24h, volume_30d) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
        ).await.unwrap();

        let mut feed = match WebSocketFeed::connect(WEBSOCKET_FEED_URL).await {
            Ok(feed) => feed,
            Err(e) => {
                eprintln!("{:?}", e);
                return;
            }
        };

        if let Err(e) = feed.subscribe(&["BTC-USD"], &[Channels::TICKER]).await {
            eprintln!("{:?}", e);
            return;
        }

        feed.next().await;

        while let Some(value) = feed.next().await {
            match value {
                Ok(v) => {
                    let price = v["price"].as_str().unwrap().parse::<f64>().unwrap();
                    let volume_24h = v["volume_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    
                    if let Err(_) = ticker_tx.send((price, volume_24h)).await {
                        println!("receiver dropped");
                        return;
                    }
                    
                    let trade_id = v["trade_id"].as_i64().unwrap() as i32;
                    let channel_type = v["type"].as_str().unwrap();
                    let sequence = v["sequence"].as_i64().unwrap() as i32;
                    let time = DateTime::parse_from_rfc3339(&v["time"].as_str().unwrap()).unwrap();
                    let product_id = v["product_id"].as_str().unwrap();
                    let side = v["side"].as_str().unwrap();
                    let last_size = v["last_size"].as_str().unwrap().parse::<f64>().unwrap();
                    let best_bid = v["best_bid"].as_str().unwrap().parse::<f64>().unwrap();
                    let best_ask = v["best_ask"].as_str().unwrap().parse::<f64>().unwrap();
                    let high_24h = v["high_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    let low_24h = v["low_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    let open_24h = v["open_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    let volume_30d = v["volume_30d"].as_str().unwrap().parse::<f64>().unwrap();
                    

                    let params: &[&(dyn ToSql + Sync)] = &[
                        &trade_id,
                        &channel_type,
                        &sequence,
                        &time,
                        &product_id,
                        &price,
                        &side,
                        &last_size,
                        &best_bid,
                        &best_ask,
                        &high_24h,
                        &low_24h,
                        &open_24h,
                        &volume_24h,
                        &volume_30d
                    ];

                    if let Err(e) = ticker_client.execute(&statement, params).await {
                        eprintln!("{:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                    return;
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut bucket = Vec::new();
        let mut interval = time::interval(Duration::from_secs(60));
        interval.tick().await;

        loop {
            select! {
                _ = async {
                    while let Some(value) = ticker_rx.recv().await {
                        bucket.push(value);
                    }
                }.fuse() => (),
                _ = interval.tick().fuse() => {
                    if let Err(_) = bucket_tx.send((chrono::Utc::now(), bucket.clone())).await {
                        println!("receiver dropped");
                        return;
                    }
                    (&mut bucket).clear();
                },
            };
        }
    });

    tokio::spawn(async move {
        let statement = bucket_client.prepare(
            "INSERT INTO candle (time, open, high, low, close, volume) 
            VALUES ($1, $2, $3, $4, $5, $6)"
        ).await.unwrap();
        
        while let Some((time, bucket)) = bucket_rx.recv().await {
            let open = bucket.iter().map(|t| t.0).next().unwrap_or(0./0.);
            let high = bucket.iter().map(|t| t.0).fold(0./0., f64::max);
            let low = bucket.iter().map(|t| t.0).fold(0./0., f64::min);
            let close = bucket.iter().map(|t| t.0).last().unwrap_or(0./0.);

            let open_volume = bucket.iter().map(|t| t.1).next().unwrap_or(0./0.);
            let close_volume = bucket.iter().map(|t| t.1).last().unwrap_or(0./0.);
            let volume = open_volume - close_volume;

            if let Err(e) = bucket_client.execute(&statement, &[&time, &open, &high, &low, &close, &volume]).await {
                eprintln!("{:?}", e);
                return;
            }
        }

    }).await?;

    Ok(())
}
