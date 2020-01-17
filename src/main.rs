use cbpro::{
    websocket::{Channels, WebSocketFeed, WEBSOCKET_FEED_URL},
    client::{PublicClient, MAIN_URL}
};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_postgres::{types::ToSql, NoTls};
use chrono::{DateTime, Timelike, Utc, NaiveDateTime};
use std::sync::Arc;
use std::collections::VecDeque;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database.
    let (client, connection) =
        tokio_postgres::connect("host=timescaledb user=postgres password=test123", NoTls).await?;
    
    let client = Arc::new(client);
    let ticker_client = Arc::clone(&client);
    let candle_client = Arc::clone(&client);
    let sma_client = Arc::clone(&client);

    let (mut ticker_tx, mut ticker_rx) = mpsc::channel(1000);
    let (mut price_tx, mut price_rx) = mpsc::channel(1000);

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

    client.simple_query(
        "CREATE TABLE IF NOT EXISTS simple_moving_average (
            id      SERIAL PRIMARY KEY,
            time    TIMESTAMP WITH TIME ZONE NOT NULL,
            close   FLOAT NOT NULL,
            sma     FLOAT
        )"
    ).await?;

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
                    let time = DateTime::parse_from_rfc3339(&v["time"].as_str().unwrap()).unwrap();
                    let last_size = v["last_size"].as_str().unwrap().parse::<f64>().unwrap();
                    
                    if let Err(_) = ticker_tx.send((time, price, last_size)).await {
                        println!("receiver dropped");
                        return;
                    }
                    
                    let volume_24h = v["volume_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    let side = v["side"].as_str().unwrap();
                    let trade_id = v["trade_id"].as_i64().unwrap() as i32;
                    let channel_type = v["type"].as_str().unwrap();
                    let sequence = v["sequence"].as_i64().unwrap() as i32;
                    let product_id = v["product_id"].as_str().unwrap();
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
        let statement = candle_client.prepare(
            "INSERT INTO candle (time, open, high, low, close, volume) 
            VALUES ($1, $2, $3, $4, $5, $6)"
        ).await.unwrap();

        let mut bucket: Vec<(f64, f64)> = Vec::new();
        let mut oldtime: Option<NaiveDateTime> = None;

        let public_client = PublicClient::new(MAIN_URL);
        let end = chrono::offset::Utc::now();
        let start = end - chrono::Duration::minutes(300);
        let rates = match public_client.get_historic_rates("BTC-USD", 60).range(start, end).json().await {
            Ok(rates) => rates,
            Err(e) => {
                eprintln!("{:?}", e);
                return;
            }
        };

        for rate in rates.as_array().unwrap().iter().rev() {
            let rate = rate.as_array().unwrap();

            let time = rate[0].as_i64().unwrap();
            let open = rate[3].as_f64().unwrap();
            let high = rate[2].as_f64().unwrap();
            let low = rate[1].as_f64().unwrap();
            let close = rate[4].as_f64().unwrap();
            let volume = rate[5].as_f64().unwrap();
            let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(time, 0), Utc);

            if let Err(_) = price_tx.send((time, close)).await {
                println!("close price receiver dropped");
                return;
            }

            if let Err(e) = candle_client.execute(&statement, &[&time, &open, &high, &low, &close, &volume]).await {
                eprintln!("{:?}", e);
                return;
            }    
        }

        while let Some((time, price, size)) = ticker_rx.recv().await {
            if let Some(oldtime) = oldtime {  
                if time.naive_utc().minute() == oldtime.minute() + 1 {
                    let open = bucket.iter().map(|t| t.0).next().unwrap_or(0./0.);
                    let high = bucket.iter().map(|t| t.0).fold(0./0., f64::max);
                    let low = bucket.iter().map(|t| t.0).fold(0./0., f64::min);
                    let close = bucket.iter().map(|t| t.0).last().unwrap_or(0./0.);
                    let volume: f64 = bucket.iter().map(|t| t.1).sum();

                    let bucket_time = oldtime
                    .with_second(0)
                    .unwrap()
                    .with_nanosecond(0)
                    .unwrap();

                    let bucket_time = DateTime::<Utc>::from_utc(bucket_time, Utc);
        
                    if let Err(e) = candle_client.execute(&statement, &[&bucket_time, &open, &high, &low, &close, &volume]).await {
                        eprintln!("{:?}", e);
                        return;
                    }

                    (&mut bucket).clear();
                }
            }
            
            oldtime = Some(time.naive_utc());
            bucket.push((price, size));
        }

    });

    tokio::spawn(async move {
        let statement = sma_client.prepare(
            "INSERT INTO simple_moving_average (time, close, sma) 
            VALUES ($1, $2, $3)"
        ).await.unwrap();

        let size = 12;
        let mut count = 0;
        let mut window: VecDeque<f64> = VecDeque::with_capacity(size);

        while let Some((time, price)) = price_rx.recv().await {
            if count == size - 1 {
                window.push_back(price);

                let sum: f64 = window.iter().map(|&x| x).sum();
                let average = sum / size as f64;

                if let Err(e) = sma_client.execute(&statement, &[&time, &price, &average]).await {
                    eprintln!("{:?}", e);
                    return;
                }

                window.pop_front();
            } else {
                window.push_back(price);
                count += 1;

                if let Err(e) = sma_client.execute(&statement, &[&time, &price, &None::<f64>]).await {
                    eprintln!("{:?}", e);
                    return;
                }
            } 
        }

    }).await?;

    Ok(())
}
