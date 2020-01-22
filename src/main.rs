use uuid::Uuid;
use futures::StreamExt;
use tokio::sync::mpsc;
use clap::{Arg, App};
use log::{error, info, warn, debug, Level};
use tokio_postgres::{types::ToSql, NoTls};
use chrono::{DateTime, Timelike, Datelike, Utc, NaiveDateTime, Duration};
use std::{sync::Arc, collections::VecDeque, iter::Iterator};
use cbpro::{
    websocket::{Channels, WebSocketFeed, SANDBOX_FEED_URL},
    client::{AuthenticatedClient, SANDBOX_URL, ORD}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    let matches = App::new("cbpro-rsi-bot")
        .version("0.1.0")
        .author("kgeronim <kevin.geronimo@outlook.com>")
        .about("Coinbase Pro RSI Mean Reversion Trading Bot")
        .arg(Arg::with_name("granularity")
            .short("g")
            .long("granularity")
            .takes_value(true)
            .possible_values(&["60", "300", "900", "3600", "21600", "86400"]))
        .arg(Arg::with_name("ema")
            .long("ema")
            .takes_value(true)
            .possible_values(&["12", "26"]))
        .arg(Arg::with_name("RSI-BUY")
            .help("Buy at or above the provided rsi")
            .required(true)
            .index(1)
            .validator(|x| {
                if x.parse::<u32>().unwrap() <= 100 {
                    Ok(())
                } else {
                    Err(String::from("value has to be between 0-100"))
                }
            }))
        .arg(Arg::with_name("RSI-SELL")
            .help("Sell at or below the provided rsi")
            .required(true)
            .index(2)
            .validator(|x| {
                if x.parse::<u32>().unwrap() <= 100 {
                    Ok(())
                } else {
                    Err(String::from("value has to be between 0-100"))
                }
            }))
        .get_matches();

    simple_logger::init_with_level(Level::Info).unwrap();
    let granularity = matches.value_of("granularity").map_or(60, |x| x.parse::<i64>().unwrap());
    let window_size = matches.value_of("ema").map_or(12.0, |x| x.parse::<f64>().unwrap());
    let rsi_buy = matches.value_of("RSI-BUY").map(|x| x.parse::<f64>().unwrap()).unwrap();
    let rsi_sell = matches.value_of("RSI-SELL").map(|x| x.parse::<f64>().unwrap()).unwrap();

    if rsi_buy >= rsi_sell {
        error!("Are you trying to lose your money?");
        panic!("Buy low sell high not the other way around")
    }

    let secret = "zTfIRWZepcUnWQBAt8AXn57+YiPFTwCHh2gipTlCkM4A1Qx17NFI+/wzB9FEoXiWNV+4BsbqMFdM46/1SOJ0hQ==";
    let pass = "mk3nv587pqf";
    let key = "f9b2fe0ffbc5eb60ca20cbbb5fc94c4d";

    let cb_client = AuthenticatedClient::new(key, pass, secret, SANDBOX_URL);
    let cb_client = Arc::new(cb_client);
    let cb_client1 = Arc::clone(&cb_client);
    let cb_client2 = Arc::clone(&cb_client);

    let (db_client, connection) =
        tokio_postgres::connect("host=timescaledb user=postgres password=test123", NoTls).await?;
    
    let db_client = Arc::new(db_client);
    let db_ticker_client = Arc::clone(&db_client);
    let db_candle_client = Arc::clone(&db_client);
    let db_ma_client = Arc::clone(&db_client);
    let db_state_client = Arc::clone(&db_client);

    let (mut ticker_tx, mut ticker_rx) = mpsc::channel(10000);
    let (mut price_tx, mut price_rx) = mpsc::channel(10000);
    let (mut rsi_tx, mut rsi_rx) = mpsc::channel(10000);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    db_client.simple_query("DROP TABLE IF EXISTS ticker;").await?;
    db_client.simple_query("DROP TABLE IF EXISTS candle;").await?;
    db_client.simple_query("DROP TABLE IF EXISTS moving_average;").await?;

    db_client.simple_query(
            "CREATE TABLE ticker (
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

    db_client.simple_query(
            "CREATE TABLE candle (
                id      SERIAL PRIMARY KEY,
                time    TIMESTAMP WITH TIME ZONE UNIQUE NOT NULL,
                open    FLOAT NOT NULL,
                high    FLOAT NOT NULL,
                low     FLOAT NOT NULL,
                close   FLOAT NOT NULL,
                volume  FLOAT NOT NULL
            )"
        ).await?;

    db_client.simple_query(
        "CREATE TABLE moving_average (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE NOT NULL,
            close       FLOAT NOT NULL,
            sma         FLOAT,
            ema         FLOAT,
            gains_ema   FLOAT,
            losses_ema  FLOAT,
            rsi         FLOAT
        )"
    ).await?;

    db_client.simple_query(
        "CREATE TABLE IF NOT EXISTS state (
            id          INT PRIMARY KEY,
            side        VARCHAR (50) NOT NULL,
            size        FLOAT,
            client_oid  UUID
        )"
    ).await?;

    tokio::spawn(async move {
        let statement = db_ticker_client.prepare(
            "INSERT INTO ticker (
                trade_id, type, sequence, 
                time, product_id, price, 
                side, last_size, best_bid, 
                best_ask, high_24h, low_24h, open_24h, volume_24h, volume_30d) 
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
        ).await.unwrap();

        let mut feed = match WebSocketFeed::connect(SANDBOX_FEED_URL).await {
            Ok(feed) => feed,
            Err(e) => {
                error!("{:?}", e);
                return;
            }
        };

        if let Err(e) = feed.subscribe(&["BTC-USD"], &[Channels::TICKER]).await {
            error!("{:?}", e);
            return;
        }

        feed.next().await;

        while let Some(value) = feed.next().await {
            match value {
                Ok(v) => {
                    let price = v["price"].as_str().unwrap().parse::<f64>().unwrap();
                    let time = DateTime::parse_from_rfc3339(&v["time"].as_str().unwrap()).unwrap();
                    let last_size = v["last_size"].as_str().unwrap().parse::<f64>().unwrap();
                    let best_bid = v["best_bid"].as_str().unwrap().parse::<f64>().unwrap();
                    let best_ask = v["best_ask"].as_str().unwrap().parse::<f64>().unwrap();
                    let custom_time = time.naive_utc().with_second(0).unwrap().with_nanosecond(0).unwrap();

                    let total_seconds = time.day0() * 86400 + time.hour() * 3600 + time.minute() * 60;
                    let custom_time = custom_time - Duration::seconds(total_seconds as i64 % granularity);

                    if let Err(_) = ticker_tx.send((custom_time, price, last_size, (best_bid, best_ask))).await {
                        error!("receiver dropped");
                        return;
                    }
                    
                    let volume_24h = v["volume_24h"].as_str().unwrap().parse::<f64>().unwrap();
                    let side = v["side"].as_str().unwrap();
                    let trade_id = v["trade_id"].as_i64().unwrap() as i32;
                    let channel_type = v["type"].as_str().unwrap();
                    let sequence = v["sequence"].as_i64().unwrap() as i32;
                    let product_id = v["product_id"].as_str().unwrap();
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

                    if let Err(e) = db_ticker_client.execute(&statement, params).await {
                        error!("{:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    return;
                }
            }
        }
    });

    tokio::spawn(async move {
        let statement = db_candle_client.prepare(
            "INSERT INTO candle (time, open, high, low, close, volume) 
            VALUES ($1, $2, $3, $4, $5, $6)"
        ).await.unwrap();

        let mut bucket: Vec<(f64, f64, NaiveDateTime)> = Vec::new();

        let end = Utc::now() - Duration::seconds(granularity - 30);
        let start = end - Duration::seconds(300 * granularity);
        let rates = match cb_client1.public()
        .get_historic_rates("BTC-USD", granularity as i32).range(start, end).json().await {
            Ok(rates) => rates,
            Err(e) => {
                error!("{:?}", e);
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

            if (close - open).is_sign_positive() {
                if let Err(_) = price_tx.send((time, close, "gain", ("old", 0.0, 0.0))).await {
                    error!("close price receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = price_tx.send((time, close, "loss", ("old", 0.0, 0.0))).await {
                    error!("close price receiver dropped");
                    return;
                }
            }

            let params: &[&(dyn ToSql + Sync)] = &[&time, &open, &high, &low, &close, &volume];
            if let Err(e) = db_candle_client.execute(&statement, params).await {
                error!("{:?}", e);
                return;
            }    
        }

        let elapsed_time = Duration::seconds(granularity);
        while let Some((time, price, size, (bid, ask))) = ticker_rx.recv().await {
            
            let first_tick = bucket.iter().map(|t| t.2).next();
            
            if let Some(first_tick) = first_tick {
                debug!("bucket start time: {:?}", first_tick);

                if time - first_tick == elapsed_time {
                    let open = bucket.iter().map(|t| t.0).next().unwrap_or(0./0.);
                    let high = bucket.iter().map(|t| t.0).fold(0./0., f64::max);
                    let low = bucket.iter().map(|t| t.0).fold(0./0., f64::min);
                    let close = bucket.iter().map(|t| t.0).last().unwrap_or(0./0.);
                    let volume: f64 = bucket.iter().map(|t| t.1).sum();
                    
                    let bucket_time = DateTime::<Utc>::from_utc(first_tick, Utc);
                    
                    if (close - open).is_sign_positive() {
                        if let Err(_) = price_tx.send((bucket_time, close, "gain", ("new", bid, ask))).await {
                            error!("close price receiver dropped");
                            return;
                        }
                    } else {
                        if let Err(_) = price_tx.send((bucket_time, close, "loss", ("new", bid, ask))).await {
                            error!("close price receiver dropped");
                            return;
                        }
                    }
                    
                    let params: &[&(dyn ToSql + Sync)] = &[&bucket_time, &open, &high, &low, &close, &volume];
                    if let Err(e) = db_candle_client.execute(&statement, params).await {
                        error!("{:?}", e);
                        return;
                    }
                    
                    (&mut bucket).clear();
                }
            }
            
            bucket.push((price, size, time));
        }
        
    });

    tokio::spawn(async move {
        let statement = db_ma_client.prepare(
            "INSERT INTO moving_average (time, close, sma, ema, gains_ema, losses_ema, rsi) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)"
        ).await.unwrap();

        let k = 2.0 / ( window_size + 1.0 );
        let mut count = 0;
        
        let mut prev_ema: [Option<f64>; 3] = [None; 3];
        let mut window: VecDeque<(f64, &str)> = VecDeque::with_capacity(window_size as usize);

        while let Some((time, price, direction, status)) = price_rx.recv().await {
            if count == window_size as usize - 1 {
                window.push_back((price, direction));

                let sma_all = window.iter().map(|t| t.0).sum::<f64>() / window_size;
                let sma_gains = window.iter().map(|t| if t.1 == "gain" {t.0} else {0.}).sum::<f64>() / window_size;
                let sma_losses = window.iter().map(|t| if t.1 == "loss" {t.0} else {0.}).sum::<f64>() / window_size;
                
                let mut ema_calc = |index, new_price, sma| {
                    if let Some(ema) = prev_ema[index] {
                        let ema = new_price * k + ema * (1.0 - k);
                        prev_ema[index] = Some(ema);
                        Some(ema)
                    } else {
                        prev_ema[index] = Some(sma);
                        None
                    }
                };

                let ema_all = ema_calc(0, price, sma_all);

                let ema_gains = if let "gain" = direction {
                   ema_calc(1, price, sma_gains)
                } else {
                    ema_calc(1, 0., sma_gains)
                };

                let ema_losses = if let "loss" = direction {
                    ema_calc(2, price, sma_losses)
                 } else {
                     ema_calc(2, 0., sma_losses)
                 };

                let rsi = if let (Some(gains), Some(losses)) = (&ema_gains , &ema_losses) {
                    Some(100.0 - (100.0 / (1.0 + (gains / losses))))
                } else {
                    None
                };

                if let Err(_) = rsi_tx.send((rsi, status)).await {
                    error!("rsi receiver dropped");
                    return;
                }

                let params: &[&(dyn ToSql + Sync)] = &[
                    &time, 
                    &price, 
                    &sma_all, 
                    &ema_all, 
                    &ema_gains, 
                    &ema_losses, 
                    &rsi
                ];
            
                if let Err(e) = db_ma_client.execute(&statement, params).await {
                    error!("{:?}", e);
                    return;
                }
                
                window.pop_front();
            } else {
                window.push_back((price, direction));
                count += 1;

                let params: &[&(dyn ToSql + Sync)] = &[
                    &time, 
                    &price, 
                    &None::<f64>, 
                    &None::<f64>, 
                    &None::<f64>, 
                    &None::<f64>, 
                    &None::<f64>
                ];

                if let Err(e) = db_ma_client.execute(&statement, params).await {
                    error!("{:?}", e);
                    return;
                }
            } 
        }

    });

    // trading starts here
    tokio::spawn(async move {
        let init_statement = db_state_client.prepare(
            "INSERT INTO state (id, side, size, client_oid) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING"
        ).await.unwrap();

        let statement = db_state_client.prepare(
            "SELECT * FROM state"
        ).await.unwrap();

        let update_statement = db_state_client.prepare(
            "UPDATE state SET side = $1, size = $2, client_oid = $3 WHERE id = 1"
        ).await.unwrap();

        let id: i32 = 1;
        let init_side: &'static str = "sell";

        let params: &[&(dyn ToSql + Sync)] = &[&id, &init_side, &None::<f64>, &None::<Uuid>];
        if let Err(e) = db_state_client.execute(&init_statement, params).await {
            error!("{:?}", e);
            return;
        }

        while let Some(status) = rsi_rx.recv().await {
            if let (Some(rsi), ("new", bid, ask)) = status {

                let (side, size, uuid): (String, Option<f64>, Option<Uuid>) = match db_state_client.query_one(&statement, &[]).await {
                    Ok(row) => (row.get("side"), row.get("size"), row.get("client_oid")),
                    Err(e) =>  {
                        error!("{:?}", e);
                        return;
                    }
                };

                let (side, size) = if let (side, size, Some(uuid)) =  (side, size, uuid) {
                    let client_oid = &uuid.to_hyphenated().to_string();
                    let order = cb_client2
                        .get_order(ORD::ClientOID(client_oid))
                        .json()
                        .await;
    
                    match order {
                        Ok(ref ord) => {
                            let status = ord["status"].as_str().unwrap();
                            let settled = ord["settled"].as_bool().unwrap();

                            if let ("done", true) = (status, settled) {
                                let side = String::from(ord["side"].as_str().unwrap());
                                let size = ord["size"].as_str()
                                    .unwrap()
                                    .parse::<f64>()
                                    .unwrap();

                                (side, Some(size))
                            } else {
                                continue
                            }
                            
                        }
                        Err(_) => {
                            warn!("Limit {} order cancelled, client_oid: {}", side, client_oid);
                            if side == "sell" {
                                info!("Reversing to buy");
                                (String::from("buy"), size)
                            } else {
                                info!("Reversing to sell", );
                                (String::from("sell"), None)
                            }
                        }
                    }
 
                } else {
                    (String::from(init_side), None)
                };

                if rsi <= rsi_buy && side == "sell" {
                    let size = 500.0 / ask;
                    let dp = 10.0_f64.powi(8);
                    let size = (size * dp).round() / dp;
                    let uuid = Uuid::new_v4();
                    let client_oid = uuid.to_hyphenated().to_string();
                    let side: &'static str = "buy";

                    let response = cb_client2
                        .place_limit_order("BTC-USD", side, ask, size)
                        .client_oid(&client_oid)
                        .cancel_after("min")
                        .json()
                        .await;

                    info!("buy limit order, client_oid: {}, price: {}, size: {}", client_oid, ask, size);
                    if let Err(e) = response {
                        error!("{:?}", e);
                        return;
                    }
                    
                    if let Err(e) = db_state_client.execute(&update_statement, &[&side, &size, &uuid]).await {
                        error!("{:?}", e);
                        return;
                    }

                } else if rsi >= rsi_sell && side == "buy" {
                    let uuid = Uuid::new_v4();
                    let client_oid = uuid.to_hyphenated().to_string();
                    let side: &'static str = "sell";
                    
                    let size = if let Some(size) = size {
                        size
                    } else {
                        error!("world is ending");
                        return
                    };

                    let response = cb_client2
                        .place_limit_order("BTC-USD", side, bid, size)
                        .client_oid(&client_oid)
                        .cancel_after("min")
                        .json()
                        .await;

                    info!("sell limit order, client_oid: {}, price: {}, size: {}", client_oid, bid, size);
                    if let Err(e) = response {
                        error!("{:?}", e);
                        return;
                    }

                    if let Err(e) = db_state_client.execute(&update_statement, &[&side, &size, &uuid]).await {
                        error!("{:?}", e);
                        return;
                    }
                };
            }
        }
    }).await?;

    Ok(())
}