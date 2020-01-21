use futures::StreamExt;
use tokio::sync::mpsc;
use uuid::Uuid;
use tokio_postgres::{types::ToSql, NoTls};
use chrono::{DateTime, Timelike, Utc, NaiveDateTime};
use std::{sync::Arc, collections::VecDeque, iter::Iterator};
use cbpro::{
    websocket::{Channels, WebSocketFeed, SANDBOX_FEED_URL},
    client::{AuthenticatedClient, SANDBOX_URL, ORD}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

/*     let secret = "M0UEpXjC2kqfiyPgO4n+kyQpMZGVxZln/CH6qrz+OQBVZexjHFqsW3v3vyzcia5fVJGz7GlBcmu2mv+1fTD14A==";
    let pass = "nu6ck2twnd";
    let key = "f7cbb7dc1096f23f471693406b1e7007"; */

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

    let (mut ticker_tx, mut ticker_rx) = mpsc::channel(100);
    let (mut price_tx, mut price_rx) = mpsc::channel(100);
    let (mut rsi_tx, mut rsi_rx) = mpsc::channel(100);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    db_client.simple_query(
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

    db_client.simple_query(
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

    db_client.simple_query(
        "CREATE TABLE IF NOT EXISTS moving_average (
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
            id          INT UNIQUE NOT NULL,
            side        VARCHAR (50) NOT NULL,
            filled_size FLOAT,
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
                    let best_bid = v["best_bid"].as_str().unwrap().parse::<f64>().unwrap();
                    let best_ask = v["best_ask"].as_str().unwrap().parse::<f64>().unwrap();
                    
                    if let Err(_) = ticker_tx.send((time, price, last_size, (best_bid, best_ask))).await {
                        println!("receiver dropped");
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
        let statement = db_candle_client.prepare(
            "INSERT INTO candle (time, open, high, low, close, volume) 
            VALUES ($1, $2, $3, $4, $5, $6)"
        ).await.unwrap();

        let mut bucket: Vec<(f64, f64)> = Vec::new();
        let mut oldtime: Option<NaiveDateTime> = None;

        let end = chrono::offset::Utc::now();
        let start = end - chrono::Duration::minutes(300);
        let rates = match cb_client1.public().get_historic_rates("BTC-USD", 60).range(start, end).json().await {
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

            if (close - open).is_sign_positive() {
                if let Err(_) = price_tx.send((time, close, "gain", ("old", 0.0, 0.0))).await {
                    println!("close price receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = price_tx.send((time, close, "loss", ("old", 0.0, 0.0))).await {
                    println!("close price receiver dropped");
                    return;
                }
            }

            if let Err(e) = db_candle_client.execute(&statement, &[&time, &open, &high, &low, &close, &volume]).await {
                eprintln!("{:?}", e);
                return;
            }    
        }

        while let Some((time, price, size, (bid, ask))) = ticker_rx.recv().await {
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

                    
                    if (close - open).is_sign_positive() {
                        if let Err(_) = price_tx.send((bucket_time, close, "gain", ("new", bid, ask))).await {
                            println!("close price receiver dropped");
                            return;
                        }
                    } else {
                        if let Err(_) = price_tx.send((bucket_time, close, "loss", ("new", bid, ask))).await {
                            println!("close price receiver dropped");
                            return;
                        }
                    }

                    if let Err(e) = db_candle_client.execute(&statement, &[&bucket_time, &open, &high, &low, &close, &volume]).await {
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
        let statement = db_ma_client.prepare(
            "INSERT INTO moving_average (time, close, sma, ema, gains_ema, losses_ema, rsi) 
            VALUES ($1, $2, $3, $4, $5, $6, $7)"
        ).await.unwrap();

        let size = 12.0;
        let k = 2.0 / ( size + 1.0 );
        let mut count = 0;
        
        let mut prev_ema: [Option<f64>; 3] = [None; 3];
        let mut window: VecDeque<(f64, &str)> = VecDeque::with_capacity(size as usize);

        while let Some((time, price, direction, status)) = price_rx.recv().await {
            if count == size as usize - 1 {
                window.push_back((price, direction));

                let sma_all = window.iter().map(|t| t.0).sum::<f64>() / size;
                let sma_gains = window.iter().map(|t| if t.1 == "gain" {t.0} else {0.}).sum::<f64>() / size;
                let sma_losses = window.iter().map(|t| if t.1 == "loss" {t.0} else {0.}).sum::<f64>() / size;
                
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
                    println!("rsi receiver dropped");
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
                    eprintln!("{:?}", e);
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
                    eprintln!("{:?}", e);
                    return;
                }
            } 
        }

    });

    // trading starts here
    tokio::spawn(async move {
        let init_statement = db_state_client.prepare(
            "INSERT INTO state (id, side, filled_size, client_oid) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING"
        ).await.unwrap();

        let statement = db_state_client.prepare(
            "SELECT * FROM state"
        ).await.unwrap();

        let update_statement = db_state_client.prepare(
            "UPDATE state SET side = $1, filled_size = $2, client_oid = $3 WHERE id = 1"
        ).await.unwrap();

        let id: i32 = 1;
        let init_side: &'static str = "sell";

        if let Err(e) = db_state_client.execute(&init_statement, &[&id, &init_side, &None::<f64>, &None::<Uuid>]).await {
            eprintln!("or here? {:?}", e);
            return;
        }

        while let Some(status) = rsi_rx.recv().await {
            if let (Some(rsi), ("new", bid, ask)) = status {

                let (side, filled_size, uuid): (String, Option<f64>, Option<Uuid>) = match db_state_client.query_one(&statement, &[]).await {
                    Ok(row) => (row.get("side"), row.get("filled_size"), row.get("client_oid")),
                    Err(e) =>  {
                        eprintln!("{:?}", e);
                        return;
                    }
                };

                println!("{:?}", uuid);

                let (side, filled_size) = if let (side, filled_size, Some(uuid)) =  (side, filled_size, uuid) {
                    let order = cb_client2
                        .get_order(ORD::ClientOID(&uuid.to_hyphenated().to_string()))
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

                                println!("{}", serde_json::to_string_pretty(&ord).unwrap());
                                (side, Some(size))
                            } else {
                                continue
                            }
                            
                        }
                        Err(e) => {
                            eprintln!("{:?}", e);
                            if side == "sell" {
                                (String::from("buy"), filled_size)
                            } else {
                                (String::from("sell"), None)
                            }
                        }
                    }
 
                } else {
                    (String::from(init_side), None)
                };

                if rsi < 80.0 && side == "sell" {
                    let size = 500.0 / ask;
                    let dp = 10.0_f64.powi(8);
                    let size = (size * dp).round() / dp;
                    let uuid = Uuid::new_v4();
                    let client_oid = uuid.to_hyphenated().to_string();
                    let side: &str = "buy";

                    let response = cb_client2
                        .place_limit_order("BTC-USD", side, ask, size)
                        .client_oid(&client_oid)
                        .cancel_after("min")
                        .json()
                        .await;

                    if let Err(e) = response {
                        eprintln!("{:?}", e);
                        return;
                    }
                    
                    if let Err(e) = db_state_client.execute(&update_statement, &[&side, &size, &uuid]).await {
                        eprintln!("{:?}", e);
                        return;
                    }

                } else if rsi > 80.0 && side == "buy" {
                    let uuid = Uuid::new_v4();
                    let client_oid = uuid.to_hyphenated().to_string();
                    let side: &str = "sell";
                    
                    let size = if let Some(filled_size) = filled_size {
                        filled_size
                    } else {
                        eprintln!("world is ending");
                        return
                    };

                    let response = cb_client2
                        .place_limit_order("BTC-USD", side, bid, size)
                        .client_oid(&client_oid)
                        .cancel_after("min")
                        .json()
                        .await;

                        if let Err(e) = response {
                            eprintln!("{:?}", e);
                            return;
                        }
    
                        if let Err(e) = db_state_client.execute(&update_statement, &[&side, &size, &uuid]).await {
                            eprintln!("{:?}", e);
                            return;
                        }
                    };

                }
        }

    }).await?;

    Ok(())
}