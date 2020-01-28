use futures::stream::StreamExt;
use tokio_postgres::{types::ToSql, NoTls};
use futures::try_join;
use chrono::{DateTime, Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let connection_string = format!("host=timescaledb user=postgres password=test123");
    let (db_client, connection) =
        tokio_postgres::connect(&connection_string, NoTls).await?;

    let db_client = std::sync::Arc::new(db_client);
    let db_client1 = std::sync::Arc::clone(&db_client);
    let db_client2 = std::sync::Arc::clone(&db_client);
    let db_client3 = std::sync::Arc::clone(&db_client);
    let db_client4 = std::sync::Arc::clone(&db_client);
    let db_client5 = std::sync::Arc::clone(&db_client);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            println!("connection error: {}", e);
        }
    });

    db_client.simple_query(
        "CREATE TABLE ma_close (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE NOT NULL,
            close       FLOAT NOT NULL
        )"
    ).await?;

    db_client.simple_query(
        "CREATE TABLE ma_emat (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE,
            emat        FLOAT
        )"
    ).await?;

    db_client.simple_query(
        "CREATE TABLE ma_ematw (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE,
            ematw       FLOAT
        )"
    ).await?;

    db_client.simple_query(
        "CREATE TABLE ma_rsi (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE,
            rsi         FLOAT
        )"
    ).await?;

    db_client.simple_query(
        "CREATE TABLE ma_macd (
            id          SERIAL PRIMARY KEY,
            time        TIMESTAMP WITH TIME ZONE,
            macd        FLOAT
        )"
    ).await?;

    let statement_close = db_client.prepare(
        "INSERT INTO ma_close (time, close) VALUES ($1, $2)"
    ).await.unwrap();

    let statement_ema12 = db_client.prepare(
        "INSERT INTO ma_emat (time, emat) VALUES ($1, $2)"
    ).await.unwrap();

    let statement_ema26 = db_client.prepare(
        "INSERT INTO ma_ematw (time, ematw) VALUES ($1, $2)"
    ).await.unwrap();

    let statement = db_client.prepare(
        "INSERT INTO ma_macd (time, macd) VALUES ($1, $2)"
    ).await.unwrap();

    let statement_rsi = db_client.prepare(
        "INSERT INTO ma_rsi (time, rsi) VALUES ($1, $2)"
    ).await.unwrap();

    let (r1, close) = cbbot::candles("BTC-USD", 60).await?;
    let (r2, mut ema12) = cbbot::ema(close, 12).await?;

    let (r3, close) = cbbot::candles("BTC-USD", 60).await?;
    let (r4, mut ema26) = cbbot::ema(close, 26).await?;


    let (r5, mut rsi) = cbbot::rsi("BTC-USD", 60).await?;
    let (r6, mut macd) = cbbot::macd("BTC-USD", 60).await?;
    
    let (r7, mut close) = cbbot::candles("BTC-USD", 60).await?;
    tokio::spawn(async move {
        while let Some((time, .., close, _, _)) = close.next().await {

            let params: &[&(dyn ToSql + Sync)] = &[&time, &close];
            if let Err(e) = db_client1.execute(&statement_close, params).await {
                print!("{:?}", e);
                return;
            }    
        }
    });

    tokio::spawn(async move {
        while let Some(ema12) = ema12.next().await {

            if let Some((time, ema12)) = ema12 {
                let params: &[&(dyn ToSql + Sync)] = &[&time, &ema12];
                if let Err(e) = db_client2.execute(&statement_ema12, params).await {
                    print!("{:?}", e);
                    return;
                }   
            } else {
                let params: &[&(dyn ToSql + Sync)] = &[&None::<DateTime<Utc>>, &None::<f64>];
                if let Err(e) = db_client2.execute(&statement_ema12, params).await {
                    print!("{:?}", e);
                    return;
                }   
            }
        }
    });

    tokio::spawn(async move {
        while let Some(ema26) = ema26.next().await {

            if let Some((time, ema26)) = ema26 {
                let params: &[&(dyn ToSql + Sync)] = &[&time, &ema26];
                if let Err(e) = db_client3.execute(&statement_ema26, params).await {
                    print!("{:?}", e);
                    return;
                } 
            } else {
                let params: &[&(dyn ToSql + Sync)] = &[&None::<DateTime<Utc>>, &None::<f64>];
                if let Err(e) = db_client3.execute(&statement_ema26, params).await {
                    print!("{:?}", e);
                    return;
                } 
            }
        }
    });

    tokio::spawn(async move {
        while let Some(macd) = macd.next().await {

            if let Some((time, macd)) = macd {
                let params: &[&(dyn ToSql + Sync)] = &[&time, &macd];
                if let Err(e) = db_client4.execute(&statement, params).await {
                    print!("{:?}", e);
                    return;
                }    
            } else {
                let params: &[&(dyn ToSql + Sync)] = &[&None::<DateTime<Utc>>, &None::<f64>];
                if let Err(e) = db_client4.execute(&statement, params).await {
                    print!("{:?}", e);
                    return;
                }    
            }
        }
    });

    tokio::spawn(async move {
        while let Some(rsi) = rsi.next().await {

            if let Some((time, rsi)) = rsi {
                let params: &[&(dyn ToSql + Sync)] = &[&time, &rsi];
                if let Err(e) = db_client5.execute(&statement_rsi, params).await {
                    print!("{:?}", e);
                    return;
                }    
            } else {
                let params: &[&(dyn ToSql + Sync)] = &[&None::<DateTime<Utc>>, &None::<f64>];
                if let Err(e) = db_client5.execute(&statement_rsi, params).await {
                    print!("{:?}", e);
                    return;
                }    
            }
        }
    });

    try_join!(r1, r2, r3, r4, r5, r6, r7)?;

    //println!("{}", serde_json::to_string_pretty(&value).unwrap());
    Ok(())
}