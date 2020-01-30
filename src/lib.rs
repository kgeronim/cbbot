use cbpro::{
    client::{PublicClient, MAIN_URL, SANDBOX_URL},
    websocket::{Channels, WebSocketFeed, MAIN_FEED_URL, SANDBOX_FEED_URL}
};
use log::{error, info};
use chrono::{DateTime, NaiveDateTime, Duration, Utc};
use tokio::{sync::mpsc, task::JoinHandle};
use futures::{stream::{self, Stream, StreamExt}, join};
use std::{collections::VecDeque, error::Error};
use rust_decimal::{Decimal, prelude::*};

pub type Candle = (DateTime<Utc>, Decimal, Decimal, Decimal, Decimal, f64, (&'static str, f64, f64));
type Indicator = Option<(DateTime<Utc>, Decimal, (&'static str, f64, f64))>;

pub struct State {
    pub prev_macd: Option<Decimal>,
    pub confirm_count: usize
}

#[derive(Copy, Clone)]
pub enum Source {
    Main,
    Sandbox
}

pub fn validator(x: String) -> Result<(), String> {
    if let Ok(x) = x.parse::<u32>() {
        if x <= 100 {
            Ok(())
        } else {
            Err(String::from("value has to be between 0-100"))
        }
    } else {
        Err(String::from("value has to be between 0-100"))
    }
}

pub async fn candles(product_id: &str, granularity: i64, source: Source) -> cbpro::error::Result<(JoinHandle<()>, impl Stream<Item=Candle> + Unpin + Send + 'static)> {
    let (cb_url, feed_url) = if let Source::Main = source {
        (MAIN_URL, MAIN_FEED_URL)
    } else {
        (SANDBOX_URL, SANDBOX_FEED_URL)
    };

    let client = PublicClient::new(cb_url);
    let end = Utc::now();
    let start = end - Duration::seconds(300 * granularity);
    
    let rates = client
        .get_historic_rates(product_id, granularity as i32)
        .range(start, end)
        .json()
        .await?;

    let rates: Vec<Candle> = rates.as_array().unwrap().iter().map(|rate| {
            let time = rate[0].as_i64().unwrap();
            let open = rate[3].as_f64().unwrap();
            let high = rate[2].as_f64().unwrap();
            let low = rate[1].as_f64().unwrap();
            let close = rate[4].as_f64().unwrap();
            let volume = rate[5].as_f64().unwrap();
            let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(time, 0), Utc);

            let open = Decimal::from_f64(open).unwrap();
            let high = Decimal::from_f64(high).unwrap();
            let low = Decimal::from_f64(low).unwrap();
            let close = Decimal::from_f64(close).unwrap();

            (time, open, high, low, close, volume, ("old", 0./0., 0./0.))
        }).rev().collect();

    let rates = stream::iter(rates);

    let mut feed = WebSocketFeed::connect(feed_url).await?;
    feed.subscribe(&[product_id], &[Channels::TICKER]).await?;

    let (mut tx, rx) = mpsc::channel(100);

    let handle = tokio::spawn(async move {
        let mut start_time = None::<i64>;
        let mut buffer: Vec<(Decimal, f64)> = Vec::new();

        while let Some(result) = feed.next().await {
            match result {
                Ok(value) => {
                    if value["type"] == Channels::TICKER {
                        let time = value["time"].as_str().unwrap();
                        let time = DateTime::parse_from_rfc3339(time).unwrap().timestamp();
                        let price = value["price"].as_str().unwrap().parse::<Decimal>().unwrap();
                        let last_size = value["last_size"].as_str().unwrap().parse::<f64>().unwrap();
                        let best_bid = value["best_bid"].as_str().unwrap().parse::<f64>().unwrap();
                        let best_ask = value["best_ask"].as_str().unwrap().parse::<f64>().unwrap();

                        if let Some(start) = start_time {
                            if time / granularity != start / granularity {
                                let open = buffer.iter().map(|x| x.0).next().unwrap();
                                let high = buffer.iter().map(|x| x.0).max().unwrap();
                                let low = buffer.iter().map(|x| x.0).min().unwrap();
                                let close = buffer.iter().map(|x| x.0).last().unwrap();
                                let volume = buffer.iter().map(|x| x.1).sum::<f64>().round();

                                info!("time: {}, open: {}, high: {}, low: {}, close: {}, volume: {}", time, open, high, low, close, volume);
                                let start = NaiveDateTime::from_timestamp((start / granularity) * granularity, 0);
                                let start = DateTime::<Utc>::from_utc(start, Utc);

                                if let Err(_) = tx.send((start, open, high, low, close, volume, ("new", best_bid, best_ask))).await {
                                    error!("candle receiver dropped");
                                    return;
                                }

                                start_time = Some(time);
                                buffer.clear();
                            }
                        } else {
                            start_time = Some(time);
                        }

                        buffer.push((price, last_size));

                    }
                },
                Err(e) => {
                    error!("candle sender error: {:?}", e.source().unwrap());
                    return
                }
            }
        }
    });
    
    let rates = rates.chain(rx);
    Ok((handle, rates))
}

pub async fn ema<T>(mut candles: T, window_size: usize) -> cbpro::error::Result<(JoinHandle<()>, impl Stream<Item=Indicator> + Unpin + Send + 'static)>
where 
    T: Stream<Item=Candle> + Unpin + Send + 'static,
{
    let (mut tx, rx) = mpsc::channel(100);

    let handle = tokio::spawn(async move {
        let const1 = Decimal::from_f64(1.0).unwrap();
        let const2 = Decimal::from_f64(2.0).unwrap();
        let dwindow_size = Decimal::from_usize(window_size).unwrap();
        
        let k: Decimal =  const2 / ( dwindow_size + const1);
        let mut count: usize = 0;
        
        let mut prev_ema = None::<Decimal>;
        let mut window: VecDeque<Decimal> = VecDeque::with_capacity(window_size);

        while let Some((time, .., close, _, direction)) = candles.next().await {
            if count == window_size - 1 {
                window.push_back(close);
                let sma = window.iter().map(|x| *x).sum::<Decimal>() / dwindow_size;
                
                if let Some(ema) = prev_ema {
                    let ema = close * k + ema * (const1 - k);
                    prev_ema = Some(ema);

                    if let Err(_) = tx.send(Some((time, ema, direction))).await {
                        error!("ema receiver dropped");
                        return;
                    }
                } else {
                    prev_ema = Some(sma);
                }

                window.pop_front();
            } else {
                window.push_back(close);
                count += 1;

                if let Err(_) = tx.send(None).await {
                    error!("ema receiver dropped");
                    return;
                }
            }
        }
    });

    Ok((handle, rx))
}

pub async fn rsi(product_id: &str, granularity: i64, source: Source) -> cbpro::error::Result<(JoinHandle<()>, impl Stream<Item=Indicator> + Unpin + Send + 'static)> {
    let (mut tx, rx) = mpsc::channel(100);

    let size: usize = 14;
    let const1 = Decimal::zero();

    let (_, gains) = candles(product_id, granularity, source).await?;
    let gains = gains.map(move |mut x|if (x.4 - x.1).is_sign_positive() {x} else {x.4 = const1; x});
    let (_, mut gains) = ema(gains, size).await?;

    let (_, losses) = candles(product_id, granularity, source).await?;
    let losses = losses.map(move |mut x|if (x.4 - x.1).is_sign_negative() {x} else {x.4 = const1; x});
    let (_, mut losses) = ema(losses, size).await?;


    let handle = tokio::spawn(async move {
        let const1 = Decimal::from_f64(100.0).unwrap();
        let const2 = Decimal::from_f64(1.0).unwrap();

        while let (Some(gains), Some(losses)) = join!(gains.next(), losses.next()) {

            if let (Some((_, gains, direction)), Some((time, losses, _))) = (gains, losses) {
                let rsi = const1 - (const1 / (const2 + (gains / losses)));
                if let Err(_) = tx.send(Some((time, rsi, direction))).await {
                    error!("rsi receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    error!("rsi receiver dropped");
                    return;
                }
            }

        }
    });

    Ok((handle, rx))
}

pub async fn macd(product_id: &str, granularity: i64, source: Source) -> cbpro::error::Result<(JoinHandle<()>, impl Stream<Item=Indicator> + Unpin + Send + 'static)> {
    let (mut tx, rx) = mpsc::channel(100);

    let (_, ema12) = candles(product_id, granularity, source).await?;
    let (_, mut ema12) = ema(ema12, 12).await?;

    let (_, ema26) = candles(product_id, granularity, source).await?;
    let (_, mut ema26) = ema(ema26, 26).await?;


    let handle = tokio::spawn(async move {
        while let (Some(ema12), Some(ema26)) = join!(ema12.next(), ema26.next()) {
            
            if let (Some((_, ema12, _)), Some((time, ema26, direction))) = (ema12, ema26) {
                let macd = ema12 - ema26;
                if let Err(_) = tx.send(Some((time, macd, direction))).await {
                    error!("macd receiver dropped");
                    return;
                }
            } else {
                if let Err(_) = tx.send(None).await {
                    error!("macd receiver dropped");
                    return;
                } 
            }
        }
    });

    Ok((handle, rx))
}