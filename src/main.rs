use log::{info, warn, error, Level};
use cbbot::Source;
use futures::{stream::StreamExt, join, try_join};
use cbpro::client::{AuthenticatedClient, MAIN_URL, SANDBOX_URL, FILL};
use clap::{Arg, App};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("cbbot")
        .version("0.1.0")
        .author("kgeronim <kevin.geronimo@outlook.com>")
        .about("Coinbase Pro RSI Mean Reversion Trading Bot")
        .arg(Arg::with_name("granularity")
            .short("g")
            .long("granularity")
            .takes_value(true)
            .possible_values(&["60", "300", "900", "3600", "21600", "86400"]))
        .arg(Arg::with_name("PRODUCT-ID")
            .help("Trading pair that will be used for trading")
            .required(true)
            .index(1))
        .arg(Arg::with_name("POSITION")
            .help("USD funds to take position with")
            .required(true)
            .index(2))
        .arg(Arg::with_name("RSI-BUY")
            .help("Buy at or above the provided rsi")
            .required(true)
            .index(3)
            .validator(cbbot::validator))
        .arg(Arg::with_name("RSI-SELL")
            .help("Sell at or below the provided rsi")
            .required(true)
            .index(4)
            .validator(cbbot::validator))
        .arg(Arg::with_name("debug")
            .short("d")
            .help("Print debug information verbosely"))
        .arg(Arg::with_name("sandbox")
            .long("sandbox")
            .help("Connect to https://public.sandbox.pro.coinbase.com/"))
        .get_matches();

    if matches.is_present("debug") {
        simple_logger::init_with_level(Level::Debug).unwrap();
    } else {
        simple_logger::init_with_level(Level::Info).unwrap();
    }

    let granularity = matches.value_of("granularity").map_or(60, |x| x.parse::<i64>().unwrap());
    let position = matches.value_of("POSITION").map(|x| x.parse::<f64>().unwrap()).unwrap();
    let rsi_buy = matches.value_of("RSI-BUY").map(|x| x.parse::<f64>().unwrap()).unwrap();
    let rsi_sell = matches.value_of("RSI-SELL").map(|x| x.parse::<f64>().unwrap()).unwrap();
    let product_id = String::from(matches.value_of("PRODUCT-ID").unwrap());

    if rsi_buy >= rsi_sell {
        error!("Buy low sell high not the other way around...");
        panic!()
    }

    let secret = if let Ok(val) = std::env::var("CBPRO_SECRET") { val } else {
            error!("CBPRO_SECRET environment variable required");
            panic!() 
    };

    let pass = if let Ok(val) = std::env::var("CBPRO_PASSPHRASE") { val } else {
        error!("CBPRO_PASSPHRASE environment variable required");
        panic!() 
    };

    let key = if let Ok(val) = std::env::var("CBPRO_KEY") { val } else {
        error!("CBPRO_KEY environment variable required");
        panic!() 
    };

    let (cb_url, feed_source) = if matches.is_present("sandbox") {
        (SANDBOX_URL, Source::Sandbox)
    } else {
        (MAIN_URL, Source::Main)
    };


    let client = AuthenticatedClient::new(key, pass, secret, cb_url);

    let products = client.public().get_products().json();
    let accounts = client.list_accounts().json();
    let (products, accounts) = try_join!(products, accounts)?;

    let products = products.as_array().unwrap();
    let mut products = products.iter().filter(|x| product_id == x["id"].as_str().unwrap());
    if let Some(_) = products.next() {
        if !product_id.contains("USD") {
            error!("Only USD trading pairs allowed. e.g., BTC-USD or LTC-USD ");
            panic!()
        }
    } else {
        error!("PRODUCT-ID not found");
        panic!()
    }

    let accounts = accounts.as_array().unwrap();
    for account in accounts {
        if account["currency"].as_str().unwrap() == "USD" {
            let balance = account["available"].as_str().unwrap().parse::<f64>().unwrap();
            if position > balance {
                error!("Not enough funds to take that position");
                panic!() 
            }
        }
    }

    let (_, mut rsi) = cbbot::rsi(&product_id, granularity, feed_source).await?;
    let (_, mut macd) = cbbot::macd(&product_id, granularity, feed_source).await?;

    let mut buy_state = cbbot::State { prev_macd: None, confirm_count: 0 };
    let mut sell_state = cbbot::State { prev_macd: None, confirm_count: 0 };
    
    while let (Some(rsi), Some(macd)) = join!(rsi.next(), macd.next()) {
        if let (Some((time, rsi, ("new", bid, ask))), Some((_, macd, _))) = (rsi, macd) {

            let holds = client
                .list_orders(&["open", "pending", "active"])
                .json()
                .await?;

            if holds.as_array().unwrap().is_empty() {

                let fills = client
                    .get_fills(FILL::ProductID(&product_id))
                    .json()
                    .await?;

                let side = &fills[0]["side"];
                
                // Buy signals happen here
                if rsi < 30.0 && side == "sell" {
                    if let None = buy_state.prev_macd {
                        buy_state.prev_macd = Some(macd);
                        info!("rsi buy signal, time: {}, rsi: {}", time, rsi)
                    }
                }

                if let Some(pm) = buy_state.prev_macd {
                    if macd > pm {
                        buy_state.confirm_count += 1;
                        info!("macd buy signal, time: {}, macd: {}, prev_macd: {:?}, confirm: {}", time, macd, buy_state.prev_macd, buy_state.confirm_count);
                    }
                    buy_state.prev_macd = Some(macd);
                }

                if buy_state.confirm_count > 1 {
                    let size = position / ask;
                    let dp = 10.0_f64.powi(8);
                    let size = (size * dp).round() / dp;
                    
                    let response = client
                        .place_limit_order(&product_id, "buy", ask, size)
                        .time_in_force("FOK")
                        .json()
                        .await;
                    
                    info!("Buy, ask: {}, rsi: {}, macd: {}, confirmations: {}", ask, rsi, macd, buy_state.confirm_count);
                    match response {
                        Ok(res) => info!("{}", serde_json::to_string_pretty(&res).unwrap()),
                        Err(e) => warn!("buy order cancelled: {:?}", e)
                    }
                    
                    buy_state.confirm_count = 0;
                    buy_state.prev_macd = None;
                }

                // Sell signals happen here
                if rsi > 70.0 && side == "buy" {
                    if let None = sell_state.prev_macd {
                        sell_state.prev_macd = Some(macd);
                        info!("rsi sell signal, time: {}, rsi: {}", time, rsi)
                    }
                }

                if let Some(pm) = sell_state.prev_macd {
                    if macd < pm {
                        sell_state.confirm_count += 1;
                        info!("macd sell signal, time: {}, macd: {}, prev_macd: {:?}, confirm: {}", time, macd, sell_state.prev_macd, sell_state.confirm_count);
                    }
                    sell_state.prev_macd = Some(macd);
                }

                if sell_state.confirm_count > 1 {
                    
                    let size = fills[0]["size"].as_str().unwrap().parse::<f64>()?;
                    let response = client
                        .place_limit_order(&product_id, "sell", bid, size)
                        .time_in_force("FOK")
                        .json()
                        .await;

                    info!("Sell, bid: {}, rsi: {}, macd: {}, confirmations: {}", bid, rsi, macd, sell_state.confirm_count);
                    match response {
                        Ok(res) => info!("{}", serde_json::to_string_pretty(&res).unwrap()),
                        Err(e) => warn!("sell order cancelled: {:?}", e)
                    }
                    
                    sell_state.confirm_count = 0;
                    sell_state.prev_macd = None;
                }
            }
        }
    }

    Ok(())
}