use log::{info, Level};
use cbbot::Source::Main;
use futures::{stream::StreamExt, join};
use cbpro::client::{AuthenticatedClient, MAIN_URL, FILL};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();

    let secret = "EZb5q3kB1Z33gGfpvyAQGwCHqv4NgKc5LuG7po3gwNGknrDIWbkwg5R02vfkcutzuDKQ1qmwaUAepyqcKqiKiQ==";
    let pass = "lk38ho6hie";
    let key = "eed13e19a36594ccf0dd1d195b092fc2";

    let client = AuthenticatedClient::new(key.to_owned(), pass.to_owned(), secret.to_owned(), MAIN_URL);

    let (_, mut rsi) = cbbot::rsi("BTC-USD", 60, Main).await?;
    let (_, mut macd) = cbbot::macd("BTC-USD", 60, Main).await?;

    let mut buy_state = State {prev_macd: None, confirm_count: 0};
    let mut sell_state = State {prev_macd: None, confirm_count: 0};
    
    let accounts = client
        .list_accounts()
        .json()
        .await?;
 
    println!("{}", serde_json::to_string_pretty(&accounts).unwrap());
    
    while let (Some(rsi), Some(macd)) = join!(rsi.next(), macd.next()) {
        if let (Some((time, rsi, ("new", _, _))), Some((_, macd, _))) = (rsi, macd) {

            let holds = client
                .get_holds("<account_id>")
                .json()
                .await?;

            if holds.as_array().unwrap().is_empty() {

                let fills = client
                    .get_fills(FILL::ProductID("BTC-USD"))
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
                    info!("Buy at, time:{}, rsi: {}, macd: {}, confirm: {}", time, rsi, macd, buy_state.confirm_count);
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
                    info!("Sell at, time:{}, rsi: {}, macd: {}, confirm: {}", time, rsi, macd, sell_state.confirm_count);
                    sell_state.confirm_count = 0;
                    sell_state.prev_macd = None;
                }
            }
        }
    }

    Ok(())
}

struct State {
    prev_macd: Option<f64>,
    confirm_count: usize
}