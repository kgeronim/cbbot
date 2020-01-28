use futures::{stream::StreamExt, join};
use cbbot::Source::Main;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let (_, mut rsi) = cbbot::rsi("BTC-USD", 60, Main).await?;
    let (_, mut macd) = cbbot::macd("BTC-USD", 60, Main).await?;

    let mut buy_state = State {prev_macd: None, confirm_count: 0};
    let mut sell_state = State {prev_macd: None, confirm_count: 0};

    while let (Some(rsi), Some(macd)) = join!(rsi.next(), macd.next()) {
        if let (Some((time, rsi, (_, _, _))), Some((_, macd, _))) = (rsi, macd) {
            
            // Buy signals happen here
            if rsi < 30.0 {
                if let None = buy_state.prev_macd {
                    buy_state.prev_macd = Some(macd);
                    println!("rsi buy signal, time: {}, rsi: {}", time, rsi)
                }
            }

            if let Some(pm) = buy_state.prev_macd {
                if macd > pm {
                    buy_state.confirm_count += 1;
                    println!("macd buy signal, time: {}, macd: {}, prev_macd: {:?}, confirm: {}", time, macd, buy_state.prev_macd, buy_state.confirm_count);
                }
                buy_state.prev_macd = Some(macd);
            }

            if buy_state.confirm_count > 1 {
                println!("Buy at, time:{}, rsi: {}, macd: {}, confirm: {}", time, rsi, macd, buy_state.confirm_count);
                buy_state.confirm_count = 0;
                buy_state.prev_macd = None;
            }

            // Sell signals happen here
            if rsi > 70.0 {
                if let None = sell_state.prev_macd {
                    sell_state.prev_macd = Some(macd);
                    println!("rsi sell signal, time: {}, rsi: {}", time, rsi)
                }
            }

            if let Some(pm) = sell_state.prev_macd {
                if macd < pm {
                    sell_state.confirm_count += 1;
                    println!("macd sell signal, time: {}, macd: {}, prev_macd: {:?}, confirm: {}", time, macd, sell_state.prev_macd, sell_state.confirm_count);
                }
                sell_state.prev_macd = Some(macd);
            }

            if sell_state.confirm_count > 1 {
                println!("Sell at, time:{}, rsi: {}, macd: {}, confirm: {}", time, rsi, macd, sell_state.confirm_count);
                sell_state.confirm_count = 0;
                sell_state.prev_macd = None;
            }
        }
    }

    Ok(())
}

struct State {
    prev_macd: Option<f64>,
    confirm_count: usize
}