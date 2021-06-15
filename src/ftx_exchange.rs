use {
    crate::exchange::*,
    async_trait::async_trait,
    chrono::prelude::*,
    ftx::rest::{OrderSide, OrderStatus, OrderType, Rest},
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_sdk::pubkey::Pubkey,
};

pub struct FtxExchangeClient {
    rest: Rest,
}

fn binance_to_ftx_pair(binance_pair: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match binance_pair {
        "SOLUSDT" => Ok("SOL/USDT"),
        "SOLUSD" => Ok("SOL/USD"),
        _ => return Err(format!("Unknown pair: {}", binance_pair).into()),
    }
}

fn ftx_to_binance_pair(ftx_pair: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match ftx_pair {
        "SOL/USDT" => Ok("SOLUSDT"),
        "SOL/USD" => Ok("SOLUSD"),
        _ => return Err(format!("Unknown pair: {}", ftx_pair).into()),
    }
}

#[async_trait]
impl ExchangeClient for FtxExchangeClient {
    async fn deposit_address(&self) -> Result<Pubkey, Box<dyn std::error::Error>> {
        Ok(self
            .rest
            .get_wallet_deposit_address("SOL", None)
            .await
            .map_err(|err| format!("{:?}", err))?
            .address
            .parse::<Pubkey>()?)
    }

    async fn balance(&self) -> Result<ExchangeBalance, Box<dyn std::error::Error>> {
        let balances = self
            .rest
            .get_wallet_balances()
            .await
            .map_err(|err| format!("{:?}", err))?;
        let sol_balance = balances
            .iter()
            .find(|b| b.coin == "SOL")
            .ok_or("No SOL balance")?;

        Ok(ExchangeBalance {
            available: sol_balance.free.to_f64().unwrap(),
            total: sol_balance.total.to_f64().unwrap(),
        })
    }

    async fn recent_deposits(&self) -> Result<Vec<DepositInfo>, Box<dyn std::error::Error>> {
        Ok(self
            .rest
            .get_wallet_deposits(None, None, None)
            .await
            .map_err(|err| format!("{:?}", err))?
            .into_iter()
            .filter_map(|wd| {
                if wd.coin == "SOL" && wd.status == ftx::rest::DepositStatus::Confirmed {
                    if let Some(tx_id) = wd.txid {
                        return Some(DepositInfo {
                            tx_id,
                            amount: wd.size.to_f64().unwrap(),
                        });
                    }
                }
                None
            })
            .collect())
    }

    async fn print_market_info(&self, pair: &str) -> Result<(), Box<dyn std::error::Error>> {
        let pair = binance_to_ftx_pair(pair)?;

        let market = self
            .rest
            .get_market(pair)
            .await
            .map_err(|err| format!("{:?}", err))?;

        println!("Price: ${}", market.price);
        println!(
            "Ask: ${}, Bid: ${}, Last: ${}",
            market.ask, market.bid, market.last,
        );
        println!(
            "24h Volume: ${}",
            market
                .volume_usd24h
                .to_f64()
                .unwrap()
                .separated_string_with_fixed_place(2)
        );
        Ok(())
    }

    async fn bid_ask(&self, pair: &str) -> Result<BidAsk, Box<dyn std::error::Error>> {
        let pair = binance_to_ftx_pair(pair)?;
        let market = self
            .rest
            .get_market(pair)
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(BidAsk {
            bid_price: market.bid.to_f64().unwrap(),
            ask_price: market.ask.to_f64().unwrap(),
        })
    }

    async fn place_sell_order(
        &self,
        pair: &str,
        price: f64,
        amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        let pair = binance_to_ftx_pair(pair)?;
        let order_info = self
            .rest
            .place_order(
                &pair,
                OrderSide::Sell,
                Some(FromPrimitive::from_f64(price).unwrap()),
                OrderType::Limit,
                FromPrimitive::from_f64(amount).unwrap(),
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(order_info.id.to_string())
    }

    async fn sell_order_status(
        &self,
        pair: &str,
        order_id: &OrderId,
    ) -> Result<SellOrderStatus, Box<dyn std::error::Error>> {
        let order_id = order_id.parse()?;

        let order_info = self
            .rest
            .get_order(order_id)
            .await
            .map_err(|err| format!("{:?}", err))?;

        assert_eq!(order_info.side, OrderSide::Sell);
        assert_eq!(order_info.r#type, OrderType::Limit);
        assert_eq!(pair, ftx_to_binance_pair(&order_info.market)?);

        // TODO: use `order_info.created_at` instead?
        let last_update = {
            let today = Local::now().date();
            NaiveDate::from_ymd(today.year(), today.month(), today.day())
        };

        Ok(SellOrderStatus {
            open: order_info.status != OrderStatus::Closed,
            price: order_info.price.unwrap_or_default().to_f64().unwrap(),
            amount: order_info.size.to_f64().unwrap(),
            filled_amount: order_info.filled_size.to_f64().unwrap(),
            last_update,
        })
    }
}

pub fn new(
    ExchangeCredentials { api_key, secret }: ExchangeCredentials,
) -> Result<FtxExchangeClient, Box<dyn std::error::Error>> {
    Ok(FtxExchangeClient {
        rest: Rest::new(api_key, secret, None),
    })
}

pub fn new_us(
    ExchangeCredentials { api_key, secret }: ExchangeCredentials,
) -> Result<FtxExchangeClient, Box<dyn std::error::Error>> {
    Ok(FtxExchangeClient {
        rest: Rest::new_us(api_key, secret, None),
    })
}
