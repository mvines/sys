use {
    crate::exchange::*,
    async_trait::async_trait,
    chrono::prelude::*,
    ftx::rest::{OrderSide as FtxOrderSide, OrderStatus as FtxOrderStatus, OrderType, Rest},
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
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

    async fn balances(
        &self,
    ) -> Result<HashMap<String, ExchangeBalance>, Box<dyn std::error::Error>> {
        let wallet_balances = self
            .rest
            .get_wallet_balances()
            .await
            .map_err(|err| format!("{:?}", err))?;

        let mut balances = HashMap::new();
        for coin in ["SOL"].iter().chain(USD_COINS) {
            if let Some(balance) = wallet_balances.iter().find(|b| b.coin == *coin) {
                balances.insert(
                    coin.to_string(),
                    ExchangeBalance {
                        available: balance.free.to_f64().unwrap(),
                        total: balance.total.to_f64().unwrap(),
                    },
                );
            }
        }
        Ok(balances)
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

    async fn print_market_info(
        &self,
        pair: &str,
        format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ftx_pair = binance_to_ftx_pair(pair)?;

        let weighted_24h_avg_price = {
            let hourly_prices = self
                .rest
                .get_historical_prices(ftx_pair, 3600, Some(24), None, None)
                .await
                .unwrap();
            assert_eq!(hourly_prices.len(), 24);

            let mut total_volume = 0.;
            let mut avg_price_weighted_sum = 0.;
            for hourly_price in hourly_prices {
                let avg_price = (hourly_price.low + hourly_price.high).to_f64().unwrap() / 2.;
                let volume = hourly_price.volume.to_f64().unwrap();

                total_volume += volume;
                avg_price_weighted_sum += avg_price * volume;
            }

            avg_price_weighted_sum / total_volume
        };

        let market = self
            .rest
            .get_market(ftx_pair)
            .await
            .map_err(|err| format!("{:?}", err))?;

        match format {
            MarketInfoFormat::All => {
                println!("Pair: {}", pair);

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
                println!("Weighted 24h average price: ${:.4}", weighted_24h_avg_price);
            }
            MarketInfoFormat::Ask => {
                println!("{}", market.ask);
            }
            MarketInfoFormat::Weighted24hAveragePrice => {
                println!("{:.4}", weighted_24h_avg_price);
            }
        }

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

    async fn place_order(
        &self,
        pair: &str,
        side: OrderSide,
        price: f64,
        amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        let pair = binance_to_ftx_pair(pair)?;
        let side = match side {
            OrderSide::Buy => FtxOrderSide::Buy,
            OrderSide::Sell => FtxOrderSide::Sell,
        };
        let order_info = self
            .rest
            .place_order(
                pair,
                side,
                Some(FromPrimitive::from_f64(price).unwrap()),
                OrderType::Limit,
                FromPrimitive::from_f64(amount).unwrap(),
                None,
                None,
                /*post_only = */ Some(true),
                None,
            )
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(order_info.id.to_string())
    }

    async fn cancel_sell_order(
        &self,
        _pair: &str,
        order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_id = order_id.parse()?;

        let result = self
            .rest
            .cancel_order(order_id)
            .await
            .map_err(|err| format!("{:?}", err))?;

        println!("Result: {}", result);
        Ok(())
    }

    async fn order_status(
        &self,
        pair: &str,
        order_id: &OrderId,
    ) -> Result<OrderStatus, Box<dyn std::error::Error>> {
        let order_id = order_id.parse()?;

        let order_info = self
            .rest
            .get_order(order_id)
            .await
            .map_err(|err| format!("{:?}", err))?;

        let side = match order_info.side {
            FtxOrderSide::Sell => OrderSide::Sell,
            FtxOrderSide::Buy => OrderSide::Buy,
        };
        assert_eq!(order_info.r#type, OrderType::Limit);
        assert_eq!(pair, ftx_to_binance_pair(&order_info.market)?);

        // TODO: use `order_info.created_at` instead?
        let last_update = {
            let today = Local::now().date();
            NaiveDate::from_ymd(today.year(), today.month(), today.day())
        };

        Ok(OrderStatus {
            open: order_info.status != FtxOrderStatus::Closed,
            side,
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
