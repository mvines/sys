use {
    crate::{
        exchange::*,
        token::{MaybeToken, Token},
    },
    async_trait::async_trait,
    chrono::{prelude::*, Duration},
    ftx::rest::{
        CancelOrder, GetFills, GetHistoricalPrices, GetLendingInfo, GetLendingRates, GetMarket,
        GetMyLendingHistory, GetOrder, GetWalletBalances, GetWalletDepositAddress,
        GetWalletDeposits, GetWalletWithdrawals, MyLendingHistory, OrderStatus as FtxOrderStatus,
        OrderType, PlaceOrder, RequestWithdrawal, Rest, Side as FtxOrderSide, SubmitLendingOffer,
        WithdrawStatus,
    },
    rust_decimal::prelude::*,
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::HashMap,
        convert::{TryFrom, TryInto},
    },
};

pub struct FtxExchangeClient {
    rest: Rest,
}

fn binance_to_ftx_pair(binance_pair: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match binance_pair {
        "SOLUSDT" => Ok("SOL/USDT"),
        "SOLUSD" => Ok("SOL/USD"),
        _ => Err(format!("Unknown pair: {}", binance_pair).into()),
    }
}

fn ftx_to_binance_pair(ftx_pair: &str) -> Result<&'static str, Box<dyn std::error::Error>> {
    match ftx_pair {
        "SOL/USDT" => Ok("SOLUSDT"),
        "SOL/USD" => Ok("SOLUSD"),
        _ => Err(format!("Unknown pair: {}", ftx_pair).into()),
    }
}

#[async_trait]
impl ExchangeClient for FtxExchangeClient {
    async fn deposit_address(
        &self,
        token: MaybeToken,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        Ok(self
            .rest
            .request(GetWalletDepositAddress {
                coin: &token.to_string(),
                method: Some("sol"),
            })
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
            .request(GetWalletBalances {})
            .await
            .map_err(|err| format!("{:?}", err))?;

        let mut balances = HashMap::new();
        for coin in ["SOL"].iter().chain(USD_COINS) {
            if let Some(balance) = wallet_balances.iter().find(|b| b.coin == *coin) {
                balances.insert(
                    coin.to_string(),
                    ExchangeBalance {
                        available: balance.available_without_borrow.to_f64().unwrap(),
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
            .request(GetWalletDeposits {
                limit: None,
                start_time: None,
                end_time: None,
            })
            .await
            .map_err(|err| format!("{:?}", err))?
            .into_iter()
            .filter_map(|wd| {
                if wd.status == ftx::rest::DepositStatus::Confirmed {
                    if let Some(tx_id) = wd.txid {
                        return Some(DepositInfo {
                            tx_id,
                            amount: wd.size.unwrap().to_f64().unwrap(),
                        });
                    }
                }
                None
            })
            .collect())
    }

    async fn recent_withdrawals(&self) -> Result<Vec<WithdrawalInfo>, Box<dyn std::error::Error>> {
        Ok(self
            .rest
            .request(GetWalletWithdrawals {
                limit: None,
                start_time: None,
                end_time: None,
            })
            .await
            .map_err(|err| format!("{:?}", err))?
            .into_iter()
            .filter_map(|wd| {
                if let Some(address) = wd.address {
                    if let (Ok(address), tag) = (address.parse::<Pubkey>(), wd.time) {
                        let token = if &wd.coin == "SOL" {
                            None
                        } else {
                            Token::from_str(&wd.coin).ok()
                        };

                        let (completed, tx_id) = match wd.status {
                            WithdrawStatus::Complete => (true, wd.txid),
                            WithdrawStatus::Cancelled => (true, None),
                            _ => (false, None),
                        };

                        return Some(WithdrawalInfo {
                            address,
                            token: token.into(),
                            amount: wd.size.to_f64().unwrap(),
                            tag,
                            completed,
                            tx_id,
                        });
                    }
                }
                None
            })
            .collect())
    }

    async fn request_withdraw(
        &self,
        address: Pubkey,
        token: MaybeToken,
        amount: f64,
        password: Option<String>,
        code: Option<String>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let coin = token.to_string();
        let size = FromPrimitive::from_f64(amount).unwrap();

        let wd = self
            .rest
            .request(RequestWithdrawal {
                coin: coin.clone(),
                size,
                address: address.to_string(),
                tag: None,
                method: Some("sol".into()),
                password,
                code,
            })
            .await?;

        assert_eq!(wd.coin, coin);
        assert_eq!(wd.address, Some(address.to_string()));
        assert_eq!(wd.size, size);
        Ok(wd.time) // `time` field is used as a tag
    }

    async fn print_market_info(
        &self,
        pair: &str,
        format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ftx_pair = binance_to_ftx_pair(pair)?;

        let hourly_prices = self
            .rest
            .request(GetHistoricalPrices {
                market_name: ftx_pair,
                resolution: 3600,
                limit: Some(24),
                start_time: None,
                end_time: None,
            })
            .await
            .unwrap();

        if hourly_prices.len() != 24 {
            return Err(format!(
                "Failed to fetch price data for last 24 hours (fetched {} hours)",
                hourly_prices.len()
            )
            .into());
        }

        let weighted_24h_avg_price = {
            let mut total_volume = 0.;
            let mut avg_price_weighted_sum = 0.;
            for hourly_price in &hourly_prices {
                let avg_price = (hourly_price.low + hourly_price.high).to_f64().unwrap() / 2.;
                let volume = hourly_price.volume.to_f64().unwrap();

                total_volume += volume;
                avg_price_weighted_sum += avg_price * volume;
            }

            avg_price_weighted_sum / total_volume
        };

        let market = self
            .rest
            .request(GetMarket::new(ftx_pair))
            .await
            .map_err(|err| format!("{:?}", err))?;

        match format {
            MarketInfoFormat::All => {
                println!(
                    "{} | Ask: ${:.2}, Bid: ${:.2}, Last: ${:.2}, 24hr Average: ${:.2}",
                    pair,
                    market.ask.unwrap(),
                    market.bid.unwrap(),
                    market.last.unwrap_or_default(),
                    weighted_24h_avg_price
                );
            }
            MarketInfoFormat::Ask => {
                println!("{}", market.ask.unwrap());
            }
            MarketInfoFormat::Hourly => {
                println!("hour,low,high,average,volume");
                for p in &hourly_prices {
                    println!(
                        "{},{},{},{},{}",
                        DateTime::<Local>::from(p.start_time),
                        p.low,
                        p.high,
                        (p.low + p.high).to_f64().unwrap() / 2.,
                        p.volume
                    );
                }
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
            .request(GetMarket::new(pair))
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(BidAsk {
            bid_price: market.bid.unwrap().to_f64().unwrap(),
            ask_price: market.ask.unwrap().to_f64().unwrap(),
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
            .request(PlaceOrder {
                market: pair,
                side,
                price: Some(FromPrimitive::from_f64(price).unwrap()),
                r#type: OrderType::Limit,
                size: FromPrimitive::from_f64(amount).unwrap(),
                reduce_only: false,
                ioc: false,
                post_only: true,
                client_id: None,
                reject_on_price_band: false,
            })
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(order_info.id.to_string())
    }

    async fn cancel_order(
        &self,
        _pair: &str,
        order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_id = order_id.parse()?;

        let result = self
            .rest
            .request(CancelOrder::new(order_id))
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
            .request(GetOrder::new(order_id))
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

        let fills = self
            .rest
            .request(GetFills {
                market_name: &order_info.market,
                order_id: Some(order_id),
                ..GetFills::default()
            })
            .await
            .map_err(|err| format!("{:?}", err))?;

        let mut fee = 0.;
        let mut fee_currency = None;
        for fill in fills {
            fee += fill.fee.to_f64().unwrap();
            if fee_currency == None {
                fee_currency = Some(fill.fee_currency);
            } else {
                assert_eq!(fee_currency, Some(fill.fee_currency));
            }
        }

        Ok(OrderStatus {
            open: order_info.status != FtxOrderStatus::Closed,
            side,
            price: order_info.price.unwrap_or_default().to_f64().unwrap(),
            amount: order_info.size.to_f64().unwrap(),
            filled_amount: order_info.filled_size.unwrap_or_default().to_f64().unwrap(),
            last_update,
            fee: fee_currency.map(|fee_currency| (fee, fee_currency)),
        })
    }

    async fn get_lending_info(
        &self,
        coin: &str,
    ) -> Result<Option<LendingInfo>, Box<dyn std::error::Error>> {
        let lending_info = self.rest.request(GetLendingInfo {}).await.unwrap();
        let lending_rate = self
            .rest
            .request(GetLendingRates {})
            .await
            .unwrap()
            .into_iter()
            .find(|rate| rate.coin == coin)
            .ok_or_else(|| format!("No lending rate available for {}", coin))?;

        const HOURS_PER_YEAR: f64 = 24. * 356.;
        Ok(lending_info
            .iter()
            .find(|lending_info| lending_info.coin == *coin)
            .map(|lending_info| LendingInfo {
                lendable: lending_info.lendable.to_f64().unwrap(),
                locked: lending_info.locked.to_f64().unwrap(),
                offered: lending_info.offered.to_f64().unwrap(),
                estimate_rate: lending_rate.estimate.to_f64().unwrap() * HOURS_PER_YEAR * 100.,
                previous_rate: lending_rate.previous.unwrap_or_default().to_f64().unwrap()
                    * HOURS_PER_YEAR
                    * 100.,
            }))
    }

    async fn get_lending_history(
        &self,
        lending_history: LendingHistory,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let one_day = Duration::days(1);

        let (mut start_time, end_time) = match lending_history {
            LendingHistory::Range {
                start_date,
                end_date,
            } => {
                let start_time = Local.from_local_date(&start_date).unwrap().and_hms(0, 0, 0);
                let end_time = Local
                    .from_local_date(&end_date)
                    .unwrap()
                    .and_hms(23, 59, 59);
                (start_time, end_time)
            }
            LendingHistory::Previous { days } => {
                let end_time = Local::now().date().and_hms(0, 0, 0) + one_day;
                let start_time = end_time - one_day * days as i32;

                (start_time, end_time - Duration::seconds(1))
            }
        };

        println!("Start date: {}", start_time);
        println!("End date:   {}", end_time);

        let mut all_proceeds = HashMap::<String, f64>::default();
        while start_time < end_time {
            let page_end_time = std::cmp::min(start_time + one_day, end_time);

            println!(
                "(Fetching history from {} to {})",
                start_time, page_end_time
            );

            let lending_history = self
                .rest
                .request(GetMyLendingHistory {
                    start_time: Some(start_time.into()),
                    end_time: Some((page_end_time - Duration::seconds(1)).into()),
                })
                .await
                .unwrap();

            for MyLendingHistory { coin, proceeds, .. } in lending_history {
                let entry: &mut f64 = all_proceeds.entry(coin).or_default();
                *entry += f64::try_from(proceeds).unwrap();
            }
            start_time = page_end_time;
        }

        Ok(all_proceeds)
    }

    async fn submit_lending_offer(
        &self,
        coin: &str,
        size: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.rest
            .request(SubmitLendingOffer {
                coin,
                size: size.try_into().unwrap(),
                rate: 0.00000000001_f64.try_into().unwrap(),
            })
            .await?;
        Ok(())
    }
}

pub fn new(
    ExchangeCredentials {
        api_key,
        secret,
        subaccount,
    }: ExchangeCredentials,
) -> Result<FtxExchangeClient, Box<dyn std::error::Error>> {
    Ok(FtxExchangeClient {
        rest: Rest::new(ftx::options::Options {
            endpoint: ftx::options::Endpoint::Com,
            key: Some(api_key),
            secret: Some(secret),
            subaccount,
        }),
    })
}

pub fn new_us(
    ExchangeCredentials {
        api_key,
        secret,
        subaccount,
    }: ExchangeCredentials,
) -> Result<FtxExchangeClient, Box<dyn std::error::Error>> {
    Ok(FtxExchangeClient {
        rest: Rest::new(ftx::options::Options {
            endpoint: ftx::options::Endpoint::Us,
            key: Some(api_key),
            secret: Some(secret),
            subaccount,
        }),
    })
}
