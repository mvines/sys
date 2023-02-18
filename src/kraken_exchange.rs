use {
    crate::{exchange::*, token::MaybeToken},
    async_trait::async_trait,
    chrono::prelude::*,
    kraken_sdk_rest::Client,
    rust_decimal::prelude::*,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

pub struct KrakenExchangeClient {
    client: Client,
}

fn normalize_coin_name(kraken_coin: &str) -> &str {
    if kraken_coin == "ZUSD" {
        "USD"
    } else {
        kraken_coin
    }
}

fn deposit_methods() -> HashMap</*coin: */ &'static str, /* method: */ &'static str> {
    HashMap::from([
        ("SOL", "Solana"),
        ("USDC", "USDC (SPL)"),
        ("mSOL", "Marinade SOL (mSOL)"),
    ])
}

#[async_trait]
impl ExchangeClient for KrakenExchangeClient {
    async fn deposit_address(
        &self,
        token: MaybeToken,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let deposit_method = *deposit_methods().get(token.name()).ok_or_else(|| {
            //dbg!(self.client.get_deposit_methods(token.to_string()).send().await?);
            format!("Unsupported deposit token: {}", token.name())
        })?;

        let deposit_addresses = self
            .client
            .get_deposit_addresses(token.to_string(), deposit_method)
            .send()
            .await?;

        assert_eq!(deposit_addresses.len(), 1); // TODO: Consider what to do with multiple deposit addresses

        Ok(deposit_addresses[0].address.parse::<Pubkey>()?)
    }

    async fn balances(
        &self,
    ) -> Result<HashMap<String, ExchangeBalance>, Box<dyn std::error::Error>> {
        //dbg!(self.client.get_open_orders().send().await?);
        let open_orders = self.client.get_open_orders().send().await?;

        // TODO: Generalize the `in_order_sol`/`in_order_usd` handling to all coins held by the
        // account
        let mut in_order_sol = 0.;
        let mut in_order_usd = 0.;

        for open_order in open_orders.open.values() {
            assert_eq!(open_order.status, "open"); // TODO: What other statuses are valid, if any?
            if open_order.descr.pair == self.preferred_solusd_pair() {
                let vol = open_order
                    .vol
                    .parse::<f64>()
                    .map_err(|err| format!("Invalid open order `vol` field: {err}"))?;
                let price =
                    open_order.descr.price.parse::<f64>().map_err(|err| {
                        format!("Invalid open order `descr.price` field: {err}")
                    })?;
                if open_order.descr.orderside == "sell" {
                    in_order_sol += vol
                } else {
                    in_order_usd += vol * price;
                }
            }
        }

        Ok(self
            .client
            .get_account_balance()
            .send()
            .await?
            .into_iter()
            .filter_map(|(coin, balance)| {
                balance
                    .parse::<f64>()
                    .ok()
                    .and_then(|balance| match coin.as_str() {
                        "SOL" => {
                            assert!(balance >= in_order_sol);
                            Some(ExchangeBalance {
                                total: balance,
                                available: balance - in_order_sol,
                            })
                        }
                        "USDC" => Some(ExchangeBalance {
                            total: balance,
                            available: balance,
                        }),
                        "ZUSD" => {
                            assert!(balance >= in_order_usd);
                            Some(ExchangeBalance {
                                total: balance,
                                available: balance - in_order_usd,
                            })
                        }
                        _ => None,
                    })
                    .map(|exchange_balance| (normalize_coin_name(&coin).into(), exchange_balance))
            })
            .collect())
    }

    async fn recent_deposits(
        &self,
    ) -> Result<Option<Vec<DepositInfo>>, Box<dyn std::error::Error>> {
        let mut successful_deposits = vec![];

        for coin in deposit_methods().keys() {
            for deposit_status in self.client.get_deposit_status(*coin).send().await? {
                //dbg!(&deposit_status);
                if deposit_status.status == "Success" {
                    successful_deposits.push(DepositInfo {
                        tx_id: deposit_status.txid,
                        amount: deposit_status.amount.parse::<f64>().unwrap(),
                    });
                }
            }
        }
        Ok(Some(successful_deposits))
    }

    async fn recent_withdrawals(&self) -> Result<Vec<WithdrawalInfo>, Box<dyn std::error::Error>> {
        // Withdrawals not currently supported for Kraken
        Ok(vec![])
    }

    async fn request_withdraw(
        &self,
        _address: Pubkey,
        _token: MaybeToken,
        _amount: f64,
        _password: Option<String>,
        _code: Option<String>,
    ) -> Result<(/* withdraw_id: */ String, /*withdraw_fee: */ f64), Box<dyn std::error::Error>>
    {
        Err("Withdrawals not currently supported for Kraken".into())
    }

    async fn print_market_info(
        &self,
        pair: &str,
        format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug)]
        struct Hlv {
            time: DateTime<Utc>,
            high: f64,
            low: f64,
            volume: f64,
        }

        let hourly_prices = self
            .client
            .get_ohlc_data(pair)
            .interval(kraken_sdk_rest::Interval::Hour1)
            .send()
            .await?
            .into_iter()
            .rev()
            .take(24)
            .filter_map(|ohlc| {
                if let (Some(high), Some(low), Some(volume)) = (
                    ohlc.high().parse::<f64>().ok(),
                    ohlc.low().parse::<f64>().ok(),
                    ohlc.volume().parse::<f64>().ok(),
                ) {
                    let naive = NaiveDateTime::from_timestamp(ohlc.time(), 0);
                    let time: DateTime<Utc> = DateTime::from_utc(naive, Utc);

                    Some(Hlv {
                        time,
                        high,
                        low,
                        volume,
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

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

        let bid_ask = self.bid_ask(pair).await?;

        match format {
            MarketInfoFormat::All => {
                println!(
                    "{} | Ask: ${:.2}, Bid: ${:.2}, 24hr Average: ${:.2}",
                    pair, bid_ask.ask_price, bid_ask.bid_price, weighted_24h_avg_price
                );
            }
            MarketInfoFormat::Ask => {
                println!("{}", bid_ask.ask_price);
            }
            MarketInfoFormat::Hourly => {
                println!("hour,low,high,average,volume");
                for p in &hourly_prices {
                    println!(
                        "{},{},{},{},{}",
                        DateTime::<Local>::from(p.time),
                        p.low,
                        p.high,
                        (p.low + p.high).to_f64().unwrap() / 2.,
                        p.volume
                    );
                }
            }
            MarketInfoFormat::Weighted24hAveragePrice => {
                println!("{weighted_24h_avg_price:.4}");
            }
        }

        Ok(())
    }

    async fn bid_ask(&self, pair: &str) -> Result<BidAsk, Box<dyn std::error::Error>> {
        let response = self.client.get_order_book(pair).count(1).send().await?;

        if let Some(order_book) = response.get(pair) {
            if let (Some(ask_price), Some(bid_price)) = (
                order_book
                    .asks
                    .get(0)
                    .and_then(|order_book_tier| order_book_tier.0.parse::<f64>().ok()),
                order_book
                    .bids
                    .get(0)
                    .and_then(|order_book_tier| order_book_tier.0.parse::<f64>().ok()),
            ) {
                return Ok(BidAsk {
                    bid_price,
                    ask_price,
                });
            }
        }
        Err("Invalid API response".into())
    }

    async fn place_order(
        &self,
        pair: &str,
        side: OrderSide,
        price: f64,
        amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        if pair != self.preferred_solusd_pair() {
            // Currently only the `preferred_solusd_pair` is supported due to limitations in how
            // the `available` token balances are computed in `Self::balances()`
            return Err(format!("Unsupported trading pair: {pair}").into());
        }

        let side = match side {
            OrderSide::Buy => kraken_sdk_rest::OrderSide::Buy,
            OrderSide::Sell => kraken_sdk_rest::OrderSide::Sell,
        };

        let response = self
            .client
            .add_limit_order(pair, side, &amount.to_string(), &price.to_string())
            .post_only()
            .send()
            .await?;
        //dbg!(&response);

        let txid = response.txid.unwrap_or_default();
        assert_eq!(txid.len(), 1);
        Ok(txid[0].to_owned())
    }

    async fn cancel_order(
        &self,
        _pair: &str,
        order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.client.cancel_order(order_id).send().await?;
        Ok(())
    }

    async fn order_status(
        &self,
        pair: &str,
        order_id: &OrderId,
    ) -> Result<OrderStatus, Box<dyn std::error::Error>> {
        let orders = self.client.query_orders_info(order_id).send().await?;

        let order = orders
            .get(order_id)
            .ok_or_else(|| format!("Unknown order id: {order_id}"))?;
        //dbg!(&order);

        assert_eq!(order.descr.ordertype, "limit");

        // Currently only the `preferred_solusd_pair` is supported due to limitations in how
        // the `available` token balances are computed in `Self::balances()`
        assert_eq!(order.descr.pair, self.preferred_solusd_pair());
        assert_eq!(order.descr.pair, pair);

        let fee = {
            let fee = order.fee.parse::<f64>().unwrap();
            if fee > f64::EPSILON {
                Some((fee, "USD".to_string()))
            } else {
                None
            }
        };

        // TODO: use `order.opentm` instead?
        let last_update = {
            let today = Local::now().date();
            NaiveDate::from_ymd(today.year(), today.month(), today.day())
        };

        Ok(OrderStatus {
            open: ["open"].contains(&order.status.as_str()),
            side: match order.descr.orderside.as_str() {
                "sell" => OrderSide::Sell,
                "buy" => OrderSide::Buy,
                side => panic!("Invalid order side: {side}"),
            },
            price: order.descr.price.parse::<f64>().unwrap(),
            amount: order.vol.parse::<f64>().unwrap(),
            filled_amount: order.vol_exec.parse::<f64>().unwrap(),
            last_update,
            fee,
        })
    }

    async fn get_lending_info(
        &self,
        _coin: &str,
    ) -> Result<Option<LendingInfo>, Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Kraken".into())
    }

    async fn get_lending_history(
        &self,
        _lending_history: LendingHistory,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Kraken".into())
    }

    async fn submit_lending_offer(
        &self,
        _coin: &str,
        _size: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Kraken".into())
    }

    fn preferred_solusd_pair(&self) -> &'static str {
        "SOLUSD"
    }
}

pub fn new(
    ExchangeCredentials {
        api_key,
        secret,
        subaccount,
    }: ExchangeCredentials,
) -> Result<KrakenExchangeClient, Box<dyn std::error::Error>> {
    if subaccount.is_some() {
        return Err("subaccounts not supported".into());
    }

    Ok(KrakenExchangeClient {
        client: Client::new(&api_key, &secret),
    })
}
