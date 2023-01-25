use {
    crate::{exchange::*, token::MaybeToken, token::Token},
    async_trait::async_trait,
    chrono::{Local, TimeZone},
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::HashMap,
        str::FromStr,
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub struct BinanceExchangeClient {
    account: binance::account::Account,
    market: binance::market::Market,
    wallet: binance::wallet::Wallet,
    preferred_solusd_pair: &'static str,
}

#[async_trait]
impl ExchangeClient for BinanceExchangeClient {
    async fn deposit_address(
        &self,
        token: MaybeToken,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        if token != MaybeToken::SOL() {
            return Err(format!("{} deposits are not supported", token).into());
        }

        if !self.account.get_account().await?.can_deposit {
            return Err("deposits not available".into());
        }

        Ok(self
            .wallet
            .deposit_address(binance::rest_model::DepositAddressQuery {
                coin: "SOL".into(),
                network: None,
            })
            .await?
            .address
            .parse::<Pubkey>()?)
    }

    async fn recent_deposits(
        &self,
    ) -> Result<Option<Vec<DepositInfo>>, Box<dyn std::error::Error>> {
        Ok(Some(
            self.wallet
                .deposit_history(&binance::rest_model::DepositHistoryQuery::default())
                .await?
                .into_iter()
                .filter_map(|dr| {
                    /* status codes: 0 = pending, 6 = credited but cannot withdraw, 1 = success */
                    if dr.status == 1 {
                        Some(DepositInfo {
                            tx_id: dr.tx_id,
                            amount: dr.amount,
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        ))
    }

    async fn recent_withdrawals(&self) -> Result<Vec<WithdrawalInfo>, Box<dyn std::error::Error>> {
        Ok(self
            .wallet
            .withdraw_history(&binance::rest_model::WithdrawalHistoryQuery::default())
            .await?
            .into_iter()
            .map(|wr| {
                /* status codes: 0 = email sent, 1 = canceled,   2 =  awaiting approval,
                3 = rejected,   4 = processing, 5 = failure,
                6 = completed */
                let (completed, tx_id) = match wr.status {
                    6 => (true, Some(wr.tx_id.expect("transaction id"))),
                    1 => (true, None),
                    _ => (false, None),
                };

                let token = if &wr.coin == "SOL" {
                    None
                } else {
                    Token::from_str(&wr.coin).ok()
                };
                WithdrawalInfo {
                    address: wr.address.parse::<Pubkey>().unwrap_or_default(),
                    token: token.into(),
                    amount: wr.amount,
                    tag: wr.withdraw_order_id.unwrap_or_default(),
                    completed,
                    tx_id,
                }
            })
            .collect())
    }

    async fn request_withdraw(
        &self,
        address: Pubkey,
        token: MaybeToken,
        amount: f64,
        _withdrawal_password: Option<String>,
        _withdrawal_code: Option<String>,
    ) -> Result<(/* withdraw_id: */ String, /*withdraw_fee: */ f64), Box<dyn std::error::Error>>
    {
        if token != MaybeToken::SOL() {
            return Err(format!("{} deposits are not supported", token).into());
        }

        let sol_info = self
            .wallet
            .all_coin_info()
            .await?
            .into_iter()
            .find(|ci| ci.coin == "SOL")
            .ok_or("SOL not found in Binance coin list")?;

        if !sol_info.deposit_all_enable {
            return Err("SOL deposits not enabled".into());
        }

        let sol_network_info = &sol_info.network_list[0];
        assert_eq!(&sol_network_info.network, "SOL");
        assert_eq!(&sol_network_info.name, "Solana");
        assert_eq!(&sol_network_info.coin, "SOL");

        if !sol_network_info.deposit_enable {
            return Err(format!(
                "Binance deposits disabled: {}",
                sol_network_info.deposit_desc
            )
            .into());
        }

        if !sol_network_info.withdraw_enable {
            return Err(format!(
                "Binance withdrawals disabled: {}",
                sol_network_info.withdraw_desc
            )
            .into());
        }

        if amount < sol_network_info.withdraw_min {
            return Err(format!(
                "Withdrawal request is below the minimum of {} SOL",
                sol_network_info.withdraw_min
            )
            .into());
        }

        let withdraw_fee = sol_network_info.withdraw_fee;

        let withdraw_order_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        self.wallet
            .withdraw(binance::rest_model::CoinWithdrawalQuery {
                coin: token.to_string(),
                network: Some("SOL".into()),
                withdraw_order_id: Some(withdraw_order_id.clone()),
                address: address.to_string(),
                amount,
                ..binance::rest_model::CoinWithdrawalQuery::default()
            })
            .await?;

        Ok((withdraw_order_id, withdraw_fee))
    }

    async fn balances(
        &self,
    ) -> Result<HashMap<String, ExchangeBalance>, Box<dyn std::error::Error>> {
        let account = self.account.get_account().await?;

        let mut balances = HashMap::new();
        for coin in ["SOL"].iter().chain(USD_COINS) {
            if let Some(balance) = account.balances.iter().find(|b| b.asset == *coin) {
                let available = balance.free;
                let total = available + balance.locked;

                balances.insert(coin.to_string(), ExchangeBalance { available, total });
            }
        }

        Ok(balances)
    }

    async fn print_market_info(
        &self,
        pair: &str,
        format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ticker_price = self.market.get_24h_price_stats(pair).await?;

        match format {
            MarketInfoFormat::All => {
                println!("Pair: {}", pair);
                println!(
                    "Ask: ${}, Bid: ${}, High: ${}, Low: ${}, ",
                    ticker_price.ask_price,
                    ticker_price.bid_price,
                    ticker_price.high_price,
                    ticker_price.low_price
                );

                let average_price = self.market.get_average_price(pair).await?;

                println!(
                    "Last {} minute average: ${}",
                    average_price.mins, average_price.price
                );
                println!(
                    "Last 24h change: ${} ({}%)",
                    ticker_price.price_change, ticker_price.price_change_percent
                );
                println!(
                    "Weighted 24h average price: ${}",
                    ticker_price.weighted_avg_price
                );
            }
            MarketInfoFormat::Ask => {
                println!("{}", ticker_price.ask_price);
            }
            MarketInfoFormat::Weighted24hAveragePrice => {
                println!("{}", ticker_price.weighted_avg_price);
            }
            MarketInfoFormat::Hourly => {
                return Err("Hourly market info currently supported for Binance".into())
            }
        }
        Ok(())
    }

    async fn bid_ask(&self, pair: &str) -> Result<BidAsk, Box<dyn std::error::Error>> {
        let binance::rest_model::PriceStats {
            ask_price,
            bid_price,
            ..
        } = self.market.get_24h_price_stats(pair).await?;

        Ok(BidAsk {
            bid_price,
            ask_price,
        })
    }

    async fn place_order(
        &self,
        pair: &str,
        side: OrderSide,
        price: f64,
        amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        // Minimum notional value for orders is $10 USD
        if price * amount < 10. {
            return Err("Total order amount must be 10 or greater".into());
        }

        Ok(self
            .account
            .place_order(binance::account::OrderRequest {
                symbol: pair.into(),
                side: match side {
                    OrderSide::Buy => binance::rest_model::OrderSide::Buy,
                    OrderSide::Sell => binance::rest_model::OrderSide::Sell,
                },
                order_type: binance::rest_model::OrderType::LimitMaker,
                price: Some(price),
                quantity: Some(amount),
                new_order_resp_type: Some(binance::rest_model::OrderResponse::Full),
                ..binance::account::OrderRequest::default()
            })
            .await?
            .client_order_id)
    }

    async fn cancel_order(
        &self,
        pair: &str,
        order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.account
            .cancel_order(binance::account::OrderCancellation {
                symbol: pair.into(),
                order_id: None,
                orig_client_order_id: Some(order_id.into()),
                new_client_order_id: None,
                recv_window: None,
            })
            .await?;

        Ok(())
    }

    async fn order_status(
        &self,
        pair: &str,
        order_id: &OrderId,
    ) -> Result<OrderStatus, Box<dyn std::error::Error>> {
        let order = self
            .account
            .order_status(binance::account::OrderStatusRequest {
                symbol: pair.into(),
                orig_client_order_id: Some(order_id.into()),
                ..binance::account::OrderStatusRequest::default()
            })
            .await?;

        assert_eq!(order.order_type, binance::rest_model::OrderType::LimitMaker);
        assert_eq!(order.time_in_force, binance::rest_model::TimeInForce::GTC);
        assert_eq!(&order.symbol, pair);
        assert_eq!(order.client_order_id, *order_id);

        let last_update = Local
            .timestamp((order.update_time / 1000) as i64, 0)
            .date()
            .naive_local();

        let side = match order.side {
            binance::rest_model::OrderSide::Sell => OrderSide::Sell,
            binance::rest_model::OrderSide::Buy => OrderSide::Buy,
        };

        let trade_fees = self.wallet.trade_fees(Some(pair.to_string())).await?;

        let fee = trade_fees.first().map(|trade_fee| {
            assert_eq!(&trade_fee.symbol, pair);
            (trade_fee.maker_commission * order.executed_qty, {
                // TODO: Avoid hard code and support pairs generically...
                assert!(matches!(trade_fee.symbol.as_str(), "SOLUSD" | "SOLBUSD"));
                if side == OrderSide::Sell {
                    "USD".into()
                } else {
                    "SOL".into()
                }
            })
        });

        Ok(OrderStatus {
            open: matches!(
                order.status,
                binance::rest_model::OrderStatus::New
                    | binance::rest_model::OrderStatus::PartiallyFilled
            ),
            side,
            price: order.price,
            amount: order.orig_qty,
            filled_amount: order.executed_qty,
            last_update,
            fee,
        })
    }

    async fn get_lending_info(
        &self,
        _coin: &str,
    ) -> Result<Option<LendingInfo>, Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Binance".into())
    }

    async fn get_lending_history(
        &self,
        _lending_history: LendingHistory,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Binance".into())
    }

    async fn submit_lending_offer(
        &self,
        _coin: &str,
        _size: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Lending not currently supported for Binance".into())
    }

    fn preferred_solusd_pair(&self) -> &'static str {
        self.preferred_solusd_pair
    }
}

fn _new(
    ExchangeCredentials {
        api_key,
        secret,
        subaccount,
    }: ExchangeCredentials,
    binance_us: bool,
) -> Result<BinanceExchangeClient, Box<dyn std::error::Error>> {
    if subaccount.is_some() {
        return Err("subaccounts not supported".into());
    }

    let config = binance::config::Config {
        rest_api_endpoint: if binance_us {
            "https://api.binance.us"
        } else {
            "https://api.binance.com"
        }
        .into(),
        binance_us_api: binance_us,
        ..binance::config::Config::default()
    };

    let account = binance::api::Binance::new_with_config(
        Some(api_key.clone()),
        Some(secret.clone()),
        &config,
    );

    let market = binance::api::Binance::new_with_config(
        Some(api_key.clone()),
        Some(secret.clone()),
        &config,
    );
    let wallet: binance::wallet::Wallet =
        binance::api::Binance::new_with_config(Some(api_key), Some(secret), &config);

    Ok(BinanceExchangeClient {
        account,
        market,
        wallet,
        preferred_solusd_pair: if binance_us { "SOLUSD" } else { "SOLBUSD" },
    })
}

pub fn new(
    exchange_credentials: ExchangeCredentials,
) -> Result<BinanceExchangeClient, Box<dyn std::error::Error>> {
    _new(exchange_credentials, false)
}

pub fn new_us(
    exchange_credentials: ExchangeCredentials,
) -> Result<BinanceExchangeClient, Box<dyn std::error::Error>> {
    _new(exchange_credentials, true)
}
