use {
    crate::{exchange::*, token::MaybeToken},
    async_trait::async_trait,
    futures::{pin_mut, stream::StreamExt},
    rust_decimal::prelude::*,
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
};

pub struct CoinbaseExchangeClient {
    client: coinbase_rs::Private,
}

#[async_trait]
impl ExchangeClient for CoinbaseExchangeClient {
    async fn deposit_address(
        &self,
        token: MaybeToken,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let accounts = self.client.accounts();
        pin_mut!(accounts);

        while let Some(account_result) = accounts.next().await {
            for account in account_result.unwrap() {
                if let Ok(id) = coinbase_rs::Uuid::from_str(&account.id) {
                    if token.name() == account.currency.code
                        && account.primary
                        && account.allow_deposits
                    {
                        let addresses = self.client.list_addresses(&id);
                        pin_mut!(addresses);

                        let mut best_pubkey_updated_at = None;
                        let mut best_pubkey = None;
                        while let Some(addresses_result) = addresses.next().await {
                            for address in addresses_result.unwrap() {
                                if address.network.as_str() == "solana" {
                                    if let Ok(pubkey) = address.address.parse::<Pubkey>() {
                                        if address.updated_at > best_pubkey_updated_at {
                                            best_pubkey_updated_at = address.updated_at;
                                            best_pubkey = Some(pubkey);
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(pubkey) = best_pubkey {
                            return Ok(pubkey);
                        }
                        break;
                    }
                }
            }
        }
        Err(format!("Unsupported deposit token: {}", token.name()).into())
    }

    async fn balances(
        &self,
    ) -> Result<HashMap<String, ExchangeBalance>, Box<dyn std::error::Error>> {
        Err("Balances not supported".into())
    }

    async fn recent_deposits(
        &self,
    ) -> Result<Option<Vec<DepositInfo>>, Box<dyn std::error::Error>> {
        Ok(None) // TODO: Return actual recent deposits. By returning `None`, deposited lots are dropped
                 // once the transaction is confirmed (see `db::drop_deposit()`).
    }

    async fn recent_withdrawals(&self) -> Result<Vec<WithdrawalInfo>, Box<dyn std::error::Error>> {
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
        Err("Withdrawals not supported".into())
    }

    async fn print_market_info(
        &self,
        _pair: &str,
        _format: MarketInfoFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Quotes not supported".into())
    }

    async fn bid_ask(&self, _pair: &str) -> Result<BidAsk, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn place_order(
        &self,
        _pair: &str,
        _side: OrderSide,
        _price: f64,
        _amount: f64,
    ) -> Result<OrderId, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn cancel_order(
        &self,
        _pair: &str,
        _order_id: &OrderId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn order_status(
        &self,
        _pair: &str,
        _order_id: &OrderId,
    ) -> Result<OrderStatus, Box<dyn std::error::Error>> {
        Err("Trading not supported".into())
    }

    async fn get_lending_info(
        &self,
        _coin: &str,
    ) -> Result<Option<LendingInfo>, Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
    }

    async fn get_lending_history(
        &self,
        _lending_history: LendingHistory,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
    }

    async fn submit_lending_offer(
        &self,
        _coin: &str,
        _size: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err("Lending not supported".into())
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
) -> Result<CoinbaseExchangeClient, Box<dyn std::error::Error>> {
    assert!(subaccount.is_none());
    Ok(CoinbaseExchangeClient {
        client: coinbase_rs::Private::new(coinbase_rs::MAIN_URL, &api_key, &secret),
    })
}
