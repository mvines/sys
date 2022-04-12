use {
    crate::token::{MaybeToken, Token},
    chrono::prelude::*,
    serde::{Deserialize, Serialize},
};

#[derive(Debug, Serialize, Deserialize)]
struct CurrencyList {
    usd: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MarketData {
    current_price: CurrencyList,
    market_cap: CurrencyList,
    total_volume: CurrencyList,
}

// This `derive` requires the `serde` dependency.
#[derive(Debug, Serialize, Deserialize)]
struct HistoryResponse {
    id: String,
    name: String,
    symbol: String,
    market_data: Option<MarketData>,
}

pub async fn get_current_price(token: &MaybeToken) -> Result<f64, Box<dyn std::error::Error>> {
    get_historical_price(crate::today(), token).await
}

pub async fn get_historical_price(
    when: NaiveDate,
    token: &MaybeToken,
) -> Result<f64, Box<dyn std::error::Error>> {
    let coin = match token.token() {
        None => "solana",
        Some(token) => match token {
            Token::USDC => "usd-coin",
            unsupported_token => {
                return Err(format!(
                    "Coin Gecko price data not available for {}",
                    unsupported_token.name()
                )
                .into())
            }
        },
    };

    let url = format!(
        "https://api.coingecko.com/api/v3/coins/{}/history?date={}-{}-{}&localization=false",
        coin,
        when.day(),
        when.month(),
        when.year()
    );

    reqwest::get(url)
        .await?
        .json::<HistoryResponse>()
        .await?
        .market_data
        .ok_or_else(|| format!("Market data not available for {}", when).into())
        .map(|market_data| market_data.current_price.usd)
}
