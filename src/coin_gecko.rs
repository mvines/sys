use {
    crate::token::{MaybeToken, Token},
    chrono::prelude::*,
    rust_decimal::prelude::*,
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

fn token_to_coin(token: &MaybeToken) -> Result<&'static str, Box<dyn std::error::Error>> {
    let coin = match token.token() {
        None => "solana",
        Some(token) => match token {
            Token::USDC => "usd-coin",
            Token::mSOL => "msol",
            Token::stSOL => "lido-staked-sol",
            Token::wSOL => "solana",
            unsupported_token => {
                return Err(format!(
                    "Coin Gecko price data not available for {}",
                    unsupported_token.name()
                )
                .into())
            }
        },
    };
    Ok(coin)
}

pub async fn get_current_price(token: &MaybeToken) -> Result<Decimal, Box<dyn std::error::Error>> {
    let coin = token_to_coin(token)?;
    let url = format!("https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd");

    #[derive(Debug, Serialize, Deserialize)]
    struct Coins {
        solana: Option<CurrencyList>,
        msol: Option<CurrencyList>,
        #[serde(rename = "lido-staked-sol")]
        stsol: Option<CurrencyList>,
    }

    let coins = reqwest::get(url).await?.json::<Coins>().await?;

    coins
        .solana
        .or(coins.msol)
        .or(coins.stsol)
        .ok_or_else(|| format!("Simple price data not available for {coin}").into())
        .map(|price| Decimal::from_f64(price.usd).unwrap())
}

pub async fn get_historical_price(
    when: NaiveDate,
    token: &MaybeToken,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    let coin = token_to_coin(token)?;

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
        .ok_or_else(|| format!("Market data not available for {when}").into())
        .map(|market_data| Decimal::from_f64(market_data.current_price.usd).unwrap())
}
