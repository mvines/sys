use {
    chrono::prelude::*,
    serde::{Deserialize, Serialize},
    solana_client::rpc_client::RpcClient,
    solana_sdk::clock::Slot,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct CurrencyList {
    pub usd: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketData {
    pub current_price: CurrencyList,
    pub market_cap: CurrencyList,
    pub total_volume: CurrencyList,
}

// This `derive` requires the `serde` dependency.
#[derive(Debug, Serialize, Deserialize)]
struct HistoryResponse {
    pub id: String,
    pub name: String,
    pub symbol: String,
    pub market_data: Option<MarketData>,
}

async fn get_opening_price(when: NaiveDate) -> Result<f64, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.coingecko.com/api/v3/coins/solana/history?date={}-{}-{}&localization=false",
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

pub async fn get_price(when: NaiveDate) -> Result<f64, Box<dyn std::error::Error>> {
    let opening_price = get_opening_price(when).await?;
    let closing_price = get_opening_price(when + chrono::Duration::days(1)).await?;
    Ok((opening_price + closing_price) / 2.)
}

pub async fn get_current_price() -> Result<f64, Box<dyn std::error::Error>> {
    let today = Local::now().date();
    get_price(NaiveDate::from_ymd(
        today.year(),
        today.month(),
        today.day(),
    ))
    .await
}

pub async fn get_block_date_and_price(
    rpc_client: &RpcClient,
    slot: Slot,
) -> Result<(NaiveDate, f64), Box<dyn std::error::Error>> {
    let block_time = rpc_client.get_block_time(slot)?;
    let local_timestamp = Local.timestamp(block_time, 0);

    let block_date = NaiveDate::from_ymd(
        local_timestamp.year(),
        local_timestamp.month(),
        local_timestamp.day(),
    );

    Ok((block_date, get_price(block_date).await?))
}
