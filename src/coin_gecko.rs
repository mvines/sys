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

pub async fn get_price(when: NaiveDate) -> Result<f64, Box<dyn std::error::Error>> {
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

pub async fn get_current_price() -> Result<f64, Box<dyn std::error::Error>> {
    let today = Utc::now().date();
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

    let block_date = NaiveDateTime::from_timestamp_opt(block_time, 0)
        .ok_or_else(|| format!("Invalid block time for slot {}: {}", slot, block_time))?
        .date();
    Ok((block_date, get_price(block_date).await?))
}
