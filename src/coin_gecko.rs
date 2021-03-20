use {
    chrono::prelude::*,
    serde::{Deserialize, Serialize},
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

pub async fn get_coin_history(when: NaiveDate) -> Result<MarketData, Box<dyn std::error::Error>> {
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
}
