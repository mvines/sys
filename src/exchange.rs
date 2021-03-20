use {
    serde::{Deserialize, Serialize},
    std::str::FromStr,
    thiserror::Error,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    BinanceUs,
}

impl FromStr for Exchange {
    type Err = ParseExchangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "binance" => Ok(Exchange::Binance),
            "binanceus" => Ok(Exchange::BinanceUs),
            _ => Err(ParseExchangeError::Invalid),
        }
    }
}

#[derive(Error, Debug)]
pub enum ParseExchangeError {
    #[error("invalid variant")]
    Invalid,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ExchangeCredentials {
    BinanceApi { api_key: String, secret_key: String },
}

pub const BINANCE_URL: &str = "https://api.binance.com";
pub const BINANCE_US_URL: &str = "https://api.binance.us";
