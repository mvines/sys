use {
    serde::{Deserialize, Serialize},
    std::str::FromStr,
    thiserror::Error,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    BinanceUs,
    Ftx,
    FtxUs,
}

impl FromStr for Exchange {
    type Err = ParseExchangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Binance" | "binance" => Ok(Exchange::Binance),
            "BinanceUs" | "binanceus" => Ok(Exchange::BinanceUs),
            "Ftx" | "ftx" => Ok(Exchange::Ftx),
            "FtxUs" | "ftxus" => Ok(Exchange::FtxUs),
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
pub struct ExchangeCredentials {
    pub api_key: String,
    pub secret: String,
}

pub const BINANCE_URL: &str = "https://api.binance.com";
pub const BINANCE_US_URL: &str = "https://api.binance.us";
