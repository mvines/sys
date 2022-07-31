pub use influxdb_client::Client;
use {
    influxdb_client::{Point, Precision},
    serde::{Deserialize, Serialize},
    std::sync::Arc,
    tokio::sync::RwLock,
};

lazy_static::lazy_static! {
    static ref POINTS: Arc<RwLock<Vec<Point>>> = Arc::new(RwLock::new(vec![]));
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub url: String,
    pub token: String,
    pub org_id: String,
    pub bucket: String,
}

pub async fn push(point: Point) {
    POINTS.write().await.push(point);
}

pub async fn send(config: Option<MetricsConfig>) {
    if let Some(config) = config {
        let client = Client::new(config.url, config.token)
            .with_org_id(config.org_id)
            .with_bucket(config.bucket)
            .with_precision(Precision::MS);
        // let client = client.insert_to_stdout();
        client
            .insert_points(
                &*POINTS.write().await,
                influxdb_client::TimestampOptions::FromPoint,
            )
            .await
            .unwrap_or_else(|err| eprintln!("Failed to send metrics: {:?}", err));
    }
}

pub mod dp {
    use {
        crate::{
            exchange::{Exchange, OrderSide},
            token::MaybeToken,
        },
        chrono::Utc,
        influxdb_client::{Point, Timestamp, Value},
        solana_sdk::pubkey::Pubkey,
    };

    fn pubkey_to_value(p: &Pubkey) -> Value {
        Value::Str(p.to_string())
    }

    fn now() -> Timestamp {
        Timestamp::Int(Utc::now().timestamp_millis())
    }

    pub fn exchange_deposit(exchange: Exchange, maybe_token: MaybeToken, ui_amount: f64) -> Point {
        Point::new("exchange_deposit")
            .tag("exchange", exchange.to_string().as_str())
            .tag("token", maybe_token.name())
            .field("amount", ui_amount)
            .timestamp(now())
    }

    pub fn exchange_withdrawal(
        exchange: Exchange,
        maybe_token: MaybeToken,
        address: &Pubkey,
        ui_amount: f64,
    ) -> Point {
        Point::new("exchange_withdrawal")
            .tag("exchange", exchange.to_string().as_str())
            .tag("token", maybe_token.name())
            .tag("address", pubkey_to_value(address))
            .field("amount", ui_amount)
            .timestamp(now())
    }

    pub fn exchange_fill(
        exchange: Exchange,
        pair: &str,
        side: OrderSide,
        maybe_token: MaybeToken,
        amount: f64,
        price: f64,
    ) -> Point {
        Point::new("exchange_fill")
            .tag("exchange", exchange.to_string().as_str())
            .tag("pair", pair)
            .tag("side", side.to_string().as_str())
            .tag("token", maybe_token.name())
            .field("price", price)
            .field("amount", amount)
            .timestamp(now())
    }
}
