use {
    crate::coin_gecko,
    chrono::prelude::*,
    rust_decimal::prelude::*,
    serde::{Deserialize, Serialize},
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        native_token::{lamports_to_sol, sol_to_lamports},
        pubkey,
        pubkey::Pubkey,
    },
    std::str::FromStr,
    strum::{EnumString, IntoStaticStr},
};

#[derive(
    Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize, EnumString, IntoStaticStr,
)]
#[allow(clippy::upper_case_acronyms)]
#[allow(non_camel_case_types)]
pub enum Token {
    USDC,
    tuUSDC,
    tuSOL,
}

impl Token {
    pub fn mint(&self) -> Pubkey {
        match self {
            Token::USDC => pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            Token::tuUSDC => pubkey!("Amig8TisuLpzun8XyGfC5HJHHGUQEscjLgoTWsCCKihg"),
            Token::tuSOL => pubkey!("H4Q3hDbuMUw8Bu72Ph8oV2xMQ7BFNbekpfQZKS2xF7jW"),
        }
    }

    pub fn ata(&self, wallet_address: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address(wallet_address, &self.mint())
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            Token::USDC => "($)",
            Token::tuUSDC => "🌷($)",
            Token::tuSOL => "🌷◎",
        }
    }

    pub fn decimals(&self) -> u8 {
        match self {
            Token::USDC => 6,
            Token::tuUSDC => 6,
            Token::tuSOL => 9,
        }
    }

    pub fn ui_amount(&self, amount: u64) -> f64 {
        spl_token::amount_to_ui_amount(amount, self.decimals())
    }

    pub fn amount(&self, ui_amount: f64) -> u64 {
        spl_token::ui_amount_to_amount(ui_amount, self.decimals())
    }

    pub fn name(&self) -> &'static str {
        self.into()
    }

    pub fn fiat_fungible(&self) -> bool {
        // Treat USDC as fully fungible for USD (following FTX's lead)
        *self == Self::USDC
    }

    pub async fn get_current_price(
        &self,
        rpc_client: &RpcClient,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if self.fiat_fungible() {
            return Ok(Decimal::from_f64(1.).unwrap());
        }
        match self {
            Token::USDC => coin_gecko::get_current_price(&MaybeToken(Some(*self))).await,
            Token::tuUSDC | Token::tuSOL => crate::tulip::get_current_price(rpc_client, self),
        }
    }

    pub async fn get_historical_price(
        &self,
        _rpc_client: &RpcClient,
        when: NaiveDate,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if self.fiat_fungible() {
            return Ok(Decimal::from_f64(1.).unwrap());
        }
        match self {
            Token::USDC => coin_gecko::get_historical_price(when, &MaybeToken(Some(*self))).await,
            unsupported_token => Err(format!(
                "Historical price data is not available for {}",
                unsupported_token.name()
            )
            .into()),
        }
    }
}

pub fn is_valid_token(value: String) -> Result<(), String> {
    Token::from_str(&value)
        .map(|_| ())
        .map_err(|_| format!("Invalid token {}", value))
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MaybeToken(Option<Token>);

impl MaybeToken {
    #[allow(non_snake_case)]
    pub fn SOL() -> Self {
        Self(None)
    }

    pub fn token(&self) -> Option<Token> {
        self.0
    }

    pub fn is_token(&self) -> bool {
        self.token().is_some()
    }

    pub fn ui_amount(&self, amount: u64) -> f64 {
        match self.0 {
            None => lamports_to_sol(amount),
            Some(token) => token.ui_amount(amount),
        }
    }

    pub fn amount(&self, ui_amount: f64) -> u64 {
        match self.0 {
            None => sol_to_lamports(ui_amount),
            Some(token) => token.amount(ui_amount),
        }
    }

    pub fn symbol(&self) -> &'static str {
        match self.0 {
            None => "◎",
            Some(token) => token.symbol(),
        }
    }

    pub fn fiat_fungible(&self) -> bool {
        match self.0 {
            None => false,
            Some(token) => token.fiat_fungible(),
        }
    }

    pub fn balance(
        &self,
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(match self.0 {
            None => rpc_client
                .get_account_with_commitment(address, rpc_client.commitment())?
                .value
                .map(|account| account.lamports)
                .unwrap_or_default(),
            Some(token) => u64::from_str(
                &rpc_client
                    .get_token_account_balance(&token.ata(address))
                    .map_err(|_| {
                        format!(
                            "Could not get balance for account {}, token {}",
                            address,
                            token.name(),
                        )
                    })?
                    .amount,
            )
            .unwrap_or_default(),
        })
    }

    pub async fn get_current_price(
        &self,
        rpc_client: &RpcClient,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        match self.0 {
            None => coin_gecko::get_current_price(self).await,
            Some(token) => token.get_current_price(rpc_client).await,
        }
    }

    pub async fn get_historical_price(
        &self,
        rpc_client: &RpcClient,
        when: NaiveDate,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        match self.0 {
            None => coin_gecko::get_historical_price(when, self).await,
            Some(token) => token.get_historical_price(rpc_client, when).await,
        }
    }
}

impl From<Option<Token>> for MaybeToken {
    fn from(maybe_token: Option<Token>) -> Self {
        Self(maybe_token)
    }
}

impl std::fmt::Display for MaybeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self.0 {
                None => "SOL",
                Some(token) => token.name(),
            }
        )
    }
}
