use {
    crate::coin_gecko,
    chrono::prelude::*,
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
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
    Debug,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    EnumString,
    IntoStaticStr,
    PartialOrd,
    Ord,
)]
#[allow(clippy::upper_case_acronyms)]
#[allow(non_camel_case_types)]
pub enum Token {
    USDC,
    USDS,
    USDT,
    UXD,
    bSOL,
    hSOL,
    mSOL,
    stSOL,
    JitoSOL,
    tuSOL,
    tuUSDC,
    tumSOL,
    tustSOL,
    wSOL,
    JLP,
    JUP,
    JTO,
    BONK,
    KMNO,
    PYTH,
    WEN,
    WIF,
    PYUSD,
}

impl Token {
    pub fn mint(&self) -> Pubkey {
        match self {
            Token::USDC => pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            Token::USDS => pubkey!("USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA"),
            Token::USDT => pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
            Token::UXD => pubkey!("7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT"),
            Token::tuUSDC => pubkey!("Amig8TisuLpzun8XyGfC5HJHHGUQEscjLgoTWsCCKihg"),
            Token::bSOL => pubkey!("bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"),
            Token::hSOL => pubkey!("he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A"),
            Token::mSOL => pubkey!("mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"),
            Token::stSOL => pubkey!("7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj"),
            Token::JitoSOL => pubkey!("J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"),
            Token::tuSOL => pubkey!("H4Q3hDbuMUw8Bu72Ph8oV2xMQ7BFNbekpfQZKS2xF7jW"),
            Token::tumSOL => pubkey!("8cn7JcYVjDZesLa3RTt3NXne4WcDw9PdUneQWuByehwW"),
            Token::tustSOL => pubkey!("27CaAiuFW3EwLcTCaiBnexqm5pxht845AHgSuq36byKX"),
            Token::wSOL => spl_token::native_mint::id(),
            Token::JLP => pubkey!("27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"),
            Token::JUP => pubkey!("JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"),
            Token::JTO => pubkey!("jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL"),
            Token::BONK => pubkey!("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"),
            Token::KMNO => pubkey!("KMNo3nJsBXfcpJTVhZcXLW7RmTwTt4GVFE7suUBo9sS"),
            Token::PYTH => pubkey!("HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3"),
            Token::WEN => pubkey!("WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk"),
            Token::WIF => pubkey!("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"),
            Token::PYUSD => pubkey!("2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo"),
        }
    }

    pub fn program_id(&self) -> Pubkey {
        match self {
            Token::USDC
            | Token::USDS
            | Token::USDT
            | Token::UXD
            | Token::tuUSDC
            | Token::bSOL
            | Token::hSOL
            | Token::mSOL
            | Token::stSOL
            | Token::JitoSOL
            | Token::tuSOL
            | Token::tumSOL
            | Token::tustSOL
            | Token::wSOL
            | Token::JLP
            | Token::JUP
            | Token::JTO
            | Token::BONK
            | Token::KMNO
            | Token::PYTH
            | Token::WEN
            | Token::WIF => spl_token::id(),
            Token::PYUSD => spl_token_2022::id(),
        }
    }
    pub fn ata(&self, wallet_address: &Pubkey) -> Pubkey {
        spl_associated_token_account::get_associated_token_address_with_program_id(
            wallet_address,
            &self.mint(),
            &self.program_id(),
        )
    }

    pub fn symbol(&self) -> &'static str {
        match self {
            Token::USDC => "($)",
            Token::USDS => "USDS$",
            Token::USDT => "USDT$",
            Token::UXD => "UXD$",
            Token::tuUSDC => "tu($)",
            Token::bSOL => "b◎",
            Token::hSOL => "h◎",
            Token::mSOL => "m◎",
            Token::stSOL => "st◎",
            Token::JitoSOL => "jito◎",
            Token::tuSOL => "tu◎",
            Token::tumSOL => "tum◎",
            Token::tustSOL => "tust◎",
            Token::wSOL => "(◎)",
            Token::JLP => "JLP/",
            Token::JUP => "JUP/",
            Token::JTO => "JTO/",
            Token::BONK => "!",
            Token::KMNO => "KMNO/",
            Token::PYTH => "PYTH/",
            Token::WEN => "WEN/",
            Token::WIF => "WIF/",
            Token::PYUSD => "PY($)/",
        }
    }

    pub fn decimals(&self) -> u8 {
        match self {
            Token::BONK | Token::WEN => 5,
            Token::USDC
            | Token::USDS
            | Token::USDT
            | Token::UXD
            | Token::tuUSDC
            | Token::JLP
            | Token::JUP
            | Token::KMNO
            | Token::PYTH
            | Token::WIF => 6,
            Token::PYUSD => 6,
            Token::stSOL
            | Token::tuSOL
            | Token::bSOL
            | Token::hSOL
            | Token::mSOL
            | Token::JitoSOL
            | Token::tumSOL
            | Token::tustSOL
            | Token::JTO
            | Token::wSOL => 9,
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
        // Treat USDC as fully fungible for USD. It can always be redeemed
        // for exactly $1 from Coinbase and Circle
        *self == Self::USDC
    }

    pub fn liquidity_token(&self) -> Option<MaybeToken> {
        None
        /*
        match self {
            Token::USDC
            | Token::USDT
            | Token::UXD
            | Token::bSOL
            | Token::mSOL
            | Token::stSOL
            | Token::JitoSOL
            | Token::wSOL
            | Token::JLP => None,
            | Token::JUP => None,
            Token::tuUSDC | Token::tuSOL | Token::tumSOL | Token::tustSOL => {
                None
                //                Some(crate::tulip::liquidity_token(self))
            }
        }
        */
    }

    pub async fn get_current_liquidity_token_rate(
        &self,
        _rpc_client: &RpcClient,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        unreachable!()
        /*
        match self {
            Token::USDC
            | Token::USDT
            | Token::UXD
            | Token::bSOL
            | Token::mSOL
            | Token::stSOL
            | Token::JitoSOL
            | Token::wSOL
            | Token::JLP => {
                unreachable!()
            }
            Token::tuUSDC | Token::tuSOL | Token::tumSOL | Token::tustSOL => {
                unreachable!()
                //crate::tulip::get_current_liquidity_token_rate(rpc_client, self).await
            }
        }
        */
    }

    pub fn balance(
        &self,
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(u64::from_str(
            &rpc_client
                .get_token_account_balance(&self.ata(address))
                .map_err(|_| {
                    format!(
                        "Could not get balance for account {}, token {}",
                        address,
                        self.name(),
                    )
                })?
                .amount,
        )
        .unwrap_or_default())
    }

    #[async_recursion::async_recursion(?Send)]
    pub async fn get_current_price(
        &self,
        _rpc_client: &RpcClient,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if self.fiat_fungible() {
            return Ok(Decimal::from_f64(1.).unwrap());
        }
        match self {
            Token::USDC
            | Token::USDS
            | Token::USDT
            | Token::UXD
            | Token::bSOL
            | Token::hSOL
            | Token::mSOL
            | Token::stSOL
            | Token::JitoSOL
            | Token::wSOL
            | Token::JLP
            | Token::JUP
            | Token::JTO
            | Token::BONK
            | Token::KMNO
            | Token::PYTH
            | Token::WEN
            | Token::WIF
            | Token::PYUSD => coin_gecko::get_current_price(&MaybeToken(Some(*self))).await,
            Token::tuUSDC | Token::tuSOL | Token::tumSOL | Token::tustSOL => {
                Err("tulip support disabled".into())
                //crate::tulip::get_current_price(rpc_client, self).await
            }
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

    pub fn format_amount(&self, amount: u64) -> String {
        self.format_ui_amount(self.ui_amount(amount))
    }

    pub fn format_ui_amount(&self, ui_amount: f64) -> String {
        format!(
            "{}{}",
            self.symbol(),
            ui_amount.separated_string_with_fixed_place(2)
        )
    }
}

pub fn is_valid_token_or_sol(value: String) -> Result<(), String> {
    if value == "SOL" {
        Ok(())
    } else {
        is_valid_token(value)
    }
}

pub fn is_valid_token(value: String) -> Result<(), String> {
    Token::from_str(&value)
        .map(|_| ())
        .map_err(|_| format!("Invalid token {value}"))
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd)]
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

    pub fn is_sol(&self) -> bool {
        !self.is_token()
    }

    pub fn is_sol_or_wsol(&self) -> bool {
        self.is_sol() || self.token() == Some(Token::wSOL)
    }

    pub fn ui_amount(&self, amount: u64) -> f64 {
        match self.0 {
            None => lamports_to_sol(amount),
            Some(token) => token.ui_amount(amount),
        }
    }

    pub fn mint(&self) -> Pubkey {
        match self.0 {
            None => spl_token::native_mint::id(),
            Some(token) => token.mint(),
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

    pub fn liquidity_token(&self) -> Option<MaybeToken> {
        match self.0 {
            None => None,
            Some(token) => token.liquidity_token(),
        }
    }

    pub async fn get_current_liquidity_token_rate(
        &self,
        rpc_client: &RpcClient,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        match self.0 {
            None => Ok(Decimal::from_usize(1).unwrap()),
            Some(token) => token.get_current_liquidity_token_rate(rpc_client).await,
        }
    }

    pub fn name(&self) -> &'static str {
        match self.0 {
            None => "SOL",
            Some(token) => token.into(),
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
        match self.0 {
            None => Ok(rpc_client
                .get_account_with_commitment(address, rpc_client.commitment())?
                .value
                .map(|account| account.lamports)
                .unwrap_or_default()),
            Some(token) => token.balance(rpc_client, address),
        }
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

    pub fn format_amount(&self, amount: u64) -> String {
        self.format_ui_amount(self.ui_amount(amount))
    }

    pub fn format_ui_amount(&self, ui_amount: f64) -> String {
        format!(
            "{}{}",
            self.symbol(),
            ui_amount.separated_string_with_fixed_place(6)
        )
    }
}

impl From<Option<Token>> for MaybeToken {
    fn from(maybe_token: Option<Token>) -> Self {
        Self(maybe_token)
    }
}

impl From<Token> for MaybeToken {
    fn from(token: Token) -> Self {
        Self(Some(token))
    }
}

impl std::fmt::Display for MaybeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
