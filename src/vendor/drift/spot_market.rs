use solana_sdk::pubkey::Pubkey;

pub const SIZE: usize = 776;

#[derive(Debug, Clone)]
#[repr(C, packed(1))]
pub struct SpotMarket {
    /// The address of the spot market. It is a pda of the market index
    pub pubkey: Pubkey,
    /// The oracle used to price the markets deposits/borrows
    pub oracle: Pubkey,
    /// The token mint of the market
    pub mint: Pubkey,
    /// The vault used to store the market's deposits
    /// The amount in the vault should be equal to or greater than deposits - borrows
    pub vault: Pubkey,
    /// The encoded display name for the market e.g. SOL
    pub name: [u8; 32],
    pub historical_oracle_data: HistoricalOracleData,
    pub historical_index_data: HistoricalIndexData,
    /// Revenue the protocol has collected in this markets token
    /// e.g. for SOL-PERP, funds can be settled in usdc and will flow into the USDC revenue pool
    pub revenue_pool: PoolBalance, // in base asset
    /// The fees collected from swaps between this market and the quote market
    /// Is settled to the quote markets revenue pool
    pub spot_fee_pool: PoolBalance,
    /// Details on the insurance fund covering bankruptcies in this markets token
    /// Covers bankruptcies for borrows with this markets token and perps settling in this markets token
    pub insurance_fund: InsuranceFund,
    /// The total spot fees collected for this market
    /// precision: QUOTE_PRECISION
    pub total_spot_fee: u128,
    /// The sum of the scaled balances for deposits across users and pool balances
    /// To convert to the deposit token amount, multiply by the cumulative deposit interest
    /// precision: SPOT_BALANCE_PRECISION
    pub deposit_balance: u128,
    /// The sum of the scaled balances for borrows across users and pool balances
    /// To convert to the borrow token amount, multiply by the cumulative borrow interest
    /// precision: SPOT_BALANCE_PRECISION
    pub borrow_balance: u128,
    /// The cumulative interest earned by depositors
    /// Used to calculate the deposit token amount from the deposit balance
    /// precision: SPOT_CUMULATIVE_INTEREST_PRECISION
    pub cumulative_deposit_interest: u128,
    /// The cumulative interest earned by borrowers
    /// Used to calculate the borrow token amount from the borrow balance
    /// precision: SPOT_CUMULATIVE_INTEREST_PRECISION
    pub cumulative_borrow_interest: u128,
    /// The total socialized loss from borrows, in the mint's token
    /// precision: token mint precision
    pub total_social_loss: u128,
    /// The total socialized loss from borrows, in the quote market's token
    /// preicision: QUOTE_PRECISION
    pub total_quote_social_loss: u128,
    /// no withdraw limits/guards when deposits below this threshold
    /// precision: token mint precision
    pub withdraw_guard_threshold: u64,
    /// The max amount of token deposits in this market
    /// 0 if there is no limit
    /// precision: token mint precision
    pub max_token_deposits: u64,
    /// 24hr average of deposit token amount
    /// precision: token mint precision
    pub deposit_token_twap: u64,
    /// 24hr average of borrow token amount
    /// precision: token mint precision
    pub borrow_token_twap: u64,
    /// 24hr average of utilization
    /// which is borrow amount over token amount
    /// precision: SPOT_UTILIZATION_PRECISION
    pub utilization_twap: u64,
    /// Last time the cumulative deposit and borrow interest was updated
    pub last_interest_ts: u64,
    /// Last time the deposit/borrow/utilization averages were updated
    pub last_twap_ts: u64,
    /// The time the market is set to expire. Only set if market is in reduce only mode
    pub expiry_ts: i64,
    /// Spot orders must be a multiple of the step size
    /// precision: token mint precision
    pub order_step_size: u64,
    /// Spot orders must be a multiple of the tick size
    /// precision: PRICE_PRECISION
    pub order_tick_size: u64,
    /// The minimum order size
    /// precision: token mint precision
    pub min_order_size: u64,
    /// The maximum spot position size
    /// if the limit is 0, there is no limit
    /// precision: token mint precision
    pub max_position_size: u64,
    /// Every spot trade has a fill record id. This is the next id to use
    pub next_fill_record_id: u64,
    /// Every deposit has a deposit record id. This is the next id to use
    pub next_deposit_record_id: u64,
    /// The initial asset weight used to calculate a deposits contribution to a users initial total collateral
    /// e.g. if the asset weight is .8, $100 of deposits contributes $80 to the users initial total collateral
    /// precision: SPOT_WEIGHT_PRECISION
    pub initial_asset_weight: u32,
    /// The maintenance asset weight used to calculate a deposits contribution to a users maintenance total collateral
    /// e.g. if the asset weight is .9, $100 of deposits contributes $90 to the users maintenance total collateral
    /// precision: SPOT_WEIGHT_PRECISION
    pub maintenance_asset_weight: u32,
    /// The initial liability weight used to calculate a borrows contribution to a users initial margin requirement
    /// e.g. if the liability weight is .9, $100 of borrows contributes $90 to the users initial margin requirement
    /// precision: SPOT_WEIGHT_PRECISION
    pub initial_liability_weight: u32,
    /// The maintenance liability weight used to calculate a borrows contribution to a users maintenance margin requirement
    /// e.g. if the liability weight is .8, $100 of borrows contributes $80 to the users maintenance margin requirement
    /// precision: SPOT_WEIGHT_PRECISION
    pub maintenance_liability_weight: u32,
    /// The initial margin fraction factor. Used to increase liability weight/decrease asset weight for large positions
    /// precision: MARGIN_PRECISION
    pub imf_factor: u32,
    /// The fee the liquidator is paid for taking over borrow/deposit
    /// precision: LIQUIDATOR_FEE_PRECISION
    pub liquidator_fee: u32,
    /// The fee the insurance fund receives from liquidation
    /// precision: LIQUIDATOR_FEE_PRECISION
    pub if_liquidation_fee: u32,
    /// The optimal utilization rate for this market.
    /// Used to determine the markets borrow rate
    /// precision: SPOT_UTILIZATION_PRECISION
    pub optimal_utilization: u32,
    /// The borrow rate for this market when the market has optimal utilization
    /// precision: SPOT_RATE_PRECISION
    pub optimal_borrow_rate: u32,
    /// The borrow rate for this market when the market has 1000 utilization
    /// precision: SPOT_RATE_PRECISION
    pub max_borrow_rate: u32,
    /// The market's token mint's decimals. To from decimals to a precision, 10^decimals
    pub decimals: u32,
    pub market_index: u16,
    /// Whether or not spot trading is enabled
    pub orders_enabled: bool,
    pub oracle_source: OracleSource,
    pub status: MarketStatus,
    /// The asset tier affects how a deposit can be used as collateral and the priority for a borrow being liquidated
    pub asset_tier: AssetTier,
    pub paused_operations: u8,
    pub if_paused_operations: u8,
    pub fee_adjustment: i16,
    pub padding1: [u8; 2],
    /// For swaps, the amount of token loaned out in the begin_swap ix
    /// precision: token mint precision
    pub flash_loan_amount: u64,
    /// For swaps, the amount in the users token account in the begin_swap ix
    /// Used to calculate how much of the token left the system in end_swap ix
    /// precision: token mint precision
    pub flash_loan_initial_token_amount: u64,
    /// The total fees received from swaps
    /// precision: token mint precision
    pub total_swap_fee: u64,
    /// When to begin scaling down the initial asset weight
    /// disabled when 0
    /// precision: QUOTE_PRECISION
    pub scale_initial_asset_weight_start: u64,
    pub padding: [u8; 48],
}

#[derive(Clone, Copy, PartialEq, Debug, Eq, PartialOrd, Ord)]
pub enum AssetTier {
    /// full priviledge
    Collateral,
    /// collateral, but no borrow
    Protected,
    /// not collateral, allow multi-borrow
    Cross,
    /// not collateral, only single borrow
    Isolated,
    /// no privilege
    Unlisted,
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum MarketStatus {
    /// warm up period for initialization, fills are paused
    Initialized,
    /// all operations allowed
    Active,
    /// Deprecated in favor of PausedOperations
    FundingPaused,
    /// Deprecated in favor of PausedOperations
    AmmPaused,
    /// Deprecated in favor of PausedOperations
    FillPaused,
    /// Deprecated in favor of PausedOperations
    WithdrawPaused,
    /// fills only able to reduce liability
    ReduceOnly,
    /// market has determined settlement price and positions are expired must be settled
    Settlement,
    /// market has no remaining participants
    Delisted,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct PoolBalance {
    /// To get the pool's token amount, you must multiply the scaled balance by the market's cumulative
    /// deposit interest
    /// precision: SPOT_BALANCE_PRECISION
    pub scaled_balance: u128,
    /// The spot market the pool is for
    pub market_index: u16,
    pub padding: [u8; 6],
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum OracleSource {
    Pyth,
    Switchboard,
    QuoteAsset,
    Pyth1K,
    Pyth1M,
    PythStableCoin,
    Prelaunch,
    PythPull,
    Pyth1KPull,
    Pyth1MPull,
    PythStableCoinPull,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
#[repr(C, packed(1))]
pub struct HistoricalOracleData {
    /// precision: PRICE_PRECISION
    pub last_oracle_price: i64,
    /// precision: PRICE_PRECISION
    pub last_oracle_conf: u64,
    /// number of slots since last update
    pub last_oracle_delay: i64,
    /// precision: PRICE_PRECISION
    pub last_oracle_price_twap: i64,
    /// precision: PRICE_PRECISION
    pub last_oracle_price_twap_5min: i64,
    /// unix_timestamp of last snapshot
    pub last_oracle_price_twap_ts: i64,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct HistoricalIndexData {
    /// precision: PRICE_PRECISION
    pub last_index_bid_price: u64,
    /// precision: PRICE_PRECISION
    pub last_index_ask_price: u64,
    /// precision: PRICE_PRECISION
    pub last_index_price_twap: u64,
    /// precision: PRICE_PRECISION
    pub last_index_price_twap_5min: u64,
    /// unix_timestamp of last snapshot
    pub last_index_price_twap_ts: i64,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct InsuranceFund {
    pub vault: Pubkey,
    pub total_shares: u128,
    pub user_shares: u128,
    pub shares_base: u128,     // exponent for lp shares (for rebasing)
    pub unstaking_period: i64, // if_unstaking_period
    pub last_revenue_settle_ts: i64,
    pub revenue_settle_period: i64,
    pub total_factor: u32, // percentage of interest for total insurance
    pub user_factor: u32,  // percentage of interest for user staked insurance
}
