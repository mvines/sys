use solana_sdk::pubkey::Pubkey;

pub const SIZE: usize = 4376;

#[derive(Eq, PartialEq, Debug)]
#[repr(C)]
pub struct User {
    /// The owner/authority of the account
    pub authority: Pubkey,
    /// An addresses that can control the account on the authority's behalf. Has limited power, cant withdraw
    pub delegate: Pubkey,
    /// Encoded display name e.g. "toly"
    pub name: [u8; 32],
    /// The user's spot positions
    pub spot_positions: [SpotPosition; 8],
    /// The user's perp positions
    pub perp_positions: [PerpPosition; 8],
    /// The user's orders
    pub orders: [Order; 32],
    /// The last time the user added perp lp positions
    pub last_add_perp_lp_shares_ts: i64,
    /// The total values of deposits the user has made
    /// precision: QUOTE_PRECISION
    pub total_deposits: u64,
    /// The total values of withdrawals the user has made
    /// precision: QUOTE_PRECISION
    pub total_withdraws: u64,
    /// The total socialized loss the users has incurred upon the protocol
    /// precision: QUOTE_PRECISION
    pub total_social_loss: u64,
    /// Fees (taker fees, maker rebate, referrer reward, filler reward) and pnl for perps
    /// precision: QUOTE_PRECISION
    pub settled_perp_pnl: i64,
    /// Fees (taker fees, maker rebate, filler reward) for spot
    /// precision: QUOTE_PRECISION
    pub cumulative_spot_fees: i64,
    /// Cumulative funding paid/received for perps
    /// precision: QUOTE_PRECISION
    pub cumulative_perp_funding: i64,
    /// The amount of margin freed during liquidation. Used to force the liquidation to occur over a period of time
    /// Defaults to zero when not being liquidated
    /// precision: QUOTE_PRECISION
    pub liquidation_margin_freed: u64,
    /// The last slot a user was active. Used to determine if a user is idle
    pub last_active_slot: u64,
    /// Every user order has an order id. This is the next order id to be used
    pub next_order_id: u32,
    /// Custom max initial margin ratio for the user
    pub max_margin_ratio: u32,
    /// The next liquidation id to be used for user
    pub next_liquidation_id: u16,
    /// The sub account id for this user
    pub sub_account_id: u16,
    /// Whether the user is active, being liquidated or bankrupt
    pub status: u8,
    /// Whether the user has enabled margin trading
    pub is_margin_trading_enabled: bool,
    /// User is idle if they haven't interacted with the protocol in 1 week and they have no orders, perp positions or borrows
    /// Off-chain keeper bots can ignore users that are idle
    pub idle: bool,
    /// number of open orders
    pub open_orders: u8,
    /// Whether or not user has open order
    pub has_open_order: bool,
    /// number of open orders with auction
    pub open_auctions: u8,
    /// Whether or not user has open order with auction
    pub has_open_auction: bool,
    pub padding: [u8; 21],
}

#[derive(Eq, PartialEq, Debug)]
#[repr(C)]
pub struct SpotPosition {
    /// The scaled balance of the position. To get the token amount, multiply by the cumulative deposit/borrow
    /// interest of corresponding market.
    /// precision: SPOT_BALANCE_PRECISION
    pub scaled_balance: u64,
    /// How many spot bids the user has open
    /// precision: token mint precision
    pub open_bids: i64,
    /// How many spot asks the user has open
    /// precision: token mint precision
    pub open_asks: i64,
    /// The cumulative deposits/borrows a user has made into a market
    /// precision: token mint precision
    pub cumulative_deposits: i64,
    /// The market index of the corresponding spot market
    pub market_index: u16,
    /// Whether the position is deposit or borrow
    pub balance_type: SpotBalanceType,
    /// Number of open orders
    pub open_orders: u8,
    pub padding: [u8; 4],
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpotBalanceType {
    Deposit,
    Borrow,
}

#[derive(Debug, Eq, PartialEq)]
#[repr(C)]
pub struct PerpPosition {
    /// The perp market's last cumulative funding rate. Used to calculate the funding payment owed to user
    /// precision: FUNDING_RATE_PRECISION
    pub last_cumulative_funding_rate: i64,
    /// the size of the users perp position
    /// precision: BASE_PRECISION
    pub base_asset_amount: i64,
    /// Used to calculate the users pnl. Upon entry, is equal to base_asset_amount * avg entry price - fees
    /// Updated when the user open/closes position or settles pnl. Includes fees/funding
    /// precision: QUOTE_PRECISION
    pub quote_asset_amount: i64,
    /// The amount of quote the user would need to exit their position at to break even
    /// Updated when the user open/closes position or settles pnl. Includes fees/funding
    /// precision: QUOTE_PRECISION
    pub quote_break_even_amount: i64,
    /// The amount quote the user entered the position with. Equal to base asset amount * avg entry price
    /// Updated when the user open/closes position. Excludes fees/funding
    /// precision: QUOTE_PRECISION
    pub quote_entry_amount: i64,
    /// The amount of open bids the user has in this perp market
    /// precision: BASE_PRECISION
    pub open_bids: i64,
    /// The amount of open asks the user has in this perp market
    /// precision: BASE_PRECISION
    pub open_asks: i64,
    /// The amount of pnl settled in this market since opening the position
    /// precision: QUOTE_PRECISION
    pub settled_pnl: i64,
    /// The number of lp (liquidity provider) shares the user has in this perp market
    /// LP shares allow users to provide liquidity via the AMM
    /// precision: BASE_PRECISION
    pub lp_shares: u64,
    /// The last base asset amount per lp the amm had
    /// Used to settle the users lp position
    /// precision: BASE_PRECISION
    pub last_base_asset_amount_per_lp: i64,
    /// The last quote asset amount per lp the amm had
    /// Used to settle the users lp position
    /// precision: QUOTE_PRECISION
    pub last_quote_asset_amount_per_lp: i64,
    /// Settling LP position can lead to a small amount of base asset being left over smaller than step size
    /// This records that remainder so it can be settled later on
    /// precision: BASE_PRECISION
    pub remainder_base_asset_amount: i32,
    /// The market index for the perp market
    pub market_index: u16,
    /// The number of open orders
    pub open_orders: u8,
    pub per_lp_base: i8,
}

#[derive(Eq, PartialEq, Debug)]
#[repr(C)]
pub struct Order {
    pub slot: u64,
    pub price: u64,
    pub base_asset_amount: u64,
    pub base_asset_amount_filled: u64,
    pub quote_asset_amount_filled: u64,
    pub trigger_price: u64,
    pub auction_start_price: i64,
    pub auction_end_price: i64,
    pub max_ts: i64,
    pub oracle_price_offset: i32,
    pub order_id: u32,
    pub market_index: u16,
    pub status: OrderStatus,
    pub order_type: OrderType,
    pub market_type: MarketType,
    pub user_order_id: u8,
    pub existing_position_direction: PositionDirection,
    pub direction: PositionDirection,
    pub reduce_only: bool,
    pub post_only: bool,
    pub immediate_or_cancel: bool,
    pub trigger_condition: OrderTriggerCondition,
    pub auction_duration: u8,
    pub padding: [u8; 3],
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum OrderStatus {
    Init,
    Open,
    Filled,
    Canceled,
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum OrderType {
    Market,
    Limit,
    TriggerMarket,
    TriggerLimit,
    Oracle,
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum OrderTriggerCondition {
    Above,
    Below,
    TriggeredAbove, // above condition has been triggered
    TriggeredBelow, // below condition has been triggered
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum MarketType {
    Spot,
    Perp,
}

#[derive(Clone, Copy, PartialEq, Debug, Eq)]
pub enum PositionDirection {
    Long,
    Short,
}
