use {
    super::{last_update::LastUpdate, reserve::BigFractionBytes},
    solana_sdk::pubkey::Pubkey,
};

static_assertions::const_assert_eq!(3336, std::mem::size_of::<Obligation>());
static_assertions::const_assert_eq!(0, std::mem::size_of::<Obligation>() % 8);
#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct Obligation {
    pub tag: u64,
    pub last_update: LastUpdate,
    pub lending_market: Pubkey,
    pub owner: Pubkey,
    pub deposits: [ObligationCollateral; 8],
    pub lowest_reserve_deposit_ltv: u64,
    pub deposited_value_sf: u128,

    pub borrows: [ObligationLiquidity; 5],
    pub borrow_factor_adjusted_debt_value_sf: u128,
    pub borrowed_assets_market_value_sf: u128,
    pub allowed_borrow_value_sf: u128,
    pub unhealthy_borrow_value_sf: u128,

    pub deposits_asset_tiers: [u8; 8],
    pub borrows_asset_tiers: [u8; 5],

    pub elevation_group: u8,

    pub num_of_obsolete_reserves: u8,

    pub has_debt: u8,

    pub referrer: Pubkey,

    pub padding_3: [u64; 128],
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ObligationCollateral {
    pub deposit_reserve: Pubkey,
    pub deposited_amount: u64,
    pub market_value_sf: u128,
    pub padding: [u64; 10],
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ObligationLiquidity {
    pub borrow_reserve: Pubkey,
    pub cumulative_borrow_rate_bsf: BigFractionBytes,
    pub padding: u64,
    pub borrowed_amount_sf: u128,
    pub market_value_sf: u128,
    pub borrow_factor_adjusted_market_value_sf: u128,

    pub padding2: [u64; 8],
}
