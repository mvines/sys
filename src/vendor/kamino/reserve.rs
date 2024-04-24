pub use fixed::types::U68F60 as Fraction;
use {
    super::{borrow_rate_curve::BorrowRateCurve, last_update::LastUpdate, token_info::TokenInfo},
    solana_sdk::pubkey::Pubkey,
};

#[derive(Default, Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct BigFractionBytes {
    pub value: [u64; 4],
    pub padding: [u64; 2],
}

static_assertions::const_assert_eq!(8616, std::mem::size_of::<Reserve>());
static_assertions::const_assert_eq!(0, std::mem::size_of::<Reserve>() % 8);
#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct Reserve {
    pub version: u64,

    pub last_update: LastUpdate,

    pub lending_market: Pubkey,

    pub farm_collateral: Pubkey,
    pub farm_debt: Pubkey,

    pub liquidity: ReserveLiquidity,

    pub reserve_liquidity_padding: [u64; 150],

    pub collateral: ReserveCollateral,

    pub reserve_collateral_padding: [u64; 150],

    pub config: ReserveConfig,

    pub config_padding: [u64; 150],

    pub padding: [u64; 240],
}

impl Reserve {
    pub fn current_supply_apr(&self) -> f64 {
        let utilization_rate = self.liquidity.utilization_rate();
        let protocol_take_rate_pct = self.config.protocol_take_rate_pct as f64 / 100.;

        let current_borrow_rate = self
            .config
            .borrow_rate_curve
            .get_borrow_rate(utilization_rate)
            .unwrap_or(Fraction::ZERO);

        (utilization_rate
            * current_borrow_rate
            * (Fraction::ONE - Fraction::from_num(protocol_take_rate_pct)))
        .checked_to_num()
        .unwrap_or(0.)
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ReserveLiquidity {
    pub mint_pubkey: Pubkey,
    pub supply_vault: Pubkey,
    pub fee_vault: Pubkey,
    pub available_amount: u64,
    pub borrowed_amount_sf: u128,
    pub market_price_sf: u128,
    pub market_price_last_updated_ts: u64,
    pub mint_decimals: u64,

    pub deposit_limit_crossed_slot: u64,
    pub borrow_limit_crossed_slot: u64,

    pub cumulative_borrow_rate_bsf: BigFractionBytes,
    pub accumulated_protocol_fees_sf: u128,
    pub accumulated_referrer_fees_sf: u128,
    pub pending_referrer_fees_sf: u128,
    pub absolute_referral_rate_sf: u128,

    pub padding2: [u64; 55],
    pub padding3: [u128; 32],
}

impl ReserveLiquidity {
    pub fn total_supply(&self) -> Fraction {
        Fraction::from(self.available_amount) + Fraction::from_bits(self.borrowed_amount_sf)
            - Fraction::from_bits(self.accumulated_protocol_fees_sf)
            - Fraction::from_bits(self.accumulated_referrer_fees_sf)
            - Fraction::from_bits(self.pending_referrer_fees_sf)
    }

    pub fn total_borrow(&self) -> Fraction {
        Fraction::from_bits(self.borrowed_amount_sf)
    }

    pub fn utilization_rate(&self) -> Fraction {
        let total_supply = self.total_supply();
        if total_supply == Fraction::ZERO {
            return Fraction::ZERO;
        }
        Fraction::from_bits(self.borrowed_amount_sf) / total_supply
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ReserveCollateral {
    pub mint_pubkey: Pubkey,
    pub mint_total_supply: u64,
    pub supply_vault: Pubkey,
    pub padding1: [u128; 32],
    pub padding2: [u128; 32],
}

static_assertions::const_assert_eq!(648, std::mem::size_of::<ReserveConfig>());
static_assertions::const_assert_eq!(0, std::mem::size_of::<ReserveConfig>() % 8);
#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ReserveConfig {
    pub status: u8,
    pub asset_tier: u8,
    pub reserved_0: [u8; 2],
    pub multiplier_side_boost: [u8; 2],
    pub multiplier_tag_boost: [u8; 8],
    pub protocol_take_rate_pct: u8,
    pub protocol_liquidation_fee_pct: u8,
    pub loan_to_value_pct: u8,
    pub liquidation_threshold_pct: u8,
    pub min_liquidation_bonus_bps: u16,
    pub max_liquidation_bonus_bps: u16,
    pub bad_debt_liquidation_bonus_bps: u16,
    pub deleveraging_margin_call_period_secs: u64,
    pub deleveraging_threshold_slots_per_bps: u64,
    pub fees: ReserveFees,
    pub borrow_rate_curve: BorrowRateCurve,
    pub borrow_factor_pct: u64,

    pub deposit_limit: u64,
    pub borrow_limit: u64,
    pub token_info: TokenInfo,

    pub deposit_withdrawal_cap: WithdrawalCaps,
    pub debt_withdrawal_cap: WithdrawalCaps,

    pub elevation_groups: [u8; 20],
    pub reserved_1: [u8; 4],
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct WithdrawalCaps {
    pub config_capacity: i64,
    pub current_total: i64,
    pub last_interval_start_timestamp: u64,
    pub config_interval_length_seconds: u64,
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub struct ReserveFees {
    pub borrow_fee_sf: u64,
    pub flash_loan_fee_sf: u64,
    pub padding: [u8; 8],
}
