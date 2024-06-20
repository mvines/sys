pub mod spot_market;
pub mod user;

use {spot_market::SpotMarket, user::SpotBalanceType};

pub const ONE_YEAR: u128 = 31536000;

pub const PERCENTAGE_PRECISION: u128 = 1_000_000; // expo -6 (represents 100%)
pub const PERCENTAGE_PRECISION_I128: i128 = PERCENTAGE_PRECISION as i128;
pub const PERCENTAGE_PRECISION_U64: u64 = PERCENTAGE_PRECISION as u64;
pub const PERCENTAGE_PRECISION_I64: i64 = PERCENTAGE_PRECISION as i64;

pub const SPOT_BALANCE_PRECISION: u128 = 1_000_000_000; // expo = -9
                                                        //pub const SPOT_BALANCE_PRECISION_U64: u64 = 1_000_000_000; // expo = -9
pub const SPOT_CUMULATIVE_INTEREST_PRECISION: u128 = 10_000_000_000; // expo = -10

pub const SPOT_UTILIZATION_PRECISION: u128 = PERCENTAGE_PRECISION; // expo = -6
pub const SPOT_UTILIZATION_PRECISION_U32: u32 = PERCENTAGE_PRECISION as u32; // expo = -6
pub const SPOT_RATE_PRECISION: u128 = PERCENTAGE_PRECISION; // expo = -6
pub const SPOT_RATE_PRECISION_U32: u32 = PERCENTAGE_PRECISION as u32; // expo = -6

pub fn token_amount_to_scaled_balance(
    token_amount: u64,
    spot_market: &SpotMarket,
    balance_type: SpotBalanceType,
) -> u64 {
    let precision_increase = 10_u128.pow(19_u32.saturating_sub(spot_market.decimals));

    let cumulative_interest = match balance_type {
        SpotBalanceType::Deposit => spot_market.cumulative_deposit_interest,
        SpotBalanceType::Borrow => spot_market.cumulative_borrow_interest,
    };

    ((token_amount as u128) * precision_increase / cumulative_interest) as u64
}

pub fn scaled_balance_to_token_amount(
    scaled_balance: u128,
    spot_market: &SpotMarket,
    balance_type: SpotBalanceType,
) -> u64 {
    let precision_increase = 10_u128.pow(19_u32.saturating_sub(spot_market.decimals));

    let cumulative_interest = match balance_type {
        SpotBalanceType::Deposit => spot_market.cumulative_deposit_interest,
        SpotBalanceType::Borrow => spot_market.cumulative_borrow_interest,
    };

    (scaled_balance * cumulative_interest / precision_increase) as u64
}

pub fn calculate_utilization(deposit_token_amount: u64, borrow_token_amount: u64) -> u128 {
    (borrow_token_amount as u128 * SPOT_UTILIZATION_PRECISION)
        .checked_div(deposit_token_amount as u128)
        .unwrap_or({
            if deposit_token_amount == 0 && borrow_token_amount == 0 {
                0_u128
            } else {
                // if there are borrows without deposits, default to maximum utilization rate
                SPOT_UTILIZATION_PRECISION
            }
        })
}

pub fn calculate_spot_market_utilization(spot_market: &SpotMarket) -> u128 {
    let deposit_token_amount = scaled_balance_to_token_amount(
        spot_market.deposit_balance,
        spot_market,
        SpotBalanceType::Deposit,
    );
    let borrow_token_amount = scaled_balance_to_token_amount(
        spot_market.borrow_balance,
        spot_market,
        SpotBalanceType::Borrow,
    );
    calculate_utilization(deposit_token_amount, borrow_token_amount)
}

#[derive(Default, Debug)]
pub struct InterestRate {
    pub borrow_rate: f64,
    pub deposit_rate: f64,
}

pub fn calculate_accumulated_interest(spot_market: &SpotMarket) -> InterestRate {
    let utilization = calculate_spot_market_utilization(spot_market);

    if utilization == 0 {
        InterestRate::default()
    } else {
        let borrow_rate = if utilization > spot_market.optimal_utilization as u128 {
            let surplus_utilization =
                utilization.saturating_sub(spot_market.optimal_utilization as u128);

            let borrow_rate_slope = (spot_market.max_borrow_rate as u128)
                .saturating_sub(spot_market.optimal_borrow_rate as u128)
                * SPOT_UTILIZATION_PRECISION
                / SPOT_UTILIZATION_PRECISION
                    .saturating_sub(spot_market.optimal_utilization as u128);

            (spot_market.optimal_borrow_rate as u128)
                + (surplus_utilization * borrow_rate_slope / SPOT_UTILIZATION_PRECISION)
        } else {
            let borrow_rate_slope = (spot_market.optimal_borrow_rate as u128)
                * SPOT_UTILIZATION_PRECISION
                / (spot_market.optimal_utilization as u128);

            utilization * borrow_rate_slope / SPOT_UTILIZATION_PRECISION
        };

        let deposit_rate = borrow_rate * utilization / SPOT_UTILIZATION_PRECISION;

        InterestRate {
            borrow_rate: borrow_rate as f64 / PERCENTAGE_PRECISION as f64,
            deposit_rate: deposit_rate as f64 / PERCENTAGE_PRECISION as f64,
        }
    }
}
