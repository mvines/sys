/// MarginFi v2 bits yanked from https://github.com/mrgnlabs/marginfi-v2/tree/main/programs/marginfi/src
use {
    fixed::types::I80F48,
    solana_sdk::pubkey::Pubkey,
    std::fmt::{Debug, Formatter},
};

const MAX_ORACLE_KEYS: usize = 5;

/// Value where total_asset_value_init_limit is considered inactive
const TOTAL_ASSET_VALUE_INIT_LIMIT_INACTIVE: u64 = 0;

macro_rules! assert_struct_size {
    ($struct: ty, $size: expr) => {
        static_assertions::const_assert_eq!(std::mem::size_of::<$struct>(), $size);
    };
}

macro_rules! assert_struct_align {
    ($struct: ty, $align: expr) => {
        static_assertions::const_assert_eq!(std::mem::align_of::<$struct>(), $align);
    };
}

assert_struct_size!(Bank, 1856);
assert_struct_align!(Bank, 8);
#[repr(C)]
#[cfg_attr(
    any(feature = "test", feature = "client"),
    derive(Debug, PartialEq, Eq, TypeLayout)
)]
#[derive(Default, Debug)]
pub struct Bank {
    pub mint: Pubkey,
    pub mint_decimals: u8,

    pub group: Pubkey,

    pub asset_share_value: WrappedI80F48,
    pub liability_share_value: WrappedI80F48,

    pub liquidity_vault: Pubkey,
    pub liquidity_vault_bump: u8,
    pub liquidity_vault_authority_bump: u8,

    pub insurance_vault: Pubkey,
    pub insurance_vault_bump: u8,
    pub insurance_vault_authority_bump: u8,
    pub collected_insurance_fees_outstanding: WrappedI80F48,

    pub fee_vault: Pubkey,
    pub fee_vault_bump: u8,
    pub fee_vault_authority_bump: u8,
    pub collected_group_fees_outstanding: WrappedI80F48,

    pub total_liability_shares: WrappedI80F48,
    pub total_asset_shares: WrappedI80F48,

    pub last_update: i64,

    pub config: BankConfig,

    /// Emissions Config Flags
    ///
    /// - EMISSIONS_FLAG_BORROW_ACTIVE: 1
    /// - EMISSIONS_FLAG_LENDING_ACTIVE: 2
    ///
    pub emissions_flags: u64,
    /// Emissions APR.
    /// Number of emitted tokens (emissions_mint) per 1e(bank.mint_decimal) tokens (bank mint) (native amount) per 1 YEAR.
    pub emissions_rate: u64,
    pub emissions_remaining: WrappedI80F48,
    pub emissions_mint: Pubkey,

    pub _padding_0: [[u64; 2]; 28],
    pub _padding_1: [[u64; 2]; 32], // 16 * 2 * 32 = 1024B
}

impl Bank {
    pub fn get_asset_amount(&self, shares: I80F48) -> I80F48 {
        shares
            .checked_mul(self.asset_share_value.into())
            .expect("bad math")
    }
    pub fn get_liability_amount(&self, shares: I80F48) -> I80F48 {
        shares
            .checked_mul(self.liability_share_value.into())
            .expect("bad math")
    }
}

#[repr(C, align(8))]
#[cfg_attr(
    any(feature = "test", feature = "client"),
    derive(PartialEq, Eq, TypeLayout)
)]
#[derive(Default, Clone, Copy)]
pub struct WrappedI80F48 {
    pub value: [u8; 16],
}

impl Debug for WrappedI80F48 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", I80F48::from_le_bytes(self.value))
    }
}

impl From<I80F48> for WrappedI80F48 {
    fn from(i: I80F48) -> Self {
        Self {
            value: i.to_le_bytes(),
        }
    }
}

impl From<WrappedI80F48> for I80F48 {
    fn from(w: WrappedI80F48) -> Self {
        Self::from_le_bytes(w.value)
    }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum BankOperationalState {
    Paused,
    Operational,
    ReduceOnly,
}

#[repr(C)]
#[derive(Default, Debug)]
pub struct InterestRateConfig {
    // Curve Params
    pub optimal_utilization_rate: WrappedI80F48,
    pub plateau_interest_rate: WrappedI80F48,
    pub max_interest_rate: WrappedI80F48,

    // Fees
    pub insurance_fee_fixed_apr: WrappedI80F48,
    pub insurance_ir_fee: WrappedI80F48,
    pub protocol_fixed_fee_apr: WrappedI80F48,
    pub protocol_ir_fee: WrappedI80F48,

    pub _padding: [[u64; 2]; 8], // 16 * 8 = 128 bytes
}

/// Calculates the fee rate for a given base rate and fees specified.
/// The returned rate is only the fee rate without the base rate.
///
/// Used for calculating the fees charged to the borrowers.
fn calc_fee_rate(base_rate: I80F48, rate_fees: I80F48, fixed_fees: I80F48) -> Option<I80F48> {
    base_rate.checked_mul(rate_fees)?.checked_add(fixed_fees)
}

impl InterestRateConfig {
    /// Return interest rate charged to borrowers and to depositors.
    /// Rate is denominated in APR (0-).
    ///
    /// Return (`lending_rate`, `borrowing_rate`, `group_fees_apr`, `insurance_fees_apr`)
    pub fn calc_interest_rate(
        &self,
        utilization_ratio: I80F48,
    ) -> Option<(I80F48, I80F48, I80F48, I80F48)> {
        let protocol_ir_fee = I80F48::from(self.protocol_ir_fee);
        let insurance_ir_fee = I80F48::from(self.insurance_ir_fee);

        let protocol_fixed_fee_apr = I80F48::from(self.protocol_fixed_fee_apr);
        let insurance_fee_fixed_apr = I80F48::from(self.insurance_fee_fixed_apr);

        let rate_fee = protocol_ir_fee + insurance_ir_fee;
        let total_fixed_fee_apr = protocol_fixed_fee_apr + insurance_fee_fixed_apr;

        let base_rate = self.interest_rate_curve(utilization_ratio)?;

        // Lending rate is adjusted for utilization ratio to symmetrize payments between borrowers and depositors.
        let lending_rate = base_rate.checked_mul(utilization_ratio)?;

        // Borrowing rate is adjusted for fees.
        // borrowing_rate = base_rate + base_rate * rate_fee + total_fixed_fee_apr
        let borrowing_rate = base_rate
            .checked_mul(I80F48::ONE.checked_add(rate_fee)?)?
            .checked_add(total_fixed_fee_apr)?;

        let group_fees_apr = calc_fee_rate(
            base_rate,
            self.protocol_ir_fee.into(),
            self.protocol_fixed_fee_apr.into(),
        )?;

        let insurance_fees_apr = calc_fee_rate(
            base_rate,
            self.insurance_ir_fee.into(),
            self.insurance_fee_fixed_apr.into(),
        )?;

        assert!(lending_rate >= I80F48::ZERO);
        assert!(borrowing_rate >= I80F48::ZERO);
        assert!(group_fees_apr >= I80F48::ZERO);
        assert!(insurance_fees_apr >= I80F48::ZERO);

        // TODO: Add liquidation discount check

        Some((
            lending_rate,
            borrowing_rate,
            group_fees_apr,
            insurance_fees_apr,
        ))
    }

    /// Piecewise linear interest rate function.
    /// The curves approaches the `plateau_interest_rate` as the utilization ratio approaches the `optimal_utilization_rate`,
    /// once the utilization ratio exceeds the `optimal_utilization_rate`, the curve approaches the `max_interest_rate`.
    ///
    /// To be clear we don't particularly appreciate the piecewise linear nature of this "curve", but it is what it is.
    fn interest_rate_curve(&self, ur: I80F48) -> Option<I80F48> {
        let optimal_ur = self.optimal_utilization_rate.into();
        let plateau_ir = self.plateau_interest_rate.into();
        let max_ir: I80F48 = self.max_interest_rate.into();

        if ur <= optimal_ur {
            ur.checked_div(optimal_ur)?.checked_mul(plateau_ir)
        } else {
            (ur - optimal_ur)
                .checked_div(I80F48::ONE - optimal_ur)?
                .checked_mul(max_ir - plateau_ir)?
                .checked_add(plateau_ir)
        }
    }
}

#[repr(u8)]
#[cfg_attr(any(feature = "test", feature = "client"), derive(PartialEq, Eq))]
#[derive(Copy, Clone, Debug)]
pub enum OracleSetup {
    None,
    PythEma,
    SwitchboardV2,
}

#[repr(u64)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RiskTier {
    Collateral,
    /// ## Isolated Risk
    /// Assets in this trance can be borrowed only in isolation.
    /// They can't be borrowed together with other assets.
    ///
    /// For example, if users has USDC, and wants to borrow XYZ which is isolated,
    /// they can't borrow XYZ together with SOL, only XYZ alone.
    Isolated,
}

assert_struct_size!(BankConfig, 544);
assert_struct_align!(BankConfig, 8);
#[repr(C)]
#[derive(Debug)]
/// TODO: Convert weights to (u64, u64) to avoid precision loss (maybe?)
pub struct BankConfig {
    pub asset_weight_init: WrappedI80F48,
    pub asset_weight_maint: WrappedI80F48,

    pub liability_weight_init: WrappedI80F48,
    pub liability_weight_maint: WrappedI80F48,

    pub deposit_limit: u64,

    pub interest_rate_config: InterestRateConfig,
    pub operational_state: BankOperationalState,

    pub oracle_setup: OracleSetup,
    pub oracle_keys: [Pubkey; MAX_ORACLE_KEYS],

    pub borrow_limit: u64,

    pub risk_tier: RiskTier,

    /// USD denominated limit for calculating asset value for initialization margin requirements.
    /// Example, if total SOL deposits are equal to $1M and the limit it set to $500K,
    /// then SOL assets will be discounted by 50%.
    ///
    /// In other words the max value of liabilities that can be backed by the asset is $500K.
    /// This is useful for limiting the damage of orcale attacks.
    ///
    /// Value is UI USD value, for example value 100 -> $100
    pub total_asset_value_init_limit: u64,

    /// Time window in seconds for the oracle price feed to be considered live.
    pub oracle_max_age: u16,

    pub _padding: [u16; 19], // 16 * 4 = 64 bytes
}

impl Default for BankConfig {
    fn default() -> Self {
        Self {
            asset_weight_init: I80F48::ZERO.into(),
            asset_weight_maint: I80F48::ZERO.into(),
            liability_weight_init: I80F48::ONE.into(),
            liability_weight_maint: I80F48::ONE.into(),
            deposit_limit: 0,
            borrow_limit: 0,
            interest_rate_config: Default::default(),
            operational_state: BankOperationalState::Paused,
            oracle_setup: OracleSetup::None,
            oracle_keys: [Pubkey::default(); MAX_ORACLE_KEYS],
            risk_tier: RiskTier::Isolated,
            total_asset_value_init_limit: TOTAL_ASSET_VALUE_INIT_LIMIT_INACTIVE,
            oracle_max_age: 0,
            _padding: [0; 19],
        }
    }
}

assert_struct_size!(MarginfiAccount, 2304);
assert_struct_align!(MarginfiAccount, 8);
#[repr(C)]
#[derive(Debug)]
pub struct MarginfiAccount {
    pub group: Pubkey,                   // 32
    pub authority: Pubkey,               // 32
    pub lending_account: LendingAccount, // 1728
    /// The flags that indicate the state of the account.
    /// This is u64 bitfield, where each bit represents a flag.
    ///
    /// Flags:
    /// - DISABLED_FLAG = 1 << 0 = 1 - This flag indicates that the account is disabled,
    /// and no further actions can be taken on it.
    pub account_flags: u64, // 8
    pub _padding: [u64; 63],             // 8 * 63 = 512
}

const MAX_LENDING_ACCOUNT_BALANCES: usize = 16;

assert_struct_size!(LendingAccount, 1728);
assert_struct_align!(LendingAccount, 8);
#[repr(C)]
#[derive(Debug)]
pub struct LendingAccount {
    pub balances: [Balance; MAX_LENDING_ACCOUNT_BALANCES], // 104 * 16 = 1664
    pub _padding: [u64; 8],                                // 8 * 8 = 64
}

impl LendingAccount {
    pub fn get_first_empty_balance(&self) -> Option<usize> {
        self.balances.iter().position(|b| !b.active)
    }
}

impl LendingAccount {
    pub fn get_balance(&self, bank_pk: &Pubkey) -> Option<&Balance> {
        self.balances
            .iter()
            .find(|balance| balance.active && balance.bank_pk.eq(bank_pk))
    }
}

assert_struct_size!(Balance, 104);
assert_struct_align!(Balance, 8);
#[repr(C)]
#[derive(Debug)]
pub struct Balance {
    pub active: bool,
    pub bank_pk: Pubkey,
    pub asset_shares: WrappedI80F48,
    pub liability_shares: WrappedI80F48,
    pub emissions_outstanding: WrappedI80F48,
    pub last_update: u64,
    pub _padding: [u64; 1],
}
