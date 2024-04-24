use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TokenInfo {
    pub name: [u8; 32],

    pub heuristic: PriceHeuristic,

    pub max_twap_divergence_bps: u64,

    pub max_age_price_seconds: u64,
    pub max_age_twap_seconds: u64,

    pub scope_configuration: ScopeConfiguration,

    pub switchboard_configuration: SwitchboardConfiguration,

    pub pyth_configuration: PythConfiguration,

    pub _padding: [u64; 20],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PriceHeuristic {
    pub lower: u64,
    pub upper: u64,
    pub exp: u64,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ScopeConfiguration {
    pub price_feed: Pubkey,
    pub price_chain: [u16; 4],
    pub twap_chain: [u16; 4],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct SwitchboardConfiguration {
    pub price_aggregator: Pubkey,
    pub twap_aggregator: Pubkey,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PythConfiguration {
    pub price: Pubkey,
}
