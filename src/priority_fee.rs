use {
    crate::helius_rpc,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        compute_budget,
        instruction::Instruction,
        native_token::lamports_to_sol,
        native_token::{sol_to_lamports, Sol},
    },
};

#[derive(Debug, Clone, Copy)]
pub enum PriorityFee {
    Auto { max_lamports: u64 },
    Exact { lamports: u64 },
}

impl PriorityFee {
    pub fn default_auto() -> Self {
        Self::Auto {
            max_lamports: sol_to_lamports(0.005), // Same max as the Jupiter V6 Swap API
        }
    }
}

impl PriorityFee {
    pub fn max_lamports(&self) -> u64 {
        match self {
            Self::Auto { max_lamports } => *max_lamports,
            Self::Exact { lamports } => *lamports,
        }
    }

    pub fn exact_lamports(&self) -> Option<u64> {
        match self {
            Self::Auto { .. } => None,
            Self::Exact { lamports } => Some(*lamports),
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct ComputeBudget {
    pub compute_unit_price_micro_lamports: u64,
    pub compute_unit_limit: u32,
}

impl ComputeBudget {
    pub fn new(compute_unit_limit: u32, priority_fee_lamports: u64) -> Self {
        Self {
            compute_unit_price_micro_lamports: priority_fee_lamports * (1e6 as u64)
                / compute_unit_limit as u64,
            compute_unit_limit,
        }
    }

    pub fn priority_fee_lamports(&self) -> u64 {
        self.compute_unit_limit as u64 * self.compute_unit_price_micro_lamports / (1e6 as u64)
    }
}

// Returns a sorted list of compute unit prices in micro lamports, from low to high
fn get_recent_priority_fees_for_instructions(
    rpc_client: &RpcClient,
    instructions: &[Instruction],
) -> Result<Vec<u64>, String> {
    let mut account_keys: Vec<_> = instructions
        .iter()
        .flat_map(|instruction| {
            instruction
                .accounts
                .iter()
                .map(|account_meta| account_meta.pubkey)
                .collect::<Vec<_>>()
        })
        .collect();
    account_keys.sort();
    account_keys.dedup();

    let mut prioritization_fees: Vec<_> = rpc_client
        .get_recent_prioritization_fees(&account_keys)
        .map(|response| {
            response
                .into_iter()
                .map(|rpf| rpf.prioritization_fee)
                .collect()
        })
        .map_err(|err| format!("Failed to invoke RPC method getRecentPrioritizationFees: {err}"))?;

    prioritization_fees.sort();

    Ok(prioritization_fees)
}

pub fn apply_priority_fee(
    rpc_client: &RpcClient,
    instructions: &mut Vec<Instruction>,
    compute_unit_limit: u32,
    priority_fee: PriorityFee,
) -> Result<(), Box<dyn std::error::Error>> {
    let compute_budget = if let Some(exact_lamports) = priority_fee.exact_lamports() {
        ComputeBudget::new(compute_unit_limit, exact_lamports)
    } else {
        let recent_compute_unit_prices =
            get_recent_priority_fees_for_instructions(rpc_client, instructions)?;

        let mean_compute_unit_price_micro_lamports =
            recent_compute_unit_prices.iter().copied().sum::<u64>()
                / recent_compute_unit_prices.len() as u64;

        /*
        let max_compute_unit_price_micro_lamports = recent_compute_unit_prices
            .iter()
            .max()
            .copied()
            .unwrap_or_default();

        println!("{recent_compute_unit_prices:?}: mean {mean_compute_unit_price_micro_lamports}, max {max_compute_unit_price_micro_lamports}");
        */

        let compute_unit_price_micro_lamports = mean_compute_unit_price_micro_lamports;

        if let Ok(priority_fee_estimate) = helius_rpc::get_priority_fee_estimate_for_instructions(
            rpc_client,
            helius_rpc::HeliusPriorityLevel::High,
            instructions,
        ) {
            println!(
                "Note: helius compute unit price (high) estimate is {priority_fee_estimate}. \
                      `sys` computed {compute_unit_price_micro_lamports}"
            );
        }

        let compute_budget = ComputeBudget {
            compute_unit_price_micro_lamports,
            compute_unit_limit,
        };

        if compute_budget.priority_fee_lamports() > priority_fee.max_lamports() {
            println!(
                "Note: Computed priority fee of {} is greater than max fee",
                Sol(priority_fee.max_lamports())
            );
            ComputeBudget::new(compute_unit_limit, priority_fee.max_lamports())
        } else {
            compute_budget
        }
    };

    println!(
        "Priority fee: {}",
        Sol(compute_budget.priority_fee_lamports())
    );
    assert!(
        0.01 > lamports_to_sol(compute_budget.priority_fee_lamports()),
        "Priority fee too large, Bug?"
    );

    instructions.push(
        compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
            compute_budget.compute_unit_limit,
        ),
    );

    instructions.push(
        compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
            compute_budget.compute_unit_price_micro_lamports,
        ),
    );

    Ok(())
}
