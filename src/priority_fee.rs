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
    Auto {
        max_lamports: u64,
        fee_percentile: u8,
    },
    Exact {
        lamports: u64,
    },
}

impl PriorityFee {
    pub fn default_auto() -> Self {
        Self::default_auto_percentile(sol_to_lamports(0.005)) // Same max as the Jupiter V6 Swap API
    }
    pub fn default_auto_percentile(max_lamports: u64) -> Self {
        Self::Auto {
            max_lamports,
            fee_percentile: 90, // Pay at this percentile of recent fees
        }
    }
}

impl PriorityFee {
    pub fn max_lamports(&self) -> u64 {
        match self {
            Self::Auto { max_lamports, .. } => *max_lamports,
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
    let compute_budget = match priority_fee {
        PriorityFee::Exact { lamports } => ComputeBudget::new(compute_unit_limit, lamports),
        PriorityFee::Auto {
            max_lamports,
            fee_percentile,
        } => {
            let recent_compute_unit_prices =
                get_recent_priority_fees_for_instructions(rpc_client, instructions)?
                    .into_iter()
                    //  .skip_while(|fee| *fee == 0) // Skip 0 fee payers
                    .map(|f| f as f64)
                    .collect::<Vec<_>>();

            let dist =
                criterion_stats::Distribution::from(recent_compute_unit_prices.into_boxed_slice());
            print!("Recent CU prices: mean={:.0}", dist.mean());
            let percentiles = dist.percentiles();
            for i in [50., 75., 85., 90., 95., 100.] {
                print!(", {i}th={:.0}", percentiles.at(i));
            }

            let compute_unit_price_micro_lamports = percentiles.at(fee_percentile as f64) as u64;
            println!(
                "\nUsing the {fee_percentile}th percentile recent CU price of {:.0}",
                compute_unit_price_micro_lamports
            );

            if let Ok(priority_fee_estimate) =
                helius_rpc::get_priority_fee_estimate_for_instructions(
                    rpc_client,
                    helius_rpc::HeliusPriorityLevel::High,
                    instructions,
                )
            {
                println!(
                    "Note: helius compute unit price (high) estimate is {priority_fee_estimate}. \
                          `sys` computed {compute_unit_price_micro_lamports}"
                );
            }

            let compute_budget = ComputeBudget {
                compute_unit_price_micro_lamports,
                compute_unit_limit,
            };

            if compute_budget.priority_fee_lamports() > max_lamports {
                println!(
                    "Note: Computed priority fee of {} exceeds the maximum priority fee",
                    Sol(compute_budget.priority_fee_lamports())
                );
                ComputeBudget::new(compute_unit_limit, max_lamports)
            } else {
                compute_budget
            }
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
