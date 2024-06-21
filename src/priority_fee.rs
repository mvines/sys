use {
    crate::{helius_rpc, RpcClients},
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
                .filter_map(|account_meta| account_meta.is_writable.then_some(account_meta.pubkey))
                .collect::<Vec<_>>()
        })
        .collect();
    account_keys.sort();
    account_keys.dedup();

    let prioritization_fees: Vec<_> = rpc_client
        .get_recent_prioritization_fees(&account_keys)
        .map(|response| {
            response
                .into_iter()
                .map(|rpf| rpf.prioritization_fee)
                .collect()
        })
        .map_err(|err| format!("Failed to invoke RPC method getRecentPrioritizationFees: {err}"))?;

    Ok(prioritization_fees)
}

pub fn apply_priority_fee(
    rpc_clients: &RpcClients,
    instructions: &mut Vec<Instruction>,
    compute_unit_limit: u32,
    priority_fee: PriorityFee,
) -> Result<u64, Box<dyn std::error::Error>> {
    let compute_budget = match priority_fee {
        PriorityFee::Exact { lamports } => ComputeBudget::new(compute_unit_limit, lamports),
        PriorityFee::Auto {
            max_lamports,
            fee_percentile,
        } => {
            let helius_compute_budget = if let Ok(helius_priority_fee_estimate) =
                helius_rpc::get_priority_fee_estimate_for_instructions(
                    rpc_clients.helius_or_default(),
                    helius_rpc::HeliusPriorityLevel::High,
                    instructions,
                ) {
                let helius_compute_budget = ComputeBudget {
                    compute_unit_price_micro_lamports: helius_priority_fee_estimate,
                    compute_unit_limit,
                };

                println!(
                    "Helius priority fee (high): {}",
                    Sol(helius_compute_budget.priority_fee_lamports())
                );

                helius_compute_budget
            } else {
                ComputeBudget::default()
            };

            let sys_compute_budget = {
                let recent_compute_unit_prices =
                    get_recent_priority_fees_for_instructions(rpc_clients.default(), instructions)?
                        .into_iter()
                        //  .skip_while(|fee| *fee == 0) // Skip 0 fee payers
                        .map(|f| f as f64)
                        .collect::<Vec<_>>();

                let ui_fee_for = |compute_unit_price_micro_lamports: f64| {
                    Sol(ComputeBudget {
                        compute_unit_price_micro_lamports: compute_unit_price_micro_lamports as u64,
                        compute_unit_limit,
                    }
                    .priority_fee_lamports())
                };

                let dist = criterion_stats::Distribution::from(
                    recent_compute_unit_prices.into_boxed_slice(),
                );
                let mut verbose_msg = format!("mean={}", ui_fee_for(dist.mean()));
                let percentiles = dist.percentiles();
                for i in [50., 75., 90., 95., 100.] {
                    verbose_msg += &format!(", {i}th={}", ui_fee_for(percentiles.at(i)));
                }

                let fee_percentile_compute_unit_price_micro_lamports =
                    percentiles.at(fee_percentile as f64) as u64;
                let mean_compute_unit_price_micro_lamports = dist.mean() as u64;

                // Use the greater of the `fee_percentile`th percentile fee or the mean fee
                let compute_unit_price_micro_lamports =
                    if fee_percentile_compute_unit_price_micro_lamports
                        > mean_compute_unit_price_micro_lamports
                    {
                        verbose_msg += &format!(". Selected {fee_percentile}th percentile");
                        fee_percentile_compute_unit_price_micro_lamports
                    } else {
                        verbose_msg += ". Selected mean)";
                        mean_compute_unit_price_micro_lamports
                    };

                let sys_compute_budget = ComputeBudget {
                    compute_unit_price_micro_lamports,
                    compute_unit_limit,
                };

                println!(
                    "Observed priority fee:      {}\n  ({verbose_msg})",
                    Sol(sys_compute_budget.priority_fee_lamports())
                );
                sys_compute_budget
            };

            let compute_budget = if sys_compute_budget.compute_unit_price_micro_lamports
                > helius_compute_budget.compute_unit_price_micro_lamports
            {
                sys_compute_budget
            } else {
                helius_compute_budget
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
        "Selected priority fee:      {}",
        Sol(compute_budget.priority_fee_lamports())
    );
    assert!(
        0.01 > lamports_to_sol(compute_budget.priority_fee_lamports()),
        "Priority fee too large, Bug?"
    );

    assert_ne!(compute_budget.compute_unit_limit, 0);
    instructions.push(
        compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(
            compute_budget.compute_unit_limit,
        ),
    );

    if compute_budget.compute_unit_price_micro_lamports > 0 {
        instructions.push(
            compute_budget::ComputeBudgetInstruction::set_compute_unit_price(
                compute_budget.compute_unit_price_micro_lamports,
            ),
        );
    }

    Ok(compute_budget.priority_fee_lamports())
}
