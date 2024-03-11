use {
    solana_client::rpc_client::RpcClient,
    solana_sdk::{instruction::Instruction, transaction::Transaction},
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum HeliusPriorityLevel {
    None,     // 0th percentile
    Low,      // 25th percentile
    Medium,   // 50th percentile
    High,     // 75th percentile
    VeryHigh, // 95th percentile
    Default,  // 50th percentile
}

pub type HeliusMicroLamportPriorityFee = f64;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct HeliusGetPriorityFeeEstimateRequest {
    pub transaction: Option<String>, // estimate fee for a serialized txn
    pub account_keys: Option<Vec<String>>, // estimate fee for a list of accounts
    pub options: Option<HeliusGetPriorityFeeEstimateOptions>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct HeliusGetPriorityFeeEstimateOptions {
    pub priority_level: Option<HeliusPriorityLevel>, // Default to MEDIUM
    pub include_all_priority_fee_levels: Option<bool>, // Include all priority level estimates in the response
    pub transaction_encoding: Option<solana_transaction_status::UiTransactionEncoding>, // Default Base58
    pub lookback_slots: Option<u8>, // number of slots to look back to calculate estimate. Valid number are 1-150, defualt is 150
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HeliusGetPriorityFeeEstimateResponse {
    pub priority_fee_estimate: Option<HeliusMicroLamportPriorityFee>,
    pub priority_fee_levels: Option<HeliusMicroLamportPriorityFeeLevels>,
}

#[derive(serde::Deserialize, Debug)]
pub struct HeliusMicroLamportPriorityFeeLevels {
    pub none: f64,
    pub low: f64,
    pub medium: f64,
    pub high: f64,
    pub very_high: f64,
    pub unsafe_max: f64,
}

pub fn get_priority_fee_estimate_for_transaction(
    rpc_client: &RpcClient,
    priority_level: HeliusPriorityLevel,
    transaction: &Transaction,
) -> Result<u64, String> {
    //println!("Invoking Helius RPC method: getPriorityFeeEstimate");

    let request = serde_json::json!([HeliusGetPriorityFeeEstimateRequest {
        options: Some(HeliusGetPriorityFeeEstimateOptions {
            priority_level: Some(priority_level),
            ..HeliusGetPriorityFeeEstimateOptions::default()
        }),
        transaction: Some(bs58::encode(bincode::serialize(transaction).unwrap()).into_string()),
        ..HeliusGetPriorityFeeEstimateRequest::default()
    }]);

    rpc_client
        .send::<HeliusGetPriorityFeeEstimateResponse>(
            solana_client::rpc_request::RpcRequest::Custom {
                method: "getPriorityFeeEstimate",
            },
            request,
        )
        .map(|response| {
            response
                .priority_fee_estimate
                .expect("priority_fee_estimate") as u64
        })
        .map_err(|err| format!("Failed to invoke RPC method getPriorityFeeEstimate: {err}"))
}

pub fn get_priority_fee_estimate_for_instructions(
    rpc_client: &RpcClient,
    priority_level: HeliusPriorityLevel,
    instructions: &[Instruction],
) -> Result<u64, String> {
    //println!("Invoking Helius RPC method: getPriorityFeeEstimate");

    let mut account_keys: Vec<_> = instructions
        .iter()
        .flat_map(|instruction| {
            instruction
                .accounts
                .iter()
                .map(|account_meta| account_meta.pubkey.to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    account_keys.sort();
    account_keys.dedup();

    let request = serde_json::json!([HeliusGetPriorityFeeEstimateRequest {
        options: Some(HeliusGetPriorityFeeEstimateOptions {
            priority_level: Some(priority_level),
            ..HeliusGetPriorityFeeEstimateOptions::default()
        }),
        account_keys: Some(account_keys),
        ..HeliusGetPriorityFeeEstimateRequest::default()
    }]);

    rpc_client
        .send::<HeliusGetPriorityFeeEstimateResponse>(
            solana_client::rpc_request::RpcRequest::Custom {
                method: "getPriorityFeeEstimate",
            },
            request,
        )
        .map(|response| {
            response
                .priority_fee_estimate
                .expect("priority_fee_estimate") as u64
        })
        .map_err(|err| format!("Failed to invoke RPC method getPriorityFeeEstimate: {err}"))
}
