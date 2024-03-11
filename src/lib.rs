use solana_client::rpc_client::{RpcClient, SerializableTransaction};

pub mod binance_exchange;
pub mod coin_gecko;
pub mod coinbase_exchange;
pub mod exchange;
pub mod ftx_exchange;
pub mod helius_rpc;
pub mod kraken_exchange;
pub mod metrics;
pub mod token;
pub mod tulip;

pub fn app_version() -> String {
    let tag = option_env!("GITHUB_REF")
        .and_then(|github_ref| github_ref.strip_prefix("refs/tags/").map(|s| s.to_string()));

    tag.unwrap_or_else(|| match option_env!("GITHUB_SHA") {
        None => "devbuild".to_string(),
        Some(commit) => commit[..8].to_string(),
    })
}

pub fn send_transaction_until_expired(
    rpc_client: &RpcClient,
    transaction: &impl SerializableTransaction,
    last_valid_block_height: u64,
) -> bool {
    loop {
        // `send_and_confirm_transaction_with_spinner()` fails with
        // "Transaction simulation failed: This transaction has already been processed" (AlreadyProcessed)
        // if the transaction was already processed by an earlier iteration of this loop
        match rpc_client.confirm_transaction(transaction.get_signature()) {
            Ok(true) => return true,
            Ok(false) => match rpc_client.get_epoch_info() {
                Ok(epoch_info) => {
                    if epoch_info.block_height > last_valid_block_height {
                        return false;
                    }
                    println!(
                        "Transaction pending for at most {} blocks",
                        last_valid_block_height.saturating_sub(epoch_info.block_height),
                    );
                }
                Err(err) => {
                    println!("Failed to get epoch info: {err:?}");
                }
            },
            Err(err) => {
                println!("Unable to determine if transaction was confirmed: {err:?}");
            }
        }

        match rpc_client.send_and_confirm_transaction_with_spinner(transaction) {
            Ok(_signature) => return true,
            Err(err) => {
                println!("Transaction failed to send: {err:?}");
            }
        }
    }
}
