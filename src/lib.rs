use solana_client::rpc_client::{RpcClient, SerializableTransaction};

pub mod binance_exchange;
pub mod coin_gecko;
pub mod coinbase_exchange;
pub mod exchange;
pub mod helius_rpc;
pub mod kraken_exchange;
pub mod metrics;
pub mod notifier;
pub mod priority_fee;
pub mod token;
pub mod vendor;
//pub mod tulip;

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
    use std::{
        thread::sleep,
        time::{Duration, Instant},
    };

    let mut last_send_attempt = None;

    loop {
        if last_send_attempt.is_none()
            || Instant::now()
                .duration_since(*last_send_attempt.as_ref().unwrap())
                .as_secs()
                > 2
        {
            let valid_msg = match rpc_client.get_epoch_info() {
                Ok(epoch_info) => {
                    if epoch_info.block_height > last_valid_block_height {
                        return false;
                    }
                    format!(
                        "{} blocks to expiry",
                        last_valid_block_height.saturating_sub(epoch_info.block_height),
                    )
                }
                Err(err) => {
                    format!("Failed to get epoch info: {err:?}")
                }
            };

            println!(
                "Sending transaction {} [{valid_msg}]",
                transaction.get_signature()
            );
            if let Err(err) = rpc_client.send_transaction(transaction) {
                println!("Transaction failed to send: {err:?}");
            }
            last_send_attempt = Some(Instant::now());
        }

        sleep(Duration::from_millis(500));

        match rpc_client.confirm_transaction(transaction.get_signature()) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(err) => {
                println!("Unable to determine if transaction was confirmed: {err:?}");
            }
        }
    }
}
