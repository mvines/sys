use solana_client::{
    rpc_client::{RpcClient, SerializableTransaction},
    rpc_response,
};

pub mod binance_exchange;
pub mod coin_gecko;
pub mod coinbase_exchange;
pub mod exchange;
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

pub struct RpcClients {
    pub default: RpcClient,

    // Optional `RpcClient` for use only for sending transactions.
    // If `None` then the `default` client is used for sending transactions
    pub send: Option<RpcClient>,
}

// Assumes `transaction` has already been signed and simulated...
pub fn send_transaction_until_expired(
    rpc_clients: &RpcClients,
    transaction: &impl SerializableTransaction,
    last_valid_block_height: u64,
) -> bool {
    use {
        solana_client::rpc_config::RpcSendTransactionConfig,
        std::{
            thread::sleep,
            time::{Duration, Instant},
        },
    };

    let send_rpc_client = rpc_clients.send.as_ref().unwrap_or(&rpc_clients.default);
    let rpc_client = &rpc_clients.default;

    let mut last_send_attempt = None;

    let config = RpcSendTransactionConfig {
        skip_preflight: true,
        ..RpcSendTransactionConfig::default()
    };

    loop {
        if last_send_attempt.is_none()
            || Instant::now()
                .duration_since(*last_send_attempt.as_ref().unwrap())
                .as_secs()
                > 2
        {
            println!("Sending transaction {}", transaction.get_signature());
            if let Err(err) = send_rpc_client.send_transaction_with_config(transaction, config) {
                println!("Unable to send transaction: {err:?}");
            }
            last_send_attempt = Some(Instant::now());
        }

        sleep(Duration::from_millis(500));

        match rpc_client.get_signature_statuses(&[*transaction.get_signature()]) {
            Ok(rpc_response::Response { context, value }) => {
                let confirmation_context_slot = context.slot;
                if let Some(ref transaction_status) = value[0] {
                    match transaction_status.err {
                        None => return true,
                        Some(ref err) => {
                            println!("Transaction failed: {err}");
                            return false;
                        }
                    }
                } else {
                    match rpc_client.get_epoch_info() {
                        Ok(epoch_info) => {
                            if epoch_info.block_height > last_valid_block_height
                                && epoch_info.absolute_slot >= confirmation_context_slot
                            {
                                println!(
                                    "Transaction expired as of slot {confirmation_context_slot}"
                                );
                                return false;
                            }
                            println!(
                                "(transaction unconfirmed as of slot {}, {} blocks until expiry)",
                                confirmation_context_slot,
                                last_valid_block_height.saturating_sub(epoch_info.block_height),
                            );
                        }
                        Err(err) => {
                            println!("Unable to get epoch info: {err:?}")
                        }
                    }
                }
            }
            Err(err) => {
                println!("Unable to get transaction status: {err:?}");
            }
        }
    }
}
