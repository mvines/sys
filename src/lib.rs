use {
    solana_clap_utils::input_validators::normalize_to_url_if_moniker,
    solana_client::{
        rpc_client::{RpcClient, SerializableTransaction},
        rpc_response,
    },
    solana_sdk::{clock::Slot, commitment_config::CommitmentConfig},
    std::{
        thread::sleep,
        time::{Duration, Instant},
    },
};

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

pub fn app_version() -> String {
    let tag = option_env!("GITHUB_REF")
        .and_then(|github_ref| github_ref.strip_prefix("refs/tags/").map(|s| s.to_string()));

    tag.unwrap_or_else(|| match option_env!("GITHUB_SHA") {
        None => "devbuild".to_string(),
        Some(commit) => commit[..8].to_string(),
    })
}

pub fn is_comma_separated_url_or_moniker_list<T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + std::fmt::Display,
{
    for url_or_moniker in string.as_ref().split(',') {
        solana_clap_utils::input_validators::is_url_or_moniker(url_or_moniker)?;
    }

    Ok(())
}

pub struct RpcClients {
    clients: Vec<(String, RpcClient)>,
    helius: Option<RpcClient>,
}

impl RpcClients {
    pub fn new(
        json_rpc_url: String,
        send_json_rpc_urls: Option<String>,
        helius: Option<String>,
    ) -> Self {
        let mut json_rpc_urls = vec![json_rpc_url];
        if let Some(send_json_rpc_urls) = send_json_rpc_urls {
            for send_json_rpc_url in send_json_rpc_urls.split(',') {
                json_rpc_urls.push(send_json_rpc_url.into());
            }
        }

        Self {
            clients: json_rpc_urls
                .into_iter()
                .map(|json_rpc_url| {
                    let json_rpc_url = normalize_to_url_if_moniker(json_rpc_url);
                    (
                        json_rpc_url.clone(),
                        RpcClient::new_with_commitment(json_rpc_url, CommitmentConfig::confirmed()),
                    )
                })
                .collect(),
            helius: helius.map(|helius_json_rpc_url| {
                RpcClient::new_with_commitment(helius_json_rpc_url, CommitmentConfig::confirmed())
            }),
        }
    }

    pub fn default(&self) -> &RpcClient {
        &self.clients[0].1
    }

    pub fn helius_or_default(&self) -> &RpcClient {
        self.helius
            .as_ref()
            .map_or_else(|| self.default(), |helius| helius)
    }
}

// Assumes `transaction` has already been signed and simulated...
pub fn send_transaction_until_expired(
    rpc_clients: &RpcClients,
    transaction: &impl SerializableTransaction,
    last_valid_block_height: u64,
) -> Option<bool> {
    send_transaction_until_expired_with_slot(rpc_clients, transaction, last_valid_block_height)
        .map(|(_context_slot, success)| success)
}

// Same as `send_transaction_until_expired` but on success returns a `Slot` that the transaction
// was observed to be confirmed at
fn send_transaction_until_expired_with_slot(
    rpc_clients: &RpcClients,
    transaction: &impl SerializableTransaction,
    last_valid_block_height: u64,
) -> Option<(Slot, bool)> {
    let mut last_send_attempt = None;

    loop {
        if last_send_attempt.is_none()
            || Instant::now()
                .duration_since(*last_send_attempt.as_ref().unwrap())
                .as_secs()
                > 2
        {
            for (json_rpc_url, rpc_client) in rpc_clients.clients.iter().rev() {
                println!(
                    "Sending transaction {} [{json_rpc_url}]",
                    transaction.get_signature()
                );

                if let Err(err) = rpc_client.send_transaction(transaction) {
                    println!("Unable to send transaction: {err:?}");
                }
            }
            last_send_attempt = Some(Instant::now());
        }

        sleep(Duration::from_millis(500));

        match rpc_clients
            .default()
            .get_signature_statuses(&[*transaction.get_signature()])
        {
            Ok(rpc_response::Response { context, value }) => {
                let confirmation_context_slot = context.slot;
                if let Some(ref transaction_status) = value[0] {
                    return Some((
                        confirmation_context_slot,
                        match transaction_status.err {
                            None => true,
                            Some(ref err) => {
                                println!("Transaction failed: {err}");
                                false
                            }
                        },
                    ));
                } else {
                    match rpc_clients.default().get_epoch_info() {
                        Ok(epoch_info) => {
                            if epoch_info.block_height > last_valid_block_height
                                && epoch_info.absolute_slot >= confirmation_context_slot
                            {
                                println!(
                                    "Transaction expired as of slot {confirmation_context_slot}"
                                );
                                return None;
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
