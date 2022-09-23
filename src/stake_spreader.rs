use {
    crate::db::*,
    crate::notifier::*,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        message::Message, native_token::lamports_to_sol, pubkey::Pubkey, signature::Signature,
        signers::Signers, system_instruction, system_program, transaction::Transaction,
    },
    std::collections::{BTreeMap, HashSet},
    sys::{send_transaction_until_expired, token::*},
};

pub async fn run<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    epoch_completed_percentage: u8,
    epoch_history: usize,
    num_validators: usize,
    included_vote_account_addresses: HashSet<Pubkey>,
    excluded_vote_account_addresses: HashSet<Pubkey>,
    authority_address: Pubkey,
    signers: T,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("authority_address: {}", authority_address);
    println!("epoch_completed_percentage: {}", epoch_completed_percentage);
    println!("epoch_history: {}", epoch_history);
    println!("num_validators: {}", num_validators);
    println!(
        "included_vote_account_addresses: {:?}",
        included_vote_account_addresses
    );
    println!(
        "excluded_vote_account_addresses: {:?}",
        excluded_vote_account_addresses
    );

    todo!();
    //Ok(())
}
