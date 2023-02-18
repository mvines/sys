use {
    chrono::prelude::*,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::UiTransactionEncoding,
    std::str::FromStr,
};

#[derive(Debug, Clone)]
pub struct GetTransactionAddrssBalanceChange {
    pub pre_amount: u64,
    pub post_amount: u64,
    pub slot: Slot,
    pub when: Option<NaiveDateTime>,
}

pub fn get_transaction_balance_change(
    rpc_client: &RpcClient,
    signature: &Signature,
    address: &Pubkey,
    address_is_token: bool,
) -> Result<GetTransactionAddrssBalanceChange, Box<dyn std::error::Error>> {
    let confirmed_transaction =
        rpc_client.get_transaction(signature, UiTransactionEncoding::Base64)?;

    let slot = confirmed_transaction.slot;
    let when = confirmed_transaction
        .block_time
        .map(|block_time| {
            NaiveDateTime::from_timestamp_opt(block_time, 0)
                .ok_or_else(|| format!("Invalid block time for slot {slot}"))
        })
        .transpose()?;

    let meta = confirmed_transaction
        .transaction
        .meta
        .ok_or("Transaction metadata not available")?;

    if meta.err.is_some() {
        return Err("Transaction was not successful".into());
    }

    let transaction = confirmed_transaction
        .transaction
        .transaction
        .decode()
        .ok_or("Unable to decode transaction")?;

    let account_index = transaction
        .message
        .static_account_keys()
        .iter()
        .position(|k| k == address)
        .ok_or_else(|| format!("Address {address} not referenced in transaction"))?;

    let pre_amount = if address_is_token {
        u64::from_str(
            &meta
                .pre_token_balances
                .unwrap()
                .iter()
                .find(|ptb| ptb.account_index as usize == account_index)
                .unwrap()
                .ui_token_amount
                .amount,
        )
        .unwrap_or_default()
    } else {
        meta.pre_balances[account_index]
    };

    let post_amount = if address_is_token {
        u64::from_str(
            &meta
                .post_token_balances
                .unwrap()
                .iter()
                .find(|ptb| ptb.account_index as usize == account_index)
                .unwrap()
                .ui_token_amount
                .amount,
        )
        .unwrap_or_default()
    } else {
        meta.post_balances[account_index]
    };

    Ok(GetTransactionAddrssBalanceChange {
        pre_amount,
        post_amount,
        slot,
        when,
    })
}
