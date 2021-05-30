use {
    solana_client::rpc_client::RpcClient,
    solana_sdk::{account_utils::StateMut, pubkey::Pubkey},
    solana_stake_program::stake_state::{Authorized, StakeState},
};

pub fn get_stake_authorized(
    rpc_client: &RpcClient,
    stake_account_address: Pubkey,
) -> Result<Authorized, Box<dyn std::error::Error>> {
    let stake_account = rpc_client.get_account(&stake_account_address)?;

    let stake_state: StakeState = stake_account.state()?;
    if let Some(meta) = stake_state.meta() {
        Ok(meta.authorized)
    } else {
        Err(format!("Invalid stake account: {}", stake_account_address).into())
    }
}
