use {
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{account_utils::StateMut, pubkey::Pubkey},
    solana_stake_program::stake_state::{Authorized, StakeState},
};

pub fn get_stake_authorized(
    rpc_client: &RpcClient,
    stake_account_address: Pubkey,
) -> Result<(Authorized, Pubkey), Box<dyn std::error::Error>> {
    let stake_account = rpc_client.get_account(&stake_account_address)?;

    if rpc_client
        .get_stake_activation(stake_account_address, None)?
        .state
        != StakeActivationState::Active
    {
        return Err(format!("Stake account must be active: {}", stake_account_address).into());
    }

    match stake_account.state() {
        Ok(StakeState::Stake(meta, stake)) => Ok((meta.authorized, stake.delegation.voter_pubkey)),
        _ => Err(format!("Invalid stake account: {}", stake_account_address).into()),
    }
}
