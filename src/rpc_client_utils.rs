use {
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{account::Account, account_utils::StateMut, pubkey::Pubkey},
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

pub fn stake_accounts_have_same_credits_observed(
    stake_account1: &Account,
    stake_account2: &Account,
) -> Result<bool, Box<dyn std::error::Error>> {
    use solana_stake_program::stake_state::Stake;

    let stake_state1 = bincode::deserialize(stake_account1.data.as_slice())
        .map_err(|err| format!("Invalid stake account 1: {}", err))?;
    let stake_state2 = bincode::deserialize(stake_account2.data.as_slice())
        .map_err(|err| format!("Invalid stake account 2: {}", err))?;

    if let (
        StakeState::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed1,
            },
        ),
        StakeState::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed2,
            },
        ),
    ) = (stake_state1, stake_state2)
    {
        return Ok(credits_observed1 == credits_observed2);
    }
    Ok(false)
}
