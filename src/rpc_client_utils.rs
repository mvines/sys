use {
    chrono::prelude::*,
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
    solana_sdk::{
        account::Account,
        account_utils::StateMut,
        clock::Slot,
        pubkey::Pubkey,
        signature::Signature,
        stake::state::{Authorized, StakeStateV2},
    },
};

pub async fn get_block_date(
    rpc_client: &RpcClient,
    slot: Slot,
) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    let block_time = rpc_client.get_block_time(slot)?;
    let local_timestamp = Local.timestamp_opt(block_time, 0).unwrap();
    Ok(NaiveDate::from_ymd_opt(
        local_timestamp.year(),
        local_timestamp.month(),
        local_timestamp.day(),
    )
    .unwrap())
}

pub fn get_stake_authorized(
    rpc_client: &RpcClient,
    stake_account_address: Pubkey,
) -> Result<(Authorized, Pubkey), Box<dyn std::error::Error>> {
    let stake_account = rpc_client.get_account(&stake_account_address)?;

    match rpc_client
        .get_stake_activation(stake_account_address, None)?
        .state
    {
        StakeActivationState::Active | StakeActivationState::Activating => {}
        state => {
            return Err(format!(
                "Stake account {stake_account_address} must be Active or Activating: {state:?}"
            )
            .into());
        }
    }

    match stake_account.state() {
        Ok(StakeStateV2::Stake(meta, stake, _stake_flags)) => {
            Ok((meta.authorized, stake.delegation.voter_pubkey))
        }
        _ => Err(format!("Invalid stake account: {stake_account_address}").into()),
    }
}

pub fn stake_accounts_have_same_credits_observed(
    stake_account1: &Account,
    stake_account2: &Account,
) -> Result<bool, Box<dyn std::error::Error>> {
    use solana_sdk::stake::state::Stake;

    let stake_state1 = bincode::deserialize(stake_account1.data.as_slice())
        .map_err(|err| format!("Invalid stake account 1: {err}"))?;
    let stake_state2 = bincode::deserialize(stake_account2.data.as_slice())
        .map_err(|err| format!("Invalid stake account 2: {err}"))?;

    if let (
        StakeStateV2::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed1,
            },
            _,
        ),
        StakeStateV2::Stake(
            _,
            Stake {
                delegation: _,
                credits_observed: credits_observed2,
            },
            _,
        ),
    ) = (stake_state1, stake_state2)
    {
        return Ok(credits_observed1 == credits_observed2);
    }
    Ok(false)
}

pub async fn get_signature_date(
    rpc_client: &RpcClient,
    signature: Signature,
) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    let statuses = rpc_client.get_signature_statuses_with_history(&[signature])?;
    if let Some(Some(ts)) = statuses.value.first() {
        let block_date = get_block_date(rpc_client, ts.slot).await?;
        Ok(block_date)
    } else {
        Err(format!("Unknown signature: {signature}").into())
    }
}
