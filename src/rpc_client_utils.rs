use {
    chrono::prelude::*,
    solana_client::rpc_client::RpcClient,
    solana_pubkey::Pubkey,
    solana_sdk::{
        account::Account,
        account_utils::StateMut,
        clock::Slot,
        signature::Signature,
        stake::state::{Authorized, StakeStateV2},
    },
};

#[derive(Clone, Debug, PartialEq)]
pub enum StakeActivationState {
    Activating,
    Active,
    Deactivating,
    Inactive,
}

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

pub fn get_stake_activation_state(
    rpc_client: &RpcClient,
    stake_account: &Account,
) -> Result<StakeActivationState, Box<dyn std::error::Error>> {
    let stake_state = stake_account
        .state()
        .map_err(|err| format!("Failed to get account state: {err}"))?;
    let stake_history_account = rpc_client.get_account(&solana_sdk::sysvar::stake_history::id())?;
    let stake_history: solana_sdk::stake_history::StakeHistory =
        solana_sdk::account::from_account(&stake_history_account).unwrap();
    let clock_account = rpc_client.get_account(&solana_sdk::sysvar::clock::id())?;
    let clock: solana_sdk::clock::Clock =
        solana_sdk::account::from_account(&clock_account).unwrap();
    let new_rate_activation_epoch = rpc_client
        .get_feature_activation_slot(&solana_sdk::feature_set::reduce_stake_warmup_cooldown::id())
        .and_then(|activation_slot: Option<solana_sdk::clock::Slot>| {
            rpc_client
                .get_epoch_schedule()
                .map(|epoch_schedule| (activation_slot, epoch_schedule))
        })
        .map(|(activation_slot, epoch_schedule)| {
            activation_slot.map(|slot| epoch_schedule.get_epoch(slot))
        })?;

    if let solana_sdk::stake::state::StakeStateV2::Stake(_, stake, _) = stake_state {
        let solana_sdk::stake::state::StakeActivationStatus {
            effective,
            activating,
            deactivating,
        } = stake.delegation.stake_activating_and_deactivating(
            clock.epoch,
            &stake_history,
            new_rate_activation_epoch,
        );
        if effective == 0 {
            return Ok(StakeActivationState::Inactive);
        }
        if activating > 0 {
            return Ok(StakeActivationState::Activating);
        }
        if deactivating > 0 {
            return Ok(StakeActivationState::Deactivating);
        }
        return Ok(StakeActivationState::Active);
    }
    Err("No stake".to_string().into())
}

pub fn get_stake_authorized(
    rpc_client: &RpcClient,
    stake_account_address: Pubkey,
) -> Result<(Authorized, Pubkey), Box<dyn std::error::Error>> {
    let stake_account = rpc_client.get_account(&stake_account_address)?;

    match get_stake_activation_state(rpc_client, &stake_account)? {
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
