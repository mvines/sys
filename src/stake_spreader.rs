use {
    crate::{db::*, notifier::*, rpc_client_utils::get_signature_date},
    log::*,
    solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig, rpc_custom_error},
    solana_sdk::{
        self,
        account::from_account,
        clock::{Clock, Epoch},
        epoch_info::EpochInfo,
        message::Message,
        pubkey::Pubkey,
        reward_type::RewardType,
        signers::Signers,
        stake::{
            self,
            state::{StakeActivationStatus, StakeStateV2},
        },
        stake_history::StakeHistory,
        sysvar::{clock, stake_history},
        transaction::Transaction,
    },
    solana_transaction_status::Reward,
    std::collections::{BTreeMap, HashMap, HashSet},
    sys::{send_transaction_until_expired, token::*},
};

const MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY: usize = 5; // Remove once Solana 1.15 ships. Ref: https://github.com/solana-labs/solana/pull/28096

fn get_epoch_commissions(
    rpc_client: &RpcClient,
    epoch_info: &EpochInfo,
    epoch: Epoch,
) -> Result<BTreeMap<Pubkey, u8>, Box<dyn std::error::Error>> {
    if epoch > epoch_info.epoch {
        return Err(format!("Future epoch, {epoch}, requested").into());
    }

    let first_slot_in_epoch = epoch_info
        .absolute_slot
        .saturating_sub(epoch_info.slot_index)
        - (epoch_info.epoch - epoch) * epoch_info.slots_in_epoch;

    let mut first_block_in_epoch = first_slot_in_epoch;
    loop {
        info!("fetching block in slot {}", first_block_in_epoch);
        match rpc_client.get_block_with_config(first_block_in_epoch, RpcBlockConfig::rewards_only())
        {
            Ok(block) => {
                return Ok(block
                    .rewards
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|reward| match reward {
                        Reward {
                            reward_type: Some(RewardType::Voting),
                            commission: Some(commission),
                            pubkey,
                            ..
                        } => Some((pubkey.parse::<Pubkey>().unwrap_or_default(), commission)),
                        _ => None,
                    })
                    .collect());
            }
            Err(err) => {
                if matches!(
                        err.kind(),
                        solana_client::client_error::ClientErrorKind::RpcError(solana_client::rpc_request::RpcError::RpcResponseError {
                            code: rpc_custom_error::JSON_RPC_SERVER_ERROR_SLOT_SKIPPED |
                            rpc_custom_error::JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                            ..
                        })
                    ) {
                        info!("slot {} skipped",first_block_in_epoch);
                        first_block_in_epoch += 1;
                        continue;
                    }
                return Err(format!(
                    "Failed to fetch the block for slot {first_block_in_epoch}: {err:?}"
                )
                .into());
            }
        }
    }
}

/// Returns a `Vec` of ("epoch staker credits earned", "validator vote account address"), ordered
/// by epoch staker credits earned.
fn get_validator_credit_scores(
    rpc_client: &RpcClient,
    epoch_info: &EpochInfo,
    epoch: Epoch,
) -> Result<Vec<ValidatorCreditScore>, Box<dyn std::error::Error>> {
    let epoch_commissions = if epoch == epoch_info.epoch {
        None
    } else {
        Some(get_epoch_commissions(rpc_client, epoch_info, epoch)?)
    };

    let vote_accounts = rpc_client.get_vote_accounts()?;

    let mut list = vote_accounts
        .current
        .into_iter()
        .chain(vote_accounts.delinquent)
        .filter_map(|vai| {
            vai.epoch_credits.iter().find(|ec| ec.0 == epoch).and_then(
                |(_, credits, prev_credits)| {
                    vai.vote_pubkey
                        .parse::<Pubkey>()
                        .ok()
                        .and_then(|vote_pubkey| {
                            let (epoch_commission, epoch_credits) = {
                                let epoch_commission = match &epoch_commissions {
                                    Some(epoch_commissions) => {
                                        *epoch_commissions.get(&vote_pubkey).unwrap()
                                    }
                                    None => vai.commission,
                                };
                                let epoch_credits = credits.saturating_sub(*prev_credits);
                                (epoch_commission, epoch_credits)
                            };

                            if epoch_credits > 0 {
                                let staker_credits =
                                    (u128::from(epoch_credits) * u128::from(100 - epoch_commission)
                                        / 100) as u64;
                                debug!(
                                    "{}: total credits {}, staker credits {} in epoch {}",
                                    vote_pubkey, epoch_credits, staker_credits, epoch,
                                );
                                Some(ValidatorCreditScore {
                                    credits: staker_credits,
                                    vote_account: vote_pubkey,
                                })
                            } else {
                                None
                            }
                        })
                },
            )
        })
        .collect::<Vec<_>>();

    list.sort_by(|a, b| b.credits.cmp(&a.credits));
    Ok(list)
}

#[allow(clippy::too_many_arguments)]
pub async fn run<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    epoch_completed_percentage: u8,
    epoch_history: u64,
    num_validators: usize,
    included_vote_account_addresses: HashSet<Pubkey>,
    excluded_vote_account_addresses: HashSet<Pubkey>,
    authority_address: Pubkey,
    signers: T,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let epoch_info = rpc_client.get_epoch_info()?;
    let current_epoch = epoch_info.epoch;

    if epoch_history >= MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY as u64 {
        // If a longer history is desired, epoch credits must be extracted from the validator vote
        // accounts directory instead of using the `get_vote_accounts` RPC method...
        return Err(format!(
            "Epoch history must be less than {MAX_RPC_VOTE_ACCOUNT_INFO_EPOCH_CREDITS_HISTORY}"
        )
        .into());
    }

    for epoch in current_epoch.saturating_sub(epoch_history)..current_epoch {
        if !db.contains_validator_credit_scores(epoch) {
            println!("Computing validator credit scores for epoch {epoch}");
            let validator_credit_scores =
                get_validator_credit_scores(rpc_client, &epoch_info, epoch).map_err(|err| {
                    format!("Failed to get validator credit score for epoch {epoch}: {err:?}")
                })?;
            db.set_validator_credit_scores(epoch, validator_credit_scores)?;
        }
    }

    let current_epoch_completed_percentage =
        (epoch_info.slot_index * 100 / epoch_info.slots_in_epoch) as u8;
    if !(1..=98).contains(&current_epoch_completed_percentage) {
        println!("Too close to an epoch boundary to make stake redelegation decisions");
        return Ok(());
    }

    if current_epoch_completed_percentage < epoch_completed_percentage {
        println!("Too soon in the current epoch to start making stake redelegation decisions");
        return Ok(());
    }

    if current_epoch < epoch_history {
        println!("Too few elapsed epochs to make stake redelegation decisions");
        return Ok(());
    }

    let addresses_and_balances = db
        .get_accounts()
        .into_iter()
        .filter_map(|ta| {
            (ta.token == MaybeToken::SOL()).then_some((ta.address, ta.last_update_balance))
        })
        .collect::<Vec<_>>();

    let maybe_accounts = rpc_client.get_multiple_accounts(
        addresses_and_balances
            .iter()
            .map(|(address, _)| *address)
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    assert_eq!(maybe_accounts.len(), addresses_and_balances.len());

    let stake_history: StakeHistory = {
        let stake_history_account = rpc_client.get_account(&stake_history::id())?;
        from_account(&stake_history_account).ok_or("Failed to deserialize stake history")?
    };

    let clock: Clock = {
        let clock_account = rpc_client.get_account(&clock::id())?;
        from_account(&clock_account).ok_or("Failed to deserialize clock")?
    };

    #[derive(Clone)]
    struct StakeAccountInfo {
        last_update_balance: u64,
        vote_account_address: Pubkey,
        busy: bool,
        locked: bool,
        meta: stake::state::Meta,
    }

    let mut stake_accounts = addresses_and_balances
        .into_iter()
        .zip(maybe_accounts.into_iter().map(Option::unwrap_or_default))
        .filter_map(|((address, last_update_balance), account)| {
            if account.owner == stake::program::id() {
                bincode::deserialize::<StakeStateV2>(account.data.as_slice())
                    .ok()
                    .and_then(|state| match state {
                        StakeStateV2::Initialized(meta) => {
                            if meta.authorized.staker == authority_address {
                                let locked = meta.lockup.is_in_force(&clock, None);
                                Some((
                                    address,
                                    StakeAccountInfo {
                                        last_update_balance,
                                        vote_account_address: Pubkey::default(),
                                        busy: false,
                                        locked,
                                        meta,
                                    },
                                ))
                            } else {
                                None
                            }
                        }
                        StakeStateV2::Stake(meta, stake, _stake_flags) => {
                            if meta.authorized.staker == authority_address {
                                let locked = meta.lockup.is_in_force(&clock, None);

                                let StakeActivationStatus {
                                    effective,
                                    activating,
                                    deactivating,
                                } = stake.delegation.stake_activating_and_deactivating(
                                    current_epoch,
                                    Some(&stake_history),
                                    None,
                                );

                                let busy = (activating + deactivating) > 0
                                    || (stake.delegation.deactivation_epoch > current_epoch
                                        && stake.delegation.deactivation_epoch < std::u64::MAX);

                                let active = busy || effective > 0;
                                Some((
                                    address,
                                    StakeAccountInfo {
                                        last_update_balance,
                                        vote_account_address: active
                                            .then_some(stake.delegation.voter_pubkey)
                                            .unwrap_or_default(),
                                        busy,
                                        locked,
                                        meta,
                                    },
                                ))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    println!("Discovered stake accounts:");
    for (address, sai) in &stake_accounts {
        #[allow(clippy::to_string_in_format_args)]
        {
            println!(
                "* {:<44}: delegated to {:<44}, busy={}, locked={}",
                address.to_string(),
                sai.vote_account_address.to_string(),
                sai.busy,
                sai.locked
            );
        }
    }

    // Gather merge candidates
    let mut merge_candidates = stake_accounts
        .iter()
        .filter(|(_, sai)| sai.vote_account_address == Pubkey::default())
        .collect::<Vec<_>>();

    println!("{} merge candidates", merge_candidates.len());
    let mut merged_from_stake_account_adddresses = HashSet::<Pubkey>::default();
    let mut merged_into_stake_account_adddresses = HashSet::<Pubkey>::default();
    while merge_candidates.len() > 1 {
        let (merge_candidate_address, merge_candidate_sai) = merge_candidates.pop().unwrap();

        // Locate the first peer in the `merge_candidates` with a compatible `lockup` and
        // `authorized`. Only merge the detritus (accounts < 1 SOL) of previous redelegate stake
        // operations.
        let peer = merge_candidates.iter().find(|(_, peer_sai)| {
            merge_candidate_sai.last_update_balance <= MaybeToken::SOL().amount(1.)
                && merge_candidate_sai.meta.authorized == peer_sai.meta.authorized
                && if merge_candidate_sai.locked {
                    peer_sai.locked && merge_candidate_sai.meta.lockup == peer_sai.meta.lockup
                } else {
                    !peer_sai.locked
                }
        });

        if let Some((peer_address, _)) = peer {
            // Merge `merge_candidate` into `peer`
            let from_address = *merge_candidate_address;
            let into_address = **peer_address;

            println!("Merging {from_address} into {into_address}");
            merged_from_stake_account_adddresses.insert(from_address);
            merged_into_stake_account_adddresses.insert(into_address);

            let (recent_blockhash, last_valid_block_height) =
                rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;
            let authority_account = rpc_client
                .get_account_with_commitment(&authority_address, rpc_client.commitment())?
                .value
                .ok_or_else(|| format!("Authority account, {authority_address}, does not exist"))?;

            let mut message = Message::new(
                &solana_sdk::stake::instruction::merge(
                    &into_address,
                    &from_address,
                    &authority_address,
                ),
                Some(&authority_address),
            );
            message.recent_blockhash = recent_blockhash;
            if rpc_client.get_fee_for_message(&message)? > authority_account.lamports {
                eprintln!("Insufficient funds for transaction fee");
                continue;
            }

            let mut transaction = Transaction::new_unsigned(message);
            let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
            if simulation_result.err.is_some() {
                eprintln!("Simulation failure: {simulation_result:?}");
                continue;
            }

            transaction.try_sign(&signers, recent_blockhash)?;
            let signature = transaction.signatures[0];
            println!("Transaction signature: {signature}");

            db.record_transfer(
                signature,
                last_valid_block_height,
                None,
                from_address,
                MaybeToken::SOL(),
                into_address,
                MaybeToken::SOL(),
                LotSelectionMethod::default(),
                None,
            )?;

            if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
                db.cancel_transfer(signature)?;
                eprintln!("Merge failed");
            } else {
                let when = get_signature_date(rpc_client, signature).await?;
                db.confirm_transfer(signature, when)?;
                db.remove_account(from_address, MaybeToken::SOL())?;
            }
        }
    }

    for into_address in merged_into_stake_account_adddresses {
        stake_accounts
            .get_mut(&into_address)
            .unwrap()
            .last_update_balance = db
            .get_account(into_address, MaybeToken::SOL())
            .unwrap()
            .last_update_balance;
    }

    // Remove stake accounts just merged into another account from consideration.
    stake_accounts.retain(|address, _| !merged_from_stake_account_adddresses.contains(address));

    // Filter out stake accounts with a balance less than 1 SOL
    stake_accounts.retain(|address, _| {
        db.get_account(*address, MaybeToken::SOL())
            .map(|ta| ta.last_update_balance)
            .unwrap_or_default()
            > MaybeToken::SOL().amount(1.)
    });

    // Select the validators to stake to: starting with the validator credit score for the current
    // epoch, add in the validator credit scores across the last `epoch_history` epochs. Then
    // select the `num_validators` validators with the highest aggregate score
    let mut validator_credit_scores =
        get_validator_credit_scores(rpc_client, &epoch_info, current_epoch)?
            .into_iter()
            .map(|vcs| (vcs.vote_account, vcs.credits))
            .collect::<HashMap<_, _>>();

    for epoch in current_epoch.saturating_sub(epoch_history)..current_epoch {
        for ValidatorCreditScore {
            vote_account,
            credits,
        } in db.get_validator_credit_scores(epoch)
        {
            *validator_credit_scores.entry(vote_account).or_default() += credits;
        }
    }

    for included_vote_account_address in included_vote_account_addresses {
        validator_credit_scores
            .entry(included_vote_account_address)
            .and_modify(|credits| *credits = u64::MAX);
    }

    let selected_validators = validator_credit_scores
        .iter()
        .filter_map(|(vote_account, credits)| {
            if excluded_vote_account_addresses.contains(vote_account) {
                None
            } else {
                Some((credits, vote_account))
            }
        })
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .rev()
        .map(|(_credits, vote_account_address)| vote_account_address)
        .take(num_validators)
        .collect::<HashSet<_>>();

    println!("Selected validator vote accounts:");
    for selected_validator in &selected_validators {
        println!("* {selected_validator}");
    }

    // Determine the amount of stake for each validator
    let stake_account_balances = stake_accounts
        .keys()
        .map(|address| {
            (
                address,
                db.get_account(*address, MaybeToken::SOL())
                    .unwrap()
                    .last_update_balance,
            )
        })
        .collect::<HashMap<_, _>>();

    let target_stake_per_validator =
        stake_account_balances.values().sum::<u64>() / num_validators as u64;
    println!(
        "Target stake per validator: {}",
        MaybeToken::SOL().format_amount(target_stake_per_validator)
    );

    #[derive(Default)]
    struct StakeByValidator {
        stake_amount: u64,
        movable_stake: Vec<(Pubkey, u64)>, // (address, stake)
    }
    let mut stake_by_validator = selected_validators
        .into_iter()
        .map(|vote_account_address| (vote_account_address, StakeByValidator::default()))
        .collect::<HashMap<_, _>>();

    let mut movable_stake = Vec::<(Pubkey, u64)>::default(); // (address, stake)

    for (address, sai) in &stake_accounts {
        let stake_amount = stake_account_balances.get(address).unwrap();
        if stake_by_validator.contains_key(&sai.vote_account_address) {
            let entry = stake_by_validator
                .get_mut(&sai.vote_account_address)
                .unwrap();
            entry.stake_amount += stake_amount;
            if !sai.busy {
                entry.movable_stake.push((*address, *stake_amount));
            }
        } else {
            movable_stake.push((*address, *stake_amount));
        };
    }

    let mut redelegations = vec![];
    println!("Target stake distribution:");
    for (vote_account_address, sbv) in stake_by_validator.iter_mut() {
        // Peel of excess stake if needed
        let mut excess_stake = sbv.stake_amount.saturating_sub(target_stake_per_validator);
        while excess_stake > 0 {
            if let Some((address, stake)) = sbv.movable_stake.pop() {
                excess_stake = excess_stake.saturating_sub(stake);
                assert!(sbv.stake_amount > stake);
                sbv.stake_amount -= stake;
                movable_stake.push((address, stake));
            } else {
                break;
            }
        }

        // Add new stake if needed
        let mut new_stake = vec![]; // Vec<(Pubkey, u64)>,
        while sbv.stake_amount < target_stake_per_validator {
            if let Some((address, stake)) = movable_stake.pop() {
                sbv.stake_amount += stake;
                new_stake.push((address, stake));
            } else {
                break;
            }
        }

        println!(
            "* {} has {}. Adding {} additional stake via {:?}",
            vote_account_address,
            MaybeToken::SOL().format_amount(sbv.stake_amount),
            MaybeToken::SOL().format_amount(new_stake.iter().map(|(_, stake)| stake).sum::<u64>()),
            new_stake
                .iter()
                .map(|(address, stake)| (address, MaybeToken::SOL().format_amount(*stake)))
                .collect::<Vec<_>>(),
        );
        redelegations.push((*vote_account_address, new_stake));
    }

    println!("{} transactions to execute", redelegations.len());
    let mut transaction_failures = 0;
    for (vote_account_address, new_stake) in redelegations {
        for (stake_account_address, _) in new_stake {
            let sai = stake_accounts.get(&stake_account_address).unwrap();
            assert!(!sai.busy, "{stake_account_address} should not be busy");
            if sai.vote_account_address == Pubkey::default() {
                println!("Delegate {stake_account_address} to {vote_account_address}");

                let (recent_blockhash, last_valid_block_height) =
                    rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

                let mut message = Message::new(
                    &[solana_sdk::stake::instruction::delegate_stake(
                        &stake_account_address,
                        vote_account_address,
                        &authority_address,
                    )],
                    Some(&authority_address),
                );
                message.recent_blockhash = recent_blockhash;

                let mut transaction = Transaction::new_unsigned(message);
                let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
                if simulation_result.err.is_some() {
                    eprintln!("Simulation failure: {simulation_result:?}");
                    transaction_failures += 1;
                    continue;
                }

                transaction.try_sign(&signers, recent_blockhash)?;
                let signature = transaction.signatures[0];
                println!("Transaction signature: {signature}");

                if !send_transaction_until_expired(
                    rpc_client,
                    &transaction,
                    last_valid_block_height,
                ) {
                    eprintln!("Delegation failed");
                    transaction_failures += 1;
                }
            } else {
                crate::process_account_redelegate(
                    db,
                    rpc_client,
                    stake_account_address,
                    *vote_account_address,
                    LotSelectionMethod::default(),
                    authority_address,
                    &signers,
                    None,
                )
                .await
                .unwrap_or_else(|err| {
                    eprintln!("Redelegation failed: {err:?}");
                    transaction_failures += 1;
                });
            }
        }
    }

    if transaction_failures > 0 {
        let msg = format!("stake spreader: {transaction_failures} transactions failed");
        notifier.send(&msg).await;
        println!("{msg}");
    }

    Ok(())
}
