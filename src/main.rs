mod binance_exchange;
mod coin_gecko;
mod db;
mod exchange;
mod field_as_string;
mod ftx_exchange;
mod notifier;
mod rpc_client_utils;

use {
    chrono::prelude::*,
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, App, AppSettings, Arg, SubCommand,
    },
    db::*,
    exchange::*,
    notifier::*,
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        message::Message,
        native_token::{lamports_to_sol, sol_to_lamports, Sol},
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signature, Signer},
        signers::Signers,
        system_instruction, system_program,
        transaction::Transaction,
    },
    solana_transaction_status::UiTransactionEncoding,
    std::{path::PathBuf, process::exit, str::FromStr},
};

// TOOD: Remove `local_stake_instruction` once 1.6.11 ships and
//              `solana_stake_program::stake_instruction::initialize` is public
mod local_stake_instruction {
    use {
        solana_sdk::{
            instruction::{AccountMeta, Instruction},
            pubkey::Pubkey,
            sysvar,
        },
        solana_stake_program::stake_state::*,
    };

    pub fn initialize(
        stake_pubkey: &Pubkey,
        authorized: &Authorized,
        lockup: &Lockup,
    ) -> Instruction {
        Instruction::new_with_bincode(
            solana_stake_program::id(),
            &solana_stake_program::stake_instruction::StakeInstruction::Initialize(
                *authorized,
                *lockup,
            ),
            vec![
                AccountMeta::new(*stake_pubkey, false),
                AccountMeta::new_readonly(sysvar::rent::id(), false),
            ],
        )
    }
}

fn naivedate_of(string: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(string, "%y/%m/%d")
        .or_else(|_| NaiveDate::parse_from_str(string, "%Y/%m/%d"))
        .map_err(|err| format!("error parsing '{}': {}", string, err))
}

fn app_version() -> String {
    let tag = option_env!("GITHUB_REF")
        .and_then(|github_ref| github_ref.strip_prefix("refs/tags/").map(|s| s.to_string()));

    tag.unwrap_or_else(|| match option_env!("GITHUB_SHA") {
        None => "devbuild".to_string(),
        Some(commit) => commit[..8].to_string(),
    })
}

async fn process_sync_exchange(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let recent_deposits = exchange_client.recent_deposits().await?;

    for pending_deposit in db.pending_deposits(exchange) {
        if let Some(deposit_info) = recent_deposits
            .iter()
            .find(|deposit_info| deposit_info.tx_id == pending_deposit.tx_id)
        {
            let missing_lamports =
                sol_to_lamports((deposit_info.amount - pending_deposit.amount).abs());
            if missing_lamports >= 10 {
                let msg = format!(
                    "Error! Deposit amount mismatch for {}! Actual amount: ◎{}, expected amount: ◎{}",
                    pending_deposit.tx_id, deposit_info.amount, pending_deposit.amount
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            } else {
                if missing_lamports != 0 {
                    // Binance will occasionally steal a lamport or two...
                    let msg = format!(
                        "{:?} just stole {} lamports from your deposit!",
                        exchange, missing_lamports
                    );
                    println!("{}", msg);
                    notifier.send(&format!("{:?}: {}", exchange, msg)).await;
                }

                let msg = format!(
                    "◎{} deposit successful ({})",
                    pending_deposit.amount, pending_deposit.tx_id
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;

                db.confirm_deposit(&pending_deposit)?;
            }
        } else {
            println!(
                "◎{} deposit pending ({})",
                pending_deposit.amount, pending_deposit.tx_id
            );
        }
    }

    for order_info in db.pending_orders(exchange) {
        let order_status = exchange_client
            .sell_order_status(&order_info.pair, &order_info.order_id)
            .await?;
        let order_summary = format!(
            "{}: ◎{} at ${} (◎{} filled)",
            order_info.pair, order_status.amount, order_status.price, order_status.filled_amount,
        );

        if order_status.open {
            if order_status.filled_amount > 0. {
                let msg = format!("Partially filled: {}", order_summary);
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            } else {
                println!("Open order: {}", order_summary);
            }
        } else {
            let msg = if (order_status.amount - order_status.filled_amount).abs() < f64::EPSILON {
                format!("Order filled: {}", order_summary)
            } else if order_status.filled_amount < f64::EPSILON {
                format!("Order cancelled: {}", order_summary)
            } else {
                panic!("TODO: Handle partial execution upon cancel");
            };
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            db.clear_order(&order_info)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_deposit<T: Signers>(
    db: &mut Db,
    rpc_client: RpcClient,
    exchange: Exchange,
    deposit_address: Pubkey,
    amount: Option<u64>,
    from_address: Pubkey,
    authority_address: Pubkey,
    signers: T,
) -> Result<(), Box<dyn std::error::Error>> {
    let (recent_blockhash, fee_calculator) = rpc_client.get_recent_blockhash()?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("From account, {}, does not exist", from_address))?;

    let authority_account = if from_address == authority_address {
        from_account.clone()
    } else {
        rpc_client
            .get_account_with_commitment(&authority_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("Authority account, {}, does not exist", authority_address))?
    };

    if authority_account.lamports < fee_calculator.lamports_per_signature {
        return Err(format!(
            "Authority has insufficient funds for the transaction fee of {}",
            Sol(fee_calculator.lamports_per_signature)
        )
        .into());
    }

    let (instructions, lamports, minimum_balance) = if from_account.owner == system_program::id() {
        let lamports = amount.unwrap_or_else(|| {
            if from_address == authority_address {
                from_account
                    .lamports
                    .saturating_sub(fee_calculator.lamports_per_signature)
            } else {
                from_account.lamports
            }
        });

        (
            vec![system_instruction::transfer(
                &from_address,
                &deposit_address,
                lamports,
            )],
            lamports,
            if from_address == authority_address {
                fee_calculator.lamports_per_signature
            } else {
                0
            },
        )
    } else if from_account.owner == solana_vote_program::id() {
        let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
            solana_vote_program::vote_state::VoteState::size_of(),
        )?;

        let lamports =
            amount.unwrap_or_else(|| from_account.lamports.saturating_sub(minimum_balance));

        (
            vec![solana_vote_program::vote_instruction::withdraw(
                &from_address,
                &authority_address,
                lamports,
                &deposit_address,
            )],
            lamports,
            minimum_balance,
        )
    } else if from_account.owner == solana_stake_program::id() {
        let lamports = amount.unwrap_or(from_account.lamports);

        (
            vec![solana_stake_program::stake_instruction::withdraw(
                &from_address,
                &authority_address,
                &deposit_address,
                lamports,
                None,
            )],
            lamports,
            0,
        )
    } else {
        return Err(format!("Unsupported `from` account owner: {}", from_account.owner).into());
    };

    if lamports == 0 {
        return Err("Nothing to deposit".into());
    }

    if from_account.lamports < lamports + minimum_balance {
        return Err("From account has insufficient funds".into());
    }

    let amount = lamports_to_sol(lamports);
    println!("From address: {}", from_address);
    if from_address != authority_address {
        println!("Authority address: {}", authority_address);
    }
    println!("Amount: {}", Sol(lamports));
    println!("{:?} deposit address: {}", exchange, deposit_address);

    let message = Message::new(&instructions, Some(&authority_address));
    if fee_calculator.calculate_fee(&message) > authority_account.lamports {
        return Err("Insufficient funds for transaction fee".into());
    }

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&signers, recent_blockhash)?;
    println!("Transaction signature: {}", transaction.signatures[0]);

    let pending_deposit = PendingDeposit {
        tx_id: transaction.signatures[0].to_string(),
        exchange,
        amount,
    };

    db.record_deposit(pending_deposit.clone())?;

    loop {
        match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
            Ok(_) => return Ok(()),
            Err(err) => {
                println!("Send transaction failed: {:?}", err);
            }
        }
        match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
            Err(err) => {
                println!("Failed to get fee calculator: {:?}", err);
            }
            Ok(None) => {
                db.cancel_deposit(&pending_deposit).expect("cancel_deposit");
                return Err("Deposit failed: {}".into());
            }
            Ok(_) => {
                println!("Blockhash has not yet expired, retrying transaction...");
            }
        };
    }
}

enum LimitOrderPrice {
    At(f64),
    AmountOverAsk(f64),
}

async fn process_exchange_sell(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    pair: String,
    amount: f64,
    price: LimitOrderPrice,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!("Symbol: {}", pair);
    println!("Ask: ${}, Bid: ${}", bid_ask.ask_price, bid_ask.bid_price,);

    let price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(extra) => bid_ask.ask_price + extra,
    };

    println!("Placing sell order for ◎{} at ${}", amount, price);

    if bid_ask.bid_price > price {
        return Err("Order price is less than bid price".into());
    }

    let order_id = exchange_client
        .place_sell_order(&pair, price, amount)
        .await?;

    db.record_order(OpenOrder {
        exchange,
        pair: pair.clone(),
        order_id,
    })?;
    Ok(())
}

fn println_lot(
    lot: &Lot,
    current_price: f64,
    total_gain: &mut f64,
    total_current_value: &mut f64,
    notifier: Option<&Notifier>,
) {
    let acquisition_value = lamports_to_sol(lot.amount) * lot.acquisition.price;
    let current_value = lamports_to_sol(lot.amount) * current_price;
    let gain = current_value - acquisition_value;

    *total_gain += gain;
    *total_current_value += current_value;

    let msg = format!(
        "{:>3}. {} | ◎{:<10.2} at ${:<6.2} | gain: ${:<12.2} | acquisition value: ${:<12.2} now: ${:<12.2} | {:?}",
        lot.lot_number,
        lot.acquisition.when,
        lamports_to_sol(lot.amount),
        lot.acquisition.price,
        gain,
        acquisition_value,
        current_value,
        lot.acquisition.kind,
    );

    notifier.map(|notifier| notifier.send(&msg));
    println!("{}", msg);
}

async fn process_account_add(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Pubkey,
    description: String,
    when: NaiveDate,
    price: Option<f64>,
    signature: Option<Signature>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (when, amount, last_update_epoch, kind) = match signature {
        Some(signature) => {
            let confirmed_transaction =
                rpc_client.get_confirmed_transaction(&signature, UiTransactionEncoding::Base64)?;

            let slot = confirmed_transaction.slot;
            let when = match confirmed_transaction.block_time {
                Some(block_time) => NaiveDateTime::from_timestamp_opt(block_time, 0)
                    .ok_or_else(|| format!("Invalid block time for slot {}", slot))?
                    .date(),
                None => {
                    println!(
                        "Block time not available for slot {}, using `--when` argument instead: {}",
                        slot, when
                    );
                    when
                }
            };

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
                .account_keys
                .iter()
                .position(|k| *k == address)
                .ok_or_else(|| format!("{} not found in the transaction {}", address, signature))?;

            let amount = meta.post_balances[account_index];

            let epoch_schdule = rpc_client.get_epoch_schedule()?;
            let last_update_epoch = epoch_schdule.get_epoch_and_slot_index(slot).0;

            (
                when,
                amount,
                last_update_epoch,
                LotAcquistionKind::Transaction { slot, signature },
            )
        }
        None => {
            let amount = rpc_client
                .get_account_with_commitment(&address, rpc_client.commitment())?
                .value
                .ok_or_else(|| format!("{} does not exist", address))?
                .lamports;
            let last_update_epoch = rpc_client.get_epoch_info()?.epoch.saturating_sub(1);
            (
                when,
                amount,
                last_update_epoch,
                LotAcquistionKind::NotAvailable,
            )
        }
    };

    println!("Adding {}", address);

    let current_price = coin_gecko::get_current_price().await?;
    let price = match price {
        Some(price) => price,
        None => coin_gecko::get_price(when).await?,
    };

    let lot = Lot {
        lot_number: db.next_lot_number(),
        acquisition: LotAcquistion { when, price, kind },
        amount,
    };
    println_lot(&lot, current_price, &mut 0., &mut 0., None);

    let account = TrackedAccount {
        address,
        description,
        last_update_epoch,
        last_update_balance: lot.amount,
        lots: vec![lot],
    };
    db.add_account(account)?;

    Ok(())
}

async fn process_account_list(db: &Db) -> Result<(), Box<dyn std::error::Error>> {
    let accounts = db.get_accounts();
    if accounts.is_empty() {
        println!("No accounts");
    } else {
        let current_price = coin_gecko::get_current_price().await?;

        let mut total_gain = 0.;
        let mut total_current_value = 0.;

        for account in accounts.values() {
            println!("{}: {}", account.address.to_string(), account.description);

            if !account.lots.is_empty() {
                for lot in &account.lots {
                    println_lot(
                        lot,
                        current_price,
                        &mut total_gain,
                        &mut total_current_value,
                        None,
                    );
                }
            } else {
                println!("  No lots");
            }
            println!();
        }

        if let Some(sweep_stake_account) = db.get_sweep_stake_account() {
            println!("Sweep stake account: {}", sweep_stake_account.address);
            println!(
                "Stake authority: {}",
                sweep_stake_account.stake_authority.display()
            );
            println!();
        }

        println!("Current price: ${:<.2}", current_price);
        println!("Current value: ${:<.2}", total_current_value);
        println!("Total gain:    ${:<.2}", total_gain);
    }
    Ok(())
}

async fn process_account_sweep<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    from_address: Pubkey,
    retain_amount: u64,
    authority_address: Pubkey,
    signers: T,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let (recent_blockhash, fee_calculator) = rpc_client.get_recent_blockhash()?;
    let epoch_info = rpc_client.get_epoch_info()?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("Account, {}, does not exist", from_address))?;

    let authority_account = if from_address == authority_address {
        from_account.clone()
    } else {
        rpc_client
            .get_account_with_commitment(&authority_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| format!("Authority account, {}, does not exist", authority_address))?
    };

    let num_transaction_signatures = 3;

    if authority_account.lamports
        < num_transaction_signatures * fee_calculator.lamports_per_signature
    {
        return Err(format!(
            "Authority has insufficient funds for the transaction fee of {}",
            Sol(num_transaction_signatures * fee_calculator.lamports_per_signature)
        )
        .into());
    }

    let transitory_stake_account = Keypair::new();

    let (mut instructions, sweep_amount) = if from_account.owner == system_program::id() {
        let lamports = if from_address == authority_address {
            from_account.lamports.saturating_sub(
                num_transaction_signatures * fee_calculator.lamports_per_signature + retain_amount,
            )
        } else {
            from_account.lamports.saturating_sub(retain_amount)
        };

        (
            vec![system_instruction::transfer(
                &from_address,
                &transitory_stake_account.pubkey(),
                lamports,
            )],
            lamports,
        )
    } else if from_account.owner == solana_vote_program::id() {
        let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
            solana_vote_program::vote_state::VoteState::size_of(),
        )?;

        let lamports = from_account
            .lamports
            .saturating_sub(minimum_balance + retain_amount);

        (
            vec![solana_vote_program::vote_instruction::withdraw(
                &from_address,
                &authority_address,
                lamports,
                &transitory_stake_account.pubkey(),
            )],
            lamports,
        )
    } else if from_account.owner == solana_stake_program::id() {
        let lamports = from_account.lamports.saturating_sub(retain_amount);

        (
            vec![solana_stake_program::stake_instruction::withdraw(
                &from_address,
                &authority_address,
                &transitory_stake_account.pubkey(),
                lamports,
                None,
            )],
            lamports,
        )
    } else {
        return Err(format!("Unsupported `from` account owner: {}", from_account.owner).into());
    };

    if sweep_amount < sol_to_lamports(1.) {
        return Err(format!(
            "{} has less than ◎1 to sweep ({})",
            from_address,
            Sol(sweep_amount)
        )
        .into());
    }

    println!("From address: {}", from_address);
    if from_address != authority_address {
        println!("Authority address: {}", authority_address);
    }
    println!("Sweep amount: {}", Sol(sweep_amount));
    println!(
        "Transitory stake address: {}",
        transitory_stake_account.pubkey()
    );

    let sweep_stake_account = db
        .get_sweep_stake_account()
        .ok_or("Sweep stake account not configured")?;
    let stake_authority_keypair = read_keypair_file(&sweep_stake_account.stake_authority)?;

    let (authorized, vote_account_address) =
        rpc_client_utils::get_stake_authorized(&rpc_client, sweep_stake_account.address)?;

    if authorized.staker != stake_authority_keypair.pubkey() {
        return Err("Stake authority mismatch".into());
    }

    instructions.append(&mut vec![
        system_instruction::allocate(
            &transitory_stake_account.pubkey(),
            std::mem::size_of::<solana_stake_program::stake_state::StakeState>() as u64,
        ),
        system_instruction::assign(
            &transitory_stake_account.pubkey(),
            &solana_stake_program::id(),
        ),
        local_stake_instruction::initialize(
            &transitory_stake_account.pubkey(),
            &authorized,
            &solana_stake_program::stake_state::Lockup::default(),
        ),
        solana_stake_program::stake_instruction::delegate_stake(
            &transitory_stake_account.pubkey(),
            &stake_authority_keypair.pubkey(),
            &vote_account_address,
        ),
    ]);

    let message = Message::new(&instructions, Some(&authority_address));
    assert_eq!(
        fee_calculator.calculate_fee(&message),
        num_transaction_signatures * fee_calculator.lamports_per_signature
    );

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    let msg = format!(
        "Sweeping {} from {} into {} (via {})",
        Sol(sweep_amount),
        from_address,
        sweep_stake_account.address,
        transitory_stake_account.pubkey(),
    );

    transaction.partial_sign(&signers, recent_blockhash);
    transaction.try_sign(
        &[&transitory_stake_account, &stake_authority_keypair],
        recent_blockhash,
    )?;
    println!("Transaction signature: {}", transaction.signatures[0]);

    let org_from_tracked_account = db
        .get_account(from_address)
        .ok_or_else(|| format!("{} is not a tracked", from_address))?;
    let mut from_tracked_account = org_from_tracked_account.clone();
    if from_tracked_account.last_update_balance < sweep_amount {
        return Err(format!(
            "Insufficient tracked funds in {}.  Last update balance is {}",
            from_address,
            Sol(from_tracked_account.last_update_balance)
        )
        .into());
    }
    from_tracked_account
        .lots
        .sort_by_key(|lot| lot.acquisition.when);
    from_tracked_account.lots.reverse();

    let transitory_lots = {
        let mut transitory_lots = vec![];
        let mut sweep_amount_remaining = sweep_amount;
        while sweep_amount_remaining > 0 {
            let mut lot = from_tracked_account.lots.pop().unwrap();
            if lot.amount <= sweep_amount_remaining {
                sweep_amount_remaining -= lot.amount;
                transitory_lots.push(lot);
            } else {
                let mut split_lot = lot.clone();
                split_lot.amount = sweep_amount_remaining;
                lot.amount -= sweep_amount_remaining;
                transitory_lots.push(split_lot);
                from_tracked_account.lots.push(lot);
                sweep_amount_remaining = 0;
            }
        }
        transitory_lots
    };

    let org_transitory_sweep_stake_accounts = db.get_transitory_sweep_stake_accounts();
    let mut transitory_sweep_stake_accounts = org_transitory_sweep_stake_accounts.clone();
    transitory_sweep_stake_accounts.push(TransitorySweepStakeAccount {
        address: transitory_stake_account.pubkey(),
        from_address,
    });
    db.set_transitory_sweep_stake_accounts(&transitory_sweep_stake_accounts)?;
    db.add_account(TrackedAccount {
        address: transitory_stake_account.pubkey(),
        description: "Transitory sweep stake".to_string(),
        last_update_epoch: epoch_info.epoch,
        last_update_balance: sweep_amount,
        lots: transitory_lots,
    })?;
    db.update_account(from_tracked_account)?;

    loop {
        match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
            Ok(_) => break,
            Err(err) => {
                println!("Send transaction failed: {:?}", err);
            }
        }
        match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
            Err(err) => {
                println!("Failed to get fee calculator: {:?}", err);
            }
            Ok(None) => {
                //db.cancel_deposit(&pending_deposit).expect("cancel_deposit");
                // undo save on failure...
                // method to merge lots on undo...

                db.update_account(org_from_tracked_account)?;
                db.remove_account(transitory_stake_account.pubkey())?;
                db.set_transitory_sweep_stake_accounts(&org_transitory_sweep_stake_accounts)?;
                return Err("Sweep failed: {}".into());
            }
            Ok(_) => {
                println!("Blockhash has not yet expired, retrying transaction...");
            }
        };
    }

    notifier.send(&msg).await;
    println!("{}", msg);
    Ok(())
}

async fn process_account_sync(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Option<Pubkey>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut accounts = match address {
        Some(address) => {
            vec![db
                .get_account(address)
                .ok_or_else(|| format!("{} does not exist", address))?]
        }
        None => db.get_accounts().values().cloned().collect(),
    };

    if accounts.is_empty() {
        println!("No accounts to sync");
        return Ok(());
    }

    let current_price = coin_gecko::get_current_price().await?;

    let addresses: Vec<Pubkey> = accounts
        .iter()
        .map(|TrackedAccount { address, .. }| *address)
        .collect::<Vec<_>>();

    let epoch_info = rpc_client.get_epoch_info()?;

    let start_epoch = accounts
        .iter()
        .map(
            |TrackedAccount {
                 last_update_epoch, ..
             }| last_update_epoch,
        )
        .min()
        .unwrap()
        + 1;

    let stop_epoch = epoch_info.epoch.saturating_sub(1);

    if start_epoch > stop_epoch {
        println!("Processed up to epoch {}", stop_epoch);
        return Ok(());
    }

    for epoch in start_epoch..=stop_epoch {
        println!("Processing epoch: {}", epoch);
        let inflation_rewards = rpc_client.get_inflation_reward(&addresses, Some(epoch))?;

        for (inflation_reward, address, mut account) in
            itertools::izip!(inflation_rewards, addresses.iter(), accounts.iter_mut(),)
        {
            assert_eq!(*address, account.address);
            if account.last_update_epoch >= epoch {
                continue;
            }

            if let Some(inflation_reward) = inflation_reward {
                account.last_update_balance += inflation_reward.amount;

                let slot = inflation_reward.effective_slot;
                let (when, price) = coin_gecko::get_block_date_and_price(&rpc_client, slot).await?;

                let lot = Lot {
                    lot_number: db.next_lot_number(),
                    acquisition: LotAcquistion {
                        when,
                        price,
                        kind: LotAcquistionKind::EpochReward { epoch, slot },
                    },
                    amount: inflation_reward.amount,
                };
                println_lot(&lot, current_price, &mut 0., &mut 0., Some(&notifier));
                account.lots.push(lot);
            }
        }
    }

    for mut account in accounts.iter_mut() {
        account.last_update_epoch = stop_epoch;

        let current_balance = rpc_client.get_balance(&account.address)?;

        if current_balance > account.last_update_balance + sol_to_lamports(1.) {
            // Larger than a 1 SOL increase? Register a new lot of unknown origin

            let slot = epoch_info.absolute_slot;
            let (when, price) = coin_gecko::get_block_date_and_price(&rpc_client, slot).await?;
            let amount = current_balance - account.last_update_balance;

            let lot = Lot {
                lot_number: db.next_lot_number(),
                acquisition: LotAcquistion {
                    when,
                    price,
                    kind: LotAcquistionKind::NotAvailable,
                },
                amount,
            };
            println_lot(&lot, current_price, &mut 0., &mut 0., Some(&notifier));
            account.lots.push(lot);
            account.last_update_balance = current_balance;
        }

        db.update_account(account.clone())?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_db_path = "sell-your-sol";
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";
    let default_when = {
        let today = Utc::now().date();
        format!("{}/{}/{}", today.year(), today.month(), today.day())
    };
    let exchanges = ["binance", "binanceus", "ftx", "ftxus"];

    let app_version = &*app_version();
    let mut app = App::new(crate_name!())
        .about(crate_description!())
        .version(app_version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::InferSubcommands)
        .arg(
            Arg::with_name("db_path")
                .long("db-path")
                .value_name("PATH")
                .takes_value(true)
                .default_value(default_db_path)
                .global(true)
                .help("Database path"),
        )
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url_or_moniker)
                .default_value(default_json_rpc_url)
                .help("JSON RPC URL for the cluster"),
        )
        .subcommand(
            SubCommand::with_name("price")
                .about("Get historical SOL price from CoinGecko")
                .arg(
                    Arg::with_name("when")
                        .value_name("YY/MM/DD")
                        .takes_value(true)
                        .default_value(&default_when)
                        .validator(|value| naivedate_of(&value).map(|_| ()))
                        .help("Date to fetch the price for"),
                ),
        )
        .subcommand(SubCommand::with_name("sync").about("Synchronize with all exchanges"))
        .subcommand(
            SubCommand::with_name("account")
                .about("Account management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("add")
                        .about("Register an account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account address to add"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Account description"),
                        )
                        .arg(
                            Arg::with_name("when")
                                .short("w")
                                .long("when")
                                .value_name("YY/MM/DD")
                                .takes_value(true)
                                .required(true)
                                .default_value(&default_when)
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .help("Date acquired"),
                        )
                        .arg(
                            Arg::with_name("transaction")
                                .short("t")
                                .long("transaction")
                                .value_name("SIGNATURE")
                                .takes_value(true)
                                .validator(is_parsable::<Signature>)
                                .help("Acquisition transaction signature"),
                        )
                        .arg(
                            Arg::with_name("price")
                                .short("p")
                                .long("price")
                                .value_name("USD")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Acquisition price per SOL [default: market price on acquisition date]"),
                        ),
                )
                .subcommand(SubCommand::with_name("ls").about("List registered accounts"))
                .subcommand(
                    SubCommand::with_name("remove")
                        .about("Unregister an account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account address to add"),
                        )
                        .arg(
                            Arg::with_name("confirm")
                                .long("confirm")
                                .takes_value(false)
                                .help("Confirm the operation"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("set-sweep-stake-account")
                        .about("Set the sweep stake account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Sweep stake account address"),
                        )
                        .arg(
                            Arg::with_name("stake_authority")
                                .index(2)
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .help("Stake authority keypair"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("sweep")
                        .about("Sweep SOL into the sweep stake account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source address to sweep from"),
                        )
                        .arg(
                            Arg::with_name("authority")
                                .index(2)
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_signer)
                                .help("Source account authority keypair"),
                        )
                        .arg(
                            Arg::with_name("retain")
                                .short("r")
                                .long("retain")
                                .value_name("SOL")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Amount of SOL to retain in the source account [default: 0]"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("sync")
                        .about("Synchronize account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(false)
                                .validator(is_valid_pubkey)
                                .help("Account to synchronize"),
                        ),
                ),
        );

    for exchange in &exchanges {
        app = app.subcommand(
            SubCommand::with_name(exchange)
                .about("Exchange interactions")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("balance")
                        .about("Get SOL balance")
                        .arg(
                            Arg::with_name("available_only")
                                .long("available")
                                .takes_value(false)
                                .help("Only display available balance"),
                        ),
                )
                .subcommand(SubCommand::with_name("address").about("Show SOL deposit address"))
                .subcommand(
                    SubCommand::with_name("market")
                        .about("Display market info for a given trading pair")
                        .arg(
                            Arg::with_name("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .default_value("SOLUSDT"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("api")
                        .about("API Management")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("set")
                                .about("Set API key")
                                .arg(Arg::with_name("api_key").required(true).takes_value(true))
                                .arg(Arg::with_name("secret").required(true).takes_value(true)),
                        )
                        .subcommand(SubCommand::with_name("show").about("Show API key"))
                        .subcommand(SubCommand::with_name("clear").about("Clear API key")),
                )
                .subcommand(
                    SubCommand::with_name("deposit")
                        .about("Deposit SOL")
                        .arg(
                            Arg::with_name("amount")
                                .index(1)
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to send, in SOL; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("from")
                                .long("from")
                                .value_name("FROM_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source account of funds"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority of the FROM_ADDRESS"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("sell")
                        .about("Place an order to sell SOL")
                        .arg(
                            Arg::with_name("amount")
                                .index(1)
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("The amount to sell, in SOL"),
                        )
                        .arg(
                            Arg::with_name("at")
                                .long("at")
                                .value_name("PRICE")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this price"),
                        )
                        .arg(
                            Arg::with_name("ask_plus")
                                .long("ask-plus")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .conflicts_with("at")
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this amount over the current ask"),
                        )
                        .arg(
                            Arg::with_name("pair")
                                .long("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .default_value("SOLUSDT")
                                .help("Market to place the order at"),
                        ),
                )
                .subcommand(SubCommand::with_name("sync").about("Synchronize exchange")),
        );
    }

    let app_matches = app.get_matches();
    let db_path = value_t_or_exit!(app_matches, "db_path", PathBuf);
    let rpc_client = RpcClient::new_with_commitment(
            normalize_to_url_if_moniker(value_t_or_exit!(
                app_matches,
                "json_rpc_url",
                String
            )),
        CommitmentConfig::confirmed(),
    );
    let mut wallet_manager = None;
    let notifier = Notifier::default();

    let mut db = db::new(&db_path).unwrap_or_else(|err| {
        eprintln!("Failed to open {}: {}", db_path.display(), err);
        exit(1)
    });

    match app_matches.subcommand() {
        ("price", Some(arg_matches)) => {
            let when = naivedate_of(&value_t_or_exit!(arg_matches, "when", String)).unwrap();
            let price = coin_gecko::get_price(when).await?;
            println!("Price on {}: ${:.2}", when, price);
        }
        ("sync", Some(_arg_matches)) => {
            for (exchange, exchange_credentials) in db.get_configured_exchanges() {
                println!("Synchronizing {:?}...", exchange);
                let exchange_client = exchange_client_new(exchange, exchange_credentials)?;
                process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                    .await?
            }
        }
        ("account", Some(account_matches)) => match account_matches.subcommand() {
            ("add", Some(arg_matches)) => {
                let price = value_t!(arg_matches, "price", f64).ok();
                let when = naivedate_of(&value_t_or_exit!(arg_matches, "when", String)).unwrap();
                let signature = value_t!(arg_matches, "transaction", Signature).ok();
                let address = pubkey_of(arg_matches, "address").unwrap();
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_else(String::default);

                process_account_add(
                    &mut db,
                    &rpc_client,
                    address,
                    description,
                    when,
                    price,
                    signature,
                )
                .await?;
                process_account_sync(&mut db, &rpc_client, Some(address), &notifier).await?;
            }
            ("ls", Some(_arg_matches)) => {
                process_account_list(&db).await?;
            }
            ("remove", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let confirm = arg_matches.is_present("confirm");

                if !confirm {
                    println!("Add --confirm to remove {}", address);
                    return Ok(());
                }

                db.remove_account(address)?;
                println!("Removed {}", address);
            }
            ("set-sweep-stake-account", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let stake_authority = std::fs::canonicalize(value_t_or_exit!(
                    arg_matches,
                    "stake_authority",
                    PathBuf
                ))?;

                let stake_authority_keypair = read_keypair_file(&stake_authority)?;
                let (authorized, _vote_account_address) =
                    rpc_client_utils::get_stake_authorized(&rpc_client, address)?;

                if authorized.staker != stake_authority_keypair.pubkey() {
                    return Err("Stake authority mismatch".into());
                }

                db.set_sweep_stake_account(SweepStakeAccount {
                    address,
                    stake_authority,
                })?;

                println!("Sweep stake account set to {}", address);
            }
            ("sweep", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "address").unwrap();
                let (authority_signer, authority_address) =
                    signer_of(arg_matches, "authority", &mut wallet_manager)?;
                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");
                let retain_amount =
                    sol_to_lamports(value_t!(arg_matches, "retain", f64).unwrap_or(0.));

                process_account_sweep(
                    &mut db,
                    &rpc_client,
                    from_address,
                    retain_amount,
                    authority_address,
                    vec![authority_signer],
                    &notifier,
                )
                .await?;
            }
            ("sync", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address");

                // TODO: when sweeping transitory_stake_accounts, if any accounts don't exist then
                //       just remove them (and associated account/lots too).
                //       The transaction might have failed...

                process_account_sync(&mut db, &rpc_client, address, &notifier).await?;
            }
            _ => unreachable!(),
        },
        (exchange, Some(exchange_matches)) => {
            assert!(exchanges.contains(&exchange), "Bug!");
            let exchange = Exchange::from_str(exchange)?;

            let exchange_client = || {
                let exchange_credentials = db
                    .get_exchange_credentials(exchange)
                    .ok_or_else(|| format!("No API key set for {:?}", exchange))?;
                exchange_client_new(exchange, exchange_credentials)
            };

            match exchange_matches.subcommand() {
                ("address", Some(_arg_matches)) => {
                    let deposit_address = exchange_client()?.deposit_address().await?;
                    println!("{}", deposit_address);
                }
                ("balance", Some(arg_matches)) => {
                    let available_only = arg_matches.is_present("available_only");

                    let balance = exchange_client()?.balance().await?;
                    if available_only {
                        println!("◎{}", balance.available);
                    } else {
                        println!(
                            "Available: ◎{}\nTotal: ◎{}",
                            balance.available, balance.total,
                        );
                    }
                }
                ("market", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    println!("Pair: {}", pair);
                    exchange_client()?.print_market_info(&pair).await?;
                }
                ("deposit", Some(arg_matches)) => {
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(sol_to_lamports(amount.parse().unwrap())),
                    };

                    let from_address =
                        pubkey_of_signer(arg_matches, "from", &mut wallet_manager)?.expect("from");

                    let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                        signer_of(arg_matches, "by", &mut wallet_manager)?
                    } else {
                        signer_of(arg_matches, "from", &mut wallet_manager).map_err(|err| {
                            format!(
                                "Authority not found, consider using the `--by` argument): {}",
                                err
                            )
                        })?
                    };

                    let authority_address = authority_address.expect("authority_address");
                    let authority_signer = authority_signer.expect("authority_signer");

                    let exchange_client = exchange_client()?;
                    let deposit_address = exchange_client.deposit_address().await?;

                    process_exchange_deposit(
                        &mut db,
                        rpc_client,
                        exchange,
                        deposit_address,
                        amount,
                        from_address,
                        authority_address,
                        vec![authority_signer],
                    )
                    .await?;
                    process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                        .await?;
                }
                ("sell", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let amount = value_t_or_exit!(arg_matches, "amount", f64);

                    let price = if let Ok(price) = value_t!(arg_matches, "at", f64) {
                        LimitOrderPrice::At(price)
                    } else if let Ok(ask_plus) = value_t!(arg_matches, "ask_plus", f64) {
                        LimitOrderPrice::AmountOverAsk(ask_plus)
                    } else {
                        return Err("--at or --ask-plus argument required".into());
                    };
                    let exchange_client = exchange_client()?;
                    process_exchange_sell(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        pair,
                        amount,
                        price,
                    )
                    .await?;
                    process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                        .await?;
                }
                ("sync", Some(_arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                        .await?;
                }
                ("api", Some(api_matches)) => match api_matches.subcommand() {
                    ("show", Some(_arg_matches)) => match db.get_exchange_credentials(exchange) {
                        Some(ExchangeCredentials { api_key, secret: _ }) => {
                            println!("API Key: {}", api_key);
                            println!("Secret: ********");
                        }
                        None => {
                            println!("No API key set for {:?}", exchange);
                        }
                    },
                    ("set", Some(arg_matches)) => {
                        let api_key = value_t_or_exit!(arg_matches, "api_key", String);
                        let secret = value_t_or_exit!(arg_matches, "secret", String);
                        db.set_exchange_credentials(
                            exchange,
                            ExchangeCredentials { api_key, secret },
                        )?;
                        println!("API key set for {:?}", exchange);
                    }
                    ("clear", Some(_arg_matches)) => {
                        db.clear_exchange_credentials(exchange)?;
                        println!("Cleared API key for {:?}", exchange);
                    }
                    _ => unreachable!(),
                },

                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };

    Ok(())
}
