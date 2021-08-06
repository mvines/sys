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
    chrono_humanize::HumanTime,
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, values_t, App, AppSettings, Arg,
        SubCommand,
    },
    db::*,
    exchange::*,
    notifier::*,
    separator::FixedPlaceSeparatable,
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::{rpc_client::RpcClient, rpc_response::StakeActivationState},
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
    std::{collections::HashSet, path::PathBuf, process::exit, str::FromStr},
};

fn is_long_term_cap_gain(acquisition: NaiveDate, disposal: Option<NaiveDate>) -> bool {
    let disposal = disposal.unwrap_or_else(|| {
        let today = Local::now().date();
        NaiveDate::from_ymd(today.year(), today.month(), today.day())
    });

    let hold_time = disposal - acquisition;
    hold_time >= chrono::Duration::days(356)
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

fn add_exchange_deposit_address_to_db(
    db: &mut Db,
    exchange: Exchange,
    deposit_address: Pubkey,
    rpc_client: &RpcClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if db.get_account(deposit_address).is_none() {
        let epoch = rpc_client.get_epoch_info()?.epoch;
        db.add_account(TrackedAccount {
            address: deposit_address,
            description: format!("{:?}", exchange),
            last_update_epoch: epoch,
            last_update_balance: 0,
            lots: vec![],
            no_sync: Some(true),
        })?;
    }
    Ok(())
}

async fn process_sync_exchange(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    rpc_client: &RpcClient,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let deposit_address = exchange_client.deposit_address().await?;
    add_exchange_deposit_address_to_db(db, exchange, deposit_address, rpc_client)?;

    let recent_deposits = exchange_client.recent_deposits().await?;

    for pending_deposit in db.pending_deposits(Some(exchange)) {
        if let Some(deposit_info) = recent_deposits.iter().find(|deposit_info| {
            deposit_info.tx_id == pending_deposit.transfer.signature.to_string()
        }) {
            let missing_lamports = (sol_to_lamports(deposit_info.amount) as i64
                - (pending_deposit.amount as i64))
                .abs();
            if missing_lamports >= 10 {
                let msg = format!(
                    "Error! Deposit amount mismatch for {}! Actual amount: ◎{}, expected amount: ◎{}",
                    pending_deposit.transfer.signature, deposit_info.amount, pending_deposit.amount
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;

                // TODO: Do something more here...?
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

                db.confirm_deposit(pending_deposit.transfer.signature)?;

                let msg = format!(
                    "{} deposit successful ({})",
                    Sol(pending_deposit.amount),
                    pending_deposit.transfer.signature
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            }
        } else {
            println!(
                "{} deposit pending ({})",
                Sol(pending_deposit.amount),
                pending_deposit.transfer.signature
            );
        }
    }

    for order_info in db.open_orders(Some(exchange)) {
        let order_status = exchange_client
            .sell_order_status(&order_info.pair, &order_info.order_id)
            .await?;
        let order_summary = format!(
            "{}: ◎{} at ${} (◎{} filled), created {}, id {}",
            order_info.pair,
            order_status.amount,
            order_status.price,
            order_status.filled_amount,
            HumanTime::from(order_info.creation_time),
            order_info.order_id,
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
            db.close_order(
                &order_info.order_id,
                sol_to_lamports(order_status.amount),
                sol_to_lamports(order_status.filled_amount),
                order_status.price,
                order_status.last_update,
            )?;
            let msg = if (order_status.amount - order_status.filled_amount).abs() < f64::EPSILON {
                format!("Order filled: {}", order_summary)
            } else if order_status.filled_amount < f64::EPSILON {
                format!("Order cancelled: {}", order_summary)
            } else {
                format!("Order partially filled: {}", order_summary)
            };
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_deposit<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    exchange: Exchange,
    deposit_address: Pubkey,
    amount: Option<u64>,
    from_address: Pubkey,
    if_source_balance_exceeds: Option<u64>,
    authority_address: Pubkey,
    signers: T,
    lot_numbers: Option<HashSet<usize>>,
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

    if let Some(if_source_balance_exceeds) = if_source_balance_exceeds {
        if from_account.lamports < if_source_balance_exceeds {
            println!(
                "Deposit declined because {} balance is less than {}",
                from_address,
                Sol(if_source_balance_exceeds)
            );
            return Ok(());
        }
    }

    if from_account.lamports < lamports + minimum_balance {
        return Err("From account has insufficient funds".into());
    }

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
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    db.record_deposit(
        signature,
        from_address,
        lamports,
        exchange,
        deposit_address,
        lot_numbers,
    )?;
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
                db.cancel_deposit(signature).expect("cancel_deposit");
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

#[allow(clippy::too_many_arguments)]
async fn process_exchange_cancel(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    order_ids: HashSet<String>,
    max_create_time: Option<DateTime<Utc>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cancelled_count = 0;
    for order_info in db.open_orders(Some(exchange)) {
        let mut cancel = false;
        if order_ids.contains(&order_info.order_id) {
            cancel = true;
        }
        if let Some(ref max_create_time) = max_create_time {
            if order_info.creation_time < *max_create_time {
                cancel = true;
            }
        }

        if cancel {
            println!("Cancelling order {}", order_info.order_id);
            cancelled_count += 1;
            exchange_client
                .cancel_sell_order(&order_info.pair, &order_info.order_id)
                .await?
        }
    }

    println!("{} orders cancelled", cancelled_count);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_sell(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    pair: String,
    amount: f64,
    price: LimitOrderPrice,
    if_balance_exceeds: Option<u64>,
    if_price_over: Option<f64>,
    price_floor: Option<f64>,
    lot_numbers: Option<HashSet<usize>>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!("Symbol: {}", pair);
    println!("Ask: ${}, Bid: ${}", bid_ask.ask_price, bid_ask.bid_price,);

    let deposit_address = exchange_client.deposit_address().await?;
    let mut deposit_account = db.get_account(deposit_address).ok_or_else(|| {
        format!(
            "Exchange deposit account does not exist: {}",
            deposit_address
        )
    })?;

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if deposit_account.last_update_balance < if_balance_exceeds {
            println!(
                "Order declined because {:?} available balance is less than {}",
                exchange,
                Sol(if_balance_exceeds)
            );
            return Ok(());
        }
    }

    let mut price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(extra) => bid_ask.ask_price + extra,
    };

    if let Some(if_price_over) = if_price_over {
        if price <= if_price_over {
            let msg = format!(
                "Order declined because price, ${}, is not greater than ${}",
                price, if_price_over,
            );
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            return Ok(());
        }
    }

    if let Some(price_floor) = price_floor {
        if price < price_floor {
            let msg = format!(
                "Proposed price, ${}, is beneath price floor. Adjusting upwards",
                price
            );
            price = price_floor;
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
        }
    }

    if bid_ask.bid_price > price {
        return Err("Order price is less than bid price".into());
    }

    println!("Placing sell order for ◎{} at ${}", amount, price);

    let order_lots = deposit_account.extract_lots(db, sol_to_lamports(amount), lot_numbers)?;
    println!("Lots");
    for lot in &order_lots {
        println_lot(lot, price, &mut 0., &mut 0., &mut false, &mut 0., None).await;
    }

    let order_id = exchange_client
        .place_sell_order(&pair, price, amount)
        .await?;
    let msg = format!(
        "Order created: {}: ◎{} at ${}, id {}",
        pair, amount, price, order_id,
    );
    db.open_order(deposit_account, exchange, pair, price, order_id, order_lots)?;
    println!("{}", msg);
    notifier.send(&format!("{:?}: {}", exchange, msg)).await;
    Ok(())
}

async fn println_lot(
    lot: &Lot,
    current_price: f64,
    total_income: &mut f64,
    total_cap_gain: &mut f64,
    long_term_cap_gain: &mut bool,
    total_current_value: &mut f64,
    notifier: Option<&Notifier>,
) {
    let current_value = lamports_to_sol(lot.amount) * current_price;
    let income = lot.income();
    let cap_gain = lot.cap_gain(current_price);

    *total_income += income;
    *total_cap_gain += cap_gain;
    *total_current_value += current_value;
    *long_term_cap_gain = is_long_term_cap_gain(lot.acquisition.when, None);

    let msg = format!(
        "{:>3}. {} | ◎{:<17.9} at ${:<6} | current value: ${:<14} | income: ${:<11} | {} cap gain: ${:<14} | {}",
        lot.lot_number,
        lot.acquisition.when,
        lamports_to_sol(lot.amount),
        lot.acquisition.price.separated_string_with_fixed_place(2),
        current_value.separated_string_with_fixed_place(2),
        income.separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            "long"
        } else {
            "short"
        },
        cap_gain.separated_string_with_fixed_place(2),
        lot.acquisition.kind,
    );

    if let Some(notifier) = notifier {
        notifier.send(&msg).await;
    }
    println!("{}", msg);
}

fn format_disposed_lot(
    disposed_lot: &DisposedLot,
    total_income: &mut f64,
    total_cap_gain: &mut f64,
    long_term_cap_gain: &mut bool,
    total_current_value: &mut f64,
) -> String {
    let cap_gain = disposed_lot.lot.cap_gain(disposed_lot.price);
    let income = disposed_lot.lot.income();

    *long_term_cap_gain =
        is_long_term_cap_gain(disposed_lot.lot.acquisition.when, Some(disposed_lot.when));
    *total_income += income;
    *total_current_value += income + cap_gain;
    *total_cap_gain += cap_gain;

    format!(
        "{:>3}. {} | ◎{:<17.9} at ${:<6} | income: ${:<11} | sold {} at ${:6} {} cap gain: ${:<14} | {} | {}",
        disposed_lot.lot.lot_number,
        disposed_lot.lot.acquisition.when,
        lamports_to_sol(disposed_lot.lot.amount),
        disposed_lot.lot.acquisition.price.separated_string_with_fixed_place(2),
        income.separated_string_with_fixed_place(2),
        disposed_lot.when,
        disposed_lot.price.separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            "long"
        } else {
            "short"
        },
        cap_gain.separated_string_with_fixed_place(2),
        disposed_lot.lot.acquisition.kind,
        disposed_lot.kind,
    )
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
                rpc_client.get_transaction(&signature, UiTransactionEncoding::Base64)?;

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
            let last_update_epoch = epoch_schdule
                .get_epoch_and_slot_index(slot)
                .0
                .saturating_sub(1);

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
    println_lot(
        &lot,
        current_price,
        &mut 0.,
        &mut 0.,
        &mut false,
        &mut 0.,
        None,
    )
    .await;

    let account = TrackedAccount {
        address,
        description,
        last_update_epoch,
        last_update_balance: lot.amount,
        lots: vec![lot],
        no_sync: None,
    };
    db.add_account(account)?;

    Ok(())
}

async fn process_account_dispose(
    db: &mut Db,
    address: Pubkey,
    amount: f64,
    description: String,
    when: NaiveDate,
    price: Option<f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let price = match price {
        Some(price) => price,
        None => coin_gecko::get_price(when).await?,
    };

    let disposed_lots =
        db.record_disposal(address, sol_to_lamports(amount), description, when, price)?;
    if !disposed_lots.is_empty() {
        println!("Disposed Lots:");
        for disposed_lot in disposed_lots {
            println!(
                "{}",
                format_disposed_lot(&disposed_lot, &mut 0., &mut 0., &mut false, &mut 0.)
            );
        }
        println!();
    }
    Ok(())
}

async fn process_account_list(
    db: &Db,
    show_all_disposed_lots: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let accounts = db.get_accounts();
    if accounts.is_empty() {
        println!("No accounts");
    } else {
        let current_price = coin_gecko::get_current_price().await?;

        let mut total_income = 0.;
        let mut total_held_lamports = 0;
        let mut total_short_term_gain = 0.;
        let mut total_long_term_gain = 0.;
        let mut total_current_value = 0.;

        let open_orders = db.open_orders(None);

        for account in accounts.values() {
            println!(
                "{}: ◎{} - {}",
                account.address.to_string(),
                lamports_to_sol(account.last_update_balance).separated_string_with_fixed_place(2),
                account.description
            );
            account.assert_lot_balance();
            total_held_lamports += account.last_update_balance;

            let open_orders = open_orders
                .iter()
                .filter(|oo| oo.deposit_address == account.address)
                .collect::<Vec<_>>();

            if !account.lots.is_empty() || !open_orders.is_empty() {
                let mut lots = account.lots.iter().collect::<Vec<_>>();
                lots.sort_by_key(|lot| lot.acquisition.when);

                let mut account_income = 0.;
                let mut account_current_value = 0.;
                let mut account_short_term_gain = 0.;
                let mut account_long_term_gain = 0.;

                for lot in lots {
                    let mut account_unrealized_gain = 0.;
                    let mut long_term_cap_gain = false;
                    println_lot(
                        lot,
                        current_price,
                        &mut account_income,
                        &mut account_unrealized_gain,
                        &mut long_term_cap_gain,
                        &mut account_current_value,
                        None,
                    )
                    .await;

                    if long_term_cap_gain {
                        account_long_term_gain += account_unrealized_gain;
                    } else {
                        account_short_term_gain += account_unrealized_gain;
                    }
                }

                for open_order in open_orders {
                    let mut lots = open_order.lots.iter().collect::<Vec<_>>();
                    lots.sort_by_key(|lot| lot.acquisition.when);
                    println!(
                        "(Open order: {} at ${}, {} lots, created {}, id {})",
                        open_order.pair,
                        open_order.price,
                        lots.len(),
                        HumanTime::from(open_order.creation_time),
                        open_order.order_id,
                    );
                    for lot in lots {
                        let mut account_unrealized_gain = 0.;
                        let mut long_term_cap_gain = false;
                        println_lot(
                            lot,
                            current_price,
                            &mut account_income,
                            &mut account_unrealized_gain,
                            &mut long_term_cap_gain,
                            &mut account_current_value,
                            None,
                        )
                        .await;

                        if long_term_cap_gain {
                            account_long_term_gain += account_unrealized_gain;
                        } else {
                            account_short_term_gain += account_unrealized_gain;
                        }
                    }
                }

                println!(
                    "    Value: ${}, income: ${}, unrealized short-term cap gain: ${}, unrealized long-term cap gain: ${}",
                    account_current_value.separated_string_with_fixed_place(2),
                    account_income.separated_string_with_fixed_place(2),
                    account_short_term_gain.separated_string_with_fixed_place(2),
                    account_long_term_gain.separated_string_with_fixed_place(2),
                );
                total_short_term_gain += account_short_term_gain;
                total_long_term_gain += account_long_term_gain;
                total_income += account_income;
                total_current_value += account_current_value;
            } else {
                println!("  No lots");
            }
            println!();
        }

        let mut disposed_lots = db.disposed_lots();
        disposed_lots.sort_by_key(|lot| lot.when);
        if !disposed_lots.is_empty() {
            println!("Disposed ({} lots):", disposed_lots.len());

            let mut disposed_lamports = 0;
            let mut disposed_income = 0.;
            let mut disposed_short_term_cap_gain = 0.;
            let mut disposed_long_term_cap_gain = 0.;
            let mut disposed_current_value = 0.;

            for (i, disposed_lot) in disposed_lots.iter().enumerate() {
                disposed_lamports += disposed_lot.lot.amount;
                let mut long_term_cap_gain = false;
                let mut disposed_cap_gain = 0.;
                let msg = format_disposed_lot(
                    &disposed_lot,
                    &mut disposed_income,
                    &mut disposed_cap_gain,
                    &mut long_term_cap_gain,
                    &mut disposed_current_value,
                );

                if show_all_disposed_lots {
                    println!("{}", msg);
                } else {
                    if disposed_lots.len() > 5 && i == disposed_lots.len().saturating_sub(5) {
                        println!("...");
                    }
                    if i > disposed_lots.len().saturating_sub(5) {
                        println!("{}", msg);
                    }
                }

                if long_term_cap_gain {
                    disposed_long_term_cap_gain += disposed_cap_gain;
                } else {
                    disposed_short_term_cap_gain += disposed_cap_gain;
                }
            }
            println!(
                "    Disposed ◎{}, value: ${}, income: ${}, short-term cap gain: ${}, long-term cap gain: ${}",
                lamports_to_sol(disposed_lamports).separated_string_with_fixed_place(2),
                disposed_current_value.separated_string_with_fixed_place(2),
                disposed_income.separated_string_with_fixed_place(2),
                disposed_short_term_cap_gain.separated_string_with_fixed_place(2),
                disposed_long_term_cap_gain.separated_string_with_fixed_place(2),
            );
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

        println!("Current Holdings Summary");
        println!(
            "  Price per SOL:       ${}",
            current_price.separated_string_with_fixed_place(2)
        );
        println!(
            "  Balance:             ◎{}",
            lamports_to_sol(total_held_lamports).separated_string_with_fixed_place(2)
        );
        println!(
            "  Value:               ${}",
            total_current_value.separated_string_with_fixed_place(2)
        );
        println!(
            "  Income:              ${}",
            total_income.separated_string_with_fixed_place(2)
        );
        println!(
            "  Short-term cap gain: ${}",
            total_short_term_gain.separated_string_with_fixed_place(2)
        );
        println!(
            "  Long-term cap gain:  ${}",
            total_long_term_gain.separated_string_with_fixed_place(2)
        );
    }
    Ok(())
}

async fn process_account_xls(db: &Db, outfile: &str) -> Result<(), Box<dyn std::error::Error>> {
    use simple_excel_writer::*;

    let mut workbook = Workbook::create(outfile);

    let mut sheet = workbook.create_sheet("Disposed SOL");
    sheet.add_column(Column { width: 15. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });

    let mut disposed_lots = db.disposed_lots();
    disposed_lots.sort_by_key(|lot| lot.when);

    workbook.write_sheet(&mut sheet, |sheet_writer| {
        sheet_writer.append_row(row![
            "Amount (SOL)",
            "Income",
            "Acq. Date",
            "Acq. Price",
            "Acquisition Description",
            "Cap Gain",
            "Sale Date",
            "Sale Price",
            "Sale Description"
        ])?;

        for disposed_lot in disposed_lots {
            sheet_writer.append_row(row![
                lamports_to_sol(disposed_lot.lot.amount),
                format!(
                    "${}",
                    disposed_lot
                        .lot
                        .income()
                        .separated_string_with_fixed_place(2)
                ),
                disposed_lot.lot.acquisition.when.to_string(),
                format!(
                    "${}",
                    disposed_lot
                        .lot
                        .acquisition
                        .price
                        .separated_string_with_fixed_place(2)
                ),
                disposed_lot.lot.acquisition.kind.to_string(),
                format!(
                    "${}",
                    disposed_lot
                        .lot
                        .cap_gain(disposed_lot.price)
                        .separated_string_with_fixed_place(2)
                ),
                disposed_lot.when.to_string(),
                format!(
                    "${}",
                    disposed_lot.price.separated_string_with_fixed_place(2)
                ),
                disposed_lot.kind.to_string()
            ])?;
        }
        Ok(())
    })?;

    let mut sheet = workbook.create_sheet("Current SOL Holdings");
    sheet.add_column(Column { width: 15. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });
    sheet.add_column(Column { width: 40. });
    sheet.add_column(Column { width: 50. });

    workbook.write_sheet(&mut sheet, |sheet_writer| {
        sheet_writer.append_row(row![
            "Amount (SOL)",
            "Income",
            "Acq. Date",
            "Acq. Price",
            "Acquisition Description",
            "Account Description",
            "Account Address"
        ])?;

        let mut rows = vec![];

        for account in db.get_accounts().values() {
            for lot in account.lots.iter() {
                rows.push((
                    lot.acquisition.when,
                    row![
                        lamports_to_sol(lot.amount),
                        format!("${}", lot.income().separated_string_with_fixed_place(2)),
                        lot.acquisition.when.to_string(),
                        format!(
                            "${}",
                            lot.acquisition.price.separated_string_with_fixed_place(2)
                        ),
                        lot.acquisition.kind.to_string(),
                        account.description.as_str(),
                        account.address.to_string()
                    ],
                ));
            }
        }

        for open_order in db.open_orders(None) {
            for lot in open_order.lots.iter() {
                rows.push((
                    lot.acquisition.when,
                    row![
                        lamports_to_sol(lot.amount),
                        format!("${}", lot.income().separated_string_with_fixed_place(2)),
                        lot.acquisition.when.to_string(),
                        format!(
                            "${}",
                            lot.acquisition.price.separated_string_with_fixed_place(2)
                        ),
                        lot.acquisition.kind.to_string(),
                        format!("Open Order: {:?} {}", open_order.exchange, open_order.pair),
                        open_order.deposit_address.to_string()
                    ],
                ));
            }
        }
        rows.sort_by_key(|row| row.0);
        for (_, row) in rows {
            sheet_writer.append_row(row)?;
        }

        Ok(())
    })?;

    workbook.close()?;
    println!("Wrote {}", outfile);

    Ok(())
}

async fn process_account_merge<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    from_address: Pubkey,
    into_address: Pubkey,
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

    let instructions = if from_account.owner == solana_stake_program::id() {
        solana_stake_program::stake_instruction::merge(
            &into_address,
            &from_address,
            &authority_address,
        )
    } else {
        // TODO: Support merging two system accounts, and possibly other variations
        return Err(format!("Unsupported `from` account owner: {}", from_account.owner).into());
    };

    println!("Merging {} into {}", from_address, into_address);
    if from_address != authority_address {
        println!("Authority address: {}", authority_address);
    }

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
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    db.record_transfer(signature, from_address, None, into_address, None)?;

    loop {
        match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
            Ok(_) => {
                db.confirm_transfer(signature)?;
                db.remove_account(from_address)?;
                return Ok(());
            }
            Err(err) => {
                println!("Send transaction failed: {:?}", err);
            }
        }
        match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
            Err(err) => {
                println!("Failed to get fee calculator: {:?}", err);
            }
            Ok(None) => {
                db.cancel_transfer(signature)?;
                return Err("Merge failed: {}".into());
            }
            Ok(_) => {
                println!("Blockhash has not yet expired, retrying transaction...");
            }
        };
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_account_sweep<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    from_address: Pubkey,
    retain_amount: u64,
    no_sweep_ok: bool,
    from_authority_address: Pubkey,
    signers: T,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let (recent_blockhash, fee_calculator) = rpc_client.get_recent_blockhash()?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("Account, {}, does not exist", from_address))?;

    let from_tracked_account = db
        .get_account(from_address)
        .ok_or_else(|| format!("Account, {}, is not tracked", from_address))?;

    if from_account.lamports < from_tracked_account.last_update_balance {
        return Err(format!(
            "{}: On-chain account balance ({}) less than tracked balance ({})",
            from_address,
            Sol(from_account.lamports),
            Sol(from_tracked_account.last_update_balance)
        )
        .into());
    }

    let authority_account = if from_address == from_authority_address {
        from_account.clone()
    } else {
        rpc_client
            .get_account_with_commitment(&from_authority_address, rpc_client.commitment())?
            .value
            .ok_or_else(|| {
                format!(
                    "Authority account, {}, does not exist",
                    from_authority_address
                )
            })?
    };

    let sweep_stake_account = db
        .get_sweep_stake_account()
        .ok_or("Sweep stake account not configured")?;
    let sweep_stake_authority_keypair = read_keypair_file(&sweep_stake_account.stake_authority)
        .map_err(|err| {
            format!(
                "Failed to read {}: {}",
                sweep_stake_account.stake_authority.display(),
                err
            )
        })?;

    let (sweep_stake_authorized, sweep_stake_vote_account_address) =
        rpc_client_utils::get_stake_authorized(&rpc_client, sweep_stake_account.address)?;

    if sweep_stake_authorized.staker != sweep_stake_authority_keypair.pubkey() {
        return Err("Stake authority mismatch".into());
    }

    let num_transaction_signatures = 1 + // from_address_authority
        1 + // transitory_stake_account
        if from_authority_address == sweep_stake_authority_keypair.pubkey() {
            0
        } else { 1 };

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
        let lamports = if from_address == from_authority_address {
            from_tracked_account.last_update_balance.saturating_sub(
                num_transaction_signatures * fee_calculator.lamports_per_signature + retain_amount,
            )
        } else {
            from_tracked_account
                .last_update_balance
                .saturating_sub(retain_amount)
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

        let lamports = from_tracked_account
            .last_update_balance
            .saturating_sub(minimum_balance + retain_amount);

        (
            vec![solana_vote_program::vote_instruction::withdraw(
                &from_address,
                &from_authority_address,
                lamports,
                &transitory_stake_account.pubkey(),
            )],
            lamports,
        )
    } else if from_account.owner == solana_stake_program::id() {
        let lamports = from_tracked_account
            .last_update_balance
            .saturating_sub(retain_amount);

        (
            vec![solana_stake_program::stake_instruction::withdraw(
                &from_address,
                &from_authority_address,
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
        let msg = format!(
            "{} has less than ◎1 to sweep ({})",
            from_address,
            Sol(sweep_amount)
        );
        return if no_sweep_ok {
            println!("{}", msg);
            Ok(())
        } else {
            Err(msg.into())
        };
    }

    println!("From address: {}", from_address);
    if from_address != from_authority_address {
        println!("Authority address: {}", from_authority_address);
    }
    println!("Sweep amount: {}", Sol(sweep_amount));
    println!(
        "Transitory stake address: {}",
        transitory_stake_account.pubkey()
    );

    instructions.append(&mut vec![
        system_instruction::allocate(
            &transitory_stake_account.pubkey(),
            std::mem::size_of::<solana_stake_program::stake_state::StakeState>() as u64,
        ),
        system_instruction::assign(
            &transitory_stake_account.pubkey(),
            &solana_stake_program::id(),
        ),
        solana_stake_program::stake_instruction::initialize(
            &transitory_stake_account.pubkey(),
            &sweep_stake_authorized,
            &solana_stake_program::stake_state::Lockup::default(),
        ),
        solana_stake_program::stake_instruction::delegate_stake(
            &transitory_stake_account.pubkey(),
            &sweep_stake_authority_keypair.pubkey(),
            &sweep_stake_vote_account_address,
        ),
    ]);

    let message = Message::new(&instructions, Some(&from_authority_address));
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
        &[&transitory_stake_account, &sweep_stake_authority_keypair],
        recent_blockhash,
    )?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    let epoch = rpc_client.get_epoch_info()?.epoch;
    db.add_transitory_sweep_stake_address(transitory_stake_account.pubkey(), epoch)?;
    db.record_transfer(
        signature,
        from_address,
        Some(sweep_amount),
        transitory_stake_account.pubkey(),
        None,
    )?;

    loop {
        match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
            Ok(_) => {
                println!("Confirming sweep: {}", signature);
                db.confirm_transfer(signature)?;
                break;
            }
            Err(err) => {
                println!("Send transaction failed: {:?}", err);
            }
        }
        match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
            Err(err) => {
                println!("Failed to get fee calculator: {:?}", err);
            }
            Ok(None) => {
                db.cancel_transfer(signature)?;
                db.remove_transitory_sweep_stake_address(transitory_stake_account.pubkey())?;
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

#[allow(clippy::too_many_arguments)]
async fn process_account_split<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    from_address: Pubkey,
    amount: u64,
    description: String,
    lot_numbers: Option<HashSet<usize>>,
    authority_address: Pubkey,
    signers: T,
) -> Result<(), Box<dyn std::error::Error>> {
    let (recent_blockhash, _fee_calculator) = rpc_client.get_recent_blockhash()?;

    let into_account = Keypair::new();

    // TODO: Support splitting two system accounts? Otherwise at least error cleanly when it's attempted

    let instructions = solana_stake_program::stake_instruction::split(
        &from_address,
        &authority_address,
        amount,
        &into_account.pubkey(),
    );

    let message = Message::new(&instructions, Some(&authority_address));

    let mut transaction = Transaction::new_unsigned(message);
    transaction.message.recent_blockhash = recent_blockhash;
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    println!(
        "Splitting {} from {} into {}",
        Sol(amount),
        from_address,
        into_account.pubkey(),
    );

    transaction.partial_sign(&signers, recent_blockhash);
    transaction.try_sign(&[&into_account], recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    let epoch = rpc_client.get_epoch_info()?.epoch;
    db.add_account(TrackedAccount {
        address: into_account.pubkey(),
        description,
        last_update_epoch: epoch.saturating_sub(1),
        last_update_balance: 0,
        lots: vec![],
        no_sync: None,
    })?;
    db.record_transfer(
        signature,
        from_address,
        Some(amount),
        into_account.pubkey(),
        lot_numbers,
    )?;

    loop {
        match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
            Ok(_) => {
                println!("Split confirmed: {}", signature);
                db.confirm_transfer(signature)?;
                break;
            }
            Err(err) => {
                println!("Send transaction failed: {:?}", err);
            }
        }
        match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
            Err(err) => {
                println!("Failed to get fee calculator: {:?}", err);
            }
            Ok(None) => {
                db.cancel_transfer(signature)?;
                db.remove_account(into_account.pubkey())?;
                return Err("Split failed: {}".into());
            }
            Ok(_) => {
                println!("Blockhash has not yet expired, retrying transaction...");
            }
        };
    }

    Ok(())
}

async fn process_account_sync(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Option<Pubkey>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    process_account_sync_pending_transfers(db, rpc_client).await?;
    process_account_sync_sweep(db, rpc_client, notifier).await?;

    let mut accounts = match address {
        Some(address) => {
            vec![db
                .get_account(address)
                .ok_or_else(|| format!("{} does not exist", address))?]
        }
        None => db.get_accounts().values().cloned().collect(),
    }
    .into_iter()
    .filter(|account| !account.no_sync.unwrap_or_default())
    .collect::<Vec<_>>();

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

    // Look for inflationary rewards
    for epoch in start_epoch..=stop_epoch {
        let msg = format!("Processing epoch: {}", epoch);
        notifier.send(&msg).await;
        println!("{}", msg);

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

                let msg = format!("{}: {}", account.address, account.description);
                notifier.send(&msg).await;
                println!("{}", msg);

                println_lot(
                    &lot,
                    current_price,
                    &mut 0.,
                    &mut 0.,
                    &mut false,
                    &mut 0.,
                    Some(&notifier),
                )
                .await;
                account.lots.push(lot);
            }
        }
    }

    // Look for unexpected balance changes (such as transaction and rent rewards)
    for mut account in accounts.iter_mut() {
        account.last_update_epoch = stop_epoch;

        let current_balance = rpc_client.get_balance(&account.address)?;

        if current_balance < account.last_update_balance {
            println!(
                "\nWarning: {} balance is less than expected. Actual: {}, expected: {}\n",
                account.address,
                Sol(current_balance),
                Sol(account.last_update_balance)
            );
        } else if current_balance > account.last_update_balance + sol_to_lamports(1.) {
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

            let msg = format!("{}: {}", account.address, account.description);
            notifier.send(&msg).await;
            println!("{}", msg);

            println_lot(
                &lot,
                current_price,
                &mut 0.,
                &mut 0.,
                &mut false,
                &mut 0.,
                Some(&notifier),
            )
            .await;
            account.lots.push(lot);
            account.last_update_balance = current_balance;
        }

        db.update_account(account.clone())?;
    }

    Ok(())
}

async fn process_account_sync_pending_transfers(
    db: &mut Db,
    rpc_client: &RpcClient,
) -> Result<(), Box<dyn std::error::Error>> {
    for PendingTransfer { signature, .. } in db.pending_transfers() {
        if rpc_client.confirm_transaction(&signature)? {
            println!("Pending transfer confirmed: {}", signature);
            db.confirm_transfer(signature)?;
        } else {
            println!("Pending transfer cancelled: {}", signature);
            db.cancel_transfer(signature)?;
        }
    }
    Ok(())
}

async fn process_account_sync_sweep(
    db: &mut Db,
    rpc_client: &RpcClient,
    _notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let transitory_sweep_stake_addresses = db.get_transitory_sweep_stake_addresses();
    if transitory_sweep_stake_addresses.is_empty() {
        return Ok(());
    }

    let sweep_stake_account_info = db
        .get_sweep_stake_account()
        .ok_or("Sweep stake account is not configured")?;

    let sweep_stake_account_authority_keypair =
        read_keypair_file(&sweep_stake_account_info.stake_authority).map_err(|err| {
            format!(
                "Failed to read {}: {}",
                sweep_stake_account_info.stake_authority.display(),
                err
            )
        })?;

    let sweep_stake_account = rpc_client
        .get_account_with_commitment(&sweep_stake_account_info.address, rpc_client.commitment())?
        .value
        .ok_or("Sweep stake account does not exist")?;

    let sweep_stake_activation = rpc_client
        .get_stake_activation(sweep_stake_account_info.address, None)
        .map_err(|err| {
            format!(
                "Unable to get activation information for sweep stake account: {}: {}",
                sweep_stake_account_info.address, err
            )
        })?;

    if sweep_stake_activation.state != StakeActivationState::Active {
        println!(
            "Sweep stake account is not active, unable to continue: {:?}",
            sweep_stake_activation
        );
        return Ok(());
    }

    for transitory_sweep_stake_address in transitory_sweep_stake_addresses {
        println!(
            "Considering merging transitory stake {}",
            transitory_sweep_stake_address
        );

        let transitory_sweep_stake_account = match rpc_client
            .get_account_with_commitment(&transitory_sweep_stake_address, rpc_client.commitment())?
            .value
        {
            None => {
                println!(
                    "  Transitory sweep stake account does not exist, removing it: {}",
                    transitory_sweep_stake_address
                );

                if let Some(tracked_account) = db.get_account(transitory_sweep_stake_address) {
                    if tracked_account.last_update_balance > 0 || !tracked_account.lots.is_empty() {
                        panic!("Tracked account is not empty: {:?}", tracked_account);

                        // TODO: Simulate a transfer to move the lots into the sweep account in
                        // this case?
                        /*
                        let signature = Signature::default();
                        db.record_transfer(
                            signature,
                            transitory_sweep_stake_address,
                            None,
                            sweep_stake_account_info.address,
                            None,
                        )?;
                        db.confirm_transfer(signature)?;
                        */
                    }
                }
                db.remove_transitory_sweep_stake_address(transitory_sweep_stake_address)?;
                continue;
            }
            Some(x) => x,
        };

        let transient_stake_activation = rpc_client
            .get_stake_activation(transitory_sweep_stake_address, None)
            .map_err(|err| {
                format!(
                    "Unable to get activation information for transient stake: {}: {}",
                    transitory_sweep_stake_address, err
                )
            })?;

        if transient_stake_activation.state != StakeActivationState::Active {
            println!(
                "  Transitory stake is not yet active: {:?}",
                transient_stake_activation
            );
            continue;
        }

        if !rpc_client_utils::stake_accounts_have_same_credits_observed(
            &sweep_stake_account,
            &transitory_sweep_stake_account,
        )? {
            println!(
                "  Transitory stake credits observed mismatch with sweep stake account: {}",
                transitory_sweep_stake_address
            );
            continue;
        }
        println!("  Merging into sweep stake account");

        let message = Message::new(
            &solana_stake_program::stake_instruction::merge(
                &sweep_stake_account_info.address,
                &transitory_sweep_stake_address,
                &sweep_stake_account_authority_keypair.pubkey(),
            ),
            Some(&sweep_stake_account_authority_keypair.pubkey()),
        );
        let mut transaction = Transaction::new_unsigned(message);

        let (recent_blockhash, _fee_calculator) = rpc_client.get_recent_blockhash()?;
        transaction.message.recent_blockhash = recent_blockhash;
        let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
        if simulation_result.err.is_some() {
            return Err(format!("Simulation failure: {:?}", simulation_result).into());
        }

        transaction.sign(&[&sweep_stake_account_authority_keypair], recent_blockhash);

        let signature = transaction.signatures[0];
        println!("Transaction signature: {}", signature);
        db.record_transfer(
            signature,
            transitory_sweep_stake_address,
            None,
            sweep_stake_account_info.address,
            None,
        )?;

        loop {
            match rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
                Ok(_) => {
                    db.confirm_transfer(signature)?;
                    break;
                }
                Err(err) => {
                    println!("Send transaction failed: {:?}", err);
                }
            }
            match rpc_client.get_fee_calculator_for_blockhash(&recent_blockhash) {
                Err(err) => {
                    println!("Failed to get fee calculator: {:?}", err);
                }
                Ok(None) => {
                    db.cancel_transfer(signature)?;
                    return Err("Sweep merge failed: {}".into());
                }
                Ok(_) => {
                    println!("Blockhash has not yet expired, retrying transaction...");
                }
            };
        }

        db.remove_transitory_sweep_stake_address(transitory_sweep_stake_address)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_db_path = "sell-your-sol";
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";
    let default_when = {
        let today = Local::now().date();
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
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .takes_value(false)
                .global(true)
                .help("Show additional information"),
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
                .subcommand(
                    SubCommand::with_name("dispose")
                        .about("Manually record the disposal of SOL from an account")
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account that the SOL was disposed from"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .index(2)
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("Amount of SOL that was disposed from the account"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Description to associate with the disposal event"),
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
                                .help("Disposal date"),
                        )
                        .arg(
                            Arg::with_name("price")
                                .short("p")
                                .long("price")
                                .value_name("USD")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Disposal price per SOL [default: market price on disposal date]"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("ls")
                        .about("List registered accounts")
                        .arg(
                            Arg::with_name("all")
                                .short("a")
                                .long("all")
                                .help("Display all lots")
                        ),
                )
                .subcommand(
                    SubCommand::with_name("xls")
                        .about("Export an Excel spreadsheet file")
                        .arg(
                            Arg::with_name("outfile")
                                .index(1)
                                .value_name("FILEPATH")
                                .takes_value(true)
                                .help(".xls file to write"),
                        ),
                )
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
                                .help("Account address to remove"),
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
                                .required(true)
                                .help("Stake authority keypair"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("merge")
                        .about("Merge one account into another")
                        .arg(
                            Arg::with_name("from_address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source address")
                        )
                        .arg(
                            Arg::with_name("into_address")
                                .long("into")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Destination address")
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the merge"),
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
                            Arg::with_name("no_sweep_ok")
                                .long("no-sweep-ok")
                                .takes_value(false)
                                .help("Exit successfully if a sweep is not possible due to low source account balance"),
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
                    SubCommand::with_name("split")
                        .about("Split an account")
                        .arg(
                            Arg::with_name("from_address")
                                .index(1)
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the account to split")
                        )
                        .arg(
                            Arg::with_name("amount")
                                .index(2)
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("The amount to split, in SOL"),
                        )
                        .arg(
                            Arg::with_name("description")
                                .short("d")
                                .long("description")
                                .value_name("TEXT")
                                .takes_value(true)
                                .help("Description of the new account"),
                        )
                        .arg(
                            Arg::with_name("by")
                                .long("by")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_valid_signer)
                                .help("Optional authority for the merge"),
                        )
                        .arg(
                            Arg::with_name("lot_numbers")
                                .long("lot")
                                .value_name("LOT NUMBER")
                                .takes_value(true)
                                .multiple(true)
                                .validator(is_parsable::<usize>)
                                .help("Lot to fund the split from [default: first in, first out]"),
                        )
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
                        )
                        .arg(
                            Arg::with_name("ask")
                                .long("ask")
                                .takes_value(false)
                                .help("Only display the current asking price")
                        )
                        .arg(
                            Arg::with_name("weighted_24h_average_price")
                                .long("weighted-24h-average-price")
                                .takes_value(false)
                                .conflicts_with("ask")
                                .help("Only display the weighted average price for the previous 24 hours"),
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
                            Arg::with_name("lot_numbers")
                                .long("lot")
                                .value_name("LOT NUMBER")
                                .takes_value(true)
                                .multiple(true)
                                .validator(is_parsable::<usize>)
                                .help(
                                    "Lot to fund the deposit from [default: first in, first out]",
                                ),
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
                        )
                        .arg(
                            Arg::with_name("if_source_balance_exceeds")
                                .long("if-source-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without depositing if the \
                                       source account balance is less than this amount",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("cancel")
                        .about("Cancel orders")
                        .arg(
                            Arg::with_name("order_id")
                                .index(1)
                                .value_name("ORDER ID")
                                .takes_value(true)
                                .multiple(true)
                                .help("The order id to cancel"),
                        )
                        .arg(
                            Arg::with_name("age")
                                .long("age")
                                .value_name("HOURS")
                                .takes_value(true)
                                .validator(is_parsable::<u32>)
                                .conflicts_with("order_id")
                                .help("Cancel orders older than this number of hours"),
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
                            Arg::with_name("lot_numbers")
                                .long("lot")
                                .value_name("LOT NUMBER")
                                .takes_value(true)
                                .multiple(true)
                                .validator(is_parsable::<usize>)
                                .help("Lots to sell from [default: first in, first out]"),
                        )
                        .arg(
                            Arg::with_name("pair")
                                .long("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .default_value("SOLUSDT")
                                .help("Market to place the order at"),
                        )
                        .arg(
                            Arg::with_name("if_balance_exceeds")
                                .long("if-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       exchange available balance is less than this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("if_price_over")
                                .long("if-price-over")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("at")
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       order would be placed at a price that is less than \
                                       or equal to this amount",
                                ),
                        )
                        .arg(
                            Arg::with_name("price_floor")
                                .long("price-floor")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .conflicts_with("if_price_over")
                                .help(
                                    "If the computed price is less than this amount then \
                                       use this amount instead",
                                ),
                        ),
                )
                .subcommand(SubCommand::with_name("sync").about("Synchronize exchange")),
        );
    }

    let app_matches = app.get_matches();
    let db_path = value_t_or_exit!(app_matches, "db_path", PathBuf);
    let verbose = app_matches.is_present("verbose");
    let rpc_client = RpcClient::new_with_commitment(
        normalize_to_url_if_moniker(value_t_or_exit!(app_matches, "json_rpc_url", String)),
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
            if verbose {
                println!("Historical price on {}: ${:.2}", when, price);
            } else {
                println!("{:.2}", price);
            }
        }
        ("sync", Some(_arg_matches)) => {
            for (exchange, exchange_credentials) in db.get_configured_exchanges() {
                println!("Synchronizing {:?}...", exchange);
                let exchange_client = exchange_client_new(exchange, exchange_credentials)?;
                process_sync_exchange(
                    &mut db,
                    exchange,
                    exchange_client.as_ref(),
                    &rpc_client,
                    &notifier,
                )
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
            ("dispose", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let amount = value_t_or_exit!(arg_matches, "amount", f64);
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_else(String::default);
                let when = naivedate_of(&value_t_or_exit!(arg_matches, "when", String)).unwrap();
                let price = value_t!(arg_matches, "price", f64).ok();

                process_account_dispose(&mut db, address, amount, description, when, price).await?;
            }
            ("ls", Some(arg_matches)) => {
                let all = arg_matches.is_present("all");
                process_account_list(&db, all).await?;
            }
            ("xls", Some(arg_matches)) => {
                let outfile = value_t_or_exit!(arg_matches, "outfile", String);
                process_account_xls(&db, &outfile).await?;
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

                let sweep_stake_authority_keypair =
                    read_keypair_file(&stake_authority).map_err(|err| {
                        format!("Failed to read {}: {}", stake_authority.display(), err)
                    })?;
                let (sweep_stake_authorized, _vote_account_address) =
                    rpc_client_utils::get_stake_authorized(&rpc_client, address)?;

                if sweep_stake_authorized.staker != sweep_stake_authority_keypair.pubkey() {
                    return Err("Stake authority mismatch".into());
                }

                db.set_sweep_stake_account(SweepStakeAccount {
                    address,
                    stake_authority,
                })?;

                println!("Sweep stake account set to {}", address);
            }
            ("merge", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "from_address").unwrap();
                let into_address = pubkey_of(arg_matches, "into_address").unwrap();

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "from_address", &mut wallet_manager).map_err(|err| {
                        format!(
                            "Authority not found, consider using the `--by` argument): {}",
                            err
                        )
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");

                process_account_merge(
                    &mut db,
                    &rpc_client,
                    from_address,
                    into_address,
                    authority_address,
                    vec![authority_signer],
                )
                .await?;
            }
            ("sweep", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "address").unwrap();
                let (from_authority_signer, from_authority_address) =
                    signer_of(arg_matches, "authority", &mut wallet_manager)?;
                let from_authority_address = from_authority_address.expect("authority_address");
                let from_authority_signer = from_authority_signer.expect("authority_signer");
                let retain_amount =
                    sol_to_lamports(value_t!(arg_matches, "retain", f64).unwrap_or(0.));
                let no_sweep_ok = arg_matches.is_present("no_sweep_ok");

                process_account_sweep(
                    &mut db,
                    &rpc_client,
                    from_address,
                    retain_amount,
                    no_sweep_ok,
                    from_authority_address,
                    vec![from_authority_signer],
                    &notifier,
                )
                .await?;
            }
            ("split", Some(arg_matches)) => {
                let from_address = pubkey_of(arg_matches, "from_address").unwrap();
                let amount = sol_to_lamports(value_t_or_exit!(arg_matches, "amount", f64));
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_else(|| format!("Split at {}", Local::now()));
                let lot_numbers = values_t!(arg_matches, "lot_numbers", usize)
                    .ok()
                    .map(|x| x.into_iter().collect());

                let (authority_signer, authority_address) = if arg_matches.is_present("by") {
                    signer_of(arg_matches, "by", &mut wallet_manager)?
                } else {
                    signer_of(arg_matches, "from_address", &mut wallet_manager).map_err(|err| {
                        format!(
                            "Authority not found, consider using the `--by` argument): {}",
                            err
                        )
                    })?
                };

                let authority_address = authority_address.expect("authority_address");
                let authority_signer = authority_signer.expect("authority_signer");

                process_account_split(
                    &mut db,
                    &rpc_client,
                    from_address,
                    amount,
                    description,
                    lot_numbers,
                    authority_address,
                    vec![authority_signer],
                )
                .await?;
            }
            ("sync", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address");
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

                    let balances = exchange_client()?.balances().await?;

                    if !available_only {
                        println!("                    Total            Available")
                    }

                    let balance = balances.get("SOL").cloned().unwrap_or_default();
                    if available_only {
                        println!(
                            " SOL ◎{}",
                            balance.available.separated_string_with_fixed_place(8)
                        );
                    } else {
                        println!(
                            " SOL {:>20} {:>20}",
                            format!("◎{}", balance.total.separated_string_with_fixed_place(8)),
                            format!(
                                "◎{}",
                                balance.available.separated_string_with_fixed_place(8)
                            ),
                        );
                    }

                    for coin in crate::exchange::USD_COINS {
                        if let Some(balance) = balances.get(*coin) {
                            if balance.total > 0. {
                                if available_only {
                                    println!(
                                        "{1:>4} ${0}",
                                        balance.available.separated_string_with_fixed_place(8),
                                        coin
                                    );
                                } else {
                                    println!(
                                        "{:>4} {:>20} {:>20}",
                                        coin,
                                        format!(
                                            "${}",
                                            balance.total.separated_string_with_fixed_place(8)
                                        ),
                                        format!(
                                            "${}",
                                            balance.available.separated_string_with_fixed_place(8)
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
                ("market", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let format = if arg_matches.is_present("weighted_24h_average_price") {
                        MarketInfoFormat::Weighted24hAveragePrice
                    } else if arg_matches.is_present("ask") {
                        MarketInfoFormat::Ask
                    } else {
                        MarketInfoFormat::All
                    };
                    exchange_client()?.print_market_info(&pair, format).await?;
                }
                ("deposit", Some(arg_matches)) => {
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(sol_to_lamports(amount.parse().unwrap())),
                    };
                    let if_source_balance_exceeds =
                        value_t!(arg_matches, "if_source_balance_exceeds", f64)
                            .ok()
                            .map(sol_to_lamports);
                    let from_address =
                        pubkey_of_signer(arg_matches, "from", &mut wallet_manager)?.expect("from");
                    let lot_numbers = values_t!(arg_matches, "lot_numbers", usize)
                        .ok()
                        .map(|x| x.into_iter().collect());

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
                    add_exchange_deposit_address_to_db(
                        &mut db,
                        exchange,
                        deposit_address,
                        &rpc_client,
                    )?;

                    process_exchange_deposit(
                        &mut db,
                        &rpc_client,
                        exchange,
                        deposit_address,
                        amount,
                        from_address,
                        if_source_balance_exceeds,
                        authority_address,
                        vec![authority_signer],
                        lot_numbers,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        &rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("cancel", Some(arg_matches)) => {
                    let order_ids: HashSet<String> = values_t!(arg_matches, "order_id", String)
                        .ok()
                        .map(|x| x.into_iter().collect())
                        .unwrap_or_default();

                    let max_create_time = value_t!(arg_matches, "age", i64)
                        .ok()
                        .map(|age| Utc::now().checked_sub_signed(chrono::Duration::hours(age)))
                        .flatten();

                    let exchange_client = exchange_client()?;
                    process_exchange_cancel(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        order_ids,
                        max_create_time,
                    )
                    .await?;

                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        &rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("sell", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let amount = value_t_or_exit!(arg_matches, "amount", f64);
                    let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64)
                        .ok()
                        .map(sol_to_lamports);
                    let if_price_over = value_t!(arg_matches, "if_price_over", f64).ok();
                    let price_floor = value_t!(arg_matches, "price_floor", f64).ok();
                    let lot_numbers = values_t!(arg_matches, "lot_numbers", usize)
                        .ok()
                        .map(|x| x.into_iter().collect());

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
                        if_balance_exceeds,
                        if_price_over,
                        price_floor,
                        lot_numbers,
                        &notifier,
                    )
                    .await?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        &rpc_client,
                        &notifier,
                    )
                    .await?;
                }
                ("sync", Some(_arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    process_sync_exchange(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        &rpc_client,
                        &notifier,
                    )
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
