mod binance_exchange;
mod coin_gecko;
mod db;
mod exchange;
mod field_as_string;
mod ftx_exchange;
mod notifier;
mod rpc_client_utils;
mod token;
mod tulip;

use {
    crate::token::*,
    chrono::prelude::*,
    chrono_humanize::HumanTime,
    clap::{
        crate_description, crate_name, value_t, value_t_or_exit, values_t, App, AppSettings, Arg,
        SubCommand,
    },
    console::{style, Style},
    db::*,
    exchange::*,
    notifier::*,
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::{
        rpc_client::RpcClient, rpc_config::RpcTransactionConfig, rpc_response::StakeActivationState,
    },
    solana_sdk::{
        clock::Slot,
        commitment_config::CommitmentConfig,
        message::Message,
        native_token::lamports_to_sol,
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signature, Signer},
        signers::Signers,
        system_instruction, system_program,
        transaction::Transaction,
    },
    solana_transaction_status::UiTransactionEncoding,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        fs,
        path::PathBuf,
        process::exit,
        str::FromStr,
    },
};

fn get_deprecated_fee_calculator(
    rpc_client: &RpcClient,
) -> solana_client::client_error::Result<solana_sdk::fee_calculator::FeeCalculator> {
    // TODO: Rework calls to avoid the use of `FeeCalculator`
    #[allow(deprecated)]
    rpc_client.get_fees().map(|fees| fees.fee_calculator)
}

pub(crate) fn today() -> NaiveDate {
    let today = Local::now().date();
    NaiveDate::from_ymd(today.year(), today.month(), today.day())
}

fn is_long_term_cap_gain(acquisition: NaiveDate, disposal: Option<NaiveDate>) -> bool {
    let disposal = disposal.unwrap_or_else(today);
    let hold_time = disposal - acquisition;
    hold_time >= chrono::Duration::days(356)
}

fn format_order_side(order_side: OrderSide) -> String {
    match order_side {
        OrderSide::Buy => style(" Buy").green(),
        OrderSide::Sell => style("Sell").red(),
    }
    .to_string()
}

fn format_filled_amount(filled_amount: f64) -> String {
    if filled_amount == 0. {
        Style::new()
    } else {
        Style::new().bold()
    }
    .apply_to(format!(" [◎{} filled]", filled_amount))
    .to_string()
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

async fn get_block_date(
    rpc_client: &RpcClient,
    slot: Slot,
) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    let block_time = rpc_client.get_block_time(slot)?;
    let local_timestamp = Local.timestamp(block_time, 0);
    Ok(NaiveDate::from_ymd(
        local_timestamp.year(),
        local_timestamp.month(),
        local_timestamp.day(),
    ))
}

async fn get_signature_date(
    rpc_client: &RpcClient,
    signature: Signature,
) -> Result<NaiveDate, Box<dyn std::error::Error>> {
    let statuses = rpc_client.get_signature_statuses_with_history(&[signature])?;
    if let Some(Some(ts)) = statuses.value.get(0) {
        let block_date = get_block_date(rpc_client, ts.slot).await?;
        Ok(block_date)
    } else {
        Err(format!("Unknown signature: {}", signature).into())
    }
}

async fn get_block_date_and_price(
    rpc_client: &RpcClient,
    slot: Slot,
    token: MaybeToken,
) -> Result<(NaiveDate, Decimal), Box<dyn std::error::Error>> {
    let block_date = get_block_date(rpc_client, slot).await?;
    Ok((
        block_date,
        token.get_historical_price(rpc_client, block_date).await?,
    ))
}

fn send_transaction_until_expired(
    rpc_client: &RpcClient,
    transaction: &Transaction,
    last_valid_block_height: u64,
) -> bool {
    loop {
        match rpc_client.get_epoch_info() {
            Ok(epoch_info) => {
                if epoch_info.block_height > last_valid_block_height {
                    return false;
                }
                println!(
                    "Transaction pending for at most {} blocks",
                    last_valid_block_height.saturating_sub(epoch_info.block_height),
                );
            }
            Err(err) => {
                println!("Failed to get epoch info: {:?}", err);
            }
        };

        match rpc_client.send_and_confirm_transaction_with_spinner(transaction) {
            Ok(_) => return true,
            Err(err) => {
                println!("Transaction failed to send: {:?}", err);
            }
        }
    }
}

fn add_exchange_deposit_address_to_db(
    db: &mut Db,
    exchange: Exchange,
    token: MaybeToken,
    deposit_address: Pubkey,
    rpc_client: &RpcClient,
) -> Result<(), Box<dyn std::error::Error>> {
    if db.get_account(deposit_address, token).is_none() {
        let epoch = rpc_client.get_epoch_info()?.epoch;
        db.add_account(TrackedAccount {
            address: deposit_address,
            token,
            description: format!("{:?}", exchange),
            last_update_epoch: epoch,
            last_update_balance: 0,
            lots: vec![],
            no_sync: Some(true),
        })?;
    }
    Ok(())
}

async fn verify_exchange_balance(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    deposit_address: &Pubkey,
) -> Result<(), Box<dyn std::error::Error>> {
    let exchange_balance = {
        let balances = exchange_client.balances().await?;
        balances
            .get(&token.to_string())
            .cloned()
            .unwrap_or_default()
            .total
    };

    let exchange_account = db
        .get_account(*deposit_address, token)
        .expect("exchange deposit address does not exist in database");
    let total_lot_balance =
        token.ui_amount(exchange_account.lots.iter().map(|lot| lot.amount).sum());

    if exchange_balance < total_lot_balance {
        eprintln!(
            "{0:?} {4} actual balance is less than local database amount. Actual {3}{1}, expected {3}{2}",
            exchange, exchange_balance, total_lot_balance,
            token.symbol(),
            token,
        );
        exit(1);
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
    // Validate current exchange SOL balance against local database
    {
        let token = MaybeToken::SOL();

        let deposit_address = exchange_client.deposit_address(token).await?;
        add_exchange_deposit_address_to_db(db, exchange, token, deposit_address, rpc_client)?;

        verify_exchange_balance(db, exchange, exchange_client, token, &deposit_address).await?;
    }

    let recent_deposits = exchange_client.recent_deposits().await?;
    let recent_withdrawals = exchange_client.recent_withdrawals().await?;
    let block_height = rpc_client
        .get_epoch_info_with_commitment(CommitmentConfig::finalized())?
        .block_height;

    for pending_withdrawal in db.pending_withdrawals(Some(exchange)) {
        let wi = recent_withdrawals
            .iter()
            .find(|wi| wi.tag == pending_withdrawal.tag)
            .unwrap_or_else(|| {
                panic!("Unknown pending withdrawal: {}", pending_withdrawal.tag);
            });

        let token = pending_withdrawal.token;

        if wi.completed {
            if let Some(ref tx_id) = wi.tx_id {
                let msg = format!(
                    "{} {}{} withdrawal to {} successful ({})",
                    token,
                    token.symbol(),
                    token
                        .ui_amount(pending_withdrawal.amount)
                        .separated_string_with_fixed_place(2),
                    wi.address,
                    tx_id,
                );
                println!("{}", msg);
                db.confirm_withdrawal(pending_withdrawal)?;
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            } else {
                println!("Pending {} withdrawal to {} cancelled", token, wi.address);
                db.cancel_withdrawal(pending_withdrawal)?;
            }
        } else {
            println!(
                "{} {}{} withdrawal to {} pending",
                token,
                token.symbol(),
                token.ui_amount(pending_withdrawal.amount),
                wi.address,
            );
        }
    }

    for pending_deposit in db.pending_deposits(Some(exchange)) {
        let confirmed = rpc_client
            .confirm_transaction_with_commitment(
                &pending_deposit.transfer.signature,
                CommitmentConfig::finalized(),
            )?
            .value;

        let token = pending_deposit.transfer.token;

        if let Some(deposit_info) = recent_deposits.iter().find(|deposit_info| {
            deposit_info.tx_id == pending_deposit.transfer.signature.to_string()
        }) {
            let missing_tokens =
                (token.amount(deposit_info.amount) as i64 - (pending_deposit.amount as i64)).abs();
            if missing_tokens >= 10 {
                let msg = format!(
                    "Error! {} deposit amount mismatch for {}! Actual amount: ◎{}, expected amount: ◎{}",
                    token,
                    pending_deposit.transfer.signature, deposit_info.amount, pending_deposit.amount
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;

                // TODO: Do something more here...?
            } else {
                if missing_tokens != 0 {
                    // Binance will occasionally steal a lamport or two...
                    let msg = format!(
                        "{:?} just stole {} tokens from your deposit!",
                        exchange, missing_tokens
                    );
                    println!("{}", msg);
                    notifier.send(&format!("{:?}: {}", exchange, msg)).await;
                }

                let when =
                    get_signature_date(rpc_client, pending_deposit.transfer.signature).await?;
                db.confirm_deposit(pending_deposit.transfer.signature, when)?;

                let msg = format!(
                    "{} {}{} deposit successful ({})",
                    token,
                    token.symbol(),
                    token.ui_amount(pending_deposit.amount),
                    pending_deposit.transfer.signature
                );
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            }
        } else if !confirmed && block_height > pending_deposit.transfer.last_valid_block_height {
            println!(
                "Pending {} deposit cancelled: {}",
                token, pending_deposit.transfer.signature
            );
            db.cancel_deposit(pending_deposit.transfer.signature)
                .expect("cancel_deposit");
        } else if confirmed {
            println!(
                "{} {}{} deposit pending ({} confirmed)",
                token,
                token.symbol(),
                token.ui_amount(pending_deposit.amount),
                pending_deposit.transfer.signature,
            );
        } else {
            println!(
                "{} {}{} deposit pending for at most {} blocks ({} unconfirmed)",
                token,
                token.symbol(),
                token.ui_amount(pending_deposit.amount),
                pending_deposit
                    .transfer
                    .last_valid_block_height
                    .saturating_sub(block_height),
                pending_deposit.transfer.signature,
            );
        }
    }

    for order_info in db.open_orders(Some(exchange), None) {
        let token = order_info.token;
        let order_status = exchange_client
            .order_status(&order_info.pair, &order_info.order_id)
            .await?;
        let order_summary = format!(
            "{}: {} {} {}{:<5} at ${:<.2}{} | id {} created {}",
            order_info.pair,
            token,
            format_order_side(order_info.side),
            token.symbol(),
            order_status.amount,
            order_status.price,
            if order_status.filled_amount == 0. {
                String::default()
            } else {
                format_filled_amount(order_status.filled_amount)
            },
            order_info.order_id,
            HumanTime::from(order_info.creation_time),
        );

        if order_status.open {
            if order_status.filled_amount > 0. {
                let msg = format!("Partial {}", order_summary);
                println!("{}", msg);
                notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            } else {
                println!("   Open {}", order_summary);
            }
        } else {
            let fee_summary = match &order_status.fee {
                Some((amount, coin)) if *amount > 0. => format!(" (fee: {} {})", amount, coin),
                _ => "".into(),
            };
            db.close_order(
                &order_info.order_id,
                token.amount(order_status.amount),
                token.amount(order_status.filled_amount),
                order_status.price,
                order_status.last_update,
                order_status.fee,
            )?;
            let msg = if (order_status.amount - order_status.filled_amount).abs() < f64::EPSILON {
                format!(" Filled {}{}", order_summary, fee_summary)
            } else if order_status.filled_amount < f64::EPSILON {
                format!(" Cancel {}{}", order_summary, fee_summary)
            } else {
                format!("Partial {}{}", order_summary, fee_summary)
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
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    deposit_address: Pubkey,
    amount: Option<u64>,
    from_address: Pubkey,
    if_source_balance_exceeds: Option<u64>,
    if_exchange_balance_less_than: Option<u64>,
    authority_address: Pubkey,
    signers: T,
    lot_numbers: Option<HashSet<usize>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(if_exchange_balance_less_than) = if_exchange_balance_less_than {
        let exchange_balance = exchange_client
            .balances()
            .await?
            .get(&token.to_string())
            .map(|b| token.amount(b.total))
            .unwrap_or(0)
            + db.pending_deposits(Some(exchange))
                .into_iter()
                .map(|pd| pd.amount)
                .sum::<u64>();

        if exchange_balance < if_exchange_balance_less_than {
            println!(
                "{0} deposit declined because {1:?} balance ({4}{2}) is less than {4}{3}",
                token,
                exchange,
                token.ui_amount(exchange_balance),
                token.ui_amount(if_exchange_balance_less_than),
                token.symbol(),
            );
            return Ok(());
        }
    }

    let from_account_balance = token.balance(rpc_client, &from_address)?;
    if let Some(if_source_balance_exceeds) = if_source_balance_exceeds {
        if from_account_balance < if_source_balance_exceeds {
            println!(
                "{} deposit declined because {} balance is less than {}{}",
                token,
                from_address,
                token.symbol(),
                token.ui_amount(if_source_balance_exceeds)
            );
            return Ok(());
        }
    }

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;
    let fee_calculator = get_deprecated_fee_calculator(rpc_client)?;

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
            lamports_to_sol(fee_calculator.lamports_per_signature)
        )
        .into());
    }

    let (instructions, amount) = match token.token() {
        /*SOL*/
        None => {
            assert_eq!(from_account.lamports, from_account_balance);

            if from_account.owner == system_program::id() {
                let amount = amount.unwrap_or_else(|| {
                    if from_address == authority_address {
                        from_account_balance.saturating_sub(fee_calculator.lamports_per_signature)
                    } else {
                        from_account_balance
                    }
                });

                (
                    vec![system_instruction::transfer(
                        &from_address,
                        &deposit_address,
                        amount,
                    )],
                    amount,
                )
            } else if from_account.owner == solana_vote_program::id() {
                let minimum_balance = rpc_client.get_minimum_balance_for_rent_exemption(
                    solana_vote_program::vote_state::VoteState::size_of(),
                )?;

                let amount =
                    amount.unwrap_or_else(|| from_account_balance.saturating_sub(minimum_balance));

                (
                    vec![solana_vote_program::vote_instruction::withdraw(
                        &from_address,
                        &authority_address,
                        amount,
                        &deposit_address,
                    )],
                    amount,
                )
            } else if from_account.owner == solana_stake_program::id() {
                let amount = amount.unwrap_or(from_account_balance);

                (
                    vec![solana_stake_program::stake_instruction::withdraw(
                        &from_address,
                        &authority_address,
                        &deposit_address,
                        amount,
                        None,
                    )],
                    amount,
                )
            } else {
                return Err(
                    format!("Unsupported `from` account owner: {}", from_account.owner).into(),
                );
            }
        }
        Some(token) => {
            let amount = amount.unwrap_or(from_account_balance);

            let mut instructions = vec![];

            if rpc_client
                .get_account_with_commitment(&token.ata(&deposit_address), rpc_client.commitment())?
                .value
                .is_none()
            {
                instructions.push(
                    spl_associated_token_account::create_associated_token_account(
                        &authority_address,
                        &deposit_address,
                        &token.mint(),
                    ),
                );
            }

            instructions.push(
                spl_token::instruction::transfer_checked(
                    &spl_token::id(),
                    &token.ata(&from_address),
                    &token.mint(),
                    &token.ata(&deposit_address),
                    &authority_address,
                    &[],
                    amount,
                    token.decimals(),
                )
                .unwrap(),
            );

            (instructions, amount)
        }
    };

    if amount == 0 {
        return Err("Nothing to deposit".into());
    }
    if from_account_balance < amount {
        return Err("From account has insufficient funds".into());
    }

    println!("From address: {} ({})", from_address, token);
    if from_address != authority_address {
        println!("Authority address: {}", authority_address);
    }
    println!("Amount: {}{}", token.symbol(), token.ui_amount(amount));
    println!(
        "{} {:?} deposit address: {}",
        token, exchange, deposit_address
    );

    let mut message = Message::new(&instructions, Some(&authority_address));
    message.recent_blockhash = recent_blockhash;
    if rpc_client.get_fee_for_message(&message)? > authority_account.lamports {
        return Err("Insufficient funds for transaction fee".into());
    }

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&signers, recent_blockhash)?;
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    db.record_deposit(
        signature,
        last_valid_block_height,
        from_address,
        amount,
        exchange,
        deposit_address,
        token,
        lot_numbers,
    )?;
    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_deposit(signature).expect("cancel_deposit");
        return Err("Deposit failed".into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_withdraw(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    deposit_address: Pubkey,
    amount: Option<u64>,
    to_address: Pubkey,
    lot_numbers: Option<HashSet<usize>>,
    withdrawal_password: Option<String>,
    withdrawal_code: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deposit_account = db
        .get_account(deposit_address, token)
        .expect("unknown deposit address");
    let _to_account = db
        .get_account(to_address, token)
        .expect("unknown to address");

    let amount = amount.unwrap_or(deposit_account.last_update_balance);

    let tag = exchange_client
        .request_withdraw(
            to_address,
            token,
            token.ui_amount(amount),
            withdrawal_password,
            withdrawal_code,
        )
        .await?;

    db.record_withdrawal(
        exchange,
        tag,
        token,
        amount,
        deposit_address,
        to_address,
        lot_numbers,
    )?;
    Ok(())
}

enum LimitOrderPrice {
    At(f64),
    AmountOverAsk(f64),
    AmountUnderBid(f64),
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_cancel(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    order_ids: HashSet<String>,
    max_create_time: Option<DateTime<Utc>>,
    side: Option<OrderSide>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cancelled_count = 0;
    for order_info in db.open_orders(Some(exchange), side) {
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
                .cancel_order(&order_info.pair, &order_info.order_id)
                .await
                .unwrap_or_else(|err| eprintln!("{:?}", err));
        }
    }

    println!("{} orders cancelled", cancelled_count);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_buy(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    pair: String,
    amount: Option<f64>,
    price: LimitOrderPrice,
    if_balance_exceeds: Option<f64>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!(
        "{} | Ask: ${}, Bid: ${}",
        pair, bid_ask.ask_price, bid_ask.bid_price
    );

    let deposit_address = exchange_client.deposit_address(token).await?;
    let deposit_account = db.get_account(deposit_address, token).ok_or_else(|| {
        format!(
            "Exchange deposit account does not exist, run `sync` first: {} ({})",
            deposit_address, token,
        )
    })?;

    let balances = exchange_client.balances().await?;
    let usd_balance = balances.get("USD").cloned().unwrap_or_default().available;

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if usd_balance < if_balance_exceeds {
            println!(
                "Order declined because {:?} available balance is less than ${}",
                exchange, if_balance_exceeds
            );
            return Ok(());
        }
    }

    let price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(_) => panic!("Bug: AmountOverAsk invalid for a buy order"),
        LimitOrderPrice::AmountUnderBid(extra) => bid_ask.ask_price - extra,
    };
    let price = (price * 100.).round() / 100.; // Round to two decimal places

    if price > bid_ask.bid_price {
        return Err("Order price is greater than bid price".into());
    }

    let amount = match amount {
        None => (usd_balance / price).floor(),
        Some(amount) => amount,
    };

    println!("Placing buy order for ◎{} at ${}", amount, price);

    let order_id = exchange_client
        .place_order(&pair, OrderSide::Buy, price, amount)
        .await?;
    let msg = format!(
        "Order created: {}: {:?} ◎{} at ${}, id {}",
        pair,
        OrderSide::Buy,
        amount,
        price,
        order_id,
    );
    db.open_order(
        OrderSide::Buy,
        deposit_account,
        exchange,
        pair,
        price,
        order_id,
        vec![],
        Some(amount),
    )?;
    println!("{}", msg);
    notifier.send(&format!("{:?}: {}", exchange, msg)).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exchange_sell(
    db: &mut Db,
    exchange: Exchange,
    exchange_client: &dyn ExchangeClient,
    token: MaybeToken,
    pair: String,
    amount: f64,
    price: LimitOrderPrice,
    if_balance_exceeds: Option<u64>,
    if_price_over: Option<f64>,
    if_price_over_basis: bool,
    price_floor: Option<f64>,
    lot_numbers: Option<HashSet<usize>>,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let bid_ask = exchange_client.bid_ask(&pair).await?;
    println!(
        "{} | Ask: ${}, Bid: ${}",
        pair, bid_ask.ask_price, bid_ask.bid_price
    );

    let deposit_address = exchange_client.deposit_address(token).await?;
    let mut deposit_account = db.get_account(deposit_address, token).ok_or_else(|| {
        format!(
            "Exchange deposit account does not exist, run `sync` first: {} ({})",
            deposit_address, token,
        )
    })?;

    if let Some(if_balance_exceeds) = if_balance_exceeds {
        if deposit_account.last_update_balance < if_balance_exceeds {
            println!(
                "Order declined because {:?} available balance is less than {}",
                exchange,
                token.ui_amount(if_balance_exceeds)
            );
            return Ok(());
        }
    }

    let price = match price {
        LimitOrderPrice::At(price) => price,
        LimitOrderPrice::AmountOverAsk(extra) => bid_ask.ask_price + extra,
        LimitOrderPrice::AmountUnderBid(_) => {
            panic!("Bug: AmountUnderBid invalid for a sell order")
        }
    };
    let mut price = (price * 100.).round() / 100.; // Round to two decimal places

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

    let order_lots = deposit_account.extract_lots(db, token.amount(amount), lot_numbers)?;
    if if_price_over_basis {
        if let Some(basis) = order_lots.iter().find_map(|lot| {
            let basis = lot.acquisition.price();
            if Decimal::from_f64(price).unwrap() < basis {
                Some(basis)
            } else {
                None
            }
        }) {
            let msg = format!(
                "Order declined because price, ${}, is less than basis ${}",
                price, basis,
            );
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
            return Ok(());
        }
    }

    if price < bid_ask.ask_price {
        return Err("Order price is less than ask price".into());
    }

    println!("Placing sell order for ◎{} at ${}", amount, price);
    println!("Lots");
    for lot in &order_lots {
        println_lot(
            deposit_account.token,
            lot,
            Decimal::from_f64(price).unwrap(),
            None,
            &mut 0.,
            &mut 0.,
            &mut false,
            &mut 0.,
            None,
        )
        .await;
    }

    let order_id = exchange_client
        .place_order(&pair, OrderSide::Sell, price, amount)
        .await?;
    let msg = format!(
        "Order created: {}: {:?} ◎{} at ${}, id {}",
        pair,
        OrderSide::Sell,
        amount,
        price,
        order_id,
    );
    db.open_order(
        OrderSide::Sell,
        deposit_account,
        exchange,
        pair,
        price,
        order_id,
        order_lots,
        None,
    )?;
    println!("{}", msg);
    notifier.send(&format!("{:?}: {}", exchange, msg)).await;
    Ok(())
}

async fn process_tulip_deposit<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    collateral_token: Token,
    liquidity_amount: Option<u64>,
    address: Pubkey,
    signers: T,
) -> Result<(), Box<dyn std::error::Error>> {
    let liquidity_token = collateral_token
        .liquidity_token()
        .ok_or_else(|| format!("{} is not a collateral token", collateral_token))?;

    let sol = MaybeToken::SOL();
    let minimum_lamport_balance = sol.amount(0.01);
    let from_account_lamports = sol.balance(rpc_client, &address)?;
    if from_account_lamports < minimum_lamport_balance {
        return Err(format!(
            "From account (SOL), {}, has insufficient funds ({}{} required)",
            address,
            sol.symbol(),
            sol.ui_amount(minimum_lamport_balance)
        )
        .into());
    }

    let liquidity_tracked_account = db
        .get_account(address, liquidity_token)
        .ok_or_else(|| format!("Unknown account {} ({})", address, liquidity_token))?;
    let liquidity_account_balance = liquidity_tracked_account.last_update_balance;

    let max_liquidity_amount = if liquidity_token.is_sol() {
        liquidity_account_balance.saturating_sub(minimum_lamport_balance)
    } else {
        liquidity_account_balance
    };
    let liquidity_amount = liquidity_amount.unwrap_or(max_liquidity_amount);

    if liquidity_amount > max_liquidity_amount {
        return Err(format!(
            "Deposit amount is too large: {0}{1} (max: {0}{2})",
            liquidity_token.symbol(),
            liquidity_token.ui_amount(liquidity_amount),
            liquidity_token.ui_amount(max_liquidity_amount)
        )
        .into());
    }
    if liquidity_amount == 0 {
        return Err("Nothing to deposit".into());
    }

    let liquidity_token_price = liquidity_token.get_current_price(rpc_client).await?;
    let collateral_token_price = collateral_token.get_current_price(rpc_client).await?;
    let liquidity_token_ui_amount = liquidity_token.ui_amount(liquidity_amount);

    println!("{}: {} -> {}", address, liquidity_token, collateral_token);
    println!(
        "Estimated deposit amount: {}{} (${})",
        liquidity_token.symbol(),
        liquidity_token.ui_amount(liquidity_amount),
        liquidity_token_price * Decimal::from_f64(liquidity_token_ui_amount).unwrap()
    );

    let instructions = tulip::deposit(rpc_client, address, liquidity_token, liquidity_amount)?;

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let mut message = Message::new(&instructions, Some(&address));
    message.recent_blockhash = recent_blockhash;

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&signers, recent_blockhash)?;
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    if db.get_account(address, collateral_token.into()).is_none() {
        let epoch = rpc_client.get_epoch_info()?.epoch;
        db.add_account(TrackedAccount {
            address,
            token: collateral_token.into(),
            description: liquidity_tracked_account.description,
            last_update_epoch: epoch,
            last_update_balance: 0,
            lots: vec![],
            no_sync: None,
        })?;
    }

    db.record_swap(
        signature,
        last_valid_block_height,
        address,
        liquidity_token,
        liquidity_token_price,
        collateral_token.into(),
        collateral_token_price,
    )?;

    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_swap(signature).expect("cancel_swap");
        return Err("Swap failed".into());
    }
    Ok(())
}

async fn process_tulip_withdraw<T: Signers>(
    db: &mut Db,
    rpc_client: &RpcClient,
    collateral_token: Token,
    liquidity_amount: Option<u64>,
    address: Pubkey,
    signers: T,
) -> Result<(), Box<dyn std::error::Error>> {
    let liquidity_token = collateral_token
        .liquidity_token()
        .ok_or_else(|| format!("{} is not a collateral token", collateral_token))?;

    let collateral_tracked_account = db
        .get_account(address, collateral_token.into())
        .ok_or_else(|| format!("Unknown account {} ({})", address, collateral_token))?;
    let collateral_account_balance = collateral_tracked_account.last_update_balance;

    let collateral_amount = match liquidity_amount {
        None => collateral_account_balance,
        Some(liquidity_amount) => collateral_token.amount(
            f64::try_from(
                Decimal::from_f64(liquidity_token.ui_amount(liquidity_amount)).unwrap()
                    / tulip::get_current_liquidity_token_rate(rpc_client, &collateral_token)
                        .await?,
            )
            .unwrap(),
        ),
    };

    if collateral_amount > collateral_account_balance {
        return Err(format!(
            "Withdraw amount is too large: {0}{1} (max: {0}{2})",
            collateral_token.symbol(),
            collateral_token.ui_amount(collateral_amount),
            collateral_token.ui_amount(collateral_account_balance)
        )
        .into());
    }
    if collateral_amount == 0 {
        return Err("Nothing to withdraw".into());
    }

    let liquidity_token_price = liquidity_token.get_current_price(rpc_client).await?;
    let collateral_token_price = collateral_token.get_current_price(rpc_client).await?;
    let collateral_token_ui_amount = collateral_token.ui_amount(collateral_amount);

    println!("{}: {} -> {}", address, collateral_token, liquidity_token);
    println!(
        "Estimated withdraw amount: {}{} (${})",
        collateral_token.symbol(),
        collateral_token.ui_amount(collateral_amount),
        collateral_token_price * Decimal::from_f64(collateral_token_ui_amount).unwrap()
    );

    let instructions = tulip::withdraw(rpc_client, address, collateral_token, collateral_amount)?;

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let mut message = Message::new(&instructions, Some(&address));
    message.recent_blockhash = recent_blockhash;

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&signers, recent_blockhash)?;
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    db.record_swap(
        signature,
        last_valid_block_height,
        address,
        collateral_token.into(),
        collateral_token_price,
        liquidity_token,
        liquidity_token_price,
    )?;

    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_swap(signature).expect("cancel_swap");
        return Err("Swap failed".into());
    }

    Ok(())
}

async fn process_sync_swaps(
    db: &mut Db,
    rpc_client: &RpcClient,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let block_height = rpc_client.get_epoch_info()?.block_height;

    for PendingSwap {
        signature,
        last_valid_block_height,
        address,
        from_token,
        to_token,
        ..
    } in db.pending_swaps()
    {
        let swap = format!("swap ({}: {} -> {})", address, from_token, to_token);

        let status = rpc_client.get_signature_status_with_commitment_and_history(
            &signature,
            rpc_client.commitment(),
            true,
        )?;
        match status {
            Some(result) => {
                if result.is_ok() {
                    println!("Pending {} confirmed: {}", swap, signature);
                    let result = rpc_client.get_transaction_with_config(
                        &signature,
                        RpcTransactionConfig {
                            commitment: Some(rpc_client.commitment()),
                            ..RpcTransactionConfig::default()
                        },
                    )?;

                    let block_time = result
                        .block_time
                        .ok_or("Transaction block time not available")?;

                    let when = Local.timestamp(block_time, 0);
                    let when = NaiveDate::from_ymd(when.year(), when.month(), when.day());

                    let transaction_status_meta = result.transaction.meta.unwrap();

                    let pre_token_balances = transaction_status_meta
                        .pre_token_balances
                        .unwrap_or_default();
                    let post_token_balances = transaction_status_meta
                        .post_token_balances
                        .unwrap_or_default();

                    let token_amount_diff = |token: &Pubkey| {
                        let token = token.to_string();
                        let pre = pre_token_balances
                            .iter()
                            .filter_map(|token_balance| {
                                if token_balance.mint == token {
                                    Some(
                                        token_balance
                                            .ui_token_amount
                                            .amount
                                            .parse::<u64>()
                                            .expect("amount"),
                                    )
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap();
                        let post = post_token_balances
                            .iter()
                            .filter_map(|token_balance| {
                                if token_balance.mint == token {
                                    Some(
                                        token_balance
                                            .ui_token_amount
                                            .amount
                                            .parse::<u64>()
                                            .expect("amount"),
                                    )
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap();
                        (post as i64 - pre as i64).abs() as u64
                    };

                    let from_amount = token_amount_diff(&from_token.mint());
                    let to_amount = token_amount_diff(&to_token.mint());
                    let msg = format!(
                        "Swapped {}{} for {}{}",
                        from_token.symbol(),
                        from_token
                            .ui_amount(from_amount)
                            .separated_string_with_fixed_place(2),
                        to_token.symbol(),
                        to_token
                            .ui_amount(to_amount)
                            .separated_string_with_fixed_place(2),
                    );
                    db.confirm_swap(signature, when, from_amount, to_amount)?;
                    notifier.send(&msg).await;
                    println!("{}", msg);
                } else {
                    println!("Pending {} failed with {:?}: {}", swap, result, signature);
                    db.cancel_swap(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    println!("Pending {} cancelled: {}", swap, signature);
                    db.cancel_swap(signature)?;
                } else {
                    println!(
                        "{} pending for at most {} blocks: {}",
                        swap,
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    );
                }
            }
        }
    }

    Ok(())
}

struct LiquidityTokenInfo {
    liquidity_token: MaybeToken,
    current_liquidity_token_rate: Decimal,
}

fn liquidity_token_ui_amount(
    ui_amount: f64,
    liquidity_token_info: Option<&LiquidityTokenInfo>,
) -> String {
    if let Some(LiquidityTokenInfo {
        liquidity_token,
        current_liquidity_token_rate,
    }) = liquidity_token_info
    {
        format!(
            " [{}{}]",
            liquidity_token.symbol(),
            f64::try_from(Decimal::from_f64(ui_amount).unwrap() * current_liquidity_token_rate)
                .unwrap()
                .separated_string_with_fixed_place(2)
        )
    } else {
        String::new()
    }
}

#[allow(clippy::too_many_arguments)]
async fn println_lot(
    token: MaybeToken,
    lot: &Lot,
    current_price: Decimal,
    liquidity_token_info: Option<&LiquidityTokenInfo>,
    total_income: &mut f64,
    total_cap_gain: &mut f64,
    long_term_cap_gain: &mut bool,
    total_current_value: &mut f64,
    notifier: Option<&Notifier>,
) {
    let current_value =
        f64::try_from(Decimal::from_f64(token.ui_amount(lot.amount)).unwrap() * current_price)
            .unwrap();
    let income = lot.income(token);
    let cap_gain = lot.cap_gain(token, current_price);

    *total_income += income;
    *total_cap_gain += cap_gain;
    *total_current_value += current_value;
    *long_term_cap_gain = is_long_term_cap_gain(lot.acquisition.when, None);

    let ui_amount = token.ui_amount(lot.amount);
    let msg = format!(
        "{:>4}. {} | {}{:<16}{} at ${:<6} | current value: ${:<14} | income: ${:<11} | {} gain: ${:<14} | {}",
        lot.lot_number,
        lot.acquisition.when,
        token.symbol(),
        ui_amount.separated_string_with_fixed_place(6),
        liquidity_token_ui_amount(ui_amount, liquidity_token_info),
        f64::try_from(lot.acquisition.price()).unwrap().separated_string_with_fixed_place(2),
        current_value.separated_string_with_fixed_place(2),
        income.separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            " long"
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
    let cap_gain = disposed_lot
        .lot
        .cap_gain(disposed_lot.token, disposed_lot.price());
    let income = disposed_lot.lot.income(disposed_lot.token);

    *long_term_cap_gain =
        is_long_term_cap_gain(disposed_lot.lot.acquisition.when, Some(disposed_lot.when));
    *total_income += income;
    *total_current_value += income + cap_gain;
    *total_cap_gain += cap_gain;

    format!(
        "{:>4}. {} | {:<4} | {}{:<17.9} at ${:<6} | income: ${:<11} | sold {} at ${:6} | {} gain: ${:<14} | {} | {}",
        disposed_lot.lot.lot_number,
        disposed_lot.lot.acquisition.when,
        disposed_lot.token,
        disposed_lot.token.symbol(),
        disposed_lot.token.ui_amount(disposed_lot.lot.amount),
        f64::try_from(disposed_lot.lot.acquisition.price()).unwrap().separated_string_with_fixed_place(2),
        income.separated_string_with_fixed_place(2),
        disposed_lot.when,
        f64::try_from(disposed_lot.price()).unwrap().separated_string_with_fixed_place(2),
        if *long_term_cap_gain {
            " long"
        } else {
            "short"
        },
        cap_gain.separated_string_with_fixed_place(2),
        disposed_lot.lot.acquisition.kind,
        disposed_lot.kind,
    )
}

#[allow(clippy::too_many_arguments)]
async fn process_account_add(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Pubkey,
    token: MaybeToken,
    description: String,
    when: Option<NaiveDate>,
    price: Option<f64>,
    income: bool,
    signature: Option<Signature>,
    no_sync: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (when, amount, last_update_epoch, kind) = match signature {
        Some(signature) => {
            let confirmed_transaction =
                rpc_client.get_transaction(&signature, UiTransactionEncoding::Base64)?;

            let slot = confirmed_transaction.slot;
            let when = match confirmed_transaction.block_time {
                Some(block_time) => Some(
                    NaiveDateTime::from_timestamp_opt(block_time, 0)
                        .ok_or_else(|| format!("Invalid block time for slot {}", slot))?
                        .date(),
                ),
                None => {
                    println!(
                        "Block time not available for slot {}, using `--when` argument instead",
                        slot
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
                .static_account_keys()
                .iter()
                .position(|k| {
                    *k == match token.token() {
                        None => address,
                        Some(token) => token.ata(&address),
                    }
                })
                .ok_or_else(|| format!("{} not found in the transaction {}", address, signature))?;

            let amount = match token.token() {
                None => meta.post_balances[account_index],
                Some(_) => u64::from_str(
                    &meta
                        .post_token_balances
                        .unwrap()
                        .iter()
                        .find(|ptb| ptb.account_index as usize == account_index)
                        .unwrap()
                        .ui_token_amount
                        .amount,
                )
                .unwrap_or_default(),
            };

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
            let amount = token.balance(rpc_client, &address)?;
            let last_update_epoch = rpc_client.get_epoch_info()?.epoch.saturating_sub(1);
            (
                when,
                amount,
                last_update_epoch,
                if income {
                    LotAcquistionKind::NotAvailable
                } else {
                    LotAcquistionKind::Fiat
                },
            )
        }
    };

    println!("Adding {} (token: {})", address, token);

    let current_price = token.get_current_price(rpc_client).await?;
    let decimal_price = match price {
        Some(price) => Decimal::from_f64(price).unwrap(),
        None => match when {
            Some(when) => token.get_historical_price(rpc_client, when).await?,
            None => current_price,
        },
    };

    let mut lots = vec![];
    if amount > 0 {
        let lot = Lot {
            lot_number: db.next_lot_number(),
            acquisition: LotAcquistion::new(when.unwrap_or_else(today), decimal_price, kind),
            amount,
        };
        println_lot(
            token,
            &lot,
            current_price,
            None,
            &mut 0.,
            &mut 0.,
            &mut false,
            &mut 0.,
            None,
        )
        .await;

        lots.push(lot);
    }

    let account = TrackedAccount {
        address,
        token,
        description,
        last_update_epoch,
        last_update_balance: amount,
        lots,
        no_sync: Some(no_sync),
    };
    db.add_account(account)?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_account_dispose(
    db: &mut Db,
    rpc_client: &RpcClient,
    address: Pubkey,
    token: MaybeToken,
    ui_amount: f64,
    description: String,
    when: Option<NaiveDate>,
    price: Option<f64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let price = match price {
        Some(price) => Decimal::from_f64(price).unwrap(),
        None => match when {
            Some(when) => token.get_historical_price(rpc_client, when).await?,
            None => token.get_current_price(rpc_client).await?,
        },
    };

    let disposed_lots = db.record_disposal(
        address,
        token,
        token.amount(ui_amount),
        description,
        when.unwrap_or_else(today),
        price,
    )?;
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

#[derive(Default, Debug, PartialEq)]
struct RealizedGain {
    income: f64,
    short_term_cap_gain: f64,
    long_term_cap_gain: f64,
}

async fn process_account_list(
    db: &Db,
    rpc_client: &RpcClient,
    account_filter: Option<Pubkey>,
    show_all_disposed_lots: bool,
    summary_only: bool,
    notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut annual_realized_gains = BTreeMap::<usize, [RealizedGain; 4]>::default();
    let mut held_tokens = HashMap::<MaybeToken, (/*price*/ Decimal, /*amount*/ u64)>::default();

    let mut accounts = db.get_accounts();
    accounts.sort_by(|a, b| {
        let mut result = a.last_update_balance.cmp(&b.last_update_balance);
        if result == std::cmp::Ordering::Equal {
            result = a.address.cmp(&b.address);
        }
        if result == std::cmp::Ordering::Equal {
            result = a.description.cmp(&b.description);
        }
        result
    });
    if accounts.is_empty() {
        println!("No accounts");
    } else {
        let mut total_income = 0.;
        let mut total_unrealized_short_term_gain = 0.;
        let mut total_unrealized_long_term_gain = 0.;
        let mut total_current_value = 0.;

        let open_orders = db.open_orders(None, None);

        for account in accounts {
            if let Some(ref account_filter) = account_filter {
                if account.address != *account_filter {
                    continue;
                }
            }

            if let std::collections::hash_map::Entry::Vacant(e) = held_tokens.entry(account.token) {
                e.insert((account.token.get_current_price(rpc_client).await?, 0));
            }

            let held_token = held_tokens.get_mut(&account.token).unwrap();
            let current_token_price = held_token.0;
            held_token.1 += account.last_update_balance;

            let ui_amount = account.token.ui_amount(account.last_update_balance);

            let liquidity_token_info =
                if let Some(liquidity_token) = account.token.liquidity_token() {
                    let current_liquidity_token_rate = account
                        .token
                        .get_current_liquidity_token_rate(rpc_client)
                        .await?;
                    Some(LiquidityTokenInfo {
                        liquidity_token,
                        current_liquidity_token_rate,
                    })
                } else {
                    None
                };

            let msg = format!(
                "{} ({}): {}{}{} - {}",
                account.address,
                account.token,
                account.token.symbol(),
                ui_amount.separated_string_with_fixed_place(2),
                liquidity_token_ui_amount(ui_amount, liquidity_token_info.as_ref()),
                account.description
            );
            println!("{}", msg);
            if account.last_update_balance > 0 {
                notifier.send(&msg).await;
            }
            account.assert_lot_balance();

            if summary_only {
                continue;
            }

            let open_orders = open_orders
                .iter()
                .filter(|oo| oo.deposit_address == account.address && oo.token == account.token)
                .collect::<Vec<_>>();

            if !account.lots.is_empty() || !open_orders.is_empty() {
                let mut lots = account.lots.iter().collect::<Vec<_>>();
                lots.sort_by_key(|lot| lot.acquisition.when);

                let mut account_income = 0.;
                let mut account_current_value = 0.;
                let mut account_unrealized_short_term_gain = 0.;
                let mut account_unrealized_long_term_gain = 0.;

                for lot in lots {
                    let mut account_unrealized_gain = 0.;
                    let mut long_term_cap_gain = false;
                    println_lot(
                        account.token,
                        lot,
                        current_token_price,
                        liquidity_token_info.as_ref(),
                        &mut account_income,
                        &mut account_unrealized_gain,
                        &mut long_term_cap_gain,
                        &mut account_current_value,
                        None,
                    )
                    .await;

                    annual_realized_gains
                        .entry(lot.acquisition.when.year() as usize)
                        .or_default()[lot.acquisition.when.month0() as usize / 3]
                        .income += lot.income(account.token);

                    if long_term_cap_gain {
                        account_unrealized_long_term_gain += account_unrealized_gain;
                    } else {
                        account_unrealized_short_term_gain += account_unrealized_gain;
                    }
                }

                for open_order in open_orders {
                    let mut lots = open_order.lots.iter().collect::<Vec<_>>();
                    lots.sort_by_key(|lot| lot.acquisition.when);
                    let ui_amount = open_order.ui_amount.unwrap_or_else(|| {
                        account
                            .token
                            .ui_amount(lots.iter().map(|lot| lot.amount).sum::<u64>())
                    });
                    println!(
                        " [Open {}: {} {}{} at ${} | id {} created {}]",
                        open_order.pair,
                        format_order_side(open_order.side),
                        account.token.symbol(),
                        ui_amount,
                        open_order.price,
                        open_order.order_id,
                        HumanTime::from(open_order.creation_time),
                    );
                    for lot in lots {
                        let mut account_unrealized_gain = 0.;
                        let mut long_term_cap_gain = false;
                        println_lot(
                            account.token,
                            lot,
                            current_token_price,
                            liquidity_token_info.as_ref(),
                            &mut account_income,
                            &mut account_unrealized_gain,
                            &mut long_term_cap_gain,
                            &mut account_current_value,
                            None,
                        )
                        .await;

                        annual_realized_gains
                            .entry(lot.acquisition.when.year() as usize)
                            .or_default()[lot.acquisition.when.month0() as usize / 3]
                            .income += lot.income(account.token);

                        if long_term_cap_gain {
                            account_unrealized_long_term_gain += account_unrealized_gain;
                        } else {
                            account_unrealized_short_term_gain += account_unrealized_gain;
                        }
                    }
                }

                println!(
                    "    Value: ${}, income: ${}, unrealized short-term cap gain: ${}, unrealized long-term cap gain: ${}",
                    account_current_value.separated_string_with_fixed_place(2),
                    account_income.separated_string_with_fixed_place(2),
                    account_unrealized_short_term_gain.separated_string_with_fixed_place(2),
                    account_unrealized_long_term_gain.separated_string_with_fixed_place(2),
                );

                total_unrealized_short_term_gain += account_unrealized_short_term_gain;
                total_unrealized_long_term_gain += account_unrealized_long_term_gain;
                total_income += account_income;
                total_current_value += account_current_value;
            } else {
                println!("  No lots");
            }
            println!();
        }

        if account_filter.is_some() || summary_only {
            return Ok(());
        }

        let mut disposed_lots = db.disposed_lots();
        disposed_lots.sort_by_key(|lot| lot.when);
        if !disposed_lots.is_empty() {
            println!("Disposed ({} lots):", disposed_lots.len());

            let mut disposed_income = 0.;
            let mut disposed_short_term_cap_gain = 0.;
            let mut disposed_long_term_cap_gain = 0.;
            let mut disposed_value = 0.;

            for (i, disposed_lot) in disposed_lots.iter().enumerate() {
                let mut long_term_cap_gain = false;
                let mut disposed_cap_gain = 0.;
                let msg = format_disposed_lot(
                    disposed_lot,
                    &mut disposed_income,
                    &mut disposed_cap_gain,
                    &mut long_term_cap_gain,
                    &mut disposed_value,
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

                annual_realized_gains
                    .entry(disposed_lot.lot.acquisition.when.year() as usize)
                    .or_default()[disposed_lot.lot.acquisition.when.month0() as usize / 3]
                    .income += disposed_lot.lot.income(disposed_lot.token);

                let mut realized_gain = &mut annual_realized_gains
                    .entry(disposed_lot.when.year() as usize)
                    .or_default()[disposed_lot.when.month0() as usize / 3];

                if long_term_cap_gain {
                    disposed_long_term_cap_gain += disposed_cap_gain;
                    realized_gain.long_term_cap_gain += disposed_cap_gain;
                } else {
                    disposed_short_term_cap_gain += disposed_cap_gain;
                    realized_gain.short_term_cap_gain += disposed_cap_gain;
                }
            }
            println!(
                "    Disposed value: ${} (income: ${}, short-term cap gain: ${}, long-term cap gain: ${})",
                disposed_value.separated_string_with_fixed_place(2),
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

        println!("Realized Gains");
        println!(
            "  Year    | Income           | Short-term cap gain | Long-term cap gain  | Total"
        );
        for (year, quarters) in annual_realized_gains {
            for (q, realized_gain) in quarters.iter().enumerate() {
                if *realized_gain != RealizedGain::default() {
                    println!(
                        "  {} Q{} | ${:15} | ${:18} | ${:18} | ${:18}",
                        year,
                        q + 1,
                        realized_gain.income.separated_string_with_fixed_place(2),
                        realized_gain
                            .short_term_cap_gain
                            .separated_string_with_fixed_place(2),
                        realized_gain
                            .long_term_cap_gain
                            .separated_string_with_fixed_place(2),
                        (realized_gain.income
                            + realized_gain.short_term_cap_gain
                            + realized_gain.long_term_cap_gain)
                            .separated_string_with_fixed_place(2),
                    );
                }
            }
        }
        println!();

        println!("Current Holdings Summary");
        for (held_token, (current_token_price, total_held_amount)) in held_tokens {
            println!(
                "  {: >4}:                {}{} (${} per {}; ${})",
                held_token.to_string(),
                held_token.symbol(),
                held_token
                    .ui_amount(total_held_amount)
                    .separated_string_with_fixed_place(3),
                f64::try_from(current_token_price)
                    .unwrap()
                    .separated_string_with_fixed_place(3),
                held_token,
                f64::try_from(
                    Decimal::from_f64(held_token.ui_amount(total_held_amount)).unwrap()
                        * current_token_price
                )
                .unwrap()
                .separated_string_with_fixed_place(2),
            );
        }
        println!(
            "  Value:               ${}",
            total_current_value.separated_string_with_fixed_place(2)
        );
        println!(
            "  Income:              ${} (realized)",
            total_income.separated_string_with_fixed_place(2)
        );
        println!(
            "  Short-term cap gain: ${} (unrealized)",
            total_unrealized_short_term_gain.separated_string_with_fixed_place(2)
        );
        println!(
            "  Long-term cap gain:  ${} (unrealized)",
            total_unrealized_long_term_gain.separated_string_with_fixed_place(2)
        );
    }
    Ok(())
}

async fn process_account_xls(
    db: &Db,
    outfile: &str,
    filter_by_year: Option<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    use simple_excel_writer::*;

    let mut workbook = Workbook::create(outfile);

    let mut sheet = workbook.create_sheet(&match filter_by_year {
        Some(year) => format!("Disposed in {}", year),
        None => "Disposed".into(),
    });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 15. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });
    sheet.add_column(Column { width: 12. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 10. });
    sheet.add_column(Column { width: 40. });

    let mut disposed_lots = db.disposed_lots();
    disposed_lots.sort_by_key(|lot| lot.when);

    if let Some(year) = filter_by_year {
        // Exclude disposed lots that were neither acquired nor disposed of in the filter year
        disposed_lots.retain(|disposed_lot| {
            (disposed_lot.lot.acquisition.when.year() == year
                && disposed_lot.lot.income(disposed_lot.token) > 0.)
                || disposed_lot.when.year() == year
        })
    }

    workbook.write_sheet(&mut sheet, |sheet_writer| {
        sheet_writer.append_row(row![
            "Token",
            "Amount",
            "Income (USD)",
            "Acq. Date",
            "Acq. Price (USD)",
            "Acquisition Description",
            "Cap Gain (USD)",
            "Cap Gain Type",
            "Sale Date",
            "Sale Price (USD)",
            "Fee (USD)",
            "Sale Description"
        ])?;

        for disposed_lot in disposed_lots {
            let long_term_cap_gain =
                is_long_term_cap_gain(disposed_lot.lot.acquisition.when, Some(disposed_lot.when));

            let mut income = disposed_lot.lot.income(disposed_lot.token);
            if let Some(year) = filter_by_year {
                if disposed_lot.lot.acquisition.when.year() != year {
                    income = 0. // Exclude income from other years
                }
            }

            sheet_writer.append_row(row![
                disposed_lot.token.to_string(),
                disposed_lot.token.ui_amount(disposed_lot.lot.amount),
                income,
                disposed_lot.lot.acquisition.when.to_string(),
                disposed_lot.lot.acquisition.price().to_string(),
                disposed_lot.lot.acquisition.kind.to_string(),
                disposed_lot
                    .lot
                    .cap_gain(disposed_lot.token, disposed_lot.price()),
                if long_term_cap_gain { "Long" } else { "Short" },
                disposed_lot.when.to_string(),
                disposed_lot.price().to_string(),
                disposed_lot
                    .kind
                    .fee()
                    .map(|(amount, currency)| {
                        assert_eq!(currency, "USD");
                        *amount
                    })
                    .unwrap_or_default(),
                disposed_lot.kind.to_string()
            ])?;
        }
        Ok(())
    })?;

    let mut current_holdings_rows = vec![];
    let mut current_holdings_by_year_rows = vec![];

    #[derive(Clone)]
    enum R {
        Number(f64),
        Text(String),
    }

    impl ToCellValue for R {
        fn to_cell_value(&self) -> CellValue {
            match self {
                R::Number(x) => x.to_cell_value(),
                R::Text(x) => x.to_cell_value(),
            }
        }
    }

    for account in db.get_accounts() {
        for lot in account.lots.iter() {
            let row = (
                lot.acquisition.when,
                vec![
                    R::Text(account.token.to_string()),
                    R::Number(account.token.ui_amount(lot.amount)),
                    R::Number(lot.income(account.token)),
                    R::Text(lot.acquisition.when.to_string()),
                    R::Text(lot.acquisition.price().to_string()),
                    R::Text(lot.acquisition.kind.to_string()),
                    R::Text(account.description.clone()),
                    R::Text(account.address.to_string()),
                ],
            );
            current_holdings_rows.push(row.clone());
            if let Some(year) = filter_by_year {
                if lot.acquisition.when.year() == year {
                    current_holdings_by_year_rows.push(row);
                    continue;
                }
            }
        }
    }

    for open_order in db.open_orders(None, Some(OrderSide::Sell)) {
        for lot in open_order.lots.iter() {
            let row = (
                lot.acquisition.when,
                vec![
                    R::Text(open_order.token.to_string()),
                    R::Number(open_order.token.ui_amount(lot.amount)),
                    R::Number(lot.income(open_order.token)),
                    R::Text(lot.acquisition.when.to_string()),
                    R::Text(lot.acquisition.price().to_string()),
                    R::Text(lot.acquisition.kind.to_string()),
                    R::Text(format!(
                        "Open Order: {:?} {}",
                        open_order.exchange, open_order.pair
                    )),
                    R::Text(open_order.deposit_address.to_string()),
                ],
            );
            current_holdings_rows.push(row.clone());
            if let Some(year) = filter_by_year {
                if lot.acquisition.when.year() == year {
                    current_holdings_by_year_rows.push(row);
                    continue;
                }
            }
        }
    }
    current_holdings_rows.sort_by_key(|row| row.0);
    current_holdings_by_year_rows.sort_by_key(|row| row.0);

    let mut write_holdings = |name: String, rows: Vec<(_, Vec<R>)>| {
        let mut sheet = workbook.create_sheet(&name);

        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 15. });
        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 12. });
        sheet.add_column(Column { width: 10. });
        sheet.add_column(Column { width: 40. });
        sheet.add_column(Column { width: 40. });
        sheet.add_column(Column { width: 50. });

        workbook.write_sheet(&mut sheet, |sheet_writer| {
            sheet_writer.append_row(row![
                "Token",
                "Amount",
                "Income (USD)",
                "Acq. Date",
                "Acq. Price (USD)",
                "Acquisition Description",
                "Account Description",
                "Account Address"
            ])?;

            for (_, row) in rows {
                sheet_writer.append_row(Row::from_iter(row.into_iter()))?;
            }

            Ok(())
        })
    };
    if let Some(year) = filter_by_year {
        write_holdings(
            format!("Holdings acquired in {}", year),
            current_holdings_by_year_rows,
        )?;
    }
    write_holdings("All Holdings".to_string(), current_holdings_rows)?;

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
    let token = MaybeToken::SOL(); // TODO: Support merging tokens one day

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

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

    let instructions = if from_account.owner == solana_stake_program::id() {
        solana_stake_program::stake_instruction::merge(
            &into_address,
            &from_address,
            &authority_address,
        )
    } else {
        // TODO: Support merging two system accounts, tokens, and possibly other variations
        return Err(format!("Unsupported `from` account owner: {}", from_account.owner).into());
    };

    println!("Merging {} into {}", from_address, into_address);
    if from_address != authority_address {
        println!("Authority address: {}", authority_address);
    }

    let mut message = Message::new(&instructions, Some(&authority_address));
    message.recent_blockhash = recent_blockhash;
    if rpc_client.get_fee_for_message(&message)? > authority_account.lamports {
        return Err("Insufficient funds for transaction fee".into());
    }

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&signers, recent_blockhash)?;
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    db.record_transfer(
        signature,
        last_valid_block_height,
        from_address,
        None,
        into_address,
        token,
        None,
    )?;

    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_transfer(signature)?;
        return Err("Merge failed".into());
    }
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;
    db.remove_account(from_address, token)?;
    Ok(())
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
    let token = MaybeToken::SOL();

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;
    let fee_calculator = get_deprecated_fee_calculator(rpc_client)?;

    let from_account = rpc_client
        .get_account_with_commitment(&from_address, rpc_client.commitment())?
        .value
        .ok_or_else(|| format!("Account, {}, does not exist", from_address))?;

    let from_tracked_account = db
        .get_account(from_address, token)
        .ok_or_else(|| format!("Account, {}, is not tracked", from_address))?;

    if from_account.lamports < from_tracked_account.last_update_balance {
        return Err(format!(
            "{}: On-chain account balance ({}) less than tracked balance ({})",
            from_address,
            token.ui_amount(from_account.lamports),
            token.ui_amount(from_tracked_account.last_update_balance)
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
        rpc_client_utils::get_stake_authorized(rpc_client, sweep_stake_account.address)?;

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
            token.ui_amount(num_transaction_signatures * fee_calculator.lamports_per_signature)
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

    if sweep_amount < token.amount(1.) {
        let msg = format!(
            "{} has less than {}1 to sweep ({})",
            from_address,
            token.symbol(),
            token.ui_amount(sweep_amount)
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
    println!("Sweep amount: {}", token.ui_amount(sweep_amount));
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

    let mut message = Message::new(&instructions, Some(&from_authority_address));
    message.recent_blockhash = recent_blockhash;
    assert_eq!(
        rpc_client.get_fee_for_message(&message)?,
        num_transaction_signatures * fee_calculator.lamports_per_signature
    );

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    let msg = format!(
        "Sweeping {} from {} into {} (via {})",
        token.ui_amount(sweep_amount),
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
        last_valid_block_height,
        from_address,
        Some(sweep_amount),
        transitory_stake_account.pubkey(),
        token,
        None,
    )?;

    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_transfer(signature)?;
        db.remove_transitory_sweep_stake_address(transitory_stake_account.pubkey())?;
        return Err("Sweep failed".into());
    }
    println!("Confirming sweep: {}", signature);
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;

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
    into_keypair: Option<Keypair>,
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Support splitting two system accounts? Tokens? Otherwise at least error cleanly when it's attempted
    let token = MaybeToken::SOL(); // TODO: Support splitting tokens one day

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let into_keypair = into_keypair.unwrap_or_else(Keypair::new);
    if db.get_account(into_keypair.pubkey(), token).is_some() {
        return Err(format!(
            "Account {} ({}) already exists",
            into_keypair.pubkey(),
            token
        )
        .into());
    }

    let instructions = solana_stake_program::stake_instruction::split(
        &from_address,
        &authority_address,
        amount,
        &into_keypair.pubkey(),
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
        token.ui_amount(amount),
        from_address,
        into_keypair.pubkey(),
    );

    transaction.partial_sign(&signers, recent_blockhash);
    transaction.try_sign(&[&into_keypair], recent_blockhash)?;

    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    let epoch = rpc_client.get_epoch_info()?.epoch;
    db.add_account(TrackedAccount {
        address: into_keypair.pubkey(),
        token,
        description,
        last_update_epoch: epoch.saturating_sub(1),
        last_update_balance: 0,
        lots: vec![],
        no_sync: None,
    })?;
    db.record_transfer(
        signature,
        last_valid_block_height,
        from_address,
        Some(amount),
        into_keypair.pubkey(),
        token,
        lot_numbers,
    )?;

    if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
        db.cancel_transfer(signature)?;
        db.remove_account(into_keypair.pubkey(), MaybeToken::SOL())?;
        return Err("Split failed".into());
    }
    println!("Split confirmed: {}", signature);
    let when = get_signature_date(rpc_client, signature).await?;
    db.confirm_transfer(signature, when)?;
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
            // sync all tokens for the given address...
            let accounts = db.get_account_tokens(address);
            if accounts.is_empty() {
                return Err(format!("{} does not exist", address).into());
            }
            accounts
        }
        None => db.get_accounts(),
    }
    .into_iter()
    .filter(|account| !account.no_sync.unwrap_or_default())
    .collect::<Vec<_>>();

    let current_sol_price = MaybeToken::SOL().get_current_price(rpc_client).await?;

    let addresses: Vec<Pubkey> = accounts
        .iter()
        .map(|TrackedAccount { address, .. }| *address)
        .collect::<Vec<_>>();

    let epoch_info = rpc_client.get_epoch_info()?;
    let stop_epoch = epoch_info.epoch.saturating_sub(1);

    let start_epoch = accounts
        .iter()
        .map(
            |TrackedAccount {
                 last_update_epoch, ..
             }| last_update_epoch,
        )
        .min()
        .unwrap_or(&stop_epoch)
        + 1;

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
                assert!(!account.token.is_token()); // Only SOL accounts can receive inflationary rewards

                account.last_update_balance += inflation_reward.amount;

                let slot = inflation_reward.effective_slot;
                let (when, price) =
                    get_block_date_and_price(rpc_client, slot, account.token).await?;

                let lot = Lot {
                    lot_number: db.next_lot_number(),
                    acquisition: LotAcquistion::new(
                        when,
                        price,
                        LotAcquistionKind::EpochReward { epoch, slot },
                    ),
                    amount: inflation_reward.amount,
                };

                let msg = format!("{}: {}", account.address, account.description);
                notifier.send(&msg).await;
                println!("{}", msg);

                println_lot(
                    account.token,
                    &lot,
                    current_sol_price,
                    None,
                    &mut 0.,
                    &mut 0.,
                    &mut false,
                    &mut 0.,
                    Some(notifier),
                )
                .await;
                account.lots.push(lot);
            }
        }
    }

    // Look for unexpected balance changes (such as transaction and rent rewards)
    for mut account in accounts.iter_mut() {
        account.last_update_epoch = stop_epoch;

        let current_balance = account.token.balance(rpc_client, &account.address)?;
        if current_balance < account.last_update_balance {
            println!(
                "\nWarning: {} ({}) balance is less than expected. Actual: {}{}, expected: {}{}\n",
                account.address,
                account.token,
                account.token.symbol(),
                account.token.ui_amount(current_balance),
                account.token.symbol(),
                account.token.ui_amount(account.last_update_balance)
            );
        } else if current_balance > account.last_update_balance + account.token.amount(0.005) {
            let slot = epoch_info.absolute_slot;
            let current_token_price = account.token.get_current_price(rpc_client).await?;
            let (when, decimal_price) =
                get_block_date_and_price(rpc_client, slot, account.token).await?;
            let amount = current_balance - account.last_update_balance;

            let lot = Lot {
                lot_number: db.next_lot_number(),
                acquisition: LotAcquistion::new(
                    when,
                    decimal_price,
                    LotAcquistionKind::NotAvailable,
                ),
                amount,
            };

            let msg = format!(
                "{} ({}): {}",
                account.address, account.token, account.description
            );
            notifier.send(&msg).await;
            println!("{}", msg);

            println_lot(
                account.token,
                &lot,
                current_token_price,
                None,
                &mut 0.,
                &mut 0.,
                &mut false,
                &mut 0.,
                Some(notifier),
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
    let block_height = rpc_client.get_epoch_info()?.block_height;
    for PendingTransfer {
        signature,
        last_valid_block_height,
        ..
    } in db.pending_transfers()
    {
        let status = rpc_client.get_signature_status_with_commitment_and_history(
            &signature,
            rpc_client.commitment(),
            true,
        )?;
        match status {
            Some(result) => {
                if result.is_ok() {
                    println!("Pending transfer confirmed: {}", signature);
                    let when = get_signature_date(rpc_client, signature).await?;
                    db.confirm_transfer(signature, when)?;
                } else {
                    println!("Pending transfer failed with {:?}: {}", result, signature);
                    db.cancel_transfer(signature)?;
                }
            }
            None => {
                if block_height > last_valid_block_height {
                    println!("Pending transfer cancelled: {}", signature);
                    db.cancel_transfer(signature)?;
                } else {
                    println!(
                        "Transfer pending for at most {} blocks: {}",
                        last_valid_block_height.saturating_sub(block_height),
                        signature
                    );
                }
            }
        }
    }
    Ok(())
}

async fn process_account_sync_sweep(
    db: &mut Db,
    rpc_client: &RpcClient,
    _notifier: &Notifier,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = MaybeToken::SOL();

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

                if let Some(tracked_account) = db.get_account(transitory_sweep_stake_address, token)
                {
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

        let (recent_blockhash, last_valid_block_height) =
            rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

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
            last_valid_block_height,
            transitory_sweep_stake_address,
            None,
            sweep_stake_account_info.address,
            token,
            None,
        )?;

        if !send_transaction_until_expired(rpc_client, &transaction, last_valid_block_height) {
            db.cancel_transfer(signature)?;
            return Err("Merge failed".into());
        }
        let when = get_signature_date(rpc_client, signature).await?;
        db.confirm_transfer(signature, when)?;
        db.remove_transitory_sweep_stake_address(transitory_sweep_stake_address)?;
    }
    Ok(())
}

fn is_valid_token_or_sol(value: String) -> Result<(), String> {
    if value == "SOL" {
        Ok(())
    } else {
        is_valid_token(value)
    }
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
                .about("Get token price")
                .arg(
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("SOL")
                        .help("Token type"),
                )
                .arg(
                    Arg::with_name("when")
                        .value_name("YY/MM/DD")
                        .takes_value(true)
                        .required(false)
                        .validator(|value| naivedate_of(&value).map(|_| ()))
                        .help("Date to fetch the price for [default: current price]"),
                )
        )
        .subcommand(SubCommand::with_name("sync").about("Synchronize with all exchanges and accounts"))
        .subcommand(
            SubCommand::with_name("db")
                .about("Database management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("import")
                        .about("Import another database")
                        .arg(
                            Arg::with_name("other_db_path")
                                .value_name("PATH")
                                .takes_value(true)
                                .help("Path to the database to import"),
                        )
                )
        )
        .subcommand(
            SubCommand::with_name("account")
                .about("Account management")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("add")
                        .about("Register an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
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
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .help("Date acquired (ignored if the --transaction argument is provided) [default: now]"),
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
                                .help("Acquisition price per SOL/token [default: market price on acquisition date]"),
                        )
                        .arg(
                            Arg::with_name("income")
                                .long("income")
                                .takes_value(false)
                                .conflicts_with("transaction")
                                .help("Consider the acquisition value to be subject to income tax [default: post-tax fiat]"),
                        )
                        .arg(
                            Arg::with_name("no_sync")
                                .long("no-sync")
                                .takes_value(false)
                                .help("Never synchronize this account with the on-chain state (advanced; uncommon)"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("dispose")
                        .about("Manually record the disposal of SOL/tokens from an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account that the SOL/tokens was/where disposed from"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("Amount of SOL/tokens that was/where disposed from the account"),
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
                                .validator(|value| naivedate_of(&value).map(|_| ()))
                                .help("Disposal date [default: now]"),
)
                        .arg(
                            Arg::with_name("price")
                                .short("p")
                                .long("price")
                                .value_name("USD")
                                .takes_value(true)
                                .validator(is_parsable::<f64>)
                                .help("Disposal price per SOL/token [default: market price on disposal date]"),
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
                        )
                        .arg(
                            Arg::with_name("account")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .validator(is_valid_pubkey)
                                .help("Limit output to this address"),
                        )
                        .arg(
                            Arg::with_name("summary")
                                .long("summary")
                                .takes_value(false)
                                .help("Limit output to summary line"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("xls")
                        .about("Export an Excel spreadsheet file")
                        .arg(
                            Arg::with_name("outfile")
                                .value_name("FILEPATH")
                                .takes_value(true)
                                .help(".xls file to write"),
                        )
                        .arg(
                            Arg::with_name("year")
                                .long("year")
                                .value_name("YYYY")
                                .takes_value(true)
                                .validator(is_parsable::<usize>)
                                .help("Limit export to realized gains affecting the given year"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("remove")
                        .about("Unregister an account")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("address")
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
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Sweep stake account address"),
                        )
                        .arg(
                            Arg::with_name("stake_authority")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .required(true)
                                .help("Stake authority keypair"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("merge")
                        .about("Merge one stake account into another")
                        .arg(
                            Arg::with_name("from_address")
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
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Source address to sweep from"),
                        )
                        .arg(
                            Arg::with_name("authority")
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
                        .about("Split a stake account")
                        .arg(
                            Arg::with_name("from_address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address of the account to split")
                        )
                        .arg(
                            Arg::with_name("amount")
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
                                .help("Optional authority for the split"),
                        )
                        .arg(
                            Arg::with_name("into_keypair")
                                .long("into")
                                .value_name("KEYPAIR")
                                .takes_value(true)
                                .validator(is_keypair)
                                .help("Optional keypair of the split destination [default: randomly generated]"),
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
                        .about("Synchronize an account address")
                        .arg(
                            Arg::with_name("address")
                                .value_name("ADDRESS")
                                .takes_value(true)
                                .required(false)
                                .validator(is_valid_pubkey)
                                .help("Account to synchronize"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("lot")
                        .about("Account lot management")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("swap")
                                .about("Swap lots in the local database only")
                                .arg(
                                    Arg::with_name("lot_number1")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("First lot number"),
                                )
                                .arg(
                                    Arg::with_name("lot_number2")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Second lot number"),
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("delete")
                                .about("Delete a lot from the local database only. \
                                        Useful if the on-chain state is out of sync with the database")
                                .arg(
                                    Arg::with_name("lot_number")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Lot number to delete. Must not be a disposed lot"),
                                )
                                .arg(
                                    Arg::with_name("confirm")
                                        .long("confirm")
                                        .takes_value(false)
                                        .help("Confirm the operation"),
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("move")
                                .about("Move a lot to a new addresses in the local database only. \
                                        Useful if the on-chain state is out of sync with the database")
                                .arg(
                                    Arg::with_name("lot_number")
                                        .value_name("LOT NUMBER")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_parsable::<usize>)
                                        .help("Lot number to move. Must not be a disposed lot"),
                                )
                                .arg(
                                    Arg::with_name("to_address")
                                        .value_name("RECIPIENT_ADDRESS")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(is_valid_pubkey)
                                        .help("Address to receive the lot"),
                                )
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("tulip")
                .about("tulip.garden")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("deposit")
                        .about("Deposit liquidity")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token)
                                .help("Collateral token type"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("Amount of liquidity to deposit; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("from")
                                .long("from")
                                .value_name("FROM_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_signer)
                                .help("Source account of funds"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("withdraw")
                        .about("Withdraw liquidity")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token)
                                .help("Collateral token type"),
                        )
                        .arg(
                            Arg::with_name("to")
                                .value_name("RECIPIENT_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_signer)
                                .help("Address to receive the withdrawal of funds"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("Amount of liquidity to withdraw; accepts keyword ALL"),
                        )
                )
        );

    for exchange in &exchanges {
        app = app.subcommand(
            SubCommand::with_name(exchange)
                .about("Exchange interactions")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(
                    SubCommand::with_name("balance")
                        .about("Get exchange balance")
                        .arg(
                            Arg::with_name("available_only")
                                .long("available")
                                .takes_value(false)
                                .help("Only display available balance")
                        )
                        .arg(
                            Arg::with_name("total_only")
                                .long("total")
                                .takes_value(false)
                                .conflicts_with("available_only")
                                .help("Only display total balance")
                        )
                        .arg(
                            Arg::with_name("integer")
                                .long("integer")
                                .takes_value(false)
                                .help("Output integer values with no currency symbols")
                        )
                )
                .subcommand(
                    SubCommand::with_name("address")
                        .about("Show deposit address")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .default_value("SOL")
                                .help("Token type"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("market")
                        .about("Display market info for a given trading pair")
                        .arg(
                            Arg::with_name("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .default_value("SOLUSD")
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
                        )
                        .arg(
                            Arg::with_name("hourly")
                                .long("hourly")
                                .takes_value(false)
                                .conflicts_with_all(&["ask", "weighted_24h_average_price"])
                                .help("Display hourly price information for the previous 24 hours"),
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
                                .arg(Arg::with_name("secret").required(true).takes_value(true))
                                .arg(Arg::with_name("subaccount").takes_value(true)),
                        )
                        .subcommand(SubCommand::with_name("show").about("Show API key"))
                        .subcommand(SubCommand::with_name("clear").about("Clear API key")),
                )
                .subcommand(
                    SubCommand::with_name("deposit")
                        .about("Deposit SOL or SPL Tokens")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to deposit; accepts keyword ALL"),
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
                        )
                        .arg(
                            Arg::with_name("if_exchange_balance_less_than")
                                .long("if-exchange-balance-less-than")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without depositing if the \
                                        exchange SOL balance is less than this amount",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("withdraw")
                        .about("Withdraw SOL or SPL Tokens")
                        .arg(
                            Arg::with_name("token")
                                .value_name("SOL or SPL Token")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_token_or_sol)
                                .help("Token type"),
                        )
                        .arg(
                            Arg::with_name("to")
                                .value_name("RECIPIENT_ADDRESS")
                                .takes_value(true)
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Address to receive the withdrawal of funds"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .required(true)
                                .help("The amount to withdraw; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("lot_numbers")
                                .long("lot")
                                .value_name("LOT NUMBER")
                                .takes_value(true)
                                .multiple(true)
                                .validator(is_parsable::<usize>)
                                .help(
                                    "Lot to fund the withdrawal from [default: first in, first out]",
                                ),
                        )
                        .arg(
                            Arg::with_name("code")
                                .long("code")
                                .value_name("CODE")
                                .takes_value(true)
                                .help("2FA withdrawal code"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("cancel")
                        .about("Cancel orders")
                        .arg(
                            Arg::with_name("order_id")
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
                        )
                        .arg(
                            Arg::with_name("side")
                                .long("side")
                                .required(true)
                                .default_value("both")
                                .possible_values(&["both", "buy", "sell"])
                                .help("Restrict to only buy or sell orders")
                        )
                )
                .subcommand(
                    SubCommand::with_name("buy")
                        .about("Place an order to buy SOL")
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .required(true)
                                .help("The amount to buy, in SOL; accepts keyword ALL"),
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
                            Arg::with_name("bid_minus")
                                .long("bid-minus")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .conflicts_with("at")
                                .validator(is_parsable::<f64>)
                                .help("Place a limit order at this amount under the current bid"),
                        )
                        .arg(
                            Arg::with_name("pair")
                                .long("pair")
                                .value_name("TRADING_PAIR")
                                .takes_value(true)
                                .default_value("SOLUSD")
                                .help("Market to place the order in"),
                        )
                        .arg(
                            Arg::with_name("if_balance_exceeds")
                                .long("if-balance-exceeds")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount)
                                .help(
                                    "Exit successfully without placing a buy order if the \
                                       exchange available balance is less than this amount",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("sell")
                        .about("Place an order to sell SOL")
                        .arg(
                            Arg::with_name("amount")
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
                                .default_value("SOLUSD")
                                .help("Market to place the order in"),
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
                            Arg::with_name("if_price_over_basis")
                                .long("if-price-over-basis")
                                .takes_value(false)
                                .help(
                                    "Exit successfully without placing a sell order if the \
                                       order price would be less than the basis",
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
                .subcommand(
                    SubCommand::with_name("pending-deposits")
                        .about("Display pending deposits")
                        .arg(
                            Arg::with_name("quiet")
                                .long("quiet")
                                .takes_value(false)
                                .help(
                                    "Disable output and exit with a non-zero status code \
                                        if any deposits are pending"
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("pending-withdrawals")
                        .about("Display pending withdrawals")
                        .arg(
                            Arg::with_name("quiet")
                                .long("quiet")
                                .takes_value(false)
                                .help(
                                    "Disable output and exit with a non-zero status code \
                                        if any withdrawals are pending"
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("lend")
                        .about("Make a lending offer")
                        .arg(
                            Arg::with_name("coin")
                                .value_name("COIN")
                                .takes_value(true)
                                .required(true)
                                .help("The coin to lend"),
                        )
                        .arg(
                            Arg::with_name("amount")
                                .value_name("AMOUNT")
                                .takes_value(true)
                                .validator(is_amount_or_all)
                                .help("The amount to lend; accepts keyword ALL"),
                        )
                        .arg(
                            Arg::with_name("available")
                                .short("a")
                                .long("available")
                                .requires("amount")
                                .takes_value(false)
                                .help("Invert AMOUNT to mean, the amount to keep available and lend the rest"),
                        )
                )
                .subcommand(
                    SubCommand::with_name("lending-history")
                        .about("Display lending history")
                        .setting(AppSettings::SubcommandRequiredElseHelp)
                        .setting(AppSettings::InferSubcommands)
                        .subcommand(
                            SubCommand::with_name("range")
                                .about("Display lending history for the given date range")
                                .arg(
                                    Arg::with_name("start_date")
                                        .value_name("YY/MM/DD")
                                        .takes_value(true)
                                        .required(true)
                                        .validator(|value| naivedate_of(&value).map(|_| ()))
                                        .help("Start date, inclusive")
                                )
                                .arg(
                                    Arg::with_name("end_date")
                                        .value_name("YY/MM/DD")
                                        .takes_value(true)
                                        .required(true)
                                        .default_value(&default_when)
                                        .validator(|value| naivedate_of(&value).map(|_| ()))
                                        .help("End date, inclusive")
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("previous")
                                .about("Display lending history for previous days")
                                .arg(
                                    Arg::with_name("days")
                                        .value_name("DAYS")
                                        .default_value("1")
                                        .validator(is_parsable::<usize>)
                                        .help("Number of days, including today")
                                )
                        )
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

    if !db_path.exists() {
        fs::create_dir_all(&db_path)?;
    }

    let mut db_fd_lock = fd_lock::RwLock::new(fs::File::open(&db_path).unwrap());
    let _db_write_lock = loop {
        match db_fd_lock.try_write() {
            Ok(lock) => break lock,
            Err(err) => {
                eprintln!(
                    "Unable to lock database directory: {}: {}",
                    db_path.display(),
                    err
                );
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut db = db::new(&db_path).unwrap_or_else(|err| {
        eprintln!("Failed to open {}: {}", db_path.display(), err);
        exit(1)
    });

    match app_matches.subcommand() {
        ("price", Some(arg_matches)) => {
            let when = value_t!(arg_matches, "when", String)
                .map(|s| naivedate_of(&s).unwrap())
                .ok();
            let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());

            let (price, verbose_msg) = if let Some(when) = when {
                (
                    token.get_historical_price(&rpc_client, when).await?,
                    format!("Historical {} price on {}", token, when),
                )
            } else {
                (
                    token.get_current_price(&rpc_client).await?,
                    format!("Current {} price", token),
                )
            };

            if verbose {
                println!("{}: ${:.2}", verbose_msg, price);

                if let Some(liquidity_token) = token.liquidity_token() {
                    let rate = token.get_current_liquidity_token_rate(&rpc_client).await?;
                    println!(
                        "Liquidity token: {} (rate: {}, inv: {})",
                        liquidity_token,
                        rate,
                        Decimal::from_usize(1).unwrap() / rate
                    );
                }
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
            process_sync_swaps(&mut db, &rpc_client, &notifier).await?;
            process_account_sync(&mut db, &rpc_client, None, &notifier).await?;
        }
        ("db", Some(db_matches)) => match db_matches.subcommand() {
            ("import", Some(arg_matches)) => {
                let other_db_path = value_t_or_exit!(arg_matches, "other_db_path", PathBuf);

                let mut other_db_fd_lock =
                    fd_lock::RwLock::new(fs::File::open(&other_db_path).unwrap());
                let _other_db_write_lock = loop {
                    match other_db_fd_lock.try_write() {
                        Ok(lock) => break lock,
                        Err(err) => {
                            eprintln!(
                                "Unable to lock database directory: {}: {}",
                                other_db_path.display(),
                                err
                            );
                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                };

                let other_db = db::new(&other_db_path).unwrap_or_else(|err| {
                    eprintln!("Failed to open {}: {}", other_db_path.display(), err);
                    exit(1)
                });

                println!("Importing {}", other_db_path.display());
                db.import_db(other_db)?;
            }
            _ => unreachable!(),
        },
        ("account", Some(account_matches)) => match account_matches.subcommand() {
            ("lot", Some(lot_matches)) => match lot_matches.subcommand() {
                ("swap", Some(arg_matches)) => {
                    let lot_number1 = value_t_or_exit!(arg_matches, "lot_number1", usize);
                    let lot_number2 = value_t_or_exit!(arg_matches, "lot_number2", usize);
                    println!("Swapping lots {} and {}", lot_number1, lot_number2);
                    db.swap_lots(lot_number1, lot_number2)?;
                }
                ("move", Some(arg_matches)) => {
                    let lot_number = value_t_or_exit!(arg_matches, "lot_number", usize);
                    let to_address =
                        pubkey_of_signer(arg_matches, "to_address", &mut wallet_manager)?
                            .expect("to");
                    db.move_lot(lot_number, to_address)?;
                }
                ("delete", Some(arg_matches)) => {
                    let lot_number = value_t_or_exit!(arg_matches, "lot_number", usize);
                    let confirm = arg_matches.is_present("confirm");

                    if !confirm {
                        println!("Add --confirm to remove lot {}", lot_number);
                        return Ok(());
                    }
                    db.delete_lot(lot_number)?;
                }
                _ => unreachable!(),
            },
            ("add", Some(arg_matches)) => {
                let price = value_t!(arg_matches, "price", f64).ok();
                let income = arg_matches.is_present("income");
                let when = value_t!(arg_matches, "when", String)
                    .map(|s| naivedate_of(&s).unwrap())
                    .ok();
                let signature = value_t!(arg_matches, "transaction", Signature).ok();
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = value_t!(arg_matches, "token", Token).ok();
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_default();
                let no_sync = arg_matches.is_present("no_sync");

                process_account_add(
                    &mut db,
                    &rpc_client,
                    address,
                    token.into(),
                    description,
                    when,
                    price,
                    income,
                    signature,
                    no_sync,
                )
                .await?;
                process_account_sync(&mut db, &rpc_client, Some(address), &notifier).await?;
            }
            ("dispose", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = value_t!(arg_matches, "token", Token).ok();
                let amount = value_t_or_exit!(arg_matches, "amount", f64);
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_default();
                let when = value_t!(arg_matches, "when", String)
                    .map(|s| naivedate_of(&s).unwrap())
                    .ok();
                let price = value_t!(arg_matches, "price", f64).ok();

                process_account_dispose(
                    &mut db,
                    &rpc_client,
                    address,
                    token.into(),
                    amount,
                    description,
                    when,
                    price,
                )
                .await?;
            }
            ("ls", Some(arg_matches)) => {
                let all = arg_matches.is_present("all");
                let summary = arg_matches.is_present("summary");
                let account_filter = pubkey_of(arg_matches, "account");
                process_account_list(&db, &rpc_client, account_filter, all, summary, &notifier)
                    .await?;
            }
            ("xls", Some(arg_matches)) => {
                let outfile = value_t_or_exit!(arg_matches, "outfile", String);
                let filter_by_year = value_t!(arg_matches, "year", i32).ok();
                process_account_xls(&db, &outfile, filter_by_year).await?;
            }
            ("remove", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address").unwrap();
                let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                let confirm = arg_matches.is_present("confirm");

                let account = db
                    .get_account(address, token)
                    .ok_or_else(|| format!("Account {} ({}) does not exist", address, token))?;
                if !account.lots.is_empty() {
                    return Err(format!("Account {} ({}) is not empty", address, token).into());
                }

                if !confirm {
                    println!("Add --confirm to remove {} ({})", address, token);
                    return Ok(());
                }

                db.remove_account(address, token)?;
                println!("Removed {} ({})", address, token);
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
                    MaybeToken::SOL().amount(value_t!(arg_matches, "retain", f64).unwrap_or(0.));
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
                let amount = MaybeToken::SOL().amount(value_t_or_exit!(arg_matches, "amount", f64));
                let description = value_t!(arg_matches, "description", String)
                    .ok()
                    .unwrap_or_else(|| format!("Split at {}", Local::now()));
                let lot_numbers = values_t!(arg_matches, "lot_numbers", usize)
                    .ok()
                    .map(|x| x.into_iter().collect());
                let into_keypair = keypair_of(arg_matches, "into_keypair");

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
                    into_keypair,
                )
                .await?;
            }
            ("sync", Some(arg_matches)) => {
                let address = pubkey_of(arg_matches, "address");
                process_account_sync(&mut db, &rpc_client, address, &notifier).await?;
            }
            _ => unreachable!(),
        },
        ("tulip", Some(tulip_matches)) => match tulip_matches.subcommand() {
            ("deposit", Some(arg_matches)) => {
                let token = value_t_or_exit!(arg_matches, "token", Token);
                let amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => None,
                    amount => Some(token.amount(amount.parse().unwrap())),
                };
                let (signer, address) = signer_of(arg_matches, "from", &mut wallet_manager)
                    .map_err(|err| {
                        format!(
                            "Authority not found, consider using the `--by` argument): {}",
                            err
                        )
                    })?;
                let address = address.expect("address");
                let signer = signer.expect("signer");

                process_tulip_deposit(&mut db, &rpc_client, token, amount, address, vec![signer])
                    .await?;
                process_sync_swaps(&mut db, &rpc_client, &notifier).await?;
            }
            ("withdraw", Some(arg_matches)) => {
                let token = value_t_or_exit!(arg_matches, "token", Token);
                let amount = match arg_matches.value_of("amount").unwrap() {
                    "ALL" => None,
                    amount => Some(token.amount(amount.parse().unwrap())),
                };
                let (signer, address) =
                    signer_of(arg_matches, "to", &mut wallet_manager).map_err(|err| {
                        format!(
                            "Authority not found, consider using the `--by` argument): {}",
                            err
                        )
                    })?;
                let address = address.expect("address");
                let signer = signer.expect("signer");

                process_tulip_withdraw(&mut db, &rpc_client, token, amount, address, vec![signer])
                    .await?;
                process_sync_swaps(&mut db, &rpc_client, &notifier).await?;
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
                ("address", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let deposit_address = exchange_client()?.deposit_address(token).await?;
                    println!("{} deposit address: {}", token, deposit_address);
                }
                ("pending-deposits", Some(arg_matches)) => {
                    let quiet = arg_matches.is_present("quiet");

                    let pending_deposits = db.pending_deposits(Some(exchange));
                    if quiet {
                        if !pending_deposits.is_empty() {
                            return Err(
                                format!("{} deposits pending", pending_deposits.len()).into()
                            );
                        }
                    } else {
                        for pending_deposit in pending_deposits {
                            let token = pending_deposit.transfer.token;
                            println!(
                                "{} deposit pending: {}{} (signature: {})",
                                token,
                                token.symbol(),
                                token.ui_amount(pending_deposit.amount),
                                pending_deposit.transfer.signature
                            );
                        }
                    }
                }
                ("pending-withdrawals", Some(arg_matches)) => {
                    let quiet = arg_matches.is_present("quiet");

                    let pending_withdrawals = db.pending_withdrawals(Some(exchange));
                    if quiet {
                        if !pending_withdrawals.is_empty() {
                            return Err(format!(
                                "{} withdrawals pending",
                                pending_withdrawals.len()
                            )
                            .into());
                        }
                    } else {
                        for pending_withdrawals in pending_withdrawals {
                            let token = pending_withdrawals.token;
                            println!(
                                "{} withdrawal pending: {}{} (destination: {})",
                                token,
                                token.symbol(),
                                token.ui_amount(pending_withdrawals.amount),
                                pending_withdrawals.to_address,
                            );
                        }
                    }
                }
                ("balance", Some(arg_matches)) => {
                    let available_only = arg_matches.is_present("available_only");
                    let total_only = arg_matches.is_present("total_only");
                    let integer = arg_matches.is_present("integer");

                    let balances = exchange_client()?.balances().await?;

                    if !(available_only || total_only) {
                        println!("                   Total            Available")
                    }

                    let balance = balances.get("SOL").cloned().unwrap_or_default();

                    let print_balance = |coin: &str, symbol: &str, balance: &ExchangeBalance| {
                        let symbol = if integer { "" } else { symbol };
                        let available_balance = format!(
                            "{}{}",
                            symbol,
                            if integer {
                                balance.available.floor().to_string()
                            } else {
                                balance.available.separated_string_with_fixed_place(8)
                            }
                        );

                        let total_balance = format!(
                            "{}{}",
                            symbol,
                            if integer {
                                balance.total.floor().to_string()
                            } else {
                                balance.total.separated_string_with_fixed_place(8)
                            }
                        );

                        if available_only {
                            println!("{} {}", coin, available_balance);
                        } else if total_only {
                            println!("{} {}", coin, total_balance);
                        } else {
                            println!("{} {:>20} {:>20}", coin, total_balance, available_balance);
                        }
                    };

                    print_balance("SOL", "◎", &balance);
                    for coin in crate::exchange::USD_COINS {
                        if let Some(balance) = balances.get(*coin) {
                            if balance.total > 0. {
                                print_balance(coin, "$", balance);
                            }
                        }
                    }
                }
                ("market", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let format = if arg_matches.is_present("weighted_24h_average_price") {
                        MarketInfoFormat::Weighted24hAveragePrice
                    } else if arg_matches.is_present("hourly") {
                        MarketInfoFormat::Hourly
                    } else if arg_matches.is_present("ask") {
                        MarketInfoFormat::Ask
                    } else {
                        MarketInfoFormat::All
                    };
                    exchange_client()?.print_market_info(&pair, format).await?;
                }
                ("deposit", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(token.amount(amount.parse().unwrap())),
                    };
                    let if_source_balance_exceeds =
                        value_t!(arg_matches, "if_source_balance_exceeds", f64)
                            .ok()
                            .map(|x| token.amount(x));
                    let if_exchange_balance_less_than =
                        value_t!(arg_matches, "if_exchange_balance_less_than", f64)
                            .ok()
                            .map(|x| token.amount(x));
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
                    let deposit_address = exchange_client.deposit_address(token).await?;
                    add_exchange_deposit_address_to_db(
                        &mut db,
                        exchange,
                        token,
                        deposit_address,
                        &rpc_client,
                    )?;

                    process_exchange_deposit(
                        &mut db,
                        &rpc_client,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        deposit_address,
                        amount,
                        from_address,
                        if_source_balance_exceeds,
                        if_exchange_balance_less_than,
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
                ("withdraw", Some(arg_matches)) => {
                    let token = MaybeToken::from(value_t!(arg_matches, "token", Token).ok());
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(token.amount(amount.parse().unwrap())),
                    };
                    let to_address =
                        pubkey_of_signer(arg_matches, "to", &mut wallet_manager)?.expect("to");

                    let lot_numbers = values_t!(arg_matches, "lot_numbers", usize)
                        .ok()
                        .map(|x| x.into_iter().collect());

                    let withdrawal_password = None; // TODO: Support reading password from stdin
                    let withdrawal_code = value_t!(arg_matches, "code", String).ok();

                    let exchange_client = exchange_client()?;
                    let deposit_address = exchange_client.deposit_address(token).await?;
                    add_exchange_deposit_address_to_db(
                        &mut db,
                        exchange,
                        token,
                        deposit_address,
                        &rpc_client,
                    )?;

                    process_exchange_withdraw(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        deposit_address,
                        amount,
                        to_address,
                        lot_numbers,
                        withdrawal_password,
                        withdrawal_code,
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

                    let max_create_time = value_t!(arg_matches, "age", i64).ok().and_then(|age| {
                        Utc::now().checked_sub_signed(chrono::Duration::hours(age))
                    });

                    let side = value_t_or_exit!(arg_matches, "side", String);
                    let side = match side.as_str() {
                        "buy" => Some(OrderSide::Buy),
                        "sell" => Some(OrderSide::Sell),
                        "both" => None,
                        _ => unreachable!(),
                    };

                    let exchange_client = exchange_client()?;
                    process_exchange_cancel(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        order_ids,
                        max_create_time,
                        side,
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
                ("buy", Some(arg_matches)) => {
                    let token = MaybeToken::SOL();
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let amount = match arg_matches.value_of("amount").unwrap() {
                        "ALL" => None,
                        amount => Some(str::parse::<f64>(amount).unwrap()),
                    };

                    let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64).ok();

                    let price = if let Ok(price) = value_t!(arg_matches, "at", f64) {
                        LimitOrderPrice::At(price)
                    } else if let Ok(bid_minus) = value_t!(arg_matches, "bid_minus", f64) {
                        LimitOrderPrice::AmountUnderBid(bid_minus)
                    } else {
                        return Err("--at or --bid-minus argument required".into());
                    };

                    let exchange_client = exchange_client()?;
                    process_exchange_buy(
                        &mut db,
                        exchange,
                        exchange_client.as_ref(),
                        token,
                        pair,
                        amount,
                        price,
                        if_balance_exceeds,
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
                ("sell", Some(arg_matches)) => {
                    let token = MaybeToken::SOL();
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    let amount = value_t_or_exit!(arg_matches, "amount", f64);
                    let if_balance_exceeds = value_t!(arg_matches, "if_balance_exceeds", f64)
                        .ok()
                        .map(|x| token.amount(x));
                    let if_price_over = value_t!(arg_matches, "if_price_over", f64).ok();
                    let if_price_over_basis = arg_matches.is_present("if_price_over_basis");
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
                        token,
                        pair,
                        amount,
                        price,
                        if_balance_exceeds,
                        if_price_over,
                        if_price_over_basis,
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
                ("lend", Some(arg_matches)) => {
                    let coin = value_t_or_exit!(arg_matches, "coin", String);
                    let amount = arg_matches.value_of("amount");
                    let available = arg_matches.is_present("available");

                    let exchange_client = exchange_client()?;

                    let lending_info = exchange_client
                        .get_lending_info(&coin)
                        .await?
                        .ok_or_else(|| format!("Lending not available for {}", coin))?;

                    if let Some(amount) = amount {
                        let amount = if available {
                            if amount == "ALL" {
                                0.
                            } else {
                                lending_info.lendable - amount.parse::<f64>().unwrap()
                            }
                        } else if amount == "ALL" {
                            lending_info.lendable
                        } else {
                            amount.parse::<f64>().unwrap()
                        }
                        .floor()
                        .max(0.);

                        let additional_amount = amount - lending_info.offered;
                        if additional_amount.abs() > f64::EPSILON {
                            let msg = format!(
                                "Lending offer: {} {} (change: {}) at {:.1}%",
                                amount.separated_string_with_fixed_place(2),
                                coin,
                                additional_amount.separated_string_with_fixed_place(2),
                                lending_info.estimate_rate,
                            );
                            exchange_client.submit_lending_offer(&coin, amount).await?;
                            println!("{}", msg);
                            notifier.send(&format!("{:?}: {}", exchange, msg)).await;
                        } else {
                            println!(
                                "Lending offer unchanged: {}",
                                lending_info.offered.separated_string_with_fixed_place(2)
                            );
                        }
                    } else {
                        println!(
                            "Available:     {}",
                            lending_info.lendable.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Current offer: {}",
                            lending_info.offered.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Locked:        {}",
                            lending_info.locked.separated_string_with_fixed_place(2),
                        );
                        println!(
                            "Current rate:  {:.1}% (estimated)",
                            lending_info.estimate_rate
                        );
                        println!("Previous rate: {:.1}%", lending_info.previous_rate);
                    }
                }
                ("lending-history", Some(lending_history_matches)) => {
                    let exchange_client = exchange_client()?;
                    let lending_history = match lending_history_matches.subcommand() {
                        ("range", Some(arg_matches)) => {
                            let start_date =
                                naivedate_of(&value_t_or_exit!(arg_matches, "start_date", String))
                                    .unwrap();
                            let end_date =
                                naivedate_of(&value_t_or_exit!(arg_matches, "end_date", String))
                                    .unwrap();
                            exchange_client.get_lending_history(LendingHistory::Range {
                                start_date,
                                end_date,
                            })
                        }
                        ("previous", Some(arg_matches)) => {
                            let days = value_t_or_exit!(arg_matches, "days", usize);
                            exchange_client.get_lending_history(LendingHistory::Previous { days })
                        }
                        _ => unreachable!(),
                    }
                    .await?;

                    for (coin, amount) in lending_history.iter() {
                        println!("{}: {}", coin, amount.separated_string_with_fixed_place(2));
                    }
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
                        Some(ExchangeCredentials {
                            api_key,
                            subaccount,
                            ..
                        }) => {
                            println!("API Key: {}", api_key);
                            println!("Secret: ********");
                            if let Some(subaccount) = subaccount {
                                println!("Subaccount: {}", subaccount);
                            }
                        }
                        None => {
                            println!("No API key set for {:?}", exchange);
                        }
                    },
                    ("set", Some(arg_matches)) => {
                        let api_key = value_t_or_exit!(arg_matches, "api_key", String);
                        let secret = value_t_or_exit!(arg_matches, "secret", String);
                        let subaccount = value_t!(arg_matches, "subaccount", String).ok();
                        db.set_exchange_credentials(
                            exchange,
                            ExchangeCredentials {
                                api_key,
                                secret,
                                subaccount,
                            },
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
