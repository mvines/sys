mod binance_exchange;
mod coin_gecko;
mod db;
mod exchange;
mod ftx_exchange;
mod notifier;

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
        signers::Signers,
        system_instruction, system_program,
        transaction::Transaction,
    },
    std::{path::PathBuf, process::exit, str::FromStr},
};

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
            assert!(
                (deposit_info.amount - pending_deposit.amount).abs() < f64::EPSILON,
                "Deposit amount mismatch!"
            );
            let msg = format!(
                "◎{} deposit successful ({})",
                pending_deposit.amount, pending_deposit.tx_id
            );
            println!("{}", msg);
            notifier.send(&format!("{:?}: {}", exchange, msg)).await;

            db.confirm_deposit(&pending_deposit)?;
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

    if lamports == 0 || from_account.lamports < lamports + minimum_balance {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let default_db_path = "sell-your-sol";
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
        .subcommand(SubCommand::with_name("sync").about("Synchronize with all exchanges"));

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
        "https://api.mainnet-beta.solana.com".to_string(),
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
            let market_data = coin_gecko::get_coin_history(when).await?;
            println!("Price on {}: ${:.2}", when, market_data.current_price.usd);
        }
        ("sync", Some(_arg_matches)) => {
            for (exchange, exchange_credentials) in db.get_configured_exchanges() {
                println!("Synchronizing {:?}...", exchange);
                let exchange_client = exchange_client_new(exchange, exchange_credentials)?;
                process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                    .await?
            }
        }
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
                        .await?
                }
                ("sync", Some(_arg_matches)) => {
                    let exchange_client = exchange_client()?;
                    process_sync_exchange(&mut db, exchange, exchange_client.as_ref(), &notifier)
                        .await?
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
