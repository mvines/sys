use {
    chrono::prelude::*,
    clap::{crate_description, crate_name, value_t_or_exit, App, AppSettings, Arg, SubCommand},
    db::*,
    exchange::*,
    serde::Deserialize,
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
    tokio_binance::AccountClient,
};
mod coin_gecko;
mod db;
mod exchange;

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

async fn process_sync(db: &mut Db) -> Result<(), Box<dyn std::error::Error>> {
    for exchange in db.get_configured_exchanges() {
        println!("Synchronizing with {:?}...", exchange);
        let deposit_history = {
            let account_client = exchange_account_client(exchange, db).await?;
            let withdrawal_client = account_client.to_withdraw_client();
            withdrawal_client
                .get_deposit_history()
                .with_asset("SOL")
                .json::<DepositHistory>()
                .await?
                .deposit_list
        };

        for pending_deposit in db.pending_exchange_deposits(exchange) {
            if let Some(deposit_record) = deposit_history
                .iter()
                .find(|deposit_record| deposit_record.tx_id == pending_deposit.tx_id)
            {
                if deposit_record.success() {
                    println!(
                        "◎{} deposit successful ({})",
                        pending_deposit.amount, pending_deposit.tx_id
                    );
                    db.confirm_exchange_deposit(&pending_deposit)?;
                    // TODO: add notifier...
                    continue;
                } else {
                    println!(
                        "◎{} deposit pending (visible on {:?}) ({})",
                        pending_deposit.amount, exchange, pending_deposit.tx_id
                    );
                }
            } else {
                println!(
                    "◎{} deposit pending (not visible on {:?} yet) ({})",
                    pending_deposit.amount, exchange, pending_deposit.tx_id
                );
            }
        }
    }

    Ok(())
}

async fn process_exchange_balance(
    db: &mut Db,
    exchange: Exchange,
) -> Result<(), Box<dyn std::error::Error>> {
    let account_client = exchange_account_client(exchange, &db).await?;
    let account_info = account_client.get_account().json::<AccountInfo>().await?;
    let sol_balance = account_info
        .balances
        .iter()
        .find(|b| b.asset == "SOL")
        .expect("SOL");
    println!(
        "Available: ◎{}\nIn order:  ◎{}",
        sol_balance.free, sol_balance.locked
    );
    Ok(())
}

async fn process_exchange_deposit<T: Signers>(
    db: &mut Db,
    rpc_client: RpcClient,
    exchange: Exchange,
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

    let account_client = exchange_account_client(exchange, &db).await?;
    if !account_client
        .get_account()
        .json::<AccountInfo>()
        .await?
        .can_deposit
    {
        return Err(format!("{:?} deposits not available", exchange).into());
    }
    let withdrawal_client = account_client.to_withdraw_client();

    let deposit_address = withdrawal_client
        .get_deposit_address("SOL")
        .with_status(true)
        .json::<DepositAddress>()
        .await?
        .address
        .parse::<Pubkey>()?;

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
        return Err(format!("Unsupport from account owner: {}", from_account.owner).into());
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

    //transaction.try_sign(&vec![authority_signer], recent_blockhash)?;
    transaction.try_sign(&signers, recent_blockhash)?;
    println!("Transaction signature: {}", transaction.signatures[0]);

    let pending_deposit = PendingDeposit {
        tx_id: transaction.signatures[0].to_string(),
        exchange,
        amount,
    };

    db.record_exchange_deposit(pending_deposit.clone())?;

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
                db.cancel_exchange_deposit(&pending_deposit)
                    .expect("cancel_exchange_deposit");
                return Err("Deposit failed: {}".into());
            }
            Ok(_) => {
                println!("Blockhash has not yet expired, retrying transaction...");
            }
        };
    }

    process_sync(db).await
}

async fn process_exchange_market(
    db: &mut Db,
    exchange: Exchange,
    pair: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let account_client = exchange_account_client(exchange, &db).await?;
    let market_data_client = account_client.to_market_data_client();

    let average_price = market_data_client
        .get_average_price(&pair)
        .json::<AveragePrice>()
        .await?;

    let ticker_price = market_data_client
        .get_24hr_ticker_price()
        .with_symbol(&pair)
        .json::<TickerPrice>()
        .await?;

    println!("Symbol: {}", ticker_price.symbol);
    println!(
        "Ask: ${}, Bid: ${}, High: ${}, Low: ${}, ",
        ticker_price.ask_price,
        ticker_price.bid_price,
        ticker_price.high_price,
        ticker_price.low_price
    );
    println!(
        "Last {} minute average: ${}",
        average_price.mins, average_price.price
    );
    println!(
        "Last 24h change: ${} ({}%), Weighted average price: ${}",
        ticker_price.price_change,
        ticker_price.price_change_percent,
        ticker_price.weighted_avg_price
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let default_db_path = "sell-your-sol";
    let default_when = {
        let today = Utc::now().date();
        format!("{}/{}/{}", today.year(), today.month(), today.day())
    };
    let exchanges = ["binance", "binanceus"];

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
        .subcommand(SubCommand::with_name("sync").about("Synchronize with exchanges"));

    for exchange in &exchanges {
        app = app.subcommand(
            SubCommand::with_name(exchange)
                .about("Exchange interactions")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .setting(AppSettings::InferSubcommands)
                .subcommand(SubCommand::with_name("balance").about("Get SOL balance"))
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
                                .arg(
                                    Arg::with_name("secret_key")
                                        .required(true)
                                        .takes_value(true),
                                ),
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
                ),
        );
    }

    let app_matches = app.get_matches();
    let db_path = value_t_or_exit!(app_matches, "db_path", PathBuf);
    let rpc_client = RpcClient::new_with_commitment(
        "https://api.mainnet-beta.solana.com".to_string(),
        CommitmentConfig::confirmed(),
    );
    let mut wallet_manager = None;

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
            process_sync(&mut db).await?;
        }
        (exchange, Some(exchange_matches)) => {
            assert!(exchanges.contains(&exchange), "Bug!");

            let exchange = Exchange::from_str(exchange)?;
            match exchange_matches.subcommand() {
                ("balance", Some(_arg_matches)) => {
                    process_exchange_balance(&mut db, exchange).await?;
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

                    process_exchange_deposit(
                        &mut db,
                        rpc_client,
                        exchange,
                        amount,
                        from_address,
                        authority_address,
                        vec![authority_signer],
                    )
                    .await?;
                }
                ("market", Some(arg_matches)) => {
                    let pair = value_t_or_exit!(arg_matches, "pair", String);
                    process_exchange_market(&mut db, exchange, pair).await?;
                }
                ("api", Some(api_matches)) => match api_matches.subcommand() {
                    ("show", Some(_arg_matches)) => match db.get_exchange_credentials(exchange) {
                        Some(ExchangeCredentials::BinanceApi {
                            api_key,
                            secret_key: _,
                        }) => {
                            println!("API Key: {}", api_key);
                            println!("Secret Key: ********");
                        }
                        None => {
                            println!("No API key set for {:?}", exchange);
                        }
                    },
                    ("set", Some(arg_matches)) => {
                        let api_key = value_t_or_exit!(arg_matches, "api_key", String);
                        let secret_key = value_t_or_exit!(arg_matches, "secret_key", String);
                        db.set_exchange_credentials(
                            exchange,
                            ExchangeCredentials::BinanceApi {
                                api_key,
                                secret_key,
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

async fn exchange_account_client(
    exchange: Exchange,
    db: &Db,
) -> Result<AccountClient, Box<dyn std::error::Error>> {
    let ExchangeCredentials::BinanceApi {
        api_key,
        secret_key,
    } = db
        .get_exchange_credentials(exchange)
        .ok_or_else(|| format!("No API key set for {:?}", exchange))?;

    let url = match exchange {
        Exchange::Binance => BINANCE_URL,
        Exchange::BinanceUs => BINANCE_US_URL,
    };

    Ok(AccountClient::connect(api_key, secret_key, url)?)
}

#[derive(Debug, Deserialize)]
struct AveragePrice {
    mins: usize,
    price: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerPrice {
    ask_price: String,
    bid_price: String,
    high_price: String,
    low_price: String,
    price_change: String,
    price_change_percent: String,
    symbol: String,
    volume: String,
    quote_volume: String,
    weighted_avg_price: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountInfoBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    account_type: String,
    balances: Vec<AccountInfoBalance>,
    can_deposit: bool,
    can_trade: bool,
    can_withdraw: bool,
}

#[derive(Debug, Deserialize)]
struct DepositAddress {
    address: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepositRecord {
    address: String,
    asset: String,
    amount: f64,
    tx_id: String,
    status: usize, // 0 = pending, 1 = success, 6 = credited but cannot withdraw
}

impl DepositRecord {
    fn success(&self) -> bool {
        self.status == 1
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepositHistory {
    deposit_list: Vec<DepositRecord>,
    success: bool,
}

/*
async fn binance_order_test() -> Result<(), Box<dyn std::error::Error>> {
    let client = AccountClient::connect(api_key, secret_key, BINANCE_URL)?;
    let response = client
        .get_open_orders()
        .with_symbol("SOLUSDT")
        .json::<serde_json::Value>()
        .await?;
    println!("get_open_orders: {:?}", response);
    // https://docs.rs/tokio-binance/1.0.0/tokio_binance/struct.AccountClient.html#method.place_limit_order

    let withdrawal_client = client.to_withdraw_client();
    //    let client = WithdrawalClient::connect(api_key, secret_key, BINANCE_US_URL)?;

    let response = withdrawal_client
        .get_deposit_address("SOL")
        .with_status(true)
        .json::<serde_json::Value>()
        .await?;
    println!("get_deexchange_clientposit_address: {:?}", response);

    /*
    let response = withdrawal_client
        .get_order("SOLUSDT", ID::ClientOId("<uuid>"))
        // optional: processing time for request; default is 5000, can't be above 60000.
        .with_recv_window(8000)
        //
        .json::<serde_json::Value>()
        .await?;
        */

     Ok(())
}
*/
