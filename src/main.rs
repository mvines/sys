/*

  sys binance deposit <amount> from <source-address> [by <authority>] (cli --keypair is default authority)
       -- support system/vote/stake accounts
       -- need ledger support
       -- eventually must be an enrolled account

  ====

  sys binance sell <amount> with <pair> at [limit order for +x% over spot or fixed amount, default +0%]
        -- no market orders
        -- can't sell for less than the current price

          - in `sys sync`, check for completed sells (need new Db structure for this...)
              - need to manage partial fills
 =====
   atomic DB updates?

*/
use {
    chrono::prelude::*,
    clap::{crate_description, crate_name, value_t_or_exit, App, AppSettings, Arg, SubCommand},
    db::*,
    exchange::*,
    serde::Deserialize,
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

async fn sync(db: &mut Db) -> Result<(), Box<dyn std::error::Error>> {
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
                        " ◎{} deposit successful ({})",
                        pending_deposit.amount, pending_deposit.tx_id
                    );
                    db.confirm_exchange_deposit(&pending_deposit)?;
                    // TODO: add notifier...
                    continue;
                }
            }
            println!(
                "  ◎{} deposit pending ({})",
                pending_deposit.amount, pending_deposit.tx_id
            );
        }
    }

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
                .about("Interact with the exchange")
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
                .subcommand(SubCommand::with_name("deposit").about("Deposit SOL")),
        );
    }

    let app_matches = app.get_matches();
    let db_path = value_t_or_exit!(app_matches, "db_path", PathBuf);

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
            sync(&mut db).await?;
        }
        (exchange, Some(exchange_matches)) => {
            assert!(exchanges.contains(&exchange), "Bug!");

            let exchange = Exchange::from_str(exchange)?;
            match exchange_matches.subcommand() {
                ("balance", Some(_arg_matches)) => {
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
                }
                ("deposit", Some(_arg_matches)) => {
                    let account_client = exchange_account_client(exchange, &db).await?;
                    let account_info = account_client.get_account().json::<AccountInfo>().await?;
                    if !account_info.can_deposit {
                        return Err("Unable to deposit".into());
                    }
                    let withdrawal_client = account_client.to_withdraw_client();

                    let deposit_address = withdrawal_client
                        .get_deposit_address("SOL")
                        .with_status(true)
                        .json::<DepositAddress>()
                        .await?;
                    println!("{}", deposit_address.address);
                }
                ("market", Some(arg_matches)) => {
                    let account_client = exchange_account_client(exchange, &db).await?;
                    let pair = value_t_or_exit!(arg_matches, "pair", String);

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

    /*
    db.record_exchange_deposit(PendingDeposit {
        signature: "txid".to_string(),
        exchange: Exchange::Binance,
        amount: 123.,
    })?;
    db.record_exchange_deposit(PendingDeposit {
        signature: "txid2".to_string(),
        exchange: Exchange::Binance,
        amount: 321.,
    })?;

    let pd = db.pending_exchange_deposits();
    println!("hi: {} {:?}", pd.len(), pd);

    if !pd.is_empty() {
        db.confirm_exchange_deposit(&pd[0])?;
        let pd = db.pending_exchange_deposits();
        println!("hi2: {} {:?}", pd.len(), pd);
    }
    */

    /*
    let resp = coin_gecko::get_coin_history(Utc.ymd(2021, 3, 20)).await?;
    println!("{}", resp.current_price.usd);
    */

    /*
    binance()?
    */
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
async fn binance() -> Result<(), Box<dyn std::error::Error>> {
use serde_json::Value;

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

    let response = withdrawal_client
        .get_deposit_history()
        .with_asset("SOL")
        // optional: 0(0:pending,6: credited but cannot withdraw, 1:success)
        //.with_status(1)
        .json::<serde_json::Value>()
        .await?;

    // "amount"...
    // "txId"...
    println!("get_deposit_history: {:?}", response);

    let response = withdrawal_client
        .get_account_status()
        .json::<serde_json::Value>()
        .await?;
    println!("get_account_status: {:?}", response);

    let response = withdrawal_client
        .get_system_status()
        .json::<serde_json::Value>()
        .await?;
    /*
       {
           "status": 0,              // 0: normal，1：system maintenance
           "msg": "normal"           // normal or system maintenance
       }
    */
    println!("get_system_status: {:?}", response);

    let response = withdrawal_client.get_api_status().json::<Value>().await?;
    /*
    {
           "success": true,     // Query result
           "status": {          // API trading status detail
                   "isLocked": false,  // API trading function is locked or not
    */
    println!("get_api_status: {:?}", response);

    Ok(())
}
*/
