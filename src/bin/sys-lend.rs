use {
    clap::{value_t, value_t_or_exit, values_t_or_exit, App, AppSettings, Arg, SubCommand},
    solana_account_decoder::UiAccountEncoding,
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::{self, RpcFilterType},
    },
    solana_sdk::{
        commitment_config::CommitmentConfig,
        instruction::{AccountMeta, Instruction},
        message::Message,
        native_token::sol_to_lamports,
        pubkey,
        pubkey::Pubkey,
        system_program, sysvar,
        transaction::Transaction,
    },
    std::collections::{HashMap, HashSet},
    sys::{
        app_version, metrics,
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        send_transaction_until_expired,
        token::*,
        vendor::{kamino, marginfi_v2},
    },
};

lazy_static::lazy_static! {
    static ref SUPPORTED_TOKENS: HashMap<&'static str, HashSet::<MaybeToken>> = HashMap::from([
        ("mfi", HashSet::from([Token::USDC.into(), Token::USDT.into(), Token::UXD.into()])) ,
        ("kamino-main", HashSet::from([Token::USDC.into(), Token::USDT.into()])) ,
        ("kamino-jlp", HashSet::from([Token::USDC.into(), Token::JLP.into()])) ,
        ("kamino-altcoins", HashSet::from([Token::USDC.into()]))
    ]);
}

#[derive(PartialEq, Clone, Copy)]
enum Operation {
    Deposit,
    Withdraw,
}

mod dp {
    use super::*;

    pub fn supply_balance(
        pool: &str,
        address: &Pubkey,
        maybe_token: MaybeToken,
        ui_amount: f64,
    ) -> metrics::Point {
        metrics::Point::new("sys_lend::supply_balance")
            .tag("pool", pool)
            .tag("address", metrics::dp::pubkey_to_value(address))
            .tag("token", maybe_token.name())
            .field("amount", ui_amount)
            .timestamp(metrics::dp::now())
    }

    pub fn supply_apy(pool: &str, maybe_token: MaybeToken, apy_bps: u64) -> metrics::Point {
        metrics::Point::new("sys_lend::supply_apy")
            .tag("pool", pool)
            .tag("token", maybe_token.name())
            .field("apy_bps", apy_bps as f64)
            .timestamp(metrics::dp::now())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";

    let pools = SUPPORTED_TOKENS.keys().copied().collect::<Vec<_>>();

    let app_version = &*app_version();
    let app = App::new("sys-lend")
        .about("Interact with lending pools")
        .version(app_version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::InferSubcommands)
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL")
                .takes_value(true)
                .validator(is_url_or_moniker)
                .default_value(default_json_rpc_url)
                .help("JSON RPC URL for the cluster"),
        )
        .arg(
            Arg::with_name("priority_fee_exact")
                .long("priority-fee-exact")
                .value_name("SOL")
                .takes_value(true)
                .validator(is_parsable::<f64>)
                .help("Exactly specify the Solana priority fee to use for transactions"),
        )
        .arg(
            Arg::with_name("priority_fee_auto")
                .long("priority-fee-auto")
                .value_name("SOL")
                .takes_value(true)
                .conflicts_with("priority_fee_exact")
                .validator(is_parsable::<f64>)
                .help(
                    "Automatically select the Solana priority fee to use for transactions, \
                       but do not exceed the specified amount of SOL [default]",
                ),
        )
        .subcommand(
            SubCommand::with_name("deposit")
                .about("Deposit tokens into a lending pool")
                .arg(
                    Arg::with_name("pool")
                        .long("into")
                        .value_name("POOL")
                        .takes_value(true)
                        .required(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool to deposit into. If multiple pools are provided, the pool with the highest APY is selected"),
                )
                .arg(
                    Arg::with_name("signer")
                        .value_name("KEYPAIR")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_signer)
                        .help("Wallet"),
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
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to deposit"),
                ),
        )
        .subcommand(
            SubCommand::with_name("withdraw")
                .about("Withdraw tokens from a lending pool")
                .arg(
                    Arg::with_name("pool")
                        .long("from")
                        .value_name("POOL")
                        .takes_value(true)
                        .required(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool to withdraw from. If multiple pools are provided, the pool with the lowest APY is selected"),
                )
                .arg(
                    Arg::with_name("skip_withdraw_if_only_one_pool_remains")
                        .long("skip-if-only-one-pool-remains")
                        .takes_value(false)
                        .help("Do not withdraw if only one lending pool remains"),
                )
                .arg(
                    Arg::with_name("signer")
                        .value_name("KEYPAIR")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_signer)
                        .help("Wallet"),
                )
                .arg(
                    Arg::with_name("amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount_or_all)
                        .required(true)
                        .help("The amount to withdraw; accepts keyword ALL"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to withdraw"),
                ),
        )
        .subcommand(
            SubCommand::with_name("supply-balance")
                .about("Display the current supplied balance for a lending pool")
                .arg(
                    Arg::with_name("pool")
                        .value_name("POOL")
                        .takes_value(true)
                        .required(true)
                        .possible_values(&pools)
                        .help("Lending pool"),
                )
                .arg(
                    Arg::with_name("address")
                        .value_name("ADDRESS")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_pubkey)
                        .help("Wallet address"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to deposit"),
                ),
        )
        .subcommand(
            SubCommand::with_name("supply-apy")
                .about("Display the current supply APY for a lending pool")
                .arg(
                    Arg::with_name("pool")
                        .value_name("POOL")
                        .takes_value(true)
                        .required(true)
                        .possible_values(&pools)
                        .help("Lending pool"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to deposit"),
                )
                .arg(
                    Arg::with_name("raw")
                        .long("raw")
                        .takes_value(false)
                        .help("Only output raw numerical value"),
                )
                .arg(
                    Arg::with_name("bps")
                        .long("bps")
                        .takes_value(false)
                        .help("Display in Basis Points instead of Percent"),
                ),
        );

    let app_matches = app.get_matches();
    let rpc_client = RpcClient::new_with_commitment(
        normalize_to_url_if_moniker(value_t_or_exit!(app_matches, "json_rpc_url", String)),
        CommitmentConfig::confirmed(),
    );
    let priority_fee = if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_exact", f64)
    {
        PriorityFee::Exact {
            lamports: sol_to_lamports(ui_priority_fee),
        }
    } else if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_auto", f64) {
        PriorityFee::Auto {
            max_lamports: sol_to_lamports(ui_priority_fee),
        }
    } else {
        PriorityFee::default_auto()
    };

    let mut wallet_manager = None;
    let notifier = Notifier::default();

    fn pool_supply_apr(
        rpc_client: &RpcClient,
        pool: &str,
        token: MaybeToken,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        Ok(if pool.starts_with("kamino-") {
            kamino_apr(rpc_client, pool, token)?
        } else if pool == "mfi" {
            mfi_apr(rpc_client, token)?
        } else {
            unreachable!()
        })
    }

    fn pool_supply_balance(
        rpc_client: &RpcClient,
        pool: &str,
        token: MaybeToken,
        address: Pubkey,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        Ok(if pool.starts_with("kamino-") {
            kamino_deposited_amount(rpc_client, pool, address, token)?
        } else if pool == "mfi" {
            mfi_balance(rpc_client, address, token)?.0
        } else {
            unreachable!()
        })
    }

    match app_matches.subcommand() {
        ("supply-apy", Some(matches)) => {
            let pool = value_t_or_exit!(matches, "pool", String);
            let token = MaybeToken::from(value_t!(matches, "token", Token).ok());
            let raw = matches.is_present("raw");
            let bps = matches.is_present("bps");

            let apy = apr_to_apy(pool_supply_apr(&rpc_client, &pool, token)?) * 100.;
            let apy_as_bps = (apy * 100.) as u64;

            let value = if bps {
                format!("{}", apy_as_bps)
            } else {
                format!("{:.2}", apy)
            };

            let msg = if raw {
                value.to_string()
            } else {
                format!("{pool} {token} {value}{}", if bps { "bps" } else { "%" })
            };
            if !raw {
                notifier.send(&msg).await;
            }
            metrics::push(dp::supply_apy(&pool, token, apy_as_bps)).await;
            println!("{msg}");
        }
        ("supply-balance", Some(matches)) => {
            let pool = value_t_or_exit!(matches, "pool", String);
            let address = pubkey_of(matches, "address").unwrap();
            let token = MaybeToken::from(value_t!(matches, "token", Token).ok());

            let amount = pool_supply_balance(&rpc_client, &pool, token, address)?;
            let apr = pool_supply_apr(&rpc_client, &pool, token)?;

            let msg = format!(
                "{}: {} supplied at {:.2}%",
                pool,
                token.format_amount(amount),
                apr_to_apy(apr) * 100.
            );
            notifier.send(&msg).await;
            metrics::push(dp::supply_balance(
                &pool,
                &address,
                token,
                token.ui_amount(amount),
            ))
            .await;
            println!("{msg}");
        }
        ("deposit" | "withdraw", Some(matches)) => {
            let op = match app_matches.subcommand().0 {
                "withdraw" => Operation::Withdraw,
                "deposit" => Operation::Deposit,
                _ => unreachable!(),
            };

            let (signer, address) = signer_of(matches, "signer", &mut wallet_manager)?;
            let address = address.expect("address");
            let signer = signer.expect("signer");
            let skip_withdraw_if_only_one_pool_remains =
                matches.is_present("skip_withdraw_if_only_one_pool_remains");

            let token = MaybeToken::from(value_t!(matches, "token", Token).ok());
            let pools = values_t_or_exit!(matches, "pool", String);

            let token_balance = token.balance(&rpc_client, &address)?;
            let amount = match matches.value_of("amount").unwrap() {
                "ALL" => {
                    if op == Operation::Deposit {
                        token_balance
                    } else {
                        u64::MAX
                    }
                }
                amount => token.amount(amount.parse::<f64>().unwrap()),
            };

            if op == Operation::Deposit {
                if amount > token_balance {
                    return Err(format!(
                        "Deposit amount of {} is greater than current balance of {}",
                        token.format_amount(amount),
                        token.format_amount(token_balance),
                    )
                    .into());
                }
                if amount == 0 {
                    println!("Nothing to deposit");
                    return Ok(());
                }
            }

            for pool in &pools {
                if !SUPPORTED_TOKENS
                    .get(pool.as_str())
                    .unwrap()
                    .contains(&token)
                {
                    return Err(format!("{token} is not supported by {pool}").into());
                }
            }

            let pools = match op {
                Operation::Deposit => pools,
                Operation::Withdraw => pools
                    .into_iter()
                    .filter(|pool| {
                        let supply_balance = pool_supply_balance(&rpc_client, pool, token, address)
                            .unwrap_or_else(|err| {
                                panic!("Unable to read balance for {pool}: {err}")
                            });

                        if amount == u64::MAX {
                            supply_balance > 0
                        } else {
                            supply_balance >= amount
                        }
                    })
                    .collect(),
            };
            if pools.is_empty() {
                return Err("No available pools".into());
            }

            if skip_withdraw_if_only_one_pool_remains && pools.len() == 1 {
                println!("Taking no action due to --skip-if-only-one-pool-remains flag");
                return Ok(());
            }

            let ordering = if op == Operation::Deposit {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };

            let pool = if pools.len() > 1 {
                let mut selected_pool = None;
                let mut selected_apr = None;

                for pool in &pools {
                    let apr = pool_supply_apr(&rpc_client, pool, token)?;
                    if selected_pool.is_none()
                        || selected_apr.partial_cmp(&Some(apr)) == Some(ordering)
                    {
                        selected_pool = Some(pool);
                        selected_apr = Some(apr);
                    }
                }

                match selected_pool {
                    None => return Err("Bug? No pools available".into()),
                    Some(pool) => pool,
                }
            } else {
                &pools[0]
            }
            .clone();

            let DepositOrWithdrawResult {
                mut instructions,
                required_compute_units,
                amount,
            } = if pool.starts_with("kamino-") {
                kamino_deposit_or_withdraw(op, &rpc_client, &pool, address, token, amount)?
            } else if pool == "mfi" {
                mfi_deposit_or_withdraw(op, &rpc_client, address, token, amount, false)?
            } else {
                unreachable!();
            };

            if op == Operation::Deposit {
                println!("Depositing {} into {}", token.format_amount(amount), pool,);
            } else {
                println!("Withdrawing {} from {}", token.format_amount(amount), pool,);
            }

            apply_priority_fee(
                &rpc_client,
                &mut instructions,
                required_compute_units,
                priority_fee,
            )?;

            let (recent_blockhash, last_valid_block_height) =
                rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

            let mut message = Message::new(&instructions, Some(&address));
            message.recent_blockhash = recent_blockhash;

            let mut transaction = Transaction::new_unsigned(message);
            let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
            if simulation_result.err.is_some() {
                return Err(format!("Simulation failure: {simulation_result:?}").into());
            }

            transaction.try_sign(&vec![signer], recent_blockhash)?;
            let signature = transaction.signatures[0];

            let msg = if op == Operation::Deposit {
                format!(
                    "Depositing {} from {} into {} via {}",
                    token.format_amount(amount),
                    address,
                    pool,
                    signature
                )
            } else {
                format!(
                    "Withdrew {} from {} into {} via {}",
                    token.format_amount(amount),
                    pool,
                    address,
                    signature
                )
            };
            notifier.send(&msg).await;
            println!("{msg}");

            if !send_transaction_until_expired(&rpc_client, &transaction, last_valid_block_height) {
                let msg = format!("Deposit failed: {signature}");
                notifier.send(&msg).await;
                return Err(msg.into());
            }
        }
        _ => unreachable!(),
    }

    metrics::send(metrics::env_config()).await;
    Ok(())
}

fn apr_to_apy(apr: f64) -> f64 {
    let compounding_periods = 365. * 24.; // hourly compounding
    (1. + apr / compounding_periods).powf(compounding_periods) - 1.
}

//////////////////////////////////////////////////////////////////////////////
///[ MarginFi Stuff ] ////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

const MFI_LEND_PROGRAM: Pubkey = pubkey!["MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"];
const MARGINFI_GROUP: Pubkey = pubkey!["4qp6Fx6tnZkY5Wropq9wUYgtFxXKwE6viZxFHg3rdAG8"];

fn mfi_lookup_bank_address(token: MaybeToken) -> Result<Pubkey, Box<dyn std::error::Error>> {
    match token.token() {
        Some(Token::USDC) => Some(pubkey!["2s37akK2eyBbp8DZgCm7RtsaEz8eJP3Nxd4urLHQv7yB"]),
        Some(Token::USDT) => Some(pubkey!["HmpMfL8942u22htC4EMiWgLX931g3sacXFR6KjuLgKLV"]),
        Some(Token::UXD) => Some(pubkey!["BeNBJrAh1tZg5sqgt8D6AWKJLD5KkBrfZvtcgd7EuiAR"]),
        _ => None,
    }
    .ok_or_else(|| format!("mfi_load_bank: {token} is not supported").into())
}

fn mfi_load_bank(
    rpc_client: &RpcClient,
    bank_address: Pubkey,
) -> Result<marginfi_v2::Bank, Box<dyn std::error::Error>> {
    fn unsafe_load_bank(
        rpc_client: &RpcClient,
        address: Pubkey,
    ) -> Result<marginfi_v2::Bank, Box<dyn std::error::Error>> {
        const LEN: usize = std::mem::size_of::<marginfi_v2::Bank>();
        let account_data: [u8; LEN] = rpc_client.get_account_data(&address)?[8..LEN + 8]
            .try_into()
            .unwrap();
        let reserve = unsafe { std::mem::transmute(account_data) };
        Ok(reserve)
    }

    unsafe_load_bank(rpc_client, bank_address)
}

fn mfi_calc_bank_apr(bank: &marginfi_v2::Bank) -> f64 {
    let total_deposits = bank.get_asset_amount(bank.total_asset_shares.into());
    let total_borrow = bank.get_liability_amount(bank.total_liability_shares.into());

    /*
    println!(
        "Pool deposits: {}",
        token.format_amount(total_deposits.floor().to_num::<u64>())
    );
    println!(
        "Pool liability: {}",
        token.format_amount(total_borrow.floor().to_num::<u64>())
    );
    */

    bank.config
        .interest_rate_config
        .calc_interest_rate(total_borrow / total_deposits)
        .unwrap()
        .0
        .to_num::<f64>()
}

fn mfi_apr(rpc_client: &RpcClient, token: MaybeToken) -> Result<f64, Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(rpc_client, bank_address)?;
    Ok(mfi_calc_bank_apr(&bank))
}

fn mfi_load_user_account(
    wallet_address: Pubkey,
) -> Result<(Pubkey, marginfi_v2::MarginfiAccount), Box<dyn std::error::Error>> {
    // Big mistake to require using `getProgramAccounts` to locate a MarginFi account for a wallet
    // address. Most public RPC endpoints have disabled this method. Leach off MarginFi's RPC
    // endpoint for this expensive call since they designed their shit wrong
    let rpc_client = RpcClient::new_with_commitment(
        // From https://github.com/mrgnlabs/mrgn-account-search/blob/822fe107a8f787b82a494a32130b45613ca94481/src/pages/api/search.ts#L10
        "https://mrgn.rpcpool.com/c293bade994b3864b52c6bbbba4b",
        CommitmentConfig::confirmed(),
    );

    let mut user_accounts = rpc_client
        .get_program_accounts_with_config(
            &MFI_LEND_PROGRAM,
            RpcProgramAccountsConfig {
                filters: Some(vec![
                    RpcFilterType::DataSize(2312),
                    RpcFilterType::Memcmp(rpc_filter::Memcmp::new_raw_bytes(
                        40,
                        wallet_address.to_bytes().to_vec(),
                    )),
                    RpcFilterType::Memcmp(rpc_filter::Memcmp::new_raw_bytes(
                        8,
                        MARGINFI_GROUP.to_bytes().to_vec(),
                    )),
                ]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..RpcAccountInfoConfig::default()
                },
                ..RpcProgramAccountsConfig::default()
            },
        )?
        .into_iter();

    let (user_account_address, user_account_data) = user_accounts
        .next()
        .ok_or_else(|| format!("No MarginFi account found for {}", wallet_address))?;

    if user_accounts.next().is_some() {
        return Err(format!("Multiple MarginFi account found for {}", wallet_address).into());
    }

    Ok((user_account_address, {
        const LEN: usize = std::mem::size_of::<marginfi_v2::MarginfiAccount>();
        let data: [u8; LEN] = user_account_data.data[8..LEN + 8].try_into().unwrap();
        unsafe { std::mem::transmute::<[u8; LEN], marginfi_v2::MarginfiAccount>(data) }
    }))
}

fn mfi_balance(
    rpc_client: &RpcClient,
    wallet_address: Pubkey,
    token: MaybeToken,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(rpc_client, bank_address)?;
    let (_user_account_address, user_account) = mfi_load_user_account(wallet_address)?;

    match user_account.lending_account.get_balance(&bank_address) {
        None => Ok((0, 0)),
        Some(balance) => {
            let deposit = bank.get_asset_amount(balance.asset_shares.into());
            let liablilty = bank.get_liability_amount(balance.liability_shares.into());
            Ok((
                deposit.floor().to_num::<u64>(),
                liablilty.floor().to_num::<u64>(),
            ))
        }
    }
}

fn mfi_deposit_or_withdraw(
    op: Operation,
    rpc_client: &RpcClient,
    wallet_address: Pubkey,
    token: MaybeToken,
    amount: u64,
    verbose: bool,
) -> Result<DepositOrWithdrawResult, Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(rpc_client, bank_address)?;
    if verbose {
        println!(
            "Deposit Limit: {}",
            token.format_amount(bank.config.deposit_limit)
        );
    }

    let (user_account_address, user_account) = mfi_load_user_account(wallet_address)?;

    let (instructions, required_compute_units, amount) = match op {
        Operation::Deposit => {
            // Marginfi: Lending Account Deposit
            let marginfi_account_deposit_data = {
                let mut v = vec![0xab, 0x5e, 0xeb, 0x67, 0x52, 0x40, 0xd4, 0x8c];
                v.extend(amount.to_le_bytes());
                v
            };

            let instruction = Instruction::new_with_bytes(
                MFI_LEND_PROGRAM,
                &marginfi_account_deposit_data,
                vec![
                    // Marginfi Group
                    AccountMeta::new_readonly(MARGINFI_GROUP, false),
                    // Marginfi Account
                    AccountMeta::new(user_account_address, false),
                    // Signer
                    AccountMeta::new(wallet_address, true),
                    // Bank
                    AccountMeta::new(bank_address, false),
                    // Signer Token Account
                    AccountMeta::new(
                        spl_associated_token_account::get_associated_token_address(
                            &wallet_address,
                            &token.mint(),
                        ),
                        false,
                    ),
                    // Bank Liquidity Vault
                    AccountMeta::new(bank.liquidity_vault, false),
                    // Token Program
                    AccountMeta::new_readonly(spl_token::id(), false),
                ],
            );

            (vec![instruction], 50_000, amount)
        }
        Operation::Withdraw => {
            let withdraw_amount = if amount == u64::MAX {
                let balance = user_account
                    .lending_account
                    .get_balance(&bank_address)
                    .ok_or_else(|| format!("No {token} deposit found"))?;

                let deposit = bank.get_asset_amount(balance.asset_shares.into());
                deposit.floor().to_num::<u64>()
            } else {
                amount
            };

            let liquidity_vault_authority = Pubkey::create_program_address(
                &[
                    b"liquidity_vault_auth",
                    &bank_address.to_bytes(),
                    &[bank.liquidity_vault_authority_bump],
                ],
                &MFI_LEND_PROGRAM,
            )
            .expect("valid liquidity_vault_authority");

            // Marginfi: Lending Account Withdraw
            let marginfi_account_withdraw_data = {
                let mut v = vec![0x24, 0x48, 0x4a, 0x13, 0xd2, 0xd2, 0xc0, 0xc0];
                v.extend(withdraw_amount.to_le_bytes());
                v.extend([1, if amount == u64::MAX { 1 } else { 0 }]); // WithdrawAll flag
                v
            };

            let mut account_meta = vec![
                // Marginfi Group
                AccountMeta::new_readonly(MARGINFI_GROUP, false),
                // Marginfi Account
                AccountMeta::new(user_account_address, false),
                // Signer
                AccountMeta::new(wallet_address, true),
                // Bank
                AccountMeta::new(bank_address, false),
                // Signer Token Account
                AccountMeta::new(
                    spl_associated_token_account::get_associated_token_address(
                        &wallet_address,
                        &token.mint(),
                    ),
                    false,
                ),
                // Bank Liquidity Vault Authority
                AccountMeta::new(liquidity_vault_authority, false),
                // Bank Liquidity Vault
                AccountMeta::new(bank.liquidity_vault, false),
                // Token Program
                AccountMeta::new_readonly(spl_token::id(), false),
            ];

            for balance in &user_account.lending_account.balances {
                if balance.active && !(amount == u64::MAX && balance.bank_pk == bank_address) {
                    account_meta.push(AccountMeta::new_readonly(balance.bank_pk, false));

                    let balance_bank = mfi_load_bank(rpc_client, balance.bank_pk)?;
                    account_meta.push(AccountMeta::new_readonly(
                        balance_bank.config.oracle_keys[0],
                        false,
                    ));
                }
            }

            let instructions = vec![
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &wallet_address,
                    &wallet_address,
                    &bank.mint,
                    &spl_token::id(),
                ),

                Instruction::new_with_bytes(
                    MFI_LEND_PROGRAM,
                    &marginfi_account_withdraw_data,
                    account_meta,
                )
            ];

            (instructions, 110_000, withdraw_amount)
        }
    };

    Ok(DepositOrWithdrawResult {
        instructions,
        required_compute_units,
        amount,
    })
}

struct DepositOrWithdrawResult {
    instructions: Vec<Instruction>,
    required_compute_units: u32,
    amount: u64,
}

//////////////////////////////////////////////////////////////////////////////
///[ Kamino Stuff ] //////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

const KAMINO_LEND_PROGRAM: Pubkey = pubkey!["KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"];
const FARMS_PROGRAM: Pubkey = pubkey!["FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr"];

fn kamino_unsafe_load_reserve(
    rpc_client: &RpcClient,
    address: Pubkey,
) -> Result<kamino::Reserve, Box<dyn std::error::Error>> {
    const LEN: usize = std::mem::size_of::<kamino::Reserve>();
    let account_data: [u8; LEN] = rpc_client.get_account_data(&address)?[8..LEN + 8]
        .try_into()
        .unwrap();
    let reserve = unsafe { std::mem::transmute(account_data) };
    Ok(reserve)
}

fn kamino_load_pool_reserve(
    rpc_client: &RpcClient,
    pool: &str,
    token: MaybeToken,
) -> Result<(Pubkey, kamino::Reserve), Box<dyn std::error::Error>> {
    let market_reserve_map = match pool {
        "kamino-main" => HashMap::from([
            (
                Some(Token::USDC),
                pubkey!["D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"],
            ),
            (
                Some(Token::USDT),
                pubkey!["H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S"],
            ),
        ]),
        "kamino-altcoins" => HashMap::from([(
            Some(Token::USDC),
            pubkey!["9TD2TSv4pENb8VwfbVYg25jvym7HN6iuAR6pFNSrKjqQ"],
        )]),
        "kamino-jlp" => HashMap::from([
            (
                Some(Token::USDC),
                pubkey!["Ga4rZytCpq1unD4DbEJ5bkHeUz9g3oh9AAFEi6vSauXp"],
            ),
            (
                Some(Token::JLP),
                pubkey!["DdTmCCjv7zHRD1hJv3E8bpnSEQBzdKkzB1j9ApXX5QoP"],
            ),
        ]),
        _ => HashMap::default(),
    };

    let market_reserve_address = *market_reserve_map
        .get(&token.token())
        .ok_or_else(|| format!("{pool}: {token} is not supported"))?;

    let reserve = kamino_unsafe_load_reserve(rpc_client, market_reserve_address)?;

    Ok((market_reserve_address, reserve))
}

fn kamino_apr(
    rpc_client: &RpcClient,
    pool: &str,
    token: MaybeToken,
) -> Result<f64, Box<dyn std::error::Error>> {
    let (_market_reserve_address, reserve) = kamino_load_pool_reserve(rpc_client, pool, token)?;
    Ok(reserve.current_supply_apr())
}

fn kamino_find_obligation_address(wallet_address: Pubkey, lending_market: Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[
            &[0],
            &[0],
            &wallet_address.to_bytes(),
            &lending_market.to_bytes(),
            &system_program::ID.to_bytes(),
            &system_program::ID.to_bytes(),
        ],
        &KAMINO_LEND_PROGRAM,
    )
    .0
}

fn kamino_unsafe_load_obligation(
    rpc_client: &RpcClient,
    wallet_address: Pubkey,
) -> Result<kamino::Obligation, Box<dyn std::error::Error>> {
    const LEN: usize = std::mem::size_of::<kamino::Obligation>();
    let account_data: [u8; LEN] = rpc_client.get_account_data(&wallet_address)?[8..LEN + 8]
        .try_into()
        .unwrap();
    let obligation = unsafe { std::mem::transmute(account_data) };
    Ok(obligation)
}

fn kamino_deposited_amount(
    rpc_client: &RpcClient,
    pool: &str,
    wallet_address: Pubkey,
    token: MaybeToken,
) -> Result<u64, Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve) = kamino_load_pool_reserve(rpc_client, pool, token)?;
    let lending_market = reserve.lending_market;
    let market_obligation = kamino_find_obligation_address(wallet_address, lending_market);
    let obligation = kamino_unsafe_load_obligation(rpc_client, market_obligation)?;

    let collateral_deposited_amount = obligation
        .deposits
        .iter()
        .find(|collateral| collateral.deposit_reserve == market_reserve_address)
        .map(|collateral| collateral.deposited_amount)
        .unwrap_or_default();

    let collateral_exchange_rate = reserve.collateral_exchange_rate();
    Ok(collateral_exchange_rate.collateral_to_liquidity(collateral_deposited_amount))
}

fn kamino_deposit_or_withdraw(
    op: Operation,
    rpc_client: &RpcClient,
    pool: &str,
    wallet_address: Pubkey,
    token: MaybeToken,
    amount: u64,
) -> Result<DepositOrWithdrawResult, Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve) = kamino_load_pool_reserve(rpc_client, pool, token)?;

    let lending_market = reserve.lending_market;

    let lending_market_authority =
        Pubkey::find_program_address(&[b"lma", &lending_market.to_bytes()], &KAMINO_LEND_PROGRAM).0;

    let reserve_farm_state = reserve.farm_collateral;
    let reserve_liquidity_supply = reserve.liquidity.supply_vault;
    let reserve_collateral_mint = reserve.collateral.mint_pubkey;
    let reserve_destination_deposit_collateral = reserve.collateral.supply_vault;

    let market_obligation = kamino_find_obligation_address(wallet_address, lending_market);
    let obligation = kamino_unsafe_load_obligation(rpc_client, market_obligation)?;

    let obligation_market_reserves = obligation
        .deposits
        .iter()
        .filter(|c| c.deposit_reserve != Pubkey::default())
        .map(|c| c.deposit_reserve)
        .collect::<Vec<_>>();

    let mut instructions = vec![];

    // Instruction: Kamino: Refresh Reserve

    let refresh_reserves = obligation_market_reserves.iter().map(|reserve_address| {
        if *reserve_address != market_reserve_address {
            (
                *reserve_address,
                kamino_unsafe_load_reserve(rpc_client, *reserve_address).unwrap_or_else(|err| {
                    // TODO: propagate failure up instead of panic..
                    panic!("unable to load reserve {reserve_address}: {err}")
                }),
            )
        } else {
            (*reserve_address, reserve)
        }
    });

    for (reserve_address, reserve) in refresh_reserves {
        let pyth_oracle = if reserve.config.token_info.pyth_configuration.price == Pubkey::default()
        {
            KAMINO_LEND_PROGRAM
        } else {
            reserve.config.token_info.pyth_configuration.price
        };
        let scope_prices =
            if reserve.config.token_info.scope_configuration.price_feed == Pubkey::default() {
                KAMINO_LEND_PROGRAM
            } else {
                reserve.config.token_info.scope_configuration.price_feed
            };

        instructions.push(Instruction::new_with_bytes(
            KAMINO_LEND_PROGRAM,
            &[0x02, 0xda, 0x8a, 0xeb, 0x4f, 0xc9, 0x19, 0x66],
            vec![
                // Reserve
                AccountMeta::new(reserve_address, false),
                // Lending Market
                AccountMeta::new_readonly(lending_market, false),
                // Pyth Oracle
                AccountMeta::new_readonly(pyth_oracle, false),
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Switchboard Twap Oracle
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Scope Prices
                AccountMeta::new_readonly(scope_prices, false),
            ],
        ));
    }

    // Instruction: Kamino: Refresh Obligation
    let mut refresh_obligation_account_metas = vec![
        // Lending Market
        AccountMeta::new_readonly(lending_market, false),
        // Obligation
        AccountMeta::new(market_obligation, false),
    ];

    for obligation_market_reserve in &obligation_market_reserves {
        refresh_obligation_account_metas.push(AccountMeta::new(*obligation_market_reserve, false));
    }

    instructions.push(Instruction::new_with_bytes(
        KAMINO_LEND_PROGRAM,
        &[0x21, 0x84, 0x93, 0xe4, 0x97, 0xc0, 0x48, 0x59],
        refresh_obligation_account_metas,
    ));

    // Instruction: Kamino: Refresh Obligation Farms For Reserve
    let kamino_refresh_obligation_farms_for_reserve = Instruction::new_with_bytes(
        KAMINO_LEND_PROGRAM,
        &[
            0x8c, 0x90, 0xfd, 0x15, 0x0a, 0x4a, 0xf8, 0x03, // mode: u8
            0x00,
        ],
        vec![
            // Crank
            AccountMeta::new(wallet_address, true),
            // Obligation
            AccountMeta::new(market_obligation, false),
            // Lending Market Authority
            AccountMeta::new(lending_market_authority, false),
            // Reserve
            AccountMeta::new(market_reserve_address, false),
            // Reserve Farm State
            AccountMeta::new(reserve_farm_state, false),
            // Obligation Farm User State
            AccountMeta::new(
                Pubkey::find_program_address(
                    &[
                        b"user",
                        &reserve_farm_state.to_bytes(),
                        &market_obligation.to_bytes(),
                    ],
                    &FARMS_PROGRAM,
                )
                .0,
                false,
            ),
            // Lending Market
            AccountMeta::new_readonly(lending_market, false),
            // Farms Program
            AccountMeta::new_readonly(FARMS_PROGRAM, false),
            // Rent
            AccountMeta::new_readonly(sysvar::rent::ID, false),
            // Token Program
            AccountMeta::new_readonly(spl_token::id(), false),
            // System Program
            AccountMeta::new_readonly(system_program::ID, false),
        ],
    );

    instructions.push(kamino_refresh_obligation_farms_for_reserve.clone());

    let amount = match op {
        Operation::Withdraw => {
            let withdraw_amount = if amount == u64::MAX {
                let collateral_deposited_amount = obligation
                    .deposits
                    .iter()
                    .find(|collateral| collateral.deposit_reserve == market_reserve_address)
                    .map(|collateral| collateral.deposited_amount)
                    .unwrap_or_default();

                let collateral_exchange_rate = reserve.collateral_exchange_rate();
                collateral_exchange_rate.collateral_to_liquidity(collateral_deposited_amount)
            } else {
                amount
            };

            // Instruction: Withdraw Obligation Collateral And Redeem Reserve Collateral

            let collateral_exchange_rate = reserve.collateral_exchange_rate();
            let kamino_withdraw_obligation_collateral_and_redeem_reserve_collateral_data = {
                let mut v = vec![0x4b, 0x5d, 0x5d, 0xdc, 0x22, 0x96, 0xda, 0xc4];
                v.extend(
                    collateral_exchange_rate
                        .liquidity_to_collateral(amount)
                        .to_le_bytes(),
                );
                v
            };

            instructions.push(Instruction::new_with_bytes(
                KAMINO_LEND_PROGRAM,
                &kamino_withdraw_obligation_collateral_and_redeem_reserve_collateral_data,
                vec![
                    // Owner
                    AccountMeta::new(wallet_address, true),
                    // Obligation
                    AccountMeta::new(market_obligation, false),
                    // Lending Market
                    AccountMeta::new_readonly(lending_market, false),
                    // Lending Market Authority
                    AccountMeta::new(lending_market_authority, false),
                    // Reserve
                    AccountMeta::new(market_reserve_address, false),
                    // Reserve Source Collateral
                    AccountMeta::new(reserve_destination_deposit_collateral, false),
                    // Reserve Collateral Mint
                    AccountMeta::new(reserve_collateral_mint, false),
                    // Reserve Liquidity Supply
                    AccountMeta::new(reserve_liquidity_supply, false),
                    // User Liquidity
                    AccountMeta::new(
                        spl_associated_token_account::get_associated_token_address(
                            &wallet_address,
                            &token.mint(),
                        ),
                        false,
                    ),
                    // User Destination Collateral
                    AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                    // Token Program
                    AccountMeta::new_readonly(spl_token::id(), false),
                    // Sysvar: Instructions
                    AccountMeta::new_readonly(sysvar::instructions::ID, false),
                ],
            ));

            withdraw_amount
        }
        Operation::Deposit => {
            // Instruction: Kamino: Deposit Reserve Liquidity and Obligation Collateral

            let kamino_deposit_reserve_liquidity_and_obligation_collateral_data = {
                let mut v = vec![0x81, 0xc7, 0x04, 0x02, 0xde, 0x27, 0x1a, 0x2e];
                v.extend(amount.to_le_bytes());
                v
            };
            instructions.push(Instruction::new_with_bytes(
                KAMINO_LEND_PROGRAM,
                &kamino_deposit_reserve_liquidity_and_obligation_collateral_data,
                vec![
                    // Owner
                    AccountMeta::new(wallet_address, true),
                    // Obligation
                    AccountMeta::new(market_obligation, false),
                    // Lending Market
                    AccountMeta::new_readonly(lending_market, false),
                    // Lending Market Authority
                    AccountMeta::new(lending_market_authority, false),
                    // Reserve
                    AccountMeta::new(market_reserve_address, false),
                    // Reserve Liquidity Supply
                    AccountMeta::new(reserve_liquidity_supply, false),
                    // Reserve Collateral Mint
                    AccountMeta::new(reserve_collateral_mint, false),
                    // Reserve Destination Deposit Collateral
                    AccountMeta::new(reserve_destination_deposit_collateral, false),
                    // User Source Liquidity
                    AccountMeta::new(
                        spl_associated_token_account::get_associated_token_address(
                            &wallet_address,
                            &token.mint(),
                        ),
                        false,
                    ),
                    // User Destination Collateral
                    AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                    // Token Program
                    AccountMeta::new_readonly(spl_token::id(), false),
                    // Sysvar: Instructions
                    AccountMeta::new_readonly(sysvar::instructions::ID, false),
                ],
            ));

            amount
        }
    };

    // Instruction: Kamino: Refresh Obligation Farms For Reserve
    instructions.push(kamino_refresh_obligation_farms_for_reserve);

    Ok(DepositOrWithdrawResult {
        instructions,
        required_compute_units: 500_000,
        amount,
    })
}
