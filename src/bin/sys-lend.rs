use {
    clap::{value_t, value_t_or_exit, App, AppSettings, Arg, SubCommand},
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
    std::collections::HashMap,
    sys::{
        app_version,
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        send_transaction_until_expired,
        token::*,
        vendor::{kamino, marginfi_v2},
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";

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
                        .value_name("POOL")
                        .takes_value(true)
                        .required(true)
                        .possible_values(&["kamino-main", "kamino-altcoins", "kamino-jlp", "mfi"])
                        .help("Lending pool"),
                )
                .arg(
                    Arg::with_name("from")
                        .value_name("FROM_ADDRESS")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_signer)
                        .help("Account holding the deposit"),
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
        );

    let matches = app.get_matches();
    let rpc_client = RpcClient::new_with_commitment(
        normalize_to_url_if_moniker(value_t_or_exit!(matches, "json_rpc_url", String)),
        CommitmentConfig::confirmed(),
    );
    let priority_fee = if let Ok(ui_priority_fee) = value_t!(matches, "priority_fee_exact", f64) {
        PriorityFee::Exact {
            lamports: sol_to_lamports(ui_priority_fee),
        }
    } else if let Ok(ui_priority_fee) = value_t!(matches, "priority_fee_auto", f64) {
        PriorityFee::Auto {
            max_lamports: sol_to_lamports(ui_priority_fee),
        }
    } else {
        PriorityFee::default_auto()
    };

    let mut wallet_manager = None;
    let notifier = Notifier::default();

    match matches.subcommand() {
        ("deposit", Some(matches)) => {
            let (signer, address) = signer_of(matches, "from", &mut wallet_manager)?;
            let address = address.expect("address");
            let signer = signer.expect("signer");

            let pool = value_t_or_exit!(matches, "pool", String);
            let token = MaybeToken::from(value_t!(matches, "token", Token).ok());

            let token_balance = token.balance(&rpc_client, &address)?;
            let deposit_amount = match matches.value_of("amount").unwrap() {
                "ALL" => token_balance,
                amount => token.amount(amount.parse::<f64>().unwrap()),
            };

            if deposit_amount > token_balance {
                println!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                );
                println!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                );
                println!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                );
                println!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                );
                println!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                );
                /*
                return Err(format!(
                    "Deposit amount of {} is greater than current balance of {}",
                    token.format_amount(deposit_amount),
                    token.format_amount(token_balance),
                ).into());
                */
            }
            println!(
                "Depositing {} into {}",
                token.format_amount(deposit_amount),
                pool,
            );

            let (mut instructions, required_compute_units, apr) = if pool.starts_with("kamino-") {
                kamino_deposit(&rpc_client, &pool, address, token, deposit_amount)?
            } else if pool == "mfi" {
                mfi_deposit(address, token, deposit_amount, false)?
            } else {
                unreachable!();
            };

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

            let msg = format!(
                "Depositing {} from {} into {} for {:.1}% APR via {}",
                token.format_amount(deposit_amount),
                address,
                pool,
                apr * 100.,
                signature
            );
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

    Ok(())
}

fn mfi_deposit(
    address: Pubkey,
    token: MaybeToken,
    deposit_amount: u64,
    verbose: bool,
) -> Result<(Vec<Instruction>, u32, f64), Box<dyn std::error::Error>> {
    const MFI_LEND_PROGRAM: Pubkey = pubkey!["MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"];

    // Big mistake to require using `getProgramAccounts` to locate a MarginFi account for a wallet
    // address. Most public RPC endpoints have disabled this method. Leach off MarginFi's RPC
    // endpoint for this expensive call since they designed their shit wrong
    let rpc_client = RpcClient::new_with_commitment(
        // From https://github.com/mrgnlabs/mrgn-account-search/blob/822fe107a8f787b82a494a32130b45613ca94481/src/pages/api/search.ts#L10
        "https://mrgn.rpcpool.com/c293bade994b3864b52c6bbbba4b",
        CommitmentConfig::confirmed(),
    );

    let marginfi_group = pubkey!["4qp6Fx6tnZkY5Wropq9wUYgtFxXKwE6viZxFHg3rdAG8"];

    let (bank_address, bank_liquidity_vault) = match token.token() {
        Some(Token::USDC) => Some((
            pubkey!["2s37akK2eyBbp8DZgCm7RtsaEz8eJP3Nxd4urLHQv7yB"],
            pubkey!["7jaiZR5Sk8hdYN9MxTpczTcwbWpb5WEoxSANuUwveuat"],
        )),
        Some(Token::USDT) => Some((
            pubkey!["HmpMfL8942u22htC4EMiWgLX931g3sacXFR6KjuLgKLV"],
            pubkey!["77t6Fi9qj4s4z22K1toufHtstM8rEy7Y3ytxik7mcsTy"],
        )),
        Some(Token::UXD) => Some((
            pubkey!["BeNBJrAh1tZg5sqgt8D6AWKJLD5KkBrfZvtcgd7EuiAR"],
            pubkey!["D3kBozm2vqgroJwkBquvDySkkZBn5usu6rYhbgPfdDEA"],
        )),
        _ => None,
    }
    .ok_or_else(|| format!("Depositing {token} into mfi is not supported"))?;

    let mut user_accounts = rpc_client
        .get_program_accounts_with_config(
            &MFI_LEND_PROGRAM,
            RpcProgramAccountsConfig {
                filters: Some(vec![
                    RpcFilterType::DataSize(2312),
                    RpcFilterType::Memcmp(rpc_filter::Memcmp::new_raw_bytes(
                        40,
                        address.to_bytes().to_vec(),
                    )),
                    RpcFilterType::Memcmp(rpc_filter::Memcmp::new_raw_bytes(
                        8,
                        marginfi_group.to_bytes().to_vec(),
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
        .ok_or_else(|| format!("No MarginFi account found for {}", address))?;

    if user_accounts.next().is_some() {
        return Err(format!("Multiple MarginFi account found for {}", address).into());
    }

    fn unsafe_load_bank(
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<marginfi_v2::Bank, Box<dyn std::error::Error>> {
        const LEN: usize = std::mem::size_of::<marginfi_v2::Bank>();
        let account_data: [u8; LEN] = rpc_client.get_account_data(address)?[8..LEN + 8]
            .try_into()
            .unwrap();
        let reserve = unsafe { std::mem::transmute(account_data) };
        Ok(reserve)
    }

    let bank = unsafe_load_bank(&rpc_client, &bank_address)?;

    let total_deposits = bank.get_asset_amount(bank.total_asset_shares.into());
    let total_borrow = bank.get_liability_amount(bank.total_liability_shares.into());
    let apr = bank
        .config
        .interest_rate_config
        .calc_interest_rate(total_borrow / total_deposits)
        .unwrap()
        .0
        .to_num::<f64>();

    if verbose {
        let user_account = {
            const LEN: usize = std::mem::size_of::<marginfi_v2::MarginfiAccount>();
            let data: [u8; LEN] = user_account_data.data[8..LEN + 8].try_into().unwrap();
            unsafe { std::mem::transmute::<[u8; LEN], marginfi_v2::MarginfiAccount>(data) }
        };

        if let Some(balance) = user_account.lending_account.get_balance(&bank_address) {
            let deposit = bank.get_asset_amount(balance.asset_shares.into());
            println!(
                "Current user deposits: {}",
                token.format_amount(deposit.floor().to_num::<u64>())
            );

            let liablilty = bank.get_liability_amount(balance.liability_shares.into());
            println!(
                "Current user liablilty: {}",
                token.format_amount(liablilty.floor().to_num::<u64>())
            );
        }

        println!(
            "Deposit Limit: {}",
            token.format_amount(bank.config.deposit_limit)
        );
        println!(
            "Pool deposits: {}",
            token.format_amount(total_deposits.floor().to_num::<u64>())
        );
        println!(
            "Pool liability: {}",
            token.format_amount(total_borrow.floor().to_num::<u64>())
        );
    }

    let marginfi_account_deposit_data = {
        let mut v = vec![0xab, 0x5e, 0xeb, 0x67, 0x52, 0x40, 0xd4, 0x8c];
        v.extend(deposit_amount.to_le_bytes());
        v
    };

    // Marginfi: Lending Account Deposit
    let instructions = vec![Instruction::new_with_bytes(
        MFI_LEND_PROGRAM,
        &marginfi_account_deposit_data,
        vec![
            // Marginfi Group
            AccountMeta::new_readonly(marginfi_group, false),
            // Marginfi Account
            AccountMeta::new(user_account_address, false),
            // Signer
            AccountMeta::new(address, true),
            // Bank
            AccountMeta::new(bank_address, false),
            // Signer Token Account
            AccountMeta::new(
                spl_associated_token_account::get_associated_token_address(&address, &token.mint()),
                false,
            ),
            // Bank Liquidity Vault
            AccountMeta::new(bank_liquidity_vault, false),
            // Token Program
            AccountMeta::new_readonly(spl_token::id(), false),
        ],
    )];

    Ok((instructions, 50_000, apr))
}

fn kamino_deposit(
    rpc_client: &RpcClient,
    pool: &str,
    address: Pubkey,
    token: MaybeToken,
    deposit_amount: u64,
) -> Result<(Vec<Instruction>, u32, f64), Box<dyn std::error::Error>> {
    const KAMINO_LEND_PROGRAM: Pubkey = pubkey!["KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"];
    const FARMS_PROGRAM: Pubkey = pubkey!["FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr"];

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
        "kamino-jlp" => HashMap::from([(
            Some(Token::USDC),
            pubkey!["Ga4rZytCpq1unD4DbEJ5bkHeUz9g3oh9AAFEi6vSauXp"],
        )]),
        _ => HashMap::default(),
    };

    let market_reserve_address = *market_reserve_map
        .get(&token.token())
        .ok_or_else(|| format!("Depositing {token} into {pool} is not supported"))?;

    fn unsafe_load_reserve(
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<kamino::Reserve, Box<dyn std::error::Error>> {
        const LEN: usize = std::mem::size_of::<kamino::Reserve>();
        let account_data: [u8; LEN] = rpc_client.get_account_data(address)?[8..LEN + 8]
            .try_into()
            .unwrap();
        let reserve = unsafe { std::mem::transmute(account_data) };
        Ok(reserve)
    }

    let reserve = unsafe_load_reserve(rpc_client, &market_reserve_address)?;
    let apr = reserve.current_supply_apr();

    let lending_market = reserve.lending_market;

    let lending_market_authority =
        Pubkey::find_program_address(&[b"lma", &lending_market.to_bytes()], &KAMINO_LEND_PROGRAM).0;

    let reserve_farm_state = reserve.farm_collateral;
    let scope_prices = reserve.config.token_info.scope_configuration.price_feed;
    let reserve_liquidity_supply = reserve.liquidity.supply_vault;
    let reserve_collateral_mint = reserve.collateral.mint_pubkey;
    let reserve_destination_deposit_collateral = reserve.collateral.supply_vault;

    let market_obligation = Pubkey::find_program_address(
        &[
            &[0],
            &[0],
            &address.to_bytes(),
            &lending_market.to_bytes(),
            &system_program::ID.to_bytes(),
            &system_program::ID.to_bytes(),
        ],
        &KAMINO_LEND_PROGRAM,
    )
    .0;

    fn unsafe_load_obligation(
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<kamino::Obligation, Box<dyn std::error::Error>> {
        const LEN: usize = std::mem::size_of::<kamino::Obligation>();
        let account_data: [u8; LEN] = rpc_client.get_account_data(address)?[8..LEN + 8]
            .try_into()
            .unwrap();
        let obligation = unsafe { std::mem::transmute(account_data) };
        Ok(obligation)
    }

    let obligation = unsafe_load_obligation(rpc_client, &market_obligation)?;

    let obligation_market_reserves = obligation
        .deposits
        .iter()
        .filter(|c| c.deposit_reserve != Pubkey::default())
        .map(|c| c.deposit_reserve)
        .collect::<Vec<_>>();

    let mut instructions = vec![];

    // Instruction: Kamino: Refresh Reserve

    let mut refresh_reserves: Vec<(Pubkey, Pubkey)> = obligation_market_reserves
        .iter()
        .filter_map(|reserve_address| {
            if *reserve_address != market_reserve_address {
                let reserve =
                    unsafe_load_reserve(rpc_client, reserve_address).unwrap_or_else(|err| {
                        // TODO: propagate failure up instead of panic..
                        panic!("unable to load reserve {reserve_address}: {err}")
                    });

                Some((
                    *reserve_address,
                    reserve.config.token_info.pyth_configuration.price,
                ))
            } else {
                None
            }
        })
        .collect();

    refresh_reserves.push((
        market_reserve_address,
        reserve.config.token_info.pyth_configuration.price,
    ));
    for (refresh_reserve, pyth_oracle) in refresh_reserves.iter() {
        instructions.push(Instruction::new_with_bytes(
            KAMINO_LEND_PROGRAM,
            &[0x02, 0xda, 0x8a, 0xeb, 0x4f, 0xc9, 0x19, 0x66],
            vec![
                // Reserve
                AccountMeta::new(*refresh_reserve, false),
                // Lending Market
                AccountMeta::new_readonly(lending_market, false),
                // Pyth Oracle
                AccountMeta::new_readonly(*pyth_oracle, false),
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
            AccountMeta::new(address, true),
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

    let kamino_deposit_reserve_liquidity_and_obligation_collateral_data = {
        let mut v = vec![0x81, 0xc7, 0x04, 0x02, 0xde, 0x27, 0x1a, 0x2e];
        v.extend(deposit_amount.to_le_bytes());
        v
    };
    instructions.push(kamino_refresh_obligation_farms_for_reserve.clone());

    // Instruction: Kamino: Deposit Reserve Liquidity and Obligation Collateral
    instructions.push(Instruction::new_with_bytes(
        KAMINO_LEND_PROGRAM,
        &kamino_deposit_reserve_liquidity_and_obligation_collateral_data,
        vec![
            // Owner
            AccountMeta::new(address, true),
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
                spl_associated_token_account::get_associated_token_address(&address, &token.mint()),
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

    // Instruction: Kamino: Refresh Obligation Farms For Reserve
    instructions.push(kamino_refresh_obligation_farms_for_reserve);

    Ok((instructions, 500_000, apr))
}
