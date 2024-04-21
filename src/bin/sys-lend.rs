use {
    clap::{value_t, value_t_or_exit, App, AppSettings, Arg, SubCommand},
    solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig},
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
    sys::{
        app_version,
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        send_transaction_until_expired,
        token::*,
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
                        .possible_values(&["kamino-main", "kamino-altcoins", "kamino-jup", "mfi"])
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
                    Arg::with_name("token")
                        .value_name("SOL or SPL Token")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to deposit"),
                )
                .arg(
                    Arg::with_name("retain")
                        .short("r")
                        .long("retain")
                        .value_name("UI_AMOUNT")
                        .takes_value(true)
                        .validator(is_parsable::<f64>)
                        .default_value("0.01")
                        .help("Amount of tokens to retain in the account"),
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
            let retain_amount = token.amount(value_t_or_exit!(matches, "retain", f64));

            let balance = token.balance(&rpc_client, &address)?;
            let deposit_amount = balance.saturating_sub(retain_amount);

            if deposit_amount == 0 {
                println!(
                    "Current balance: {}\n\
                    Retain amount: {}\n\
                    \n\
                    Nothing to deposit",
                    token.format_amount(balance),
                    token.format_amount(retain_amount)
                );
                return Ok(());
            }
            println!(
                "Depositing {} into {}",
                token.format_amount(deposit_amount),
                pool
            );

            let (mut instructions, required_compute_units) = if pool.starts_with("kamino-") {
                kamino_deposit(&pool, address, token, deposit_amount)?
            } else if pool == "mfi" {
                mfi_deposit(address, token, deposit_amount)?
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
            println!("Transaction signature: {signature}");

            if !send_transaction_until_expired(&rpc_client, &transaction, last_valid_block_height) {
                return Err("Deposit failed".into());
            }

            let msg = format!(
                "Deposited {} from {} into {}",
                token.format_amount(deposit_amount),
                address,
                pool
            );
            notifier.send(&msg).await;
            println!("{msg}");
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn mfi_deposit(
    address: Pubkey,
    token: MaybeToken,
    deposit_amount: u64,
) -> Result<(Vec<Instruction>, u32), Box<dyn std::error::Error>> {
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

    let marginfi_accounts = rpc_client.get_program_accounts_with_config(
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
                data_slice: Some(UiDataSliceConfig {
                    offset: 0,
                    length: 0,
                }),
                commitment: None,
                min_context_slot: None,
            },
            ..RpcProgramAccountsConfig::default()
        },
    )?;

    if marginfi_accounts.is_empty() {
        return Err(format!("No MarginFi account found for {}", address).into());
    }
    if marginfi_accounts.len() > 1 {
        return Err(format!("Multiple MarginFi account found for {}", address).into());
    }
    let marginfi_account = marginfi_accounts[0].0;

    let (bank, bank_liquidity_vault) = match token.token() {
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
            AccountMeta::new(marginfi_account, false),
            // Signer
            AccountMeta::new(address, true),
            // Bank
            AccountMeta::new(bank, false),
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

    Ok((instructions, 50_000))
}

fn kamino_deposit(
    pool: &str,
    address: Pubkey,
    token: MaybeToken,
    deposit_amount: u64,
) -> Result<(Vec<Instruction>, u32), Box<dyn std::error::Error>> {
    const KAMINO_LEND_PROGRAM: Pubkey = pubkey!["KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"];
    const FARMS_PROGRAM: Pubkey = pubkey!["FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr"];

    struct KaminoMarket {
        market_reserve: Vec<KaminoMarketReserve>,
        active_reserve: usize,
        reverse_refresh_obligation_reserve: bool,
        lending_market_authority: Pubkey,
        lending_market: Pubkey,
        reserve_farm_state: Pubkey,
        scope_prices: Pubkey,
        reserve_liquidity_supply: Pubkey,
        reserve_collateral_mint: Pubkey,
        reserve_destination_deposit_collateral: Pubkey,
    }

    struct KaminoMarketReserve {
        reserve: Pubkey,
        pyth_oracle: Pubkey,
    }

    let market = match pool {
        "kamino-main" => match token.token() {
            Some(Token::USDC) => Some(KaminoMarket {
                market_reserve: vec![
                    KaminoMarketReserve {
                        reserve: pubkey!["H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S"],
                        pyth_oracle: pubkey!["3vxLXJqLqF3JG5TCbYycbKWRBbCJQLxQmBGCkyqEEefL"],
                    },
                    KaminoMarketReserve {
                        reserve: pubkey!["D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"],
                        pyth_oracle: pubkey!["Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD"],
                    },
                ],
                active_reserve: 1,
                reverse_refresh_obligation_reserve: true,
                lending_market_authority: pubkey!["9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo"],
                lending_market: pubkey!["7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"],
                reserve_farm_state: pubkey!["JAvnB9AKtgPsTEoKmn24Bq64UMoYcrtWtq42HHBdsPkh"],
                scope_prices: pubkey!["3NJYftD5sjVfxSnUdZ1wVML8f3aC6mp1CXCL6L7TnU8C"],
                reserve_liquidity_supply: pubkey!["Bgq7trRgVMeq33yt235zM2onQ4bRDBsY5EWiTetF4qw6"],
                reserve_collateral_mint: pubkey!["B8V6WVjPxW1UGwVDfxH2d2r8SyT4cqn7dQRK6XneVa7D"],
                reserve_destination_deposit_collateral: pubkey![
                    "3DzjXRfxRm6iejfyyMynR4tScddaanrePJ1NJU2XnPPL"
                ],
            }),
            Some(Token::USDT) => Some(KaminoMarket {
                market_reserve: vec![
                    KaminoMarketReserve {
                        reserve: pubkey!["D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"],
                        pyth_oracle: pubkey!["Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD"],
                    },
                    KaminoMarketReserve {
                        reserve: pubkey!["H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S"],
                        pyth_oracle: pubkey!["3vxLXJqLqF3JG5TCbYycbKWRBbCJQLxQmBGCkyqEEefL"],
                    },
                ],
                active_reserve: 1,
                reverse_refresh_obligation_reserve: false,
                lending_market_authority: pubkey!["9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo"],
                lending_market: pubkey!["7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"],
                reserve_farm_state: pubkey!["5pCqu9RFdL6QoN7KK4gKnAU6CjQFJot8nU7wpFK8Zwou"],
                scope_prices: pubkey!["3NJYftD5sjVfxSnUdZ1wVML8f3aC6mp1CXCL6L7TnU8C"],
                reserve_liquidity_supply: pubkey!["2Eff8Udy2G2gzNcf2619AnTx3xM4renEv4QrHKjS1o9N"],
                reserve_collateral_mint: pubkey!["B8zf4kojJbwgCRKA7rLaLhRCZBGhgAJp8wPBVZZHMhSv"],
                reserve_destination_deposit_collateral: pubkey![
                    "CTCpzgNbPwWQSYamu4ZomgFuHf8DUGwq8hSYWVLurSJD"
                ],
            }),
            _ => None,
        },
        "kamino-altcoins" => {
            if token.token() == Some(Token::USDC) {
                Some(KaminoMarket {
                    market_reserve: vec![KaminoMarketReserve {
                        reserve: pubkey!["9TD2TSv4pENb8VwfbVYg25jvym7HN6iuAR6pFNSrKjqQ"],
                        pyth_oracle: pubkey!["Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD"],
                    }],
                    active_reserve: 0,
                    reverse_refresh_obligation_reserve: false,
                    lending_market_authority: pubkey![
                        "81BgcfZuZf9bESLvw3zDkh7cZmMtDwTPgkCvYu7zx26o"
                    ],
                    lending_market: pubkey!["ByYiZxp8QrdN9qbdtaAiePN8AAr3qvTPppNJDpf5DVJ5"],
                    reserve_farm_state: pubkey!["23UsLhyeuZBCRJNVFkPrmMCfXuka8hQa8S6spXwTEHcc"],
                    scope_prices: pubkey!["3NJYftD5sjVfxSnUdZ1wVML8f3aC6mp1CXCL6L7TnU8C"],
                    reserve_liquidity_supply: pubkey![
                        "HTyrXvSvBbD7WstvU3oqFTBZM1fPZJPxVRvwLAmCTDyJ"
                    ],
                    reserve_collateral_mint: pubkey![
                        "A2mcvn3kQXwG9XPUPgjghXJDqvYHTpkCJE3wtKqU1VRn"
                    ],
                    reserve_destination_deposit_collateral: pubkey![
                        "8bGWMt65Y7RV2DV5sxNRxFM5jsUhBMSo8u24pbRPjQLY"
                    ],
                })
            } else {
                None
            }
        }
        "kamino-jup" => {
            if token.token() == Some(Token::USDC) {
                Some(KaminoMarket {
                    market_reserve: vec![KaminoMarketReserve {
                        reserve: pubkey!["Ga4rZytCpq1unD4DbEJ5bkHeUz9g3oh9AAFEi6vSauXp"],
                        pyth_oracle: pubkey!["Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD"],
                    }],
                    active_reserve: 0,
                    reverse_refresh_obligation_reserve: false,
                    lending_market_authority: pubkey![
                        "B9spsrMK6pJicYtukaZzDyzsUQLgc3jbx5gHVwdDxb6y"
                    ],
                    lending_market: pubkey!["DxXdAyU3kCjnyggvHmY5nAwg5cRbbmdyX3npfDMjjMek"],
                    reserve_farm_state: pubkey!["EGDhupegCXLtonYDSY67c4dzw86S9eMxsntQ1yxWSoHv"],
                    scope_prices: pubkey!["3NJYftD5sjVfxSnUdZ1wVML8f3aC6mp1CXCL6L7TnU8C"],
                    reserve_liquidity_supply: pubkey![
                        "GENey8es3EgGiNTM8H8gzA3vf98haQF8LHiYFyErjgrv"
                    ],
                    reserve_collateral_mint: pubkey![
                        "32XLsweyeQwWgLKRVAzS72nxHGU1JmmNQQZ3C3q6fBjJ"
                    ],
                    reserve_destination_deposit_collateral: pubkey![
                        "6WnymZBTAekuHf9DgsaDKJ397oEZ3qMApNMHg9qjqhgm"
                    ],
                })
            } else {
                None
            }
        }
        _ => None,
    }
    .ok_or_else(|| format!("Depositing {token} into {pool} is not supported"))?;

    let market_obligation = Pubkey::find_program_address(
        &[
            &[0],
            &[0],
            &address.to_bytes(),
            &market.lending_market.to_bytes(),
            &system_program::ID.to_bytes(),
            &system_program::ID.to_bytes(),
        ],
        &KAMINO_LEND_PROGRAM,
    )
    .0;

    let market_obligation_farm_user_state = Pubkey::find_program_address(
        &[
            b"user",
            &market.reserve_farm_state.to_bytes(),
            &market_obligation.to_bytes(),
        ],
        &FARMS_PROGRAM,
    )
    .0;

    let mut instructions = vec![];

    // Instruction: Kamino: Refresh Reserve
    for market_reserve in &market.market_reserve {
        instructions.push(Instruction::new_with_bytes(
            KAMINO_LEND_PROGRAM,
            &[0x02, 0xda, 0x8a, 0xeb, 0x4f, 0xc9, 0x19, 0x66],
            vec![
                // Reserve
                AccountMeta::new(market_reserve.reserve, false),
                // Lending Market
                AccountMeta::new_readonly(market.lending_market, false),
                // Pyth Oracle
                AccountMeta::new_readonly(market_reserve.pyth_oracle, false),
                // Switchboard Price Oracle
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Switchboard Twap Oracle
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Scope Prices
                AccountMeta::new_readonly(market.scope_prices, false),
            ],
        ));
    }

    // Instruction: Kamino: Refresh Obligation
    let mut refresh_obligation_account_metas = vec![
        // Lending Market
        AccountMeta::new_readonly(market.lending_market, false),
        // Obligation
        AccountMeta::new(market_obligation, false),
    ];

    let market_reserve_iter: Vec<&KaminoMarketReserve> =
        if market.reverse_refresh_obligation_reserve {
            market.market_reserve.iter().rev().collect()
        } else {
            market.market_reserve.iter().collect()
        };

    for market_reserve in &market_reserve_iter {
        refresh_obligation_account_metas.push(AccountMeta::new(market_reserve.reserve, false));
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
            AccountMeta::new(market.lending_market_authority, false),
            // Reserve
            AccountMeta::new(market.market_reserve[market.active_reserve].reserve, false),
            // Reserve Farm State
            AccountMeta::new(market.reserve_farm_state, false),
            // Obligation Farm User State
            AccountMeta::new(market_obligation_farm_user_state, false),
            // Lending Market
            AccountMeta::new_readonly(market.lending_market, false),
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
            AccountMeta::new_readonly(market.lending_market, false),
            // Lending Market Authority
            AccountMeta::new(market.lending_market_authority, false),
            // Reserve
            AccountMeta::new(market.market_reserve[market.active_reserve].reserve, false),
            // Reserve Liquidity Supply
            AccountMeta::new(market.reserve_liquidity_supply, false),
            // Reserve Collateral Mint
            AccountMeta::new(market.reserve_collateral_mint, false),
            // Reserve Destination Deposit Collateral
            AccountMeta::new(market.reserve_destination_deposit_collateral, false),
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

    Ok((instructions, 1_500_000))
}
