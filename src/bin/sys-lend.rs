use {
    clap::{value_t, value_t_or_exit, values_t, App, AppSettings, Arg, SubCommand},
    solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig},
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{
            RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSimulateTransactionAccountsConfig,
            RpcSimulateTransactionConfig,
        },
        rpc_filter::{self, RpcFilterType},
    },
    solana_sdk::{
        address_lookup_table::{state::AddressLookupTable, AddressLookupTableAccount},
        clock::Slot,
        commitment_config::CommitmentConfig,
        instruction::{AccountMeta, Instruction},
        message::{self, Message, VersionedMessage},
        native_token::{lamports_to_sol, sol_to_lamports},
        program_pack::Pack,
        pubkey,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        system_instruction, system_program, sysvar,
        transaction::{Transaction, VersionedTransaction},
    },
    std::collections::{BTreeMap, HashMap, HashSet},
    sys::{
        metrics,
        notifier::*,
        priority_fee::{apply_priority_fee, PriorityFee},
        send_transaction_until_expired,
        token::*,
        vendor::{
            kamino, marginfi_v2,
            solend::{self, math::TryMul},
        },
        *,
    },
};

lazy_static::lazy_static! {
    static ref SUPPORTED_TOKENS: HashMap<&'static str, HashSet::<Token>> = HashMap::from([
        ("solend-main", HashSet::from([
            Token::USDC,
            Token::USDT,
            Token::wSOL,
        ])) ,
        ("solend-turbosol", HashSet::from([
            Token::USDC,
        ])) ,
        ("solend-jlp", HashSet::from([
            Token::USDC,
            Token::wSOL,
        ])) ,
        ("mfi", HashSet::from([
            Token::USDC,
            Token::USDT,
            Token::UXD,
            Token::wSOL,
        ])) ,
        ("kamino-main", HashSet::from([
            Token::USDC,
            Token::USDT,
            Token::JitoSOL,
            Token::wSOL,
        ])) ,
        ("kamino-jlp", HashSet::from([
            Token::USDC,
            Token::JLP,
        ])) ,
        ("kamino-altcoins", HashSet::from([
            Token::USDC,
            Token::JUP,
            Token::JTO,
            Token::PYTH,
            Token::WEN,
            Token::WIF,
            Token::BONK,
        ]))
    ]);
}

#[derive(PartialEq, Clone, Copy, Debug)]
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
    }

    pub fn supply_apy(pool: &str, maybe_token: MaybeToken, apy_bps: u64) -> metrics::Point {
        metrics::Point::new("sys_lend::supply_apy")
            .tag("pool", pool)
            .tag("token", maybe_token.name())
            .field("apy_bps", apy_bps as f64)
    }

    pub fn total_supply_balance_and_apy(
        address: &Pubkey,
        maybe_token: MaybeToken,
        ui_amount: f64,
        apy_bps: u64,
    ) -> metrics::Point {
        metrics::Point::new("sys_lend::total_supply_balance_and_apy")
            .tag("address", metrics::dp::pubkey_to_value(address))
            .tag("token", maybe_token.name())
            .field("amount", ui_amount)
            .field("apy_bps", apy_bps as f64)
    }

    pub fn principal_balance_change(
        pool: &str,
        address: &Pubkey,
        maybe_token: MaybeToken,
        ui_amount: f64,
    ) -> metrics::Point {
        metrics::Point::new("sys_lend::principal_balance_change")
            .tag("pool", pool)
            .tag("address", metrics::dp::pubkey_to_value(address))
            .tag("token", maybe_token.name())
            .field("amount", ui_amount)
    }

    pub fn priority_fee(
        command: &str,
        address: &Pubkey,
        maybe_token: MaybeToken,
        priority_fee: f64,
    ) -> metrics::Point {
        metrics::Point::new("sys_lend::priority_fee")
            .tag("command", command)
            .tag("address", metrics::dp::pubkey_to_value(address))
            .tag("token", maybe_token.name())
            .field("priority_fee", priority_fee)
    }
}

fn is_token_supported(token: &Token, pools: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    for pool in pools {
        if !SUPPORTED_TOKENS.get(pool.as_str()).unwrap().contains(token) {
            return Err(format!("{token} is not supported by {pool}").into());
        }
    }

    Ok(())
}

fn supported_pools_for_token(token: Token) -> Vec<String> {
    let mut supported_tokens: Vec<_> = SUPPORTED_TOKENS
        .iter()
        .filter_map(|(pool, tokens)| {
            if tokens.contains(&token) {
                Some(pool.to_string())
            } else {
                None
            }
        })
        .collect();
    supported_tokens.sort();
    supported_tokens
}

#[derive(Clone)]
struct AccountDataCache<'a> {
    cache: HashMap<Pubkey, (Vec<u8>, Slot)>,
    rpc_client: &'a RpcClient,
}

impl<'a> AccountDataCache<'a> {
    fn new(rpc_client: &'a RpcClient) -> Self {
        Self {
            cache: HashMap::default(),
            rpc_client,
        }
    }

    fn address_cached(&mut self, address: &Pubkey) -> bool {
        self.cache.contains_key(address)
    }

    fn get(&mut self, address: Pubkey) -> Result<(&[u8], Slot), Box<dyn std::error::Error>> {
        if !self.address_cached(&address) {
            let result = self
                .rpc_client
                .get_account_with_commitment(&address, self.rpc_client.commitment())?;
            self.cache.insert(
                address,
                (result.value.unwrap_or_default().data, result.context.slot),
            );
        }

        self.cache
            .get(&address)
            .map(|(data, context_slot)| (data.as_ref(), *context_slot))
            .ok_or_else(|| format!("{address} not present in cache").into())
    }

    fn simulate_then_add(
        &mut self,
        instructions: &[Instruction],
        fee_payer: Option<Pubkey>,
        address_lookup_table_accounts: &[AddressLookupTableAccount],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut writable_addresses: Vec<_> = instructions
            .iter()
            .flat_map(|instruction| {
                instruction
                    .accounts
                    .iter()
                    .filter_map(|account_meta| {
                        account_meta.is_writable.then_some(account_meta.pubkey)
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        writable_addresses.sort();
        writable_addresses.dedup();

        let fee_payer = fee_payer.unwrap_or(pubkey!["mvinesvseigL3uSWwSQr5tp8KX67kX2Ys6zydT9Wnbo"]); // TODO: Any fee payer will do. For now hard code one

        let transaction: VersionedTransaction = if address_lookup_table_accounts.is_empty() {
            Transaction::new_unsigned(Message::new(instructions, Some(&fee_payer))).into()
        } else {
            let signer_count = {
                let mut signer_addresses: Vec<_> = instructions
                    .iter()
                    .flat_map(|instruction| {
                        instruction
                            .accounts
                            .iter()
                            .filter_map(|account_meta| {
                                account_meta.is_signer.then_some(account_meta.pubkey)
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect();
                signer_addresses.push(fee_payer);
                signer_addresses.sort();
                signer_addresses.dedup();
                signer_addresses.len()
            };

            VersionedTransaction {
                signatures: [solana_sdk::signature::Signature::default()].repeat(signer_count),
                message: VersionedMessage::V0(message::v0::Message::try_compile(
                    &fee_payer,
                    instructions,
                    address_lookup_table_accounts,
                    solana_sdk::hash::Hash::default(),
                )?),
            }
        };

        let result = self.rpc_client.simulate_transaction_with_config(
            &transaction,
            RpcSimulateTransactionConfig {
                sig_verify: false,
                replace_recent_blockhash: true,
                commitment: Some(CommitmentConfig::processed()),
                accounts: Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64Zstd),
                    addresses: writable_addresses
                        .iter()
                        .map(|address| address.to_string())
                        .collect(),
                }),
                ..RpcSimulateTransactionConfig::default()
            },
        )?;

        if let Some(err) = result.value.err {
            return Err(format!(
                "Failed to simulate instructions: {err} [logs: {:?}]",
                result.value.logs
            )
            .into());
        }
        let writable_accounts = result.value.accounts.expect("accounts");
        if writable_accounts.len() != writable_addresses.len() {
            return Err("Return address length mismatch".into());
        }

        for (address, account) in writable_addresses.iter().zip(&writable_accounts) {
            let account_data = account
                .as_ref()
                .unwrap()
                .decode::<solana_sdk::account::Account>()
                .unwrap()
                .data;

            // Always update cache even if `address` is already cached
            self.cache
                .insert(*address, (account_data, result.context.slot));
        }
        Ok(())
    }
}

fn pool_supply_apr(
    pool: &str,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<f64, Box<dyn std::error::Error>> {
    Ok(if pool.starts_with("kamino-") {
        kamino_apr(pool, token, account_data_cache)?
    } else if pool.starts_with("solend-") {
        solend_apr(pool, token, account_data_cache)?
    } else if pool == "mfi" {
        mfi_apr(token, account_data_cache)?
    } else {
        unreachable!()
    })
}

fn pools_supply_apr(
    pools: &[String],
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<BTreeMap<String, f64>, Box<dyn std::error::Error>> {
    let mut supply_apr = BTreeMap::new();
    for pool in pools {
        supply_apr.insert(
            pool.to_string(),
            pool_supply_apr(pool, token, account_data_cache)?,
        );
    }
    Ok(supply_apr)
}

fn pool_supply_balance(
    pool: &str,
    token: Token,
    address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<(/*balance: */ u64, /* available_balance: */ u64), Box<dyn std::error::Error>> {
    Ok(if pool.starts_with("kamino-") {
        kamino_deposited_amount(pool, address, token, account_data_cache)?
    } else if pool.starts_with("solend-") {
        solend_deposited_amount(pool, address, token, account_data_cache)?
    } else if pool == "mfi" {
        mfi_deposited_amount(address, token, account_data_cache)?
    } else {
        unreachable!()
    })
}

type PoolSupplyBalanceMap = BTreeMap<
    String,
    (
        /* apr: */ f64,
        /* balance: */ u64,
        /* available_balance: */ u64,
    ),
>;

fn pools_supply_balance(
    pools: &[String],
    token: Token,
    address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<PoolSupplyBalanceMap, Box<dyn std::error::Error>> {
    let mut supply_balance = BTreeMap::new();
    for pool in pools {
        let apr = pool_supply_apr(pool, token, account_data_cache)?;
        let (balance, available_balance) =
            pool_supply_balance(pool, token, address, account_data_cache)?;
        supply_balance.insert(pool.to_string(), (apr, balance, available_balance));
    }
    Ok(supply_balance)
}

fn pools_supply_balance_apr(
    pools_supply_balance: &PoolSupplyBalanceMap,
) -> (/* total_apr: */ f64, /* total_balance: */ u64) {
    let mut total_balance = 0;
    let mut weighted_sum = 0.;

    for (apr, balance, _available_balance) in pools_supply_balance.values() {
        weighted_sum += apr * *balance as f64;
        total_balance += balance;
    }

    let total_apr = if total_balance == 0 {
        0.
    } else {
        weighted_sum / total_balance as f64
    };
    (total_apr, total_balance)
}

const TOKEN_ACCOUNT_REQUIRED_LAMPORTS: u64 = 2_039_280;

struct InstructionsForOps {
    instructions: Vec<Instruction>,
    required_compute_units: u32,
    address_lookup_table_accounts: Vec<AddressLookupTableAccount>,
    simulation_total_apy: f64,
}

async fn build_instructions_for_ops<'a>(
    account_data_cache: &mut AccountDataCache<'a>,
    pools: &[String],
    ops: &[(Operation, &String)],
    mut amount: u64,
    address: Pubkey,
    token: Token,
    wrap_unwrap_sol: bool,
) -> Result<InstructionsForOps, Box<dyn std::error::Error>> {
    let mut instructions = vec![];
    let mut address_lookup_tables = vec![];
    let mut required_compute_units = 0;

    for (op, pool) in ops {
        let result = if pool.starts_with("kamino-") {
            kamino_deposit_or_withdraw(*op, pool, address, token, amount, account_data_cache)?
        } else if pool.starts_with("solend-") {
            solend_deposit_or_withdraw(*op, pool, address, token, amount, account_data_cache)?
        } else if *pool == "mfi" {
            mfi_deposit_or_withdraw(*op, address, token, amount, false, account_data_cache)?
        } else {
            unreachable!();
        };

        match op {
            Operation::Deposit => {
                if wrap_unwrap_sol {
                    // Wrap SOL into wSOL
                    instructions.extend(vec![
                                    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                                        &address,
                                        &address,
                                        &token.mint(),
                                        &spl_token::id(),
                                    ),
                                    system_instruction::transfer(&address, &token.ata(&address), amount),
                                    spl_token::instruction::sync_native(&spl_token::id(), &token.ata(&address)).unwrap(),
                                ]);
                    required_compute_units += 20_000;
                }
            }
            Operation::Withdraw => {
                // Ensure the destination token account exists
                instructions.push(
                                    spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                                        &address,
                                        &address,
                                        &token.mint(),
                                        &spl_token::id(),
                                    ),
                                );
                required_compute_units += 25_000;
            }
        }

        instructions.extend(result.instructions);
        if let Some(address_lookup_table) = result.address_lookup_table {
            address_lookup_tables.push(address_lookup_table);
        }
        required_compute_units += result.required_compute_units;
        amount = result.amount;

        if wrap_unwrap_sol && *op == Operation::Withdraw {
            // Unwrap wSOL into SOL

            let seed = &Keypair::new().pubkey().to_string()[..31];
            let ephemeral_token_account =
                Pubkey::create_with_seed(&address, seed, &spl_token::id()).unwrap();

            instructions.extend(vec![
                system_instruction::create_account_with_seed(
                    &address,
                    &ephemeral_token_account,
                    &address,
                    seed,
                    TOKEN_ACCOUNT_REQUIRED_LAMPORTS,
                    spl_token::state::Account::LEN as u64,
                    &spl_token::id(),
                ),
                spl_token::instruction::initialize_account(
                    &spl_token::id(),
                    &ephemeral_token_account,
                    &token.mint(),
                    &address,
                )
                .unwrap(),
                spl_token::instruction::transfer_checked(
                    &spl_token::id(),
                    &token.ata(&address),
                    &token.mint(),
                    &ephemeral_token_account,
                    &address,
                    &[],
                    amount,
                    token.decimals(),
                )
                .unwrap(),
                spl_token::instruction::close_account(
                    &spl_token::id(),
                    &ephemeral_token_account,
                    &address,
                    &address,
                    &[],
                )
                .unwrap(),
            ]);

            required_compute_units += 30_000;
        }
    }

    let address_lookup_table_accounts = address_lookup_tables
        .into_iter()
        .map(|address_lookup_table_address| {
            account_data_cache
                .get(address_lookup_table_address)
                .and_then(|(address_lookup_table_data, _context_slot)| {
                    AddressLookupTable::deserialize(address_lookup_table_data)
                        .map_err(|err| err.into())
                })
                .map(|address_lookup_table| AddressLookupTableAccount {
                    key: address_lookup_table_address,
                    addresses: address_lookup_table.addresses.to_vec(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Simulate the transaction's instructions and cache the resulting account changes
    let mut simulation_account_data_cache = account_data_cache.clone();
    simulation_account_data_cache.simulate_then_add(
        &instructions,
        Some(address),
        &address_lookup_table_accounts,
    )?;

    let simulation_supply_balance =
        pools_supply_balance(pools, token, address, &mut simulation_account_data_cache)?;

    let simulation_total_apy =
        apr_to_apy(pools_supply_balance_apr(&simulation_supply_balance).0) * 100.;

    Ok(InstructionsForOps {
        instructions,
        required_compute_units,
        address_lookup_table_accounts,
        simulation_total_apy,
    })
}

async fn send_instructions_for_ops<T: solana_sdk::signers::Signers + ?Sized>(
    rpc_clients: &RpcClients,
    address: Pubkey,
    instructions_for_ops: InstructionsForOps,
    priority_fee: PriorityFee,
    dry_run: bool,
    signers: &T,
) -> Result<
    (
        Signature,
        /*priority_fee_lamports: */ u64,
        /*transaction_confirmed:*/ Option<bool>,
    ),
    Box<dyn std::error::Error>,
> {
    let rpc_client = rpc_clients.default();

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let (transaction, priority_fee) = {
        let mut instructions = instructions_for_ops.instructions;

        let priority_fee = apply_priority_fee(
            rpc_clients,
            &mut instructions,
            instructions_for_ops.required_compute_units,
            priority_fee,
        )?;

        let message = message::v0::Message::try_compile(
            &address,
            &instructions,
            &instructions_for_ops.address_lookup_table_accounts,
            recent_blockhash,
        )?;
        (
            VersionedTransaction::try_new(VersionedMessage::V0(message), signers)?,
            priority_fee,
        )
    };

    let signature = transaction.signatures[0];

    let transaction_confirmed = if dry_run {
        println!("Dry run. Will not send transaction");
        None
    } else {
        send_transaction_until_expired(rpc_clients, &transaction, last_valid_block_height)
    };

    Ok((signature, priority_fee, transaction_confirmed))
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
            Arg::with_name("send_json_rpc_urls")
                .long("send-url")
                .value_name("URL")
                .takes_value(true)
                .validator(is_comma_separated_url_or_moniker_list)
                .help("Optional additional JSON RPC URLs, separated by commas, to \
                       submit transactions with in addition to --url"),

        )
        .arg(
            Arg::with_name("helius_json_rpc_url")
                .long("helius-url")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url)
                .help("Helium JSON RPC URL to use only for the proprietary getPriorityFeeEstimate RPC method"),
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
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool to deposit into. \
                              If multiple pools are provided, the each pool is probed and the one with the highest APY is selected \
                              [default: all support pools for the specified token]")
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
                    Arg::with_name("ui_amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount_or_all)
                        .required(true)
                        .default_value("ALL")
                        .help("The amount of tokens to deposit; accepts keyword ALL"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("TOKEN")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to deposit"),
                )
                .arg(
                    Arg::with_name("minimum_apy")
                        .long("minimum-apy")
                        .value_name("BPS")
                        .takes_value(true)
                        .validator(is_parsable::<u16>)
                        .help("Do not deposit if the resulting APY is less than this amount of BPS")
                )
                .arg(
                    Arg::with_name("minimum_ui_amount")
                        .long("minimum-amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_parsable::<f64>)
                        .default_value("0.01")
                        .help("Do not deposit if AMOUNT is less than this value")
                )
                .arg(
                    Arg::with_name("retain_ui_amount")
                        .long("retain")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount)
                        .help("Amount of tokens to always leave in the wallet regardless of requested deposit AMOUNT \
                              [default: 0.01 for SOL, 0.0 for all other tokens]"),
                )
                .arg(
                    Arg::with_name("dry_run")
                        .long("dry-run")
                        .takes_value(false)
                )
        )
        .subcommand(
            SubCommand::with_name("withdraw")
                .about("Withdraw tokens from a lending pool")
                .arg(
                    Arg::with_name("pool")
                        .long("from")
                        .value_name("POOL")
                        .takes_value(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool to withdraw from. \
                               If multiple pools are provided, the pool with the lowest APY is selected \
                               [default: all support pools for the specified token]")
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
                    Arg::with_name("ui_amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount_or_all)
                        .required(true)
                        .help("The amount of tokens to withdraw; accepts keyword ALL"),
                )
                .arg(
                    Arg::with_name("minimum_ui_amount")
                        .long("minimum-amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_parsable::<f64>)
                        .default_value("0.0")
                        .help("Do not withdraw if AMOUNT is less than this value")
                )
                .arg(
                    Arg::with_name("retain_ui_amount")
                        .long("retain")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount)
                        .default_value("0.0")
                        .help("Amount of tokens to always leave in the pool regardless of requested withdrawal AMOUNT"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("TOKEN")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to withdraw"),
                )
                .arg(
                    Arg::with_name("dry_run")
                        .long("dry-run")
                        .takes_value(false)
                )
        )
        .subcommand(
            SubCommand::with_name("rebalance")
                .about("Rebalance tokens between lending pools")
                .arg(
                    Arg::with_name("pool")
                        .long("with")
                        .value_name("POOLS")
                        .takes_value(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool to rebalance with. \
                              Tokens from the pool with the lowest APY will be moved \
                              to the pool with the highest APY \
                              [default: all supported pools for the specified token]")
                )
                .arg(
                    Arg::with_name("pool_from")
                        .long("from")
                        .value_name("POOL")
                        .takes_value(true)
                        .possible_values(&pools)
                        .help("Declare the lending pool to withdraw from \
                               [default: The behaviour described by the --pools argument]")
                )
                .arg(
                    Arg::with_name("pool_into")
                        .long("into")
                        .value_name("POOL")
                        .takes_value(true)
                        .possible_values(&pools)
                        .help("Declare the lending pool to deposit into \
                               [default: The behaviour described by the --pools argument]")
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
                    Arg::with_name("ui_amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount_or_all)
                        .required(true)
                        .help("The amount of tokens to rebalance; accepts keyword ALL"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("TOKEN")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token to rebalance"),
                )
                .arg(
                    Arg::with_name("minimum_apy")
                        .long("minimum-apy-improvement")
                        .value_name("BPS")
                        .takes_value(true)
                        .validator(is_parsable::<u16>)
                        .default_value("100")
                        .help("Skip rebalance if the APY improvement would be less than this amount of BPS")
                )
                .arg(
                    Arg::with_name("minimum_ui_amount")
                        .long("minimum-amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_parsable::<f64>)
                        .default_value("1.0")
                        .help("Do not rebalance an AMOUNT less than this value")
                )
                .arg(
                    Arg::with_name("maximum_ui_amount")
                        .long("maximum-amount")
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_parsable::<f64>)
                        .help("Do not rebalance an AMOUNT greater than this value [default: AMOUNT]")
                )
                .arg(
                    Arg::with_name("rebalance_amount_step_count")
                        .long("amount-step-count")
                        .value_name("NUMBER")
                        .takes_value(true)
                        .validator(is_parsable::<u64>)
                        .default_value("1")
                        .help("Number of incremental amount steps to try")
                )
                .arg(
                    Arg::with_name("dry_run")
                        .long("dry-run")
                        .takes_value(false)
                )
                .arg(
                    Arg::with_name("skip_apy_check")
                        .long("skip-apy-check")
                        .takes_value(false)
                        .help("Skip the --minimum-apy-improvement check"),
                )
        )
        .subcommand(
            SubCommand::with_name("supply-balance")
                .about("Display the current supplied balance for one or more lending pools")
                .alias("balance")
                .arg(
                    Arg::with_name("pool")
                        .value_name("POOL")
                        .long("for")
                        .takes_value(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool [default: all supported pools for the specified token]"),
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
                        .value_name("TOKEN")
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
                    Arg::with_name("total_only")
                        .long("total-only")
                        .takes_value(false)
                        .help("Only display the sum the balances in the pools"),
                )
        )
        .subcommand(
            SubCommand::with_name("supply-apy")
                .about("Display the current supply APY for one or more lending pools")
                .alias("apy")
                .arg(
                    Arg::with_name("pool")
                        .value_name("POOL")
                        .long("for")
                        .takes_value(true)
                        .multiple(true)
                        .possible_values(&pools)
                        .help("Lending pool [default: all supported pools for the specified token]"),
                )
                .arg(
                    Arg::with_name("token")
                        .value_name("TOKEN")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_token_or_sol)
                        .default_value("USDC")
                        .help("Token"),
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

    let rpc_clients = RpcClients::new(
        value_t_or_exit!(app_matches, "json_rpc_url", String),
        value_t!(app_matches, "send_json_rpc_urls", String).ok(),
        value_t!(app_matches, "helius_json_rpc_url", String).ok(),
    );
    let rpc_client = rpc_clients.default();
    let mut account_data_cache = AccountDataCache::new(rpc_client);

    let priority_fee = if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_exact", f64)
    {
        PriorityFee::Exact {
            lamports: sol_to_lamports(ui_priority_fee),
        }
    } else if let Ok(ui_priority_fee) = value_t!(app_matches, "priority_fee_auto", f64) {
        PriorityFee::default_auto_percentile(sol_to_lamports(ui_priority_fee))
    } else {
        PriorityFee::default_auto()
    };

    let mut wallet_manager = None;
    let notifier = Notifier::default();

    match app_matches.subcommand() {
        ("supply-apy", Some(matches)) => {
            let maybe_token = MaybeToken::from(value_t!(matches, "token", Token).ok());
            let token = maybe_token.token().unwrap_or(Token::wSOL);
            let raw = matches.is_present("raw");
            let bps = matches.is_present("bps");
            let pools = values_t!(matches, "pool", String)
                .ok()
                .unwrap_or_else(|| supported_pools_for_token(token));

            is_token_supported(&token, &pools)?;

            let supply_apr = pools_supply_apr(&pools, token, &mut account_data_cache)?;

            for (pool, apr) in supply_apr {
                let apy = apr_to_apy(apr) * 100.;
                let apy_as_bps = (apy * 100.) as u64;

                let value = if bps {
                    format!("{}", apy_as_bps)
                } else {
                    format!("{:.2}", apy_as_bps as f64 / 100.)
                };

                let msg = if raw {
                    value.to_string()
                } else {
                    format!(
                        "{pool:>15}: {maybe_token} {value:>5}{}",
                        if bps { "bps" } else { "%" }
                    )
                };
                if !raw {
                    notifier.send(&msg).await;
                }
                metrics::push(dp::supply_apy(&pool, maybe_token, apy_as_bps)).await;
                println!("{msg}");
            }
        }
        ("supply-balance", Some(matches)) => {
            let address = pubkey_of(matches, "address").unwrap();
            let maybe_token = MaybeToken::from(value_t!(matches, "token", Token).ok());
            let token = maybe_token.token().unwrap_or(Token::wSOL);
            let raw = matches.is_present("raw");
            let total_only = matches.is_present("total_only");

            let pools = values_t!(matches, "pool", String)
                .ok()
                .unwrap_or_else(|| supported_pools_for_token(token));

            is_token_supported(&token, &pools)?;

            let supply_balance =
                pools_supply_balance(&pools, token, address, &mut account_data_cache)?;

            for pool in &pools {
                let (apr, balance, available_balance) = *supply_balance.get(pool).unwrap();

                let msg = format!(
                    "{:>15}: {} supplied at {:.2}%{}",
                    pool,
                    maybe_token.format_amount(balance),
                    apr_to_apy(apr) * 100.,
                    if available_balance < balance {
                        format!(
                            ", with {} available to withdraw",
                            maybe_token.format_amount(available_balance)
                        )
                    } else {
                        "".into()
                    }
                );
                notifier.send(&msg).await;
                metrics::push(dp::supply_balance(
                    pool,
                    &address,
                    maybe_token,
                    token.ui_amount(balance),
                ))
                .await;
                if !total_only {
                    if raw {
                        println!("{}", token.ui_amount(balance))
                    } else {
                        println!("{msg}")
                    }
                }
            }

            let (total_apr, total_balance) = pools_supply_balance_apr(&supply_balance);
            let total_apy = apr_to_apy(total_apr) * 100.;

            metrics::push(dp::total_supply_balance_and_apy(
                &address,
                maybe_token,
                token.ui_amount(total_balance),
                (total_apy * 100.) as u64,
            ))
            .await;

            if raw && total_only {
                println!("{}", token.ui_amount(total_balance));
            }
            if !raw {
                println!();
                println!(
                    "Total supply:    {} at {:.2}%",
                    token.format_amount(total_balance),
                    total_apy,
                );

                let wallet_balance = maybe_token.balance(rpc_client, &address)?;
                println!(
                    "Wallet balance:  {}",
                    maybe_token.format_amount(wallet_balance)
                );
            }
        }
        ("deposit" | "withdraw" | "rebalance", Some(matches)) => {
            #[derive(Debug, PartialEq, Clone, Copy)]
            enum Command {
                Deposit,
                Withdraw,
                Rebalance,
            }

            let cmd = match app_matches.subcommand().0 {
                "withdraw" => Command::Withdraw,
                "deposit" => Command::Deposit,
                "rebalance" => Command::Rebalance,
                _ => unreachable!(),
            };

            let (signer, address) = signer_of(matches, "signer", &mut wallet_manager)?;
            let address = address.expect("address");
            let signer = signer.expect("signer");
            let dry_run = matches.is_present("dry_run");
            let skip_apy_check = matches.is_present("skip_apy_check");

            let maybe_token = MaybeToken::from(value_t!(matches, "token", Token).ok());
            let token = maybe_token.token().unwrap_or(Token::wSOL);

            let pool_from = value_t!(matches, "pool_from", String).ok();
            let pool_into = value_t!(matches, "pool_into", String).ok();

            let pools = {
                let mut pools = values_t!(matches, "pool", String)
                    .ok()
                    .unwrap_or_else(|| supported_pools_for_token(token));

                if let Some(pool_from) = &pool_from {
                    pools.push(pool_from.clone());
                }
                if let Some(pool_into) = &pool_into {
                    pools.push(pool_into.clone());
                }
                pools.sort();
                pools.dedup();
                pools
            };

            is_token_supported(&token, &pools)?;

            let minimum_apy_bps = value_t!(matches, "minimum_apy", u16).unwrap_or(0);
            let minimum_apy = minimum_apy_bps as f64 / 100.;
            let rebalance_amount_step_count = value_t!(matches, "rebalance_amount_step_count", u64)
                .unwrap_or_default()
                .clamp(1, 9);

            let minimum_amount = {
                let minimum_amount =
                    maybe_token.amount(value_t_or_exit!(matches, "minimum_ui_amount", f64));
                if minimum_amount == 0 {
                    1
                } else {
                    minimum_amount
                }
            };
            let maximum_amount = value_t!(matches, "maximum_ui_amount", f64)
                .ok()
                .map(|ui_amount| maybe_token.amount(ui_amount));

            let retain_amount =
                maybe_token.amount(value_t!(matches, "retain_ui_amount", f64).unwrap_or(0.));

            let address_token_balance = maybe_token.balance(rpc_client, &address)?.saturating_sub(
                if maybe_token.is_sol() {
                    // Never drain all the SOL from `address`.
                    // Warning: if this is ever changed from 0.01, update the --help for the
                    // `retain_ui_amount` arg
                    retain_amount.max(sol_to_lamports(0.01))
                } else {
                    retain_amount
                },
            );

            let requested_amount = match matches.value_of("ui_amount").unwrap() {
                "ALL" => u64::MAX,
                ui_amount => token.amount(ui_amount.parse::<f64>().unwrap()),
            };

            let supply_balance =
                pools_supply_balance(&pools, token, address, &mut account_data_cache)?;
            let total_apy = apr_to_apy(pools_supply_balance_apr(&supply_balance).0) * 100.;

            assert_eq!(
                TOKEN_ACCOUNT_REQUIRED_LAMPORTS,
                rpc_client
                    .get_minimum_balance_for_rent_exemption(spl_token::state::Account::LEN)?
            );

            struct OperationInfo {
                op_msg: String,
                deposit_pool_and_amount: Option<(String, u64)>,
                withdraw_pool_and_amount: Option<(String, u64)>,
                instructions_for_ops: InstructionsForOps,
            }

            #[derive(Default)]
            struct BestOperationInfo {
                maybe: Option<OperationInfo>,
            }
            impl BestOperationInfo {
                fn replace_if_greater_total_apy(&mut self, info: OperationInfo) {
                    if info.instructions_for_ops.simulation_total_apy
                        >= self
                            .maybe
                            .as_ref()
                            .map(|best_info| best_info.instructions_for_ops.simulation_total_apy)
                            .unwrap_or_default()
                    {
                        self.maybe = Some(info);
                    }
                }
            }

            let mut best_operation = BestOperationInfo::default();

            match cmd {
                Command::Deposit => {
                    assert_eq!(maximum_amount, None); // --maximum_amount is only supported by Rebalance
                    assert!(pool_from.is_none()); // Rebalance only
                    assert!(pool_into.is_none()); // Rebalance only

                    let requested_amount = match requested_amount {
                        u64::MAX => address_token_balance,
                        requested_amount => requested_amount,
                    };

                    if address_token_balance < minimum_amount {
                        println!(
                            "Minimum deposit amount of {} is greater than current wallet balance of {}",
                            maybe_token.format_amount(minimum_amount),
                            maybe_token.format_amount(address_token_balance),
                        );
                        return Ok(());
                    }

                    if address_token_balance < requested_amount {
                        println!(
                            "Requested deposit amount of {} is greater than current wallet balance of {}",
                            maybe_token.format_amount(requested_amount),
                            maybe_token.format_amount(address_token_balance),
                        );
                        return Ok(());
                    }

                    for pool in &pools {
                        print!("Probing {pool:<15} | ");

                        let probe = build_instructions_for_ops(
                            &mut account_data_cache,
                            &pools,
                            &[(Operation::Deposit, pool)],
                            requested_amount,
                            address,
                            token,
                            maybe_token.is_sol(),
                        )
                        .await
                        .and_then(|instructions_for_ops| {
                            let op_msg = format!(
                                "Deposit {} into {pool} ({total_apy:.2}% -> {:.2}%)",
                                maybe_token.format_amount(requested_amount),
                                instructions_for_ops.simulation_total_apy,
                            );

                            if !skip_apy_check
                                && instructions_for_ops.simulation_total_apy < minimum_apy
                            {
                                Err(format!("{op_msg}. {minimum_apy:.2}% minimum APY not met")
                                    .into())
                            } else {
                                Ok(OperationInfo {
                                    op_msg,
                                    deposit_pool_and_amount: Some((pool.clone(), requested_amount)),
                                    withdraw_pool_and_amount: None,
                                    instructions_for_ops,
                                })
                            }
                        });

                        match probe {
                            Err(err) => println!("FAIL | {err}"),
                            Ok(operation_info) => {
                                println!("PASS | {}", operation_info.op_msg);
                                best_operation.replace_if_greater_total_apy(operation_info);
                            }
                        }
                    }
                }
                Command::Withdraw => {
                    assert_eq!(maximum_amount, None); // --maximum_amount is only supported by Rebalance
                    assert!(pool_from.is_none()); // Rebalance only
                    assert!(pool_into.is_none()); // Rebalance only

                    for pool in &pools {
                        let withdraw_pool_available_balance = supply_balance
                            .get(pool)
                            .unwrap()
                            .2
                            .saturating_sub(retain_amount);

                        let requested_amount = match requested_amount {
                            u64::MAX => {
                                // Solend/Kamino leave 1 in sometimes :-/
                                withdraw_pool_available_balance.saturating_sub(1)
                            }
                            requested_amount => requested_amount,
                        };

                        print!("Probing {pool:<15} | ");
                        let probe = if requested_amount > withdraw_pool_available_balance {
                            Err(format!(
                                "Withdraw of {} failed due to an insufficient pool supply balance of {}",
                                maybe_token.format_amount(requested_amount),
                                maybe_token.format_amount(withdraw_pool_available_balance)
                            ).into())
                        } else if requested_amount < minimum_amount {
                            Err(format!(
                                "Withdraw of {} failed because it does not meet the minimum withdrawal amount of {}",
                                maybe_token.format_amount(requested_amount),
                                maybe_token.format_amount(minimum_amount)
                            ).into())
                        } else {
                            build_instructions_for_ops(
                                &mut account_data_cache,
                                &pools,
                                &[(Operation::Withdraw, pool)],
                                requested_amount,
                                address,
                                token,
                                maybe_token.is_sol(),
                            )
                            .await
                            .map(|instructions_for_ops| {
                                OperationInfo {
                                    op_msg: format!(
                                        "Withdraw {} from {pool} ({total_apy:.2}% -> {:.2}%)",
                                        maybe_token.format_amount(requested_amount),
                                        instructions_for_ops.simulation_total_apy,
                                    ),
                                    deposit_pool_and_amount: None,
                                    withdraw_pool_and_amount: Some((
                                        pool.clone(),
                                        requested_amount,
                                    )),
                                    instructions_for_ops,
                                }
                            })
                        };

                        match probe {
                            Err(err) => println!("FAIL | {err}"),
                            Ok(operation_info) => {
                                println!("PASS | {}", operation_info.op_msg);
                                best_operation.replace_if_greater_total_apy(operation_info);
                            }
                        }
                    }
                }
                Command::Rebalance => {
                    if pools.len() <= 1 {
                        return Err(
                            format!("Rebalancing {token} requires at least two pools").into()
                        );
                    }

                    for withdraw_pool in &pools {
                        if let Some(pool_from) = &pool_from {
                            if withdraw_pool != pool_from {
                                continue;
                            }
                        }

                        let withdraw_pool_available_balance =
                            supply_balance.get(withdraw_pool).unwrap().2;

                        let max_requested_amount = {
                            let requested_amount = match requested_amount {
                                u64::MAX => {
                                    // Solend/Kamino leave 1 in sometimes :-/
                                    withdraw_pool_available_balance
                                        .saturating_sub(1)
                                        .max(minimum_amount)
                                }
                                requested_amount => requested_amount,
                            };

                            match maximum_amount {
                                None => requested_amount,
                                Some(maximum_amount) => {
                                    if requested_amount > maximum_amount {
                                        maximum_amount
                                    } else {
                                        requested_amount
                                    }
                                }
                            }
                        };

                        let min_requested_amount = if rebalance_amount_step_count > 1 {
                            minimum_amount
                        } else {
                            max_requested_amount
                        };

                        if max_requested_amount > withdraw_pool_available_balance {
                            println!("Probing {withdraw_pool:<15}                          | FAIL | Rebalance {} failed due to an insufficient {withdraw_pool} supply balance of {}",
                                    maybe_token.format_amount(max_requested_amount),
                                    maybe_token.format_amount(withdraw_pool_available_balance)
                            );
                            continue;
                        }
                        if min_requested_amount < minimum_amount {
                            println!("Probing {withdraw_pool:<15}                          | FAIL | Rebalance {} failed because it does not meet the minimum withdrawal amount of {}",
                                    maybe_token.format_amount(min_requested_amount),
                                    maybe_token.format_amount(minimum_amount)
                            );
                            continue;
                        }
                        assert!(max_requested_amount >= min_requested_amount);

                        for deposit_pool in &pools {
                            if deposit_pool == withdraw_pool {
                                continue;
                            }
                            if let Some(pool_into) = &pool_into {
                                if deposit_pool != pool_into {
                                    continue;
                                }
                            }

                            let requested_amounts = if rebalance_amount_step_count > 1 {
                                let mut requested_amounts = (min_requested_amount..)
                                    .step_by(
                                        ((max_requested_amount - min_requested_amount)
                                            / (rebalance_amount_step_count - 1))
                                            as usize,
                                    )
                                    .take_while(|requested_amount| {
                                        *requested_amount <= max_requested_amount
                                    })
                                    .collect::<Vec<_>>();
                                requested_amounts.pop();
                                requested_amounts.push(max_requested_amount);
                                requested_amounts
                            } else {
                                vec![max_requested_amount]
                            };

                            for (index, requested_amount) in requested_amounts.iter().enumerate() {
                                let requested_amount = *requested_amount;
                                print!(
                                    "Probing {withdraw_pool:<15} -> {deposit_pool:<15} [{}/{}] | ",
                                    index + 1,
                                    requested_amounts.len()
                                );

                                let probe = build_instructions_for_ops(
                                    &mut account_data_cache,
                                    &pools,
                                    &[(Operation::Withdraw, withdraw_pool),
                                      (Operation::Deposit, deposit_pool)],
                                    requested_amount,
                                    address,
                                    token,
                                    maybe_token.is_sol(),
                                )
                                .await
                                .and_then(|instructions_for_ops| {
                                    let apy_improvement = instructions_for_ops.simulation_total_apy - total_apy;

                                    let op_msg = format!(
                                        "Rebalance {} from {withdraw_pool} to {deposit_pool} ({total_apy:.2}% -> {:.2}%) for an additional {apy_improvement:.2}%",
                                        maybe_token.format_amount(requested_amount),
                                        instructions_for_ops.simulation_total_apy
                                    );

                                    if !skip_apy_check && apy_improvement < minimum_apy {
                                        Err(format!("{op_msg} (minimum: {minimum_apy:.2}%)").into())
                                    } else {
                                        Ok(OperationInfo {
                                            op_msg,
                                            deposit_pool_and_amount: Some((
                                                deposit_pool.clone(),
                                                requested_amount,
                                            )),
                                            withdraw_pool_and_amount: Some((
                                                withdraw_pool.clone(),
                                                requested_amount,
                                            )),
                                            instructions_for_ops,
                                        })
                                    }
                                });

                                match probe {
                                    Err(err) => println!("FAIL | {err}"),
                                    Ok(operation_info) => {
                                        println!("PASS | {}", operation_info.op_msg);
                                        best_operation.replace_if_greater_total_apy(operation_info);
                                    }
                                }
                            }
                        }
                    }
                }
            };

            let operation_info = match best_operation.maybe {
                None => {
                    println!("Unable to perform {cmd:?}");
                    return Ok(());
                }
                Some(best_operation_info) => best_operation_info,
            };

            println!("{}", operation_info.op_msg);

            let (signature, priority_fee_lamports, transaction_confirmed) =
                send_instructions_for_ops(
                    &rpc_clients,
                    address,
                    operation_info.instructions_for_ops,
                    priority_fee,
                    dry_run,
                    &vec![signer],
                )
                .await?;

            if dry_run {
                return Ok(());
            }

            if transaction_confirmed.is_some() {
                metrics::push(dp::priority_fee(
                    &format!("{cmd:?}").to_lowercase(),
                    &address,
                    maybe_token,
                    lamports_to_sol(priority_fee_lamports),
                ))
                .await;
            }

            if !transaction_confirmed.unwrap_or_default() {
                let failure_msg = format!("Transaction failed: {signature}");
                notifier.send(&failure_msg).await;
                return Err(failure_msg.into());
            }
            println!("Transaction confirmed: {signature}");

            if let Some((deposit_pool, deposit_amount)) = operation_info.deposit_pool_and_amount {
                metrics::push(dp::principal_balance_change(
                    &deposit_pool,
                    &address,
                    maybe_token,
                    token.ui_amount(deposit_amount),
                ))
                .await;
            }
            if let Some((withdraw_pool, withdraw_amount)) = operation_info.withdraw_pool_and_amount
            {
                metrics::push(dp::principal_balance_change(
                    &withdraw_pool,
                    &address,
                    maybe_token,
                    -token.ui_amount(withdraw_amount),
                ))
                .await;
            }
            notifier
                .send(&format!("{} via via {signature}", operation_info.op_msg))
                .await;
        }
        _ => unreachable!(),
    }

    // Only send metrics on success
    metrics::send(metrics::env_config()).await;
    Ok(())
}

fn apr_to_apy(apr: f64) -> f64 {
    let compounding_periods = 365. * 24.; // hourly compounding
    (1. + apr / compounding_periods).powf(compounding_periods) - 1.
}

#[derive(Debug)]
struct DepositOrWithdrawResult {
    instructions: Vec<Instruction>,
    required_compute_units: u32,
    amount: u64,
    address_lookup_table: Option<Pubkey>,
}

//////////////////////////////////////////////////////////////////////////////
///[ MarginFi Stuff ] ////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

const MFI_LEND_PROGRAM: Pubkey = pubkey!["MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"];
const MARGINFI_GROUP: Pubkey = pubkey!["4qp6Fx6tnZkY5Wropq9wUYgtFxXKwE6viZxFHg3rdAG8"];

fn mfi_lookup_bank_address(token: Token) -> Result<Pubkey, Box<dyn std::error::Error>> {
    match token {
        Token::USDC => Some(pubkey!["2s37akK2eyBbp8DZgCm7RtsaEz8eJP3Nxd4urLHQv7yB"]),
        Token::USDT => Some(pubkey!["HmpMfL8942u22htC4EMiWgLX931g3sacXFR6KjuLgKLV"]),
        Token::UXD => Some(pubkey!["BeNBJrAh1tZg5sqgt8D6AWKJLD5KkBrfZvtcgd7EuiAR"]),
        Token::wSOL => Some(pubkey!["CCKtUs6Cgwo4aaQUmBPmyoApH2gUDErxNZCAntD6LYGh"]),
        _ => None,
    }
    .ok_or_else(|| format!("mfi_load_bank: {token} is not supported").into())
}

fn mfi_load_bank(
    bank_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<marginfi_v2::Bank, Box<dyn std::error::Error>> {
    let (account_data, _context_slot) = account_data_cache.get(bank_address)?;

    const LEN: usize = std::mem::size_of::<marginfi_v2::Bank>();
    let account_data: [u8; LEN] = account_data[8..LEN + 8].try_into().unwrap();
    let reserve = unsafe { std::mem::transmute::<[u8; LEN], marginfi_v2::Bank>(account_data) };
    Ok(reserve)
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

fn mfi_apr(
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<f64, Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(bank_address, account_data_cache)?;
    Ok(mfi_calc_bank_apr(&bank))
}

fn mfi_load_user_account(
    wallet_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<Option<(Pubkey, marginfi_v2::MarginfiAccount)>, Box<dyn std::error::Error>> {
    // Big mistake to require using `getProgramAccounts` to locate a MarginFi account for a wallet
    // address. Most public RPC endpoints have disabled this method. Leach off MarginFi's RPC
    // endpoint for this expensive call since they designed their shit wrong
    let mfi_rpc_client = RpcClient::new_with_commitment(
        // From https://github.com/mrgnlabs/mrgn-account-search/blob/822fe107a8f787b82a494a32130b45613ca94481/src/pages/api/search.ts#L10
        "https://mrgn.rpcpool.com/c293bade994b3864b52c6bbbba4b",
        CommitmentConfig::confirmed(),
    );

    let mut user_accounts = mfi_rpc_client
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
                    data_slice: Some(UiDataSliceConfig {
                        offset: 0,
                        length: 0,
                    }),
                    ..RpcAccountInfoConfig::default()
                },
                ..RpcProgramAccountsConfig::default()
            },
        )?
        .into_iter();

    let first_user_account = user_accounts.next();
    if user_accounts.next().is_some() {
        return Err(format!("Multiple MarginFi account found for {}", wallet_address).into());
    }

    Ok(match first_user_account {
        None => None,
        Some((user_account_address, _user_account_data)) => Some((user_account_address, {
            let (account_data, _context_slot) = account_data_cache.get(user_account_address)?;
            const LEN: usize = std::mem::size_of::<marginfi_v2::MarginfiAccount>();
            let data: [u8; LEN] = account_data[8..LEN + 8].try_into()?;
            unsafe { std::mem::transmute::<[u8; LEN], marginfi_v2::MarginfiAccount>(data) }
        })),
    })
}

fn mfi_deposited_amount(
    wallet_address: Pubkey,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<(/*balance: */ u64, /* available_balance: */ u64), Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(bank_address, account_data_cache)?;
    let user_account = match mfi_load_user_account(wallet_address, account_data_cache)? {
        None => return Ok((0, 0)),
        Some((_user_account_address, user_account)) => user_account,
    };

    let deposited_amount = match user_account.lending_account.get_balance(&bank_address) {
        None => 0,
        Some(balance) => {
            let deposit = bank.get_asset_amount(balance.asset_shares.into());
            deposit.floor().to_num::<u64>()
        }
    };
    let remaining_outflow = u64::MAX;

    Ok((deposited_amount, deposited_amount.min(remaining_outflow)))
}

fn mfi_deposit_or_withdraw(
    op: Operation,
    wallet_address: Pubkey,
    token: Token,
    amount: u64,
    verbose: bool,
    account_data_cache: &mut AccountDataCache,
) -> Result<DepositOrWithdrawResult, Box<dyn std::error::Error>> {
    let bank_address = mfi_lookup_bank_address(token)?;
    let bank = mfi_load_bank(bank_address, account_data_cache)?;
    if verbose {
        println!(
            "Deposit Limit: {}",
            token.format_amount(bank.config.deposit_limit)
        );
    }

    let (user_account_address, user_account) = mfi_load_user_account(wallet_address, account_data_cache)?
        .ok_or_else(|| format!("No MarginFi account found for {wallet_address}. Manually deposit once into MarginFi and retry"))?;

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
                v.extend(amount.to_le_bytes());
                v.extend([1, /* WithdrawAll = */ 0]);
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
                if balance.active {
                    account_meta.push(AccountMeta::new_readonly(balance.bank_pk, false));

                    let balance_bank = mfi_load_bank(balance.bank_pk, account_data_cache)?;
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

            (instructions, 110_000, amount)
        }
    };

    Ok(DepositOrWithdrawResult {
        instructions,
        required_compute_units,
        amount,
        address_lookup_table: Some(pubkey!["2FyGQ8UZ6PegCSN2Lu7QD1U2UY28GpJdDfdwEfbwxN7p"]),
    })
}

//////////////////////////////////////////////////////////////////////////////
///[ Kamino Stuff ] //////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

const KAMINO_LEND_PROGRAM: Pubkey = pubkey!["KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"];
const FARMS_PROGRAM: Pubkey = pubkey!["FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr"];

fn pubkey_or_klend_program(pubkey: Pubkey) -> Pubkey {
    if pubkey == Pubkey::default() {
        KAMINO_LEND_PROGRAM
    } else {
        pubkey
    }
}

fn kamino_unsafe_load_reserve_account_data(
    account_data: &[u8],
) -> Result<kamino::Reserve, Box<dyn std::error::Error>> {
    const LEN: usize = std::mem::size_of::<kamino::Reserve>();
    let account_data: [u8; LEN] = account_data[8..LEN + 8].try_into().unwrap();
    let reserve = unsafe { std::mem::transmute::<[u8; LEN], kamino::Reserve>(account_data) };
    Ok(reserve)
}

fn kamino_load_reserve(
    reserve_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<kamino::Reserve, Box<dyn std::error::Error>> {
    if !account_data_cache.address_cached(&reserve_address) {
        let rpc_reserve =
            kamino_unsafe_load_reserve_account_data(account_data_cache.get(reserve_address)?.0)?;

        //
        // The reserve account for some pools can be stale. Simulate a Refresh Reserve instruction and
        // read back the new reserve account data to ensure it's up to date
        //

        // Instruction: Kamino: Refresh Reserve
        let instructions = vec![Instruction::new_with_bytes(
            KAMINO_LEND_PROGRAM,
            &[0x02, 0xda, 0x8a, 0xeb, 0x4f, 0xc9, 0x19, 0x66],
            vec![
                // Reserve
                AccountMeta::new(reserve_address, false),
                // Lending Market
                AccountMeta::new_readonly(rpc_reserve.lending_market, false),
                // Pyth Oracle
                AccountMeta::new_readonly(
                    pubkey_or_klend_program(rpc_reserve.config.token_info.pyth_configuration.price),
                    false,
                ),
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Switchboard Twap Oracle
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Scope Prices
                AccountMeta::new_readonly(
                    pubkey_or_klend_program(
                        rpc_reserve.config.token_info.scope_configuration.price_feed,
                    ),
                    false,
                ),
            ],
        )];

        account_data_cache.simulate_then_add(&instructions, None, &[])?;
    }

    let (account_data, _context_slot) = account_data_cache.get(reserve_address).unwrap();
    kamino_unsafe_load_reserve_account_data(account_data)
}

fn kamino_load_pool_reserve(
    pool: &str,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<(Pubkey, kamino::Reserve), Box<dyn std::error::Error>> {
    let market_reserve_map = match pool {
        "kamino-main" => HashMap::from([
            (
                Token::USDC,
                pubkey!["D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"],
            ),
            (
                Token::USDT,
                pubkey!["H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S"],
            ),
            (
                Token::JitoSOL,
                pubkey!["EVbyPKrHG6WBfm4dLxLMJpUDY43cCAcHSpV3KYjKsktW"],
            ),
            (
                Token::wSOL,
                pubkey!["d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q"],
            ),
        ]),
        "kamino-altcoins" => HashMap::from([
            (
                Token::USDC,
                pubkey!["9TD2TSv4pENb8VwfbVYg25jvym7HN6iuAR6pFNSrKjqQ"],
            ),
            (
                Token::JUP,
                pubkey!["3AKyRviT87dt9jP3RHpfFjxmSVNbR68Wx7UejnUyaSFH"],
            ),
            (
                Token::JTO,
                pubkey!["8PYYKF4ZvteefFBmtb9SMHmhZKnDWQH86z59mPZBfhHu"],
            ),
            (
                Token::PYTH,
                pubkey!["HXSE82voKcf8x2rdeLr73yASNhzWWGcTz3Shq6UFaEHA"],
            ),
            (
                Token::WEN,
                pubkey!["G6wtWpanuKmtqqjkpHpLsp21d7DKJpWQydKojGs2kuHQ"],
            ),
            (
                Token::WIF,
                pubkey!["GvPEtF7MsZceLbrrjprfcKN9quJ7EW221c4H9TVuWQUo"],
            ),
            (
                Token::BONK,
                pubkey!["CoFdsnQeCUyJefhKK6GQaAPT9PEx8Xcs2jejtp9jgn38"],
            ),
        ]),
        "kamino-jlp" => HashMap::from([
            (
                Token::USDC,
                pubkey!["Ga4rZytCpq1unD4DbEJ5bkHeUz9g3oh9AAFEi6vSauXp"],
            ),
            (
                Token::JLP,
                pubkey!["DdTmCCjv7zHRD1hJv3E8bpnSEQBzdKkzB1j9ApXX5QoP"],
            ),
        ]),
        _ => unreachable!(),
    };
    let market_reserve_address = *market_reserve_map
        .get(&token)
        .ok_or_else(|| format!("{pool}: {token} is not supported"))?;

    let reserve = kamino_load_reserve(market_reserve_address, account_data_cache)?;

    Ok((market_reserve_address, reserve))
}

fn kamino_apr(
    pool: &str,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<f64, Box<dyn std::error::Error>> {
    let (_market_reserve_address, reserve) =
        kamino_load_pool_reserve(pool, token, account_data_cache)?;
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
    obligation_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<Option<kamino::Obligation>, Box<dyn std::error::Error>> {
    let (account_data, _context_slot) = account_data_cache.get(obligation_address)?;

    if account_data.is_empty() {
        return Ok(None);
    }

    const LEN: usize = std::mem::size_of::<kamino::Obligation>();
    let account_data: [u8; LEN] = account_data[8..LEN + 8].try_into().unwrap();
    let obligation = unsafe { std::mem::transmute::<[u8; LEN], kamino::Obligation>(account_data) };
    Ok(Some(obligation))
}

fn kamino_deposited_amount(
    pool: &str,
    wallet_address: Pubkey,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<(/*balance: */ u64, /* available_balance: */ u64), Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve) =
        kamino_load_pool_reserve(pool, token, account_data_cache)?;
    let remaining_outflow = u64::MAX;

    let obligation_address = kamino_find_obligation_address(wallet_address, reserve.lending_market);

    let deposited_amount =
        match kamino_unsafe_load_obligation(obligation_address, account_data_cache)? {
            None => 0,
            Some(obligation) => {
                let collateral_deposited_amount = obligation
                    .deposits
                    .iter()
                    .find(|collateral| collateral.deposit_reserve == market_reserve_address)
                    .map(|collateral| collateral.deposited_amount)
                    .unwrap_or_default();

                reserve
                    .collateral_exchange_rate()
                    .collateral_to_liquidity(collateral_deposited_amount)
            }
        };

    Ok((deposited_amount, deposited_amount.min(remaining_outflow)))
}

fn kamino_deposit_or_withdraw(
    op: Operation,
    pool: &str,
    wallet_address: Pubkey,
    token: Token,
    amount: u64,
    account_data_cache: &mut AccountDataCache,
) -> Result<DepositOrWithdrawResult, Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve) =
        kamino_load_pool_reserve(pool, token, account_data_cache)?;

    let lending_market_authority = Pubkey::find_program_address(
        &[b"lma", &reserve.lending_market.to_bytes()],
        &KAMINO_LEND_PROGRAM,
    )
    .0;

    let reserve_farm_state = reserve.farm_collateral;
    let reserve_liquidity_supply = reserve.liquidity.supply_vault;
    let reserve_collateral_mint = reserve.collateral.mint_pubkey;
    let reserve_destination_deposit_collateral = reserve.collateral.supply_vault;

    let obligation_address = kamino_find_obligation_address(wallet_address, reserve.lending_market);
    let obligation = kamino_unsafe_load_obligation(obligation_address, account_data_cache)?;

    if obligation.is_none() {
        return Err(format!("Manually deposit once into {pool} before using sys-lend").into());
    }

    let obligation_farm_user_state = Pubkey::find_program_address(
        &[
            b"user",
            &reserve_farm_state.to_bytes(),
            &obligation_address.to_bytes(),
        ],
        &FARMS_PROGRAM,
    )
    .0;

    let obligation_market_reserves = obligation.as_ref().map_or_else(Vec::new, |obligation| {
        obligation
            .deposits
            .iter()
            .filter(|c| c.deposit_reserve != Pubkey::default())
            .map(|c| c.deposit_reserve)
            .collect::<Vec<_>>()
    });

    let mut instructions = vec![];

    // Instruction: Kamino: Refresh Reserve

    let mut refresh_reserves = obligation_market_reserves
        .iter()
        .filter_map(|reserve_address| {
            if *reserve_address != market_reserve_address {
                Some((
                    *reserve_address,
                    kamino_load_reserve(*reserve_address, account_data_cache).unwrap_or_else(
                        |err| {
                            // TODO: propagate failure up instead of panic..
                            panic!("unable to load reserve {reserve_address}: {err}")
                        },
                    ),
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    refresh_reserves.push((market_reserve_address, reserve));

    for (reserve_address, reserve) in refresh_reserves {
        instructions.push(Instruction::new_with_bytes(
            KAMINO_LEND_PROGRAM,
            &[0x02, 0xda, 0x8a, 0xeb, 0x4f, 0xc9, 0x19, 0x66],
            vec![
                // Reserve
                AccountMeta::new(reserve_address, false),
                // Lending Market
                AccountMeta::new_readonly(reserve.lending_market, false),
                // Pyth Oracle
                AccountMeta::new_readonly(
                    pubkey_or_klend_program(reserve.config.token_info.pyth_configuration.price),
                    false,
                ),
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Switchboard Twap Oracle
                AccountMeta::new_readonly(KAMINO_LEND_PROGRAM, false),
                // Scope Prices
                AccountMeta::new_readonly(
                    pubkey_or_klend_program(
                        reserve.config.token_info.scope_configuration.price_feed,
                    ),
                    false,
                ),
            ],
        ));
    }

    // Instruction: Kamino: Refresh Obligation
    let mut refresh_obligation_account_metas = vec![
        // Lending Market
        AccountMeta::new_readonly(reserve.lending_market, false),
        // Obligation
        AccountMeta::new(obligation_address, false),
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
            AccountMeta::new(obligation_address, false),
            // Lending Market Authority
            AccountMeta::new(lending_market_authority, false),
            // Reserve
            AccountMeta::new(market_reserve_address, false),
            // Reserve Farm State
            AccountMeta::new(reserve_farm_state, false),
            // Obligation Farm User State
            AccountMeta::new(obligation_farm_user_state, false),
            // Lending Market
            AccountMeta::new_readonly(reserve.lending_market, false),
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

    if reserve_farm_state != Pubkey::default() {
        if account_data_cache
            .get(obligation_farm_user_state)?
            .0
            .is_empty()
        {
            return Err(format!("Manually deposit once into {pool} before using sys-lend").into());
        }
        instructions.push(kamino_refresh_obligation_farms_for_reserve.clone());
    }

    let amount = match op {
        Operation::Withdraw => {
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
                    AccountMeta::new(obligation_address, false),
                    // Lending Market
                    AccountMeta::new_readonly(reserve.lending_market, false),
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

            amount - 1 // HACK!! Sometimes Kamino loses a lamport? This breaks `rebalance`...
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
                    AccountMeta::new(obligation_address, false),
                    // Lending Market
                    AccountMeta::new_readonly(reserve.lending_market, false),
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
    if reserve_farm_state != Pubkey::default() {
        instructions.push(kamino_refresh_obligation_farms_for_reserve);
    }

    Ok(DepositOrWithdrawResult {
        instructions,
        required_compute_units: 500_000,
        amount,
        address_lookup_table: Some(match pool {
            "kamino-main" => pubkey!["284iwGtA9X9aLy3KsyV8uT2pXLARhYbiSi5SiM2g47M2"],
            "kamino-altcoins" => pubkey!["x2uEQSaqrZs5UnyXjiNktRhrAy6iNFeSKai9VNYFFuy"],
            "kamino-jlp" => pubkey!["GprZNyWk67655JhX6Rq9KoebQ6WkQYRhATWzkx2P2LNc"],
            _ => unreachable!(),
        }),
    })
}

//////////////////////////////////////////////////////////////////////////////
///[ Solend Stuff ] //////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

fn solend_load_reserve(
    reserve_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<(solend::state::Reserve, Slot), Box<dyn std::error::Error>> {
    if !account_data_cache.address_cached(&reserve_address) {
        let rpc_reserve =
            solend::state::Reserve::unpack(account_data_cache.get(reserve_address)?.0)?;

        //
        // The reserve account for some pools can be stale. Simulate a Refresh Reserve instruction and
        // read back the new reserve account data to ensure it's up to date
        //

        // Instruction: Solend: Refresh Reserve
        let instructions = vec![Instruction::new_with_bytes(
            solend::solend_mainnet::ID,
            &[3],
            vec![
                // Reserve
                AccountMeta::new(reserve_address, false),
                // Pyth Oracle
                AccountMeta::new_readonly(rpc_reserve.liquidity.pyth_oracle_pubkey, false),
                // Switchboard Oracle
                AccountMeta::new_readonly(rpc_reserve.liquidity.switchboard_oracle_pubkey, false),
            ],
        )];

        account_data_cache.simulate_then_add(&instructions, None, &[])?;
    }

    let (account_data, context_slot) = account_data_cache.get(reserve_address).unwrap();
    Ok((solend::state::Reserve::unpack(account_data)?, context_slot))
}

fn solend_load_reserve_for_pool(
    pool: &str,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<(Pubkey, solend::state::Reserve, Slot), Box<dyn std::error::Error>> {
    let market_reserve_map = match pool {
        "solend-main" => HashMap::from([
            (
                Token::USDC,
                pubkey!["BgxfHJDzm44T7XG68MYKx7YisTjZu73tVovyZSjJMpmw"],
            ),
            (
                Token::USDT,
                pubkey!["8K9WC8xoh2rtQNY7iEGXtPvfbDCi563SdWhCAhuMP2xE"],
            ),
            (
                Token::wSOL,
                pubkey!["8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36"],
            ),
        ]),
        "solend-turbosol" => HashMap::from([(
            Token::USDC,
            pubkey!["EjUgEaPpKMg2nqex9obb46gZQ6Ar9mWSdVKbw9A6PyXA"],
        )]),
        "solend-jlp" => HashMap::from([
            (
                Token::USDC,
                pubkey!["GShhnkfbaYy41Fd8vSEk9zoiwZSKqbH1j16jZ2afV2GG"],
            ),
            (
                Token::wSOL,
                pubkey!["8kd8cDJEioKFXckK8tP2FHNSQLDGguCFj5Vy1vK5eDGV"],
            ),
        ]),
        _ => unreachable!(),
    };
    let market_reserve_address = *market_reserve_map
        .get(&token)
        .ok_or_else(|| format!("{pool}: {token} is not supported"))?;

    let (reserve, slot) = solend_load_reserve(market_reserve_address, account_data_cache)?;

    Ok((market_reserve_address, reserve, slot))
}

fn solend_remaining_outflow_for_reserve(
    mut reserve: solend::state::Reserve,
    context_slot: Slot,
) -> Result<u64, Box<dyn std::error::Error>> {
    if reserve.rate_limiter.config.window_duration == 0 {
        Ok(u64::MAX)
    } else {
        Ok(reserve
            .rate_limiter
            .remaining_outflow(context_slot)?
            .try_floor_u64()?)
    }
}

fn solend_apr(
    pool: &str,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<f64, Box<dyn std::error::Error>> {
    let (_market_reserve_address, reserve, _context_slot) =
        solend_load_reserve_for_pool(pool, token, account_data_cache)?;

    let utilization_rate = reserve.liquidity.utilization_rate()?;
    let current_borrow_rate = reserve.current_borrow_rate().unwrap();

    let supply_apr = format!(
        "{}",
        utilization_rate.try_mul(current_borrow_rate)?.try_mul(
            solend::math::Rate::from_percent(100 - reserve.config.protocol_take_rate)
        )?
    );

    Ok(supply_apr.parse::<f64>()?)
}

fn solend_find_obligation_address(wallet_address: Pubkey, lending_market: Pubkey) -> Pubkey {
    Pubkey::create_with_seed(
        &wallet_address,
        &lending_market.to_string()[0..32],
        &solend::solend_mainnet::ID,
    )
    .unwrap()
}

fn solend_load_obligation(
    obligation_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<Option<solend::state::Obligation>, Box<dyn std::error::Error>> {
    let (account_data, _context_slot) = account_data_cache.get(obligation_address)?;

    if account_data.is_empty() {
        return Ok(None);
    }

    Ok(Some(solend::state::Obligation::unpack(account_data)?))
}

fn solend_load_lending_market(
    lending_market_address: Pubkey,
    account_data_cache: &mut AccountDataCache,
) -> Result<solend::state::LendingMarket, Box<dyn std::error::Error>> {
    let account_data = account_data_cache.get(lending_market_address)?.0;
    Ok(solend::state::LendingMarket::unpack(account_data)?)
}

fn solend_deposited_amount(
    pool: &str,
    wallet_address: Pubkey,
    token: Token,
    account_data_cache: &mut AccountDataCache,
) -> Result<(/*balance: */ u64, /* available_balance: */ u64), Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve, context_slot) =
        solend_load_reserve_for_pool(pool, token, account_data_cache)?;
    let remaining_outflow = solend_remaining_outflow_for_reserve(reserve.clone(), context_slot)?;

    let obligation_address = solend_find_obligation_address(wallet_address, reserve.lending_market);

    let deposited_amount = match solend_load_obligation(obligation_address, account_data_cache)? {
        None => 0,
        Some(obligation) => {
            let collateral_deposited_amount = obligation
                .deposits
                .iter()
                .find(|collateral| collateral.deposit_reserve == market_reserve_address)
                .map(|collateral| collateral.deposited_amount)
                .unwrap_or_default();

            reserve
                .collateral_exchange_rate()?
                .collateral_to_liquidity(collateral_deposited_amount)?
        }
    };

    Ok((deposited_amount, deposited_amount.min(remaining_outflow)))
}

fn solend_deposit_or_withdraw(
    op: Operation,
    pool: &str,
    wallet_address: Pubkey,
    token: Token,
    amount: u64,
    account_data_cache: &mut AccountDataCache,
) -> Result<DepositOrWithdrawResult, Box<dyn std::error::Error>> {
    let (market_reserve_address, reserve, _context_slot) =
        solend_load_reserve_for_pool(pool, token, account_data_cache)?;

    let obligation_address = solend_find_obligation_address(wallet_address, reserve.lending_market);
    let lending_market = solend_load_lending_market(reserve.lending_market, account_data_cache)?;

    let lending_market_authority = Pubkey::create_program_address(
        &[
            &reserve.lending_market.to_bytes(),
            &[lending_market.bump_seed],
        ],
        &solend::solend_mainnet::ID,
    )?;

    let user_liquidity_token_account =
        spl_associated_token_account::get_associated_token_address(&wallet_address, &token.mint());
    let user_collateral_token_account = spl_associated_token_account::get_associated_token_address(
        &wallet_address,
        &reserve.collateral.mint_pubkey,
    );

    let obligation =
        solend_load_obligation(obligation_address, account_data_cache)?.ok_or_else(|| {
            format!(
                "{pool} obligation account not found for {wallet_address}. \
                 Manually deposit once into {pool} before using sys-lend"
            )
        })?;

    let mut instructions = vec![];

    if account_data_cache
        .get(user_collateral_token_account)?
        .0
        .is_empty()
    {
        instructions.push(
            spl_associated_token_account::instruction::create_associated_token_account(
                &wallet_address,
                &wallet_address,
                &reserve.collateral.mint_pubkey,
                &spl_token::id(),
            ),
        );
    }
    let (amount, required_compute_units) = match op {
        Operation::Deposit => {
            // Solend: Deposit Reserve Liquidity and Obligation Collateral
            let solend_deposit_reserve_liquidity_and_obligation_collateral_data = {
                let mut v = vec![0x0e];
                v.extend(amount.to_le_bytes());
                v
            };

            instructions.push(Instruction::new_with_bytes(
                solend::solend_mainnet::ID,
                &solend_deposit_reserve_liquidity_and_obligation_collateral_data,
                vec![
                    // User Liquidity Token Account
                    AccountMeta::new(user_liquidity_token_account, false),
                    // User Collateral Token Account
                    AccountMeta::new(user_collateral_token_account, false),
                    // Lending Market
                    AccountMeta::new(market_reserve_address, false),
                    // Reserve Liquidity Supply
                    AccountMeta::new(reserve.liquidity.supply_pubkey, false),
                    // Reserve Collateral Mint
                    AccountMeta::new(reserve.collateral.mint_pubkey, false),
                    // Lending Market
                    AccountMeta::new(reserve.lending_market, false),
                    // Lending Market Authority
                    AccountMeta::new_readonly(lending_market_authority, false),
                    // Reserve Destination Deposit Collateral
                    AccountMeta::new(reserve.collateral.supply_pubkey, false),
                    // Obligation
                    AccountMeta::new(obligation_address, false),
                    // Obligation Owner
                    AccountMeta::new(wallet_address, true),
                    // Pyth Oracle
                    AccountMeta::new_readonly(reserve.liquidity.pyth_oracle_pubkey, false),
                    // Switchboard Oracle
                    AccountMeta::new_readonly(reserve.liquidity.switchboard_oracle_pubkey, false),
                    // User Transfer Authority
                    AccountMeta::new(wallet_address, true),
                    // Token Program
                    AccountMeta::new_readonly(spl_token::id(), false),
                ],
            ));
            (amount, 100_000)
        }
        Operation::Withdraw => {
            // Instruction: Solend: Refresh Reserve
            let obligation_market_reserves = obligation
                .deposits
                .iter()
                .filter(|c| c.deposit_reserve != Pubkey::default())
                .map(|c| c.deposit_reserve)
                .collect::<Vec<_>>();

            let mut refresh_reserves = obligation_market_reserves
                .iter()
                .filter_map(|reserve_address| {
                    if *reserve_address != market_reserve_address {
                        Some((
                            *reserve_address,
                            solend_load_reserve(*reserve_address, account_data_cache)
                                .unwrap_or_else(|err| {
                                    // TODO: propagate failure up instead of panic..
                                    panic!("unable to load reserve {reserve_address}: {err}")
                                })
                                .0,
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            refresh_reserves.push((market_reserve_address, reserve.clone()));

            for (reserve_address, reserve) in refresh_reserves {
                instructions.push(Instruction::new_with_bytes(
                    solend::solend_mainnet::ID,
                    &[3],
                    vec![
                        // Reserve
                        AccountMeta::new(reserve_address, false),
                        // Pyth Oracle
                        AccountMeta::new_readonly(reserve.liquidity.pyth_oracle_pubkey, false),
                        // Switchboard Oracle
                        AccountMeta::new_readonly(
                            reserve.liquidity.switchboard_oracle_pubkey,
                            false,
                        ),
                    ],
                ));
            }

            // Instruction: Solend: Refresh Obligation
            let mut refresh_obligation_account_metas = vec![
                // Obligation
                AccountMeta::new(obligation_address, false),
            ];

            for obligation_market_reserve in &obligation_market_reserves {
                refresh_obligation_account_metas
                    .push(AccountMeta::new(*obligation_market_reserve, false));
            }

            instructions.push(Instruction::new_with_bytes(
                solend::solend_mainnet::ID,
                &[0x7],
                refresh_obligation_account_metas,
            ));

            // Instruction: Solend: Withdraw Obligation Collateral And Redeem Reserve Collateral

            let collateral_exchange_rate = reserve.collateral_exchange_rate()?;
            let solend_withdraw_obligation_collateral_and_redeem_reserve_collateral_data = {
                let mut v = vec![0x0f];
                v.extend(
                    collateral_exchange_rate
                        .liquidity_to_collateral(amount)?
                        .to_le_bytes(),
                );
                v
            };

            let mut account_meta = vec![
                // Reserve Collateral Supply
                AccountMeta::new(reserve.collateral.supply_pubkey, false),
                // User Collateral Token Account
                AccountMeta::new(user_collateral_token_account, false),
                // Lending Market
                AccountMeta::new(market_reserve_address, false),
                // Obligation
                AccountMeta::new(obligation_address, false),
                // Lending Market
                AccountMeta::new(reserve.lending_market, false),
                // Lending Market Authority
                AccountMeta::new_readonly(lending_market_authority, false),
                // User Liquidity Token Account
                AccountMeta::new(user_liquidity_token_account, false),
                // Reserve Collateral Mint
                AccountMeta::new(reserve.collateral.mint_pubkey, false),
                // Reserve Liquidity Supply
                AccountMeta::new(reserve.liquidity.supply_pubkey, false),
                // Obligation Owner
                AccountMeta::new(wallet_address, true),
                // User Transfer Authority
                AccountMeta::new(wallet_address, true),
                // Token Program
                AccountMeta::new_readonly(spl_token::id(), false),
            ];

            for reserve_address in &obligation_market_reserves {
                account_meta.push(AccountMeta::new(*reserve_address, false));
            }

            instructions.push(Instruction::new_with_bytes(
                solend::solend_mainnet::ID,
                &solend_withdraw_obligation_collateral_and_redeem_reserve_collateral_data,
                account_meta,
            ));

            (
                amount - 1, // HACK!! Sometimes Solend loses a lamport? This breaks `rebalance`...
                150_000,
            )
        }
    };

    Ok(DepositOrWithdrawResult {
        instructions,
        required_compute_units,
        amount,
        address_lookup_table: Some(pubkey!["89ig7Cu6Roi9mJMqpY8sBkPYL2cnqzpgP16sJxSUbvct"]),
    })
}
