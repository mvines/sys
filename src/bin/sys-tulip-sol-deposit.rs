use {
    clap::{value_t_or_exit, App, Arg},
    solana_clap_utils::{self, input_parsers::*, input_validators::*},
    solana_client::rpc_client::RpcClient,
    solana_sdk::{commitment_config::CommitmentConfig, message::Message, transaction::Transaction},
    sys::{app_version, send_transaction_until_expired, token::*, tulip},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    solana_logger::setup_with_default("solana=info");
    let default_json_rpc_url = "https://api.mainnet-beta.solana.com";

    let app_version = &*app_version();
    let app = App::new("sys-tulip-sol-deposit")
        .about("Deposit all SOL from the provided account into Tulip")
        .version(app_version)
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
            Arg::with_name("from")
                .value_name("FROM_ADDRESS")
                .index(1)
                .takes_value(true)
                .required(true)
                .validator(is_valid_signer)
                .help("Account holding the SOL to deposit"),
        )
        .arg(
            Arg::with_name("retain")
                .short("r")
                .long("retain")
                .value_name("SOL")
                .takes_value(true)
                .validator(is_parsable::<f64>)
                .default_value("0.1")
                .help("Amount of SOL to retain in the source account"),
        );

    let matches = app.get_matches();
    let rpc_client = RpcClient::new_with_commitment(
        normalize_to_url_if_moniker(value_t_or_exit!(matches, "json_rpc_url", String)),
        CommitmentConfig::confirmed(),
    );

    let mut wallet_manager = None;

    let (signer, address) = signer_of(&matches, "from", &mut wallet_manager)?;
    let address = address.expect("address");
    let signer = signer.expect("signer");

    let sol = MaybeToken::SOL();
    let retain_amount = sol.amount(value_t_or_exit!(matches, "retain", f64));

    let balance = rpc_client.get_balance(&address)?;

    let deposit_amount = balance.saturating_sub(retain_amount);

    if deposit_amount == 0 {
        println!("Nothing to deposit");
        return Ok(());
    }
    println!("Depositing {}", sol.format_amount(deposit_amount));

    let instructions = tulip::deposit(&rpc_client, address, sol, Token::tuSOL, deposit_amount)?;

    let (recent_blockhash, last_valid_block_height) =
        rpc_client.get_latest_blockhash_with_commitment(rpc_client.commitment())?;

    let mut message = Message::new(&instructions, Some(&address));
    message.recent_blockhash = recent_blockhash;

    let mut transaction = Transaction::new_unsigned(message);
    let simulation_result = rpc_client.simulate_transaction(&transaction)?.value;
    if simulation_result.err.is_some() {
        return Err(format!("Simulation failure: {:?}", simulation_result).into());
    }

    transaction.try_sign(&vec![signer], recent_blockhash)?;
    let signature = transaction.signatures[0];
    println!("Transaction signature: {}", signature);

    if !send_transaction_until_expired(&rpc_client, &transaction, last_valid_block_height) {
        return Err("Deposit failed".into());
    }

    Ok(())
}
