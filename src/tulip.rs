/*
Ref: https://github.com/sol-farm/tulipv2-sdk/tree/main/common/src/lending
*/

use {
    crate::token::{MaybeToken, Token},
    rust_decimal::prelude::*,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{
        instruction::Instruction, program_pack::Pack, pubkey, pubkey::Pubkey,
        system_instruction::create_account_with_seed,
    },
    spl_token_lending::instruction::*,
    tulipv2_sdk_common::{lending::reserve::Reserve, math::common::TryMul},
};

const TULIP_PROGRAM_ID: Pubkey = pubkey!("4bcFeLv4nydFrsZqV5CgwCVrPhkQKsXtzfy2KyMz7ozM");

struct TulipLending {
    reserve: Pubkey,
    reserve_liquidity_oracle: Pubkey,
    reserve_liquidity_supply: Pubkey,
    reserve_collateral_mint: Pubkey,
    lending_market: Pubkey,
    liquidity_token: MaybeToken,
    #[allow(dead_code)]
    collateral_token: Token,
}

impl From<&Token> for TulipLending {
    fn from(token: &Token) -> Self {
        match token {
            Token::tuUSDC | Token::USDC => TulipLending::usdc(),
            Token::tuSOL | Token::wSOL => TulipLending::sol(),
            Token::mSOL | Token::tumSOL => TulipLending::msol(),
        }
    }
}

impl From<&MaybeToken> for TulipLending {
    fn from(maybe_token: &MaybeToken) -> Self {
        match maybe_token.token().as_ref() {
            None => TulipLending::sol(),
            Some(token) => token.into(),
        }
    }
}

impl TulipLending {
    fn usdc() -> Self {
        Self {
            reserve: pubkey!("FTkSmGsJ3ZqDSHdcnY7ejN1pWV3Ej7i88MYpZyyaqgGt"),
            reserve_liquidity_oracle: pubkey!("ExzpbWgczTgd8J58BrnESndmzBkRVfc6PhFjSGiQXgAB"),
            reserve_liquidity_supply: pubkey!("64QJd6MYXUjCBvCaZKaqxiKmaMkPUdNonE1KuY1YoGGb"),
            reserve_collateral_mint: pubkey!("Amig8TisuLpzun8XyGfC5HJHHGUQEscjLgoTWsCCKihg"),
            lending_market: pubkey!("D1cqtVThyebK9KXKGXrCEuiqaNf5L4UfM1vHgCqiJxym"),
            liquidity_token: Token::USDC.into(),
            collateral_token: Token::tuUSDC,
        }
    }

    fn sol() -> Self {
        Self {
            reserve: pubkey!("FzbfXR7sopQL29Ubu312tkqWMxSre4dYSrFyYAjUYiC4"),
            reserve_liquidity_oracle: pubkey!("DQAcms41gjYzidRooXRE9GQM1jAauPXDcEpMbVh4FEc7"),
            reserve_liquidity_supply: pubkey!("CPs1jJ5XAjhcAJsmTToWksAiPEqoLwKMbb1Z83rzaaaU"),
            reserve_collateral_mint: pubkey!("H4Q3hDbuMUw8Bu72Ph8oV2xMQ7BFNbekpfQZKS2xF7jW"),
            lending_market: pubkey!("D1cqtVThyebK9KXKGXrCEuiqaNf5L4UfM1vHgCqiJxym"),
            liquidity_token: MaybeToken::SOL(),
            collateral_token: Token::tuSOL,
        }
    }

    fn msol() -> Self {
        Self {
            reserve: pubkey!("5LKgrsUF72MityTntAHWLcXivBGxnxapikFArtKUULwX"),
            reserve_liquidity_oracle: pubkey!("7FQME1uNK5VVV3f6p3BRPAgMTzaAy2zSMKAvmEyqjQjQ"),
            reserve_liquidity_supply: pubkey!("GppNJmvMn2YRSU6gpqJEkUgic76iFjDU17145q8WF27n"),
            reserve_collateral_mint: pubkey!("8cn7JcYVjDZesLa3RTt3NXne4WcDw9PdUneQWuByehwW"),
            lending_market: pubkey!("D1cqtVThyebK9KXKGXrCEuiqaNf5L4UfM1vHgCqiJxym"),
            liquidity_token: Token::mSOL.into(),
            collateral_token: Token::tumSOL,
        }
    }

    fn get_ata(
        rpc_client: &RpcClient,
        address: &Pubkey,
        mint: &Pubkey,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let ata = spl_associated_token_account::get_associated_token_address(address, mint);

        let _ = rpc_client
            .get_token_account_balance(&ata)
            .map_err(|err| format!("Could not get balance for account {}: {}", address, err))?;

        Ok(ata)
    }

    fn collateral_ata_address(
        &self,
        rpc_client: &RpcClient,
        address: &Pubkey,
    ) -> Result<Pubkey, Box<dyn std::error::Error>> {
        Self::get_ata(rpc_client, address, &self.reserve_collateral_mint).map_err(|err| {
            format!(
                "Collateral token may not exist. \
                 To create it, run `spl-token create-account {} --owner {}`: {}",
                self.reserve_collateral_mint, address, err
            )
            .into()
        })
    }
}

pub fn liquidity_token(token: &Token) -> MaybeToken {
    TulipLending::from(token).liquidity_token
}

// Current `ui_amount` conversion rate back to the liquidity token
pub async fn get_current_liquidity_token_rate(
    rpc_client: &RpcClient,
    token: &Token,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    let tulip_lending = TulipLending::from(token);
    let reserve_account = rpc_client
        .get_account_with_commitment(&tulip_lending.reserve, rpc_client.commitment())?
        .value
        .expect("reserve_account");

    let reserve = Reserve::unpack(&reserve_account.data)?;
    let mint_decimals = reserve.liquidity.mint_decimals;

    let decimal_collateral_amount = reserve
        .collateral_exchange_rate()?
        .decimal_collateral_to_liquidity(
            spl_token::ui_amount_to_amount(1., mint_decimals).into(),
        )?;

    // Perform `spl_token::amount_to_ui_amount()` in `Decimal` representation to maintain
    // precision
    let decimal_collateral_amount_as_scaled_val = decimal_collateral_amount.to_scaled_val()?;

    let decimal_collateral_ui_amount_as_scaled_val =
        Decimal::from_u128(decimal_collateral_amount_as_scaled_val).unwrap()
            / Decimal::from_usize(10_usize.pow(mint_decimals as u32)).unwrap();

    Ok(decimal_collateral_ui_amount_as_scaled_val
        / Decimal::from_u128(
            tulipv2_sdk_common::math::decimal::Decimal::one()
                .to_scaled_val()
                .unwrap(),
        )
        .unwrap())
}

pub async fn get_current_lending_apy(
    rpc_client: &RpcClient,
    token: &MaybeToken,
) -> Result<f64, Box<dyn std::error::Error>> {
    let tulip_lending = TulipLending::from(token);
    let reserve_account = rpc_client
        .get_account_with_commitment(&tulip_lending.reserve, rpc_client.commitment())?
        .value
        .expect("reserve_account");

    let reserve = Reserve::unpack(&reserve_account.data)?;

    let apy = reserve
        .liquidity
        .utilization_rate()?
        .try_mul(reserve.current_borrow_rate()?)?;

    Ok(100. * apy.to_string().parse::<f64>()?)
}

pub async fn get_current_price(
    rpc_client: &RpcClient,
    token: &Token,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    let tulip_lending = TulipLending::from(token);
    Ok(tulip_lending
        .liquidity_token
        .get_current_price(rpc_client)
        .await?
        * get_current_liquidity_token_rate(rpc_client, token).await?)
}

pub fn deposit(
    rpc_client: &RpcClient,
    address: Pubkey,
    liquidity_token: MaybeToken,
    collateral_token: Token,
    liquidity_amount: u64,
) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
    do_lending(
        rpc_client,
        &address,
        liquidity_token,
        collateral_token,
        liquidity_amount,
        |tulip_lending, liquidity_ata_address, collateral_ata_address| {
            deposit_reserve_liquidity(
                TULIP_PROGRAM_ID,
                liquidity_amount,
                liquidity_ata_address,
                collateral_ata_address,
                tulip_lending.reserve,
                tulip_lending.reserve_liquidity_supply,
                tulip_lending.reserve_collateral_mint,
                tulip_lending.lending_market,
                address,
            )
        },
    )
}

pub fn withdraw(
    rpc_client: &RpcClient,
    address: Pubkey,
    liquidity_token: MaybeToken,
    collateral_token: Token,
    collateral_amount: u64,
) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
    do_lending(
        rpc_client,
        &address,
        liquidity_token,
        collateral_token,
        0,
        |tulip_lending, liquidity_ata_address, collateral_ata_address| {
            redeem_reserve_collateral(
                TULIP_PROGRAM_ID,
                collateral_amount,
                collateral_ata_address,
                liquidity_ata_address,
                tulip_lending.reserve,
                tulip_lending.reserve_collateral_mint,
                tulip_lending.reserve_liquidity_supply,
                tulip_lending.lending_market,
                address,
            )
        },
    )
}

fn do_lending<F>(
    rpc_client: &RpcClient,
    address: &Pubkey,
    liquidity_token: MaybeToken,
    collateral_token: Token,
    liquidity_amount: u64,
    f: F,
) -> Result<Vec<Instruction>, Box<dyn std::error::Error>>
where
    F: FnOnce(&TulipLending, Pubkey, Pubkey) -> Instruction,
{
    let tulip_lending = TulipLending::from(&collateral_token);
    if tulip_lending.liquidity_token.mint() != liquidity_token.mint() {
        return Err(format!(
            "Invalid liquidity token: {} (expected mint: {})",
            liquidity_token,
            tulip_lending.liquidity_token.mint()
        )
        .into());
    }
    assert_eq!(
        tulip_lending.collateral_token.mint(),
        collateral_token.mint()
    );

    let collateral_ata_address = tulip_lending.collateral_ata_address(rpc_client, address)?;

    let (liquidity_ata_address, setup_instructions, shutdown_instructions) =
        if liquidity_token.token().is_none() {
            let space = spl_token::state::Account::LEN;
            let minimum_balance_for_rent_exemption =
                rpc_client.get_minimum_balance_for_rent_exemption(space)?;
            let lamports = liquidity_amount + minimum_balance_for_rent_exemption;

            let seed = "wrapitup";
            let wrapped_sol_address =
                Pubkey::create_with_seed(address, seed, &spl_token::id()).unwrap();

            (
                wrapped_sol_address,
                vec![
                    create_account_with_seed(
                        address,
                        &wrapped_sol_address,
                        address,
                        seed,
                        lamports,
                        space as u64,
                        &spl_token::id(),
                    ),
                    spl_token::instruction::initialize_account(
                        &spl_token::id(),
                        &wrapped_sol_address,
                        &spl_token::native_mint::id(),
                        address,
                    )
                    .unwrap(),
                ],
                vec![spl_token::instruction::close_account(
                    &spl_token::id(),
                    &wrapped_sol_address,
                    address,
                    address,
                    &[],
                )
                .unwrap()],
            )
        } else {
            (
                TulipLending::get_ata(rpc_client, address, &liquidity_token.mint())
                    .map_err(|err| format!("Liquidity token may not exist: {}", err))?,
                vec![],
                vec![],
            )
        };

    let mut instructions = setup_instructions;
    instructions.extend([
        refresh_reserve(
            TULIP_PROGRAM_ID,
            tulip_lending.reserve,
            tulip_lending.reserve_liquidity_oracle,
        ),
        f(
            &tulip_lending,
            liquidity_ata_address,
            collateral_ata_address,
        ),
    ]);
    instructions.extend(shutdown_instructions);
    Ok(instructions)
}
