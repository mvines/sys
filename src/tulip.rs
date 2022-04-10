/*
Ref: https://github.com/sol-farm/tulipv2-sdk/tree/main/common/src/lending
*/

use {
    crate::token::Token,
    rust_decimal::prelude::*,
    solana_client::rpc_client::RpcClient,
    solana_sdk::{program_pack::Pack, pubkey},
    tulipv2_sdk_common::lending::reserve::Reserve,
};

pub fn get_current_price(
    rpc_client: &RpcClient,
    token: &Token,
) -> Result<Decimal, Box<dyn std::error::Error>> {
    let reserve_address = match token {
        Token::tuUSDC => pubkey!("FTkSmGsJ3ZqDSHdcnY7ejN1pWV3Ej7i88MYpZyyaqgGt"),
        Token::tuSOL => pubkey!("FzbfXR7sopQL29Ubu312tkqWMxSre4dYSrFyYAjUYiC4"),
        _ => panic!("Unsupported token: {:?}", token),
    };

    let reserve_account = rpc_client
        .get_account_with_commitment(&reserve_address, rpc_client.commitment())?
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

    let decimal_collateral_ui_amount = decimal_collateral_ui_amount_as_scaled_val
        / Decimal::from_u128(
            tulipv2_sdk_common::math::decimal::Decimal::one()
                .to_scaled_val()
                .unwrap(),
        )
        .unwrap();

    Ok(decimal_collateral_ui_amount)
}
