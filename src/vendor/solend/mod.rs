#![deny(missing_docs)]

//! A lending program for the Solana blockchain.

pub mod error;
//pub mod instruction;
pub mod math;
//pub mod oracles;
pub mod state;

/// mainnet program id
pub mod solend_mainnet {
    solana_pubkey::declare_id!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo");
}

/// devnet program id
pub mod solend_devnet {
    solana_pubkey::declare_id!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo");
}

/// Canonical null pubkey. Prints out as "nu11111111111111111111111111111111111111111"
pub const NULL_PUBKEY: solana_pubkey::Pubkey = solana_pubkey::Pubkey::new_from_array([
    11, 193, 238, 216, 208, 116, 241, 195, 55, 212, 76, 22, 75, 202, 40, 216, 76, 206, 27, 169,
    138, 64, 177, 28, 19, 90, 156, 0, 0, 0, 0, 0,
]);

/// Mainnet program id for Switchboard v2.
pub mod switchboard_v2_mainnet {
    solana_pubkey::declare_id!("SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f");
}

/// Devnet program id for Switchboard v2.
pub mod switchboard_v2_devnet {
    solana_pubkey::declare_id!("2TfB33aLaneQb5TNVwyDz3jSZXS6jdW2ARw1Dgf84XCG");
}
