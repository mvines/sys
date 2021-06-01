use {
    crate::{exchange::*, field_as_string},
    chrono::NaiveDate,
    pickledb::{PickleDb, PickleDbDumpPolicy},
    serde::{Deserialize, Serialize},
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::Pubkey,
        signature::Signature,
    },
    std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Io: {0}")]
    Io(#[from] std::io::Error),

    #[error("PickleDb: {0}")]
    PickleDb(#[from] pickledb::error::Error),

    #[error("Account already exists: {0}")]
    AccountAlreadyExists(Pubkey),

    #[error("Account does not exist: {0}")]
    AccountDoesNotExist(Pubkey),
}

pub type DbResult<T> = std::result::Result<T, DbError>;

pub fn new<P: AsRef<Path>>(db_path: P) -> DbResult<Db> {
    let db_path = db_path.as_ref();
    if !db_path.exists() {
        fs::create_dir_all(db_path)?;
    }

    let db_filename = db_path.join("◎.db");
    let credentials_db_filename = db_path.join("🤐.db");

    let db = if db_filename.exists() {
        PickleDb::load_json(db_filename, PickleDbDumpPolicy::DumpUponRequest)?
    } else {
        PickleDb::new_json(db_filename, PickleDbDumpPolicy::DumpUponRequest)
    };

    let credentials_db = if credentials_db_filename.exists() {
        PickleDb::load_json(credentials_db_filename, PickleDbDumpPolicy::DumpUponRequest)?
    } else {
        PickleDb::new_json(credentials_db_filename, PickleDbDumpPolicy::DumpUponRequest)
    };

    Ok(Db { db, credentials_db })
}

pub struct Db {
    db: PickleDb,
    credentials_db: PickleDb,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PendingDeposit {
    pub exchange: Exchange,
    pub tx_id: String, // transaction signature of the deposit
    pub amount: f64,   // amount of SOL deposited
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub exchange: Exchange,
    pub pair: String,
    pub order_id: String,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum LotAcquistionKind {
    EpochReward {
        epoch: Epoch,
        slot: Slot,
    },
    Transaction {
        slot: Slot,
        #[serde(with = "field_as_string")]
        signature: Signature,
    },
    NotAvailable,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LotAcquistion {
    pub when: NaiveDate,
    pub price: f64, // USD per SOL
    pub kind: LotAcquistionKind,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Lot {
    pub lot_number: usize,
    pub acquisition: LotAcquistion,
    pub amount: u64,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TrackedAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    pub description: String,
    pub last_update_epoch: Epoch,
    pub last_update_balance: u64,
    pub lots: Vec<Lot>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SweepStakeAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    pub stake_authority: PathBuf,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TransitorySweepStakeAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,

    #[serde(with = "field_as_string")]
    pub from_address: Pubkey,
}

impl Db {
    pub fn set_exchange_credentials(
        &mut self,
        exchange: Exchange,
        exchange_credentials: ExchangeCredentials,
    ) -> DbResult<()> {
        self.clear_exchange_credentials(exchange)?;

        self.credentials_db
            .set(&format!("{:?}", exchange), &exchange_credentials)
            .unwrap();

        Ok(self.credentials_db.dump()?)
    }

    pub fn get_exchange_credentials(&self, exchange: Exchange) -> Option<ExchangeCredentials> {
        self.credentials_db.get(&format!("{:?}", exchange))
    }

    pub fn clear_exchange_credentials(&mut self, exchange: Exchange) -> DbResult<()> {
        if self.get_exchange_credentials(exchange).is_some() {
            self.credentials_db.rem(&format!("{:?}", exchange)).ok();
            self.credentials_db.dump()?;
        }
        Ok(())
    }

    pub fn get_configured_exchanges(&self) -> Vec<(Exchange, ExchangeCredentials)> {
        self.credentials_db
            .get_all()
            .into_iter()
            .filter_map(|key| {
                if let Ok(exchange) = key.parse() {
                    self.get_exchange_credentials(exchange)
                        .map(|exchange_credentials| (exchange, exchange_credentials))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn save(&mut self) -> DbResult<()> {
        Ok(self.db.dump()?)
    }

    pub fn record_deposit(&mut self, deposit: PendingDeposit) -> DbResult<()> {
        if !self.db.lexists("deposits") {
            self.db.lcreate("deposits")?;
        }
        self.db.ladd("deposits", &deposit).unwrap();
        self.save()
    }

    pub fn cancel_deposit(&mut self, deposit: &PendingDeposit) -> DbResult<()> {
        assert!(self.db.lrem_value("deposits", deposit)?);
        self.save()
    }

    pub fn confirm_deposit(&mut self, deposit: &PendingDeposit) -> DbResult<()> {
        self.cancel_deposit(deposit)
    }

    pub fn pending_deposits(&self, exchange: Exchange) -> Vec<PendingDeposit> {
        self.db
            .liter("deposits")
            .filter_map(|item_iter| item_iter.get_item::<PendingDeposit>())
            .filter(|pending_deposit| pending_deposit.exchange == exchange)
            .collect()
    }

    pub fn record_order(&mut self, order: OpenOrder) -> DbResult<()> {
        if !self.db.lexists("orders") {
            self.db.lcreate("orders")?;
        }
        self.db.ladd("orders", &order).unwrap();
        self.save()
    }

    pub fn clear_order(&mut self, order: &OpenOrder) -> DbResult<()> {
        assert!(self.db.lrem_value("orders", order)?);
        self.save()
    }

    pub fn pending_orders(&self, exchange: Exchange) -> Vec<OpenOrder> {
        self.db
            .liter("orders")
            .filter_map(|item_iter| item_iter.get_item::<OpenOrder>())
            .filter(|pending_order| pending_order.exchange == exchange)
            .collect()
    }

    pub fn add_account(&mut self, account: TrackedAccount) -> DbResult<()> {
        if !self.db.lexists("accounts") {
            self.db.lcreate("accounts")?;
        }

        if self.get_account(account.address).is_some() {
            Err(DbError::AccountAlreadyExists(account.address))
        } else {
            self.db.ladd("accounts", &account).unwrap();
            self.save()
        }
    }

    pub fn update_account(&mut self, account: TrackedAccount) -> DbResult<()> {
        let position = self
            .get_account_position(account.address)
            .ok_or(DbError::AccountDoesNotExist(account.address))?;
        assert!(
            self.db
                .lpop::<TrackedAccount>("accounts", position)
                .is_some(),
            "Cannot update unknown account: {}",
            account.address
        );
        self.db.ladd("accounts", &account).unwrap();
        self.save()
    }

    pub fn remove_account(&mut self, address: Pubkey) -> DbResult<()> {
        let position = self
            .get_account_position(address)
            .ok_or(DbError::AccountDoesNotExist(address))?;
        assert!(
            self.db
                .lpop::<TrackedAccount>("accounts", position)
                .is_some(),
            "Cannot remove unknown account: {}",
            address
        );
        self.save()
    }

    fn get_account_position(&self, address: Pubkey) -> Option<usize> {
        if self.db.lexists("accounts") {
            for (position, value) in self.db.liter("accounts").enumerate() {
                if let Some(tracked_account) = value.get_item::<TrackedAccount>() {
                    if tracked_account.address == address {
                        return Some(position);
                    }
                }
            }
        }
        None
    }

    pub fn get_account(&self, address: Pubkey) -> Option<TrackedAccount> {
        if !self.db.lexists("accounts") {
            None
        } else {
            self.db
                .liter("accounts")
                .filter_map(|item_iter| item_iter.get_item::<TrackedAccount>())
                .find(|tracked_account| tracked_account.address == address)
        }
    }

    pub fn get_accounts(&self) -> HashMap<Pubkey, TrackedAccount> {
        if !self.db.lexists("accounts") {
            return HashMap::default();
        }
        self.db
            .liter("accounts")
            .filter_map(|item_iter| {
                item_iter
                    .get_item::<TrackedAccount>()
                    .map(|ta| (ta.address, ta))
            })
            .collect()
    }

    pub fn next_lot_number(&mut self) -> usize {
        let lot_number = self.db.get::<usize>("next_lot_number").unwrap_or(0);
        self.db.set("next_lot_number", &(lot_number + 1)).unwrap();
        lot_number
    }

    pub fn get_sweep_stake_account(&self) -> Option<SweepStakeAccount> {
        self.db.get("sweep-stake-account")
    }

    pub fn set_sweep_stake_account(
        &mut self,
        sweep_stake_account: SweepStakeAccount,
    ) -> DbResult<()> {
        let _ = self
            .get_account_position(sweep_stake_account.address)
            .ok_or(DbError::AccountDoesNotExist(sweep_stake_account.address))?;
        self.db
            .set("sweep-stake-account", &sweep_stake_account)
            .unwrap();
        self.save()
    }

    pub fn get_transitory_sweep_stake_accounts(&self) -> Vec<TransitorySweepStakeAccount> {
        self.db
            .get("transitory-sweep-stake-accounts")
            .unwrap_or_default()
    }

    pub fn set_transitory_sweep_stake_accounts(
        &mut self,
        transitory_sweep_stake_accounts: &[TransitorySweepStakeAccount],
    ) -> DbResult<()> {
        self.db
            .set(
                "transitory-sweep-stake-accounts",
                &transitory_sweep_stake_accounts.iter().collect::<Vec<_>>(),
            )
            .unwrap();
        self.save()
    }
}
