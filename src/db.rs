use {
    crate::exchange::*,
    pickledb::{PickleDb, PickleDbDumpPolicy},
    serde::{Deserialize, Serialize},
    std::{fs, path::Path},
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Io: {0}")]
    Io(#[from] std::io::Error),

    #[error("PickleDb: {0}")]
    PickleDb(#[from] pickledb::error::Error),
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

    pub fn get_configured_exchanges(&self) -> Vec<Exchange> {
        self.credentials_db
            .get_all()
            .into_iter()
            .filter_map(|key| key.parse().ok())
            .collect()
    }

    pub fn record_deposit(&mut self, deposit: PendingDeposit) -> DbResult<()> {
        if !self.db.lexists("deposits") {
            self.db.lcreate("deposits")?;
        }
        self.db.ladd("deposits", &deposit).unwrap();
        Ok(self.db.dump()?)
    }

    pub fn cancel_deposit(&mut self, deposit: &PendingDeposit) -> DbResult<()> {
        assert!(self.db.lrem_value("deposits", deposit)?);
        Ok(self.db.dump()?)
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
        Ok(self.db.dump()?)
    }

    pub fn clear_order(&mut self, order: &OpenOrder) -> DbResult<()> {
        assert!(self.db.lrem_value("orders", order)?);
        Ok(self.db.dump()?)
    }

    pub fn pending_orders(&self, exchange: Exchange) -> Vec<OpenOrder> {
        self.db
            .liter("orders")
            .filter_map(|item_iter| item_iter.get_item::<OpenOrder>())
            .filter(|pending_order| pending_order.exchange == exchange)
            .collect()
    }
}
