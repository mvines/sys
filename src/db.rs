use {
    crate::{exchange::*, field_as_string},
    chrono::{prelude::*, NaiveDate},
    pickledb::{PickleDb, PickleDbDumpPolicy},
    serde::{Deserialize, Serialize},
    solana_sdk::{
        clock::{Epoch, Slot},
        native_token::lamports_to_sol,
        pubkey::Pubkey,
        signature::Signature,
    },
    std::{
        collections::{BTreeMap, HashSet},
        fmt, fs,
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

    #[error("Pending transfer with signature does not exist: {0}")]
    PendingTransferDoesNotExist(Signature),

    #[error("Pending deposit with signature does not exist: {0}")]
    PendingDepositDoesNotExist(Signature),

    #[error("Account has insufficient balance: {0}")]
    AccountHasInsufficientBalance(Pubkey),

    #[error("Open order not exist: {0}")]
    OpenOrderDoesNotExist(String),
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

    Ok(Db {
        db,
        credentials_db,
        auto_save: true,
    })
}

pub struct Db {
    db: PickleDb,
    credentials_db: PickleDb,
    auto_save: bool,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PendingDeposit {
    pub exchange: Exchange,
    pub amount: u64,
    pub transfer: PendingTransfer,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PendingTransfer {
    #[serde(with = "field_as_string")]
    pub signature: Signature, // transaction signature of the transfer
    pub last_valid_block_height: u64,

    #[serde(with = "field_as_string")]
    pub from_address: Pubkey,
    #[serde(with = "field_as_string")]
    pub to_address: Pubkey,

    pub lots: Vec<Lot>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub creation_time: DateTime<Utc>,
    pub exchange: Exchange,
    pub pair: String,
    pub price: f64,
    pub order_id: String,
    pub lots: Vec<Lot>,

    #[serde(with = "field_as_string")]
    pub deposit_address: Pubkey,
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

impl fmt::Display for LotAcquistionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LotAcquistionKind::EpochReward { epoch, slot } => {
                write!(f, "epoch {} reward (slot {})", epoch, slot)
            }
            LotAcquistionKind::Transaction { signature, .. } => write!(f, "{}", signature),
            LotAcquistionKind::NotAvailable => {
                write!(f, "other")
            }
        }
    }
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
    pub amount: u64, // lamports
}

impl Lot {
    // Figure the amount of income that the Lot incurred
    pub fn income(&self) -> f64 {
        match self.acquisition.kind {
            LotAcquistionKind::EpochReward { .. } | LotAcquistionKind::NotAvailable => {
                self.acquisition.price * lamports_to_sol(self.amount)
            }
            LotAcquistionKind::Transaction { .. } => 0.,
        }
    }
    // Figure the current cap gain/loss for the Lot
    pub fn cap_gain(&self, current_price: f64) -> f64 {
        (current_price - self.acquisition.price) * lamports_to_sol(self.amount)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum LotDisposalKind {
    Usd {
        exchange: Exchange,
        pair: String,
        order_id: String,
    },
    Other {
        description: String,
    },
}

impl fmt::Display for LotDisposalKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LotDisposalKind::Usd {
                exchange,
                pair,
                order_id,
            } => write!(f, "{:?} {}, order {}", exchange, pair, order_id),
            LotDisposalKind::Other { description } => write!(f, "{}", description),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DisposedLot {
    pub lot: Lot,
    pub when: NaiveDate,
    pub price: f64, // USD per SOL
    pub kind: LotDisposalKind,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TrackedAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    pub description: String,
    pub last_update_epoch: Epoch,
    pub last_update_balance: u64,
    pub lots: Vec<Lot>,
    pub no_sync: Option<bool>,
}

fn split_lots(
    db: &mut Db,
    lots: Vec<Lot>,
    amount: u64,
    lot_numbers: Option<HashSet<usize>>,
) -> (Vec<Lot>, Vec<Lot>) {
    let mut extracted_lots = vec![];
    let mut remaining_lots = vec![];

    let mut amount_remaining = amount;
    for mut lot in lots {
        if let Some(lot_numbers) = lot_numbers.as_ref() {
            if !lot_numbers.contains(&lot.lot_number) {
                remaining_lots.push(lot);
                continue;
            }
        }

        if amount_remaining > 0 {
            if lot.amount <= amount_remaining {
                amount_remaining -= lot.amount;
                extracted_lots.push(lot);
            } else {
                let split_lot = Lot {
                    lot_number: db.next_lot_number(),
                    acquisition: lot.acquisition.clone(),
                    amount: amount_remaining,
                };
                lot.amount -= amount_remaining;
                extracted_lots.push(split_lot);
                remaining_lots.push(lot);
                amount_remaining = 0;
            }
        } else {
            remaining_lots.push(lot);
        }
    }
    remaining_lots.sort_by_key(|lot| lot.acquisition.when);
    extracted_lots.sort_by_key(|lot| lot.acquisition.when);
    assert_eq!(
        extracted_lots.iter().map(|el| el.amount).sum::<u64>(),
        amount
    );

    (extracted_lots, remaining_lots)
}

impl TrackedAccount {
    pub fn assert_lot_balance(&self) {
        let lot_balance: u64 = self.lots.iter().map(|lot| lot.amount).sum();
        assert_eq!(
            lot_balance, self.last_update_balance,
            "Lot balance mismatch: {:?}",
            self
        );
    }

    pub fn extract_lots(
        &mut self,
        db: &mut Db,
        amount: u64,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<Vec<Lot>> {
        self.assert_lot_balance();

        let mut lots = std::mem::take(&mut self.lots);
        lots.sort_by_key(|lot| lot.acquisition.when);

        let balance: u64 = lots.iter().map(|lot| lot.amount).sum();
        if balance < amount {
            return Err(DbError::AccountHasInsufficientBalance(self.address));
        }

        if !lots.is_empty() {
            // Assume the oldest lot is the rent-reserve. Extract it as the last resort
            let first_lot = lots.remove(0);
            lots.push(first_lot);
        }

        let (extracted_lots, remaining_lots) = split_lots(db, lots, amount, lot_numbers);

        self.lots = remaining_lots;
        self.last_update_balance -= amount;
        self.assert_lot_balance();
        Ok(extracted_lots)
    }

    fn merge_lots(&mut self, lots: Vec<Lot>) {
        let mut amount = 0;
        for lot in lots {
            amount += lot.amount;
            if let Some(mut existing_lot) = self
                .lots
                .iter_mut()
                .find(|l| l.acquisition == lot.acquisition)
            {
                existing_lot.amount += lot.amount;
            } else {
                self.lots.push(lot);
            }
        }
        self.last_update_balance += amount;
        self.assert_lot_balance();
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SweepStakeAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    pub stake_authority: PathBuf,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TransitorySweepStake {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
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

    fn auto_save(&mut self, auto_save: bool) -> DbResult<()> {
        self.auto_save = auto_save;
        self.save()
    }

    fn save(&mut self) -> DbResult<()> {
        if self.auto_save {
            self.db.dump()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_deposit(
        &mut self,
        signature: Signature,
        last_valid_block_height: u64,
        from_address: Pubkey,
        amount: u64,
        exchange: Exchange,
        deposit_address: Pubkey,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        if !self.db.lexists("deposits") {
            self.db.lcreate("deposits")?;
        }

        let mut from_account = self
            .get_account(from_address)
            .ok_or(DbError::AccountDoesNotExist(from_address))?;

        let deposit = PendingDeposit {
            exchange,
            amount,
            transfer: PendingTransfer {
                signature,
                last_valid_block_height,
                from_address,
                to_address: deposit_address,
                lots: from_account.extract_lots(self, amount, lot_numbers)?,
            },
        };
        self.db.ladd("deposits", &deposit).unwrap();

        self.update_account(from_account) // `update_account` calls `save`...
    }

    fn complete_deposit(&mut self, signature: Signature, success: bool) -> DbResult<()> {
        let mut pending_deposits = self.pending_deposits(None);

        let PendingDeposit { transfer, .. } = pending_deposits
            .iter()
            .find(|pd| pd.transfer.signature == signature)
            .ok_or(DbError::PendingDepositDoesNotExist(signature))?
            .clone();

        pending_deposits.retain(|pd| pd.transfer.signature != signature);

        self.db.lrem_list("deposits")?;
        self.db.lcreate("deposits")?;
        self.db.lextend("deposits", &pending_deposits).unwrap();

        self.complete_transfer_or_deposit(transfer, success) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_deposit(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_deposit(signature, false)
    }

    pub fn confirm_deposit(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_deposit(signature, true)
    }

    pub fn pending_deposits(&self, exchange: Option<Exchange>) -> Vec<PendingDeposit> {
        if !self.db.lexists("deposits") {
            // Handle buggy older databases with "deposits" saved as a value instead of list.
            if self.db.exists("deposits") {
                return self.db.get::<Vec<PendingDeposit>>("deposits").unwrap();
            }
        }
        self.db
            .liter("deposits")
            .filter_map(|item_iter| item_iter.get_item::<PendingDeposit>())
            .filter(|pending_deposit| {
                if let Some(exchange) = exchange {
                    pending_deposit.exchange == exchange
                } else {
                    true
                }
            })
            .collect()
    }

    pub fn open_order(
        &mut self,
        deposit_account: TrackedAccount,
        exchange: Exchange,
        pair: String,
        price: f64,
        order_id: String,
        lots: Vec<Lot>,
    ) -> DbResult<()> {
        let mut open_orders = self.open_orders(None);
        open_orders.push(OpenOrder {
            creation_time: Utc::now(),
            exchange,
            pair,
            price,
            order_id,
            lots,
            deposit_address: deposit_account.address,
        });
        self.db.set("orders", &open_orders).unwrap();
        self.update_account(deposit_account) // `update_account` calls `save`...
    }

    pub fn close_order(
        &mut self,
        order_id: &str,
        amount: u64,
        filled_amount: u64,
        price: f64,
        when: NaiveDate,
    ) -> DbResult<()> {
        let mut open_orders = self.open_orders(None);

        let OpenOrder {
            exchange,
            pair,
            order_id,
            lots,
            deposit_address,
            ..
        } = open_orders
            .iter()
            .find(|o| o.order_id == order_id)
            .ok_or_else(|| DbError::OpenOrderDoesNotExist(order_id.to_string()))?
            .clone();

        open_orders.retain(|o| o.order_id != order_id);
        self.db.set("orders", &open_orders).unwrap();

        let lot_balance: u64 = lots.iter().map(|lot| lot.amount).sum();
        assert_eq!(lot_balance, amount, "Order lot balance mismatch");
        assert!(filled_amount <= amount);

        let (sold_lots, cancelled_lots) = split_lots(self, lots, filled_amount, None);

        self.auto_save(false)?;
        if !sold_lots.is_empty() {
            let mut disposed_lots = self.disposed_lots();
            for lot in sold_lots {
                disposed_lots.push(DisposedLot {
                    lot,
                    when,
                    price,
                    kind: LotDisposalKind::Usd {
                        exchange,
                        pair: pair.clone(),
                        order_id: order_id.clone(),
                    },
                });
            }
            self.db.set("disposed-lots", &disposed_lots).unwrap();
        }

        if !cancelled_lots.is_empty() {
            let mut deposit_account = self
                .get_account(deposit_address)
                .ok_or(DbError::AccountDoesNotExist(deposit_address))?;

            deposit_account.merge_lots(cancelled_lots);
            self.update_account(deposit_account)?;
        }
        self.auto_save(true)
    }

    pub fn record_disposal(
        &mut self,
        from_address: Pubkey,
        amount: u64,
        description: String,
        when: NaiveDate,
        price: f64,
    ) -> DbResult<Vec<DisposedLot>> {
        let mut from_account = self
            .get_account(from_address)
            .ok_or(DbError::AccountDoesNotExist(from_address))?;

        let mut disposed_lots = self.disposed_lots();

        let lots = from_account.extract_lots(self, amount, None)?;
        for lot in lots {
            disposed_lots.push(DisposedLot {
                lot,
                when,
                price,
                kind: LotDisposalKind::Other {
                    description: description.clone(),
                },
            });
        }
        self.db.set("disposed-lots", &disposed_lots)?;
        self.update_account(from_account)?; // `update_account` calls `save`...
        Ok(disposed_lots)
    }

    pub fn open_orders(&self, exchange: Option<Exchange>) -> Vec<OpenOrder> {
        let orders: Vec<OpenOrder> = self.db.get("orders").unwrap_or_default();
        orders
            .into_iter()
            .filter(|order| {
                if let Some(exchange) = exchange {
                    order.exchange == exchange
                } else {
                    true
                }
            })
            .collect()
    }

    pub fn add_account_no_save(&mut self, account: TrackedAccount) -> DbResult<()> {
        account.assert_lot_balance();

        if !self.db.lexists("accounts") {
            self.db.lcreate("accounts")?;
        }

        if self.get_account(account.address).is_some() {
            Err(DbError::AccountAlreadyExists(account.address))
        } else {
            self.db.ladd("accounts", &account).unwrap();
            Ok(())
        }
    }

    pub fn add_account(&mut self, account: TrackedAccount) -> DbResult<()> {
        self.add_account_no_save(account)?;
        self.save()
    }

    pub fn update_account(&mut self, account: TrackedAccount) -> DbResult<()> {
        account.assert_lot_balance();

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

    fn remove_account_no_save(&mut self, address: Pubkey) -> DbResult<()> {
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
        Ok(())
    }

    pub fn remove_account(&mut self, address: Pubkey) -> DbResult<()> {
        self.remove_account_no_save(address)?;
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

    pub fn get_accounts(&self) -> BTreeMap<Pubkey, TrackedAccount> {
        if !self.db.lexists("accounts") {
            return BTreeMap::default();
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

    // The caller must call `save()`...
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

    pub fn get_transitory_sweep_stake_addresses(&self) -> HashSet<Pubkey> {
        self.db
            .get::<Vec<TransitorySweepStake>>("transitory-sweep-stake-accounts")
            .unwrap_or_default()
            .into_iter()
            .map(|tss| tss.address)
            .collect()
    }

    pub fn add_transitory_sweep_stake_address(
        &mut self,
        address: Pubkey,
        current_epoch: Epoch,
    ) -> DbResult<()> {
        let mut transitory_sweep_stake_addresses = self.get_transitory_sweep_stake_addresses();

        if transitory_sweep_stake_addresses.contains(&address) {
            Err(DbError::AccountAlreadyExists(address))
        } else {
            transitory_sweep_stake_addresses.insert(address);
            self.set_transitory_sweep_stake_addresses(transitory_sweep_stake_addresses)
        }?;

        self.add_account_no_save(TrackedAccount {
            address,
            description: "Transitory stake account".to_string(),
            last_update_balance: 0,
            last_update_epoch: current_epoch,
            lots: vec![],
            no_sync: None,
        })
    }

    pub fn remove_transitory_sweep_stake_address(&mut self, address: Pubkey) -> DbResult<()> {
        let _ = self.remove_account_no_save(address);

        let mut transitory_sweep_stake_addresses = self.get_transitory_sweep_stake_addresses();

        if !transitory_sweep_stake_addresses.contains(&address) {
            Err(DbError::AccountDoesNotExist(address))
        } else {
            transitory_sweep_stake_addresses.remove(&address);
            self.set_transitory_sweep_stake_addresses(transitory_sweep_stake_addresses)
        }
    }

    fn set_transitory_sweep_stake_addresses<T>(
        &mut self,
        transitory_sweep_stake_addresses: T,
    ) -> DbResult<()>
    where
        T: IntoIterator<Item = Pubkey>,
    {
        self.db
            .set(
                "transitory-sweep-stake-accounts",
                &transitory_sweep_stake_addresses
                    .into_iter()
                    .map(|address| TransitorySweepStake { address })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        self.save()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_transfer(
        &mut self,
        signature: Signature,
        last_valid_block_height: u64,
        from_address: Pubkey,
        amount: Option<u64>, // None = all
        to_address: Pubkey,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        let mut pending_transfers = self.pending_transfers();

        let mut from_account = self
            .get_account(from_address)
            .ok_or(DbError::AccountDoesNotExist(from_address))?;
        let _to_account = self
            .get_account(to_address)
            .ok_or(DbError::AccountDoesNotExist(to_address))?;

        pending_transfers.push(PendingTransfer {
            signature,
            last_valid_block_height,
            from_address,
            to_address,
            lots: from_account.extract_lots(
                self,
                amount.unwrap_or(from_account.last_update_balance),
                lot_numbers,
            )?,
        });

        self.db.set("transfers", &pending_transfers).unwrap();
        self.update_account(from_account) // `update_account` calls `save`...
    }

    fn complete_transfer_or_deposit(
        &mut self,
        pending_transfer: PendingTransfer,
        success: bool,
    ) -> DbResult<()> {
        let PendingTransfer {
            from_address,
            to_address,
            lots,
            ..
        } = pending_transfer;

        let mut from_account = self
            .get_account(from_address)
            .ok_or(DbError::AccountDoesNotExist(from_address))?;
        let mut to_account = self
            .get_account(to_address)
            .ok_or(DbError::AccountDoesNotExist(to_address))?;

        if success {
            to_account.merge_lots(lots);
        } else {
            from_account.merge_lots(lots);
        }

        self.auto_save(false)?;
        self.update_account(to_account)?;
        self.update_account(from_account)?;
        self.auto_save(true)
    }

    fn complete_transfer(&mut self, signature: Signature, success: bool) -> DbResult<()> {
        let mut pending_transfers = self.pending_transfers();

        let transfer = pending_transfers
            .iter()
            .find(|pt| pt.signature == signature)
            .ok_or(DbError::PendingTransferDoesNotExist(signature))?
            .clone();

        pending_transfers.retain(|pt| pt.signature != signature);
        self.db.set("transfers", &pending_transfers).unwrap();

        self.complete_transfer_or_deposit(transfer, success) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_transfer(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_transfer(signature, false)
    }

    pub fn confirm_transfer(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_transfer(signature, true)
    }

    pub fn pending_transfers(&self) -> Vec<PendingTransfer> {
        self.db.get("transfers").unwrap_or_default()
    }

    pub fn disposed_lots(&self) -> Vec<DisposedLot> {
        self.db.get("disposed-lots").unwrap_or_default()
    }
}
