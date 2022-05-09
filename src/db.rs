use {
    crate::{exchange::*, field_as_string, token::*},
    chrono::{prelude::*, NaiveDate},
    pickledb::{PickleDb, PickleDbDumpPolicy},
    rust_decimal::prelude::*,
    separator::FixedPlaceSeparatable,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::Pubkey,
        signature::Signature,
    },
    std::{
        collections::HashSet,
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

    #[error("Account does not exist: {0} ({1})")]
    AccountDoesNotExist(Pubkey, MaybeToken),

    #[error("Pending transfer with signature does not exist: {0}")]
    PendingTransferDoesNotExist(Signature),

    #[error("Pending deposit with signature does not exist: {0}")]
    PendingDepositDoesNotExist(Signature),

    #[error("Account has insufficient balance: {0}")]
    AccountHasInsufficientBalance(Pubkey),

    #[error("Open order not exist: {0}")]
    OpenOrderDoesNotExist(String),

    #[error("Lot swap failed: {0}")]
    LotSwapFailed(String),

    #[error("Lot move failed: {0}")]
    LotMoveFailed(String),

    #[error("Lot delete failed: {0}")]
    LotDeleteFailed(String),

    #[error("Import failed: {0}")]
    ImportFailed(String),
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
    pub amount: u64, // lamports/tokens
    pub transfer: PendingTransfer,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PendingWithdrawal {
    pub exchange: Exchange,
    pub tag: String,
    pub token: MaybeToken,
    pub amount: u64, // lamports/tokens

    #[serde(with = "field_as_string")]
    pub from_address: Pubkey,

    #[serde(with = "field_as_string")]
    pub to_address: Pubkey,

    pub lots: Vec<Lot>,
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

    #[serde(default = "MaybeToken::SOL")]
    pub token: MaybeToken,

    pub lots: Vec<Lot>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct PendingSwap {
    #[serde(with = "field_as_string")]
    pub signature: Signature, // transaction signature of the swap
    pub last_valid_block_height: u64,

    #[serde(with = "field_as_string")]
    pub address: Pubkey,

    pub from_token: MaybeToken,
    pub from_token_price: Decimal,

    pub to_token: MaybeToken,
    pub to_token_price: Decimal,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub side: OrderSide,
    pub creation_time: DateTime<Utc>,
    pub exchange: Exchange,
    pub pair: String,
    pub price: f64,
    pub order_id: String,
    pub lots: Vec<Lot>, // if OrderSide::Sell the lots in the order; empty if OrderSide::Buy
    pub ui_amount: Option<f64>, // if OrderSide::Buy, `Some` amount that to buy; `None` if OrderSide::Sell

    #[serde(with = "field_as_string")]
    pub deposit_address: Pubkey,

    #[serde(default = "MaybeToken::SOL")]
    pub token: MaybeToken,
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
    Exchange {
        exchange: Exchange,
        pair: String,
        order_id: String,
    },
    NotAvailable, // Generic acquisition subject to income tax
    Fiat,         // Generic acquisition with post-tax fiat
    Swap {
        #[serde(with = "field_as_string")]
        signature: Signature,
        token: MaybeToken,
        amount: Option<u64>,
    },
}

impl fmt::Display for LotAcquistionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LotAcquistionKind::EpochReward { epoch, slot } => {
                write!(f, "epoch {} reward (slot {})", epoch, slot)
            }
            LotAcquistionKind::Transaction { signature, .. } => write!(f, "{}", signature),
            LotAcquistionKind::Exchange {
                exchange,
                pair,
                order_id,
            } => write!(f, "{:?} {}, order {}", exchange, pair, order_id),
            LotAcquistionKind::Fiat => {
                write!(f, "post tax")
            }
            LotAcquistionKind::NotAvailable => {
                write!(f, "other income")
            }
            LotAcquistionKind::Swap {
                token,
                amount,
                signature,
            } => {
                if let Some(amount) = amount {
                    write!(
                        f,
                        "Swap from {}{}, {}",
                        token.symbol(),
                        token
                            .ui_amount(*amount)
                            .separated_string_with_fixed_place(2),
                        signature
                    )
                } else {
                    write!(f, "Swap from {}, {}", token, signature)
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LotAcquistion {
    pub when: NaiveDate,
    price: Option<f64>,             // USD per SOL/token
    decimal_price: Option<Decimal>, // Prefer over `price` if Some(_)
    pub kind: LotAcquistionKind,
}

impl LotAcquistion {
    pub fn new(when: NaiveDate, decimal_price: Decimal, kind: LotAcquistionKind) -> Self {
        Self {
            when,
            price: None,
            decimal_price: Some(decimal_price),
            kind,
        }
    }

    pub fn price(&self) -> Decimal {
        self.decimal_price
            .unwrap_or_else(|| Decimal::from_f64(self.price.unwrap_or_default()).unwrap())
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Lot {
    pub lot_number: usize,
    pub acquisition: LotAcquistion,
    pub amount: u64, // lamports/tokens
}

impl Lot {
    // Figure the amount of income that the Lot incurred
    pub fn income(&self, token: MaybeToken) -> f64 {
        match self.acquisition.kind {
            // These lots were acquired pre-tax
            LotAcquistionKind::EpochReward { .. } | LotAcquistionKind::NotAvailable => {
                (self.acquisition.price()
                    * Decimal::from_f64(token.ui_amount(self.amount)).unwrap())
                .try_into()
                .unwrap()
            }
            // Assume these kinds of lots are acquired with post-tax funds
            LotAcquistionKind::Exchange { .. }
            | LotAcquistionKind::Fiat
            | LotAcquistionKind::Swap { .. }
            | LotAcquistionKind::Transaction { .. } => 0.,
        }
    }
    // Figure the current cap gain/loss for the Lot
    pub fn cap_gain(&self, token: MaybeToken, current_price: Decimal) -> f64 {
        ((current_price - self.acquisition.price())
            * Decimal::from_f64(token.ui_amount(self.amount)).unwrap())
        .try_into()
        .unwrap()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum LotDisposalKind {
    Usd {
        exchange: Exchange,
        pair: String,
        order_id: String,
        fee: Option<(f64, String)>,
    },
    Other {
        description: String,
    },
    Swap {
        #[serde(with = "field_as_string")]
        signature: Signature,
        token: MaybeToken,
        amount: Option<u64>,
    },
    Fiat,
}

impl LotDisposalKind {
    pub fn fee(&self) -> Option<&(f64, String)> {
        match self {
            LotDisposalKind::Usd { fee, .. } => fee.as_ref(),
            LotDisposalKind::Other { .. }
            | LotDisposalKind::Swap { .. }
            | LotDisposalKind::Fiat { .. } => None,
        }
    }
}

impl fmt::Display for LotDisposalKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LotDisposalKind::Usd {
                exchange,
                pair,
                order_id,
                fee,
            } => write!(
                f,
                "{:?} {}, order {}{})",
                exchange,
                pair,
                order_id,
                match fee {
                    Some((amount, coin)) if *amount > 0. => format!(" (fee: {} {})", amount, coin),
                    _ => "".into(),
                }
            ),
            LotDisposalKind::Other { description } => write!(f, "{}", description),
            LotDisposalKind::Swap {
                token,
                amount,
                signature,
            } => {
                if let Some(amount) = amount {
                    write!(
                        f,
                        "Swap to {}{}, {}",
                        token.symbol(),
                        token
                            .ui_amount(*amount)
                            .separated_string_with_fixed_place(2),
                        signature
                    )
                } else {
                    write!(f, "Swap to {}, {}", token, signature)
                }
            }
            LotDisposalKind::Fiat => write!(f, "fiat"),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DisposedLot {
    pub lot: Lot,
    pub when: NaiveDate,
    price: Option<f64>,             // USD per SOL/token
    decimal_price: Option<Decimal>, // Prefer over `price` if Some(_)
    pub kind: LotDisposalKind,
    #[serde(default = "MaybeToken::SOL")]
    pub token: MaybeToken,
}

impl DisposedLot {
    pub fn price(&self) -> Decimal {
        self.decimal_price
            .unwrap_or_else(|| Decimal::from_f64(self.price.unwrap_or_default()).unwrap())
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TrackedAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    #[serde(default = "MaybeToken::SOL")]
    pub token: MaybeToken, // if token then `address` is the token owner
    pub description: String,
    pub last_update_epoch: Epoch,
    pub last_update_balance: u64, // lamports/tokens
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

    fn remove_lot(&mut self, lot_number: usize) {
        self.assert_lot_balance();
        let lots = std::mem::take(&mut self.lots);
        self.lots = lots
            .into_iter()
            .filter(|lot| lot.lot_number != lot_number)
            .collect();
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

    fn merge_or_add_lot(&mut self, new_lot: Lot) {
        for lot in self.lots.iter_mut() {
            if lot.acquisition == new_lot.acquisition {
                lot.amount += new_lot.amount;
                return;
            }
        }
        self.lots.push(new_lot);
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
        token: MaybeToken,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        if !self.db.lexists("deposits") {
            self.db.lcreate("deposits")?;
        }

        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;

        let deposit = PendingDeposit {
            exchange,
            amount,
            transfer: PendingTransfer {
                signature,
                last_valid_block_height,
                from_address,
                to_address: deposit_address,
                token,
                lots: from_account.extract_lots(self, amount, lot_numbers)?,
            },
        };
        self.db.ladd("deposits", &deposit).unwrap();

        self.update_account(from_account) // `update_account` calls `save`...
    }

    fn complete_deposit(
        &mut self,
        signature: Signature,
        success: Option<NaiveDate>,
    ) -> DbResult<()> {
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

        self.complete_transfer_or_deposit(transfer, success, false) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_deposit(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_deposit(signature, None)
    }

    pub fn confirm_deposit(&mut self, signature: Signature, when: NaiveDate) -> DbResult<()> {
        self.complete_deposit(signature, Some(when))
    }

    pub fn pending_deposits(&self, exchange: Option<Exchange>) -> Vec<PendingDeposit> {
        if !self.db.lexists("deposits") {
            // Handle buggy older databases with "deposits" saved as a value instead of list.
            if self.db.exists("deposits") {
                return self.db.get::<Vec<PendingDeposit>>("deposits").unwrap();
            }
            return vec![];
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

    #[allow(clippy::too_many_arguments)]
    pub fn record_swap(
        &mut self,
        signature: Signature,
        last_valid_block_height: u64,
        address: Pubkey,
        from_token: MaybeToken,
        from_token_price: Decimal,
        to_token: MaybeToken,
        to_token_price: Decimal,
    ) -> DbResult<()> {
        if !self.db.lexists("swaps") {
            self.db.lcreate("swaps")?;
        }

        let _ = self
            .get_account(address, from_token)
            .ok_or(DbError::AccountDoesNotExist(address, from_token))?;

        let pendining_swap = PendingSwap {
            signature,
            last_valid_block_height,
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
        };
        self.db.ladd("swaps", &pendining_swap).unwrap();
        self.save()
    }

    fn complete_swap(
        &mut self,
        signature: Signature,
        success: Option<(NaiveDate, u64, u64)>,
    ) -> DbResult<()> {
        let mut pending_swaps = self.pending_swaps();
        let PendingSwap {
            signature,
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
            ..
        } = pending_swaps
            .iter()
            .find(|pd| pd.signature == signature)
            .ok_or(DbError::PendingDepositDoesNotExist(signature))?
            .clone();

        pending_swaps.retain(|pd| pd.signature != signature);

        self.db.lrem_list("swaps")?;
        self.db.lcreate("swaps")?;
        self.db.lextend("swaps", &pending_swaps).unwrap();

        let mut from_account = self
            .get_account(address, from_token)
            .ok_or(DbError::AccountDoesNotExist(address, from_token))?;
        let mut to_account = self
            .get_account(address, to_token)
            .ok_or(DbError::AccountDoesNotExist(address, to_token))?;

        self.auto_save(false)?;
        if let Some((when, from_amount, to_amount)) = success {
            let lots = from_account.extract_lots(self, from_amount, None)?;
            let mut disposed_lots = self.disposed_lots();

            let to_amount_over_from_amount = to_amount as f64 / from_amount as f64;
            for lot in lots {
                let lot_from_amount = lot.amount as f64;
                let lot_to_amount = lot_from_amount * to_amount_over_from_amount;

                disposed_lots.push(DisposedLot {
                    lot,
                    when,
                    price: None,
                    decimal_price: Some(from_token_price),
                    kind: LotDisposalKind::Swap {
                        signature,
                        token: to_token,
                        amount: Some(lot_to_amount as u64),
                    },
                    token: from_token,
                });
            }
            self.db.set("disposed-lots", &disposed_lots).unwrap();

            to_account.merge_or_add_lot(Lot {
                lot_number: self.next_lot_number(),
                acquisition: LotAcquistion {
                    price: None,
                    decimal_price: Some(to_token_price),
                    when,
                    kind: LotAcquistionKind::Swap {
                        signature,
                        token: from_token,
                        amount: Some(from_amount),
                    },
                },
                amount: to_amount,
            });
            to_account.last_update_balance += to_amount;
            self.update_account(from_account)?;
            self.update_account(to_account)?;
        }
        self.auto_save(true)
    }

    pub fn cancel_swap(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_swap(signature, None)
    }

    pub fn confirm_swap(
        &mut self,
        signature: Signature,
        when: NaiveDate,
        from_amount: u64,
        to_amount: u64,
    ) -> DbResult<()> {
        self.complete_swap(signature, Some((when, from_amount, to_amount)))
    }

    pub fn pending_swaps(&self) -> Vec<PendingSwap> {
        if !self.db.lexists("swaps") {
            return vec![];
        }
        self.db
            .liter("swaps")
            .filter_map(|item_iter| item_iter.get_item::<PendingSwap>())
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_withdrawal(
        &mut self,
        exchange: Exchange,
        tag: String,
        token: MaybeToken,
        amount: u64,
        from_address: Pubkey,
        to_address: Pubkey,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        if !self.db.lexists("withdrawals") {
            self.db.lcreate("withdrawals")?;
        }

        {
            let pending_withdrawals = self.pending_withdrawals(None);
            if pending_withdrawals.iter().any(|pw| pw.tag == tag) {
                panic!("Withdrawal tag already present in database: {}", tag);
            }
        }

        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;

        let lots = if token.fiat_fungible() {
            // invent a new lot if `token.fiat_fungible()`
            assert!(from_account.lots.is_empty());

            let today = Local::now().date();
            let when = NaiveDate::from_ymd(today.year(), today.month(), today.day());

            vec![Lot {
                lot_number: self.next_lot_number(),
                acquisition: LotAcquistion {
                    price: Some(1.),
                    decimal_price: None,
                    when,
                    kind: LotAcquistionKind::Fiat,
                },
                amount,
            }]
        } else {
            from_account.extract_lots(self, amount, lot_numbers)?
        };

        let withdrawal = PendingWithdrawal {
            exchange,
            tag,
            token,
            amount,
            from_address,
            to_address,
            lots,
        };

        self.db.ladd("withdrawals", &withdrawal).unwrap();
        self.update_account(from_account) // `update_account` calls `save`.../
    }

    fn remove_pending_withdrawal(&mut self, tag: &str) -> DbResult<()> {
        let mut pending_withdrawals = self.pending_withdrawals(None);
        pending_withdrawals.retain(|pw| pw.tag != tag);

        self.db.lrem_list("withdrawals")?;
        self.db.lcreate("withdrawals")?;
        self.db
            .lextend("withdrawals", &pending_withdrawals)
            .unwrap();

        Ok(())
    }

    pub fn cancel_withdrawal(
        &mut self,
        PendingWithdrawal {
            tag,
            from_address,
            token,
            lots,
            ..
        }: PendingWithdrawal,
    ) -> DbResult<()> {
        self.remove_pending_withdrawal(&tag)?;

        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;
        if !token.fiat_fungible() {
            from_account.merge_lots(lots);
        }
        self.update_account(from_account) // `update_account` calls `save`...
    }

    pub fn confirm_withdrawal(
        &mut self,
        PendingWithdrawal {
            tag,
            to_address,
            token,
            lots,
            ..
        }: PendingWithdrawal,
    ) -> DbResult<()> {
        self.remove_pending_withdrawal(&tag)?;

        let mut to_account = self
            .get_account(to_address, token)
            .ok_or(DbError::AccountDoesNotExist(to_address, token))?;

        to_account.merge_lots(lots);
        self.update_account(to_account) // `update_account` calls `save`...
    }

    pub fn pending_withdrawals(&self, exchange: Option<Exchange>) -> Vec<PendingWithdrawal> {
        if !self.db.lexists("withdrawals") {
            return vec![];
        }

        self.db
            .liter("withdrawals")
            .filter_map(|item_iter| item_iter.get_item::<PendingWithdrawal>())
            .filter(|pending_withdrawal| {
                if let Some(exchange) = exchange {
                    pending_withdrawal.exchange == exchange
                } else {
                    true
                }
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_order(
        &mut self,
        side: OrderSide,
        deposit_account: TrackedAccount,
        exchange: Exchange,
        pair: String,
        price: f64,
        order_id: String,
        lots: Vec<Lot>,
        ui_amount: Option<f64>,
    ) -> DbResult<()> {
        match side {
            OrderSide::Buy => {
                assert!(lots.is_empty());
                assert!(ui_amount.is_some())
            }
            OrderSide::Sell => {
                assert!(!lots.is_empty());
                assert!(ui_amount.is_none())
            }
        }
        let mut open_orders = self.open_orders(None, None);
        open_orders.push(OpenOrder {
            side,
            creation_time: Utc::now(),
            exchange,
            pair,
            price,
            order_id,
            lots,
            deposit_address: deposit_account.address,
            token: deposit_account.token,
            ui_amount,
        });
        self.db.set("orders", &open_orders).unwrap();
        self.update_account(deposit_account) // `update_account` calls `save`...
    }

    #[allow(dead_code)]
    pub fn update_order_price(&mut self, order_id: &str, price: f64) -> DbResult<()> {
        let orders: Vec<_> = self
            .db
            .get::<Vec<OpenOrder>>("orders")
            .unwrap_or_default()
            .into_iter()
            .map(|mut order| {
                if order.order_id == order_id {
                    order.price = price
                }
                order
            })
            .collect::<Vec<_>>();
        self.db.set("orders", &orders).unwrap();
        self.save()
    }

    pub fn close_order(
        &mut self,
        order_id: &str,
        amount: u64,
        filled_amount: u64,
        price: f64,
        when: NaiveDate,
        fee: Option<(f64, String)>,
    ) -> DbResult<()> {
        self.auto_save(false)?;
        let mut open_orders = self.open_orders(None, None);

        let OpenOrder {
            exchange,
            side,
            pair,
            order_id,
            lots,
            deposit_address,
            token,
            ..
        } = open_orders
            .iter()
            .find(|o| o.order_id == order_id)
            .ok_or_else(|| DbError::OpenOrderDoesNotExist(order_id.to_string()))?
            .clone();

        open_orders.retain(|o| o.order_id != order_id);
        self.db.set("orders", &open_orders).unwrap();

        match side {
            OrderSide::Buy => {
                assert!(lots.is_empty());

                if filled_amount > 0 {
                    let mut deposit_account = self
                        .get_account(deposit_address, token)
                        .ok_or(DbError::AccountDoesNotExist(deposit_address, token))?;

                    deposit_account.merge_lots(vec![Lot {
                        lot_number: self.next_lot_number(),
                        acquisition: LotAcquistion {
                            when,
                            price: Some(price),
                            decimal_price: None,
                            kind: LotAcquistionKind::Exchange {
                                exchange,
                                pair,
                                order_id,
                            },
                        },
                        amount: filled_amount,
                    }]);
                    self.update_account(deposit_account)?;
                }
            }
            OrderSide::Sell => {
                let lot_balance: u64 = lots.iter().map(|lot| lot.amount).sum();
                assert_eq!(lot_balance, amount, "Order lot balance mismatch");
                assert!(filled_amount <= amount);

                let (filled_lots, cancelled_lots) = split_lots(self, lots, filled_amount, None);

                if !filled_lots.is_empty() {
                    let mut disposed_lots = self.disposed_lots();
                    for lot in filled_lots {
                        // Split fee proportionally across all disposed lots
                        let fee = fee.clone().map(|(fee_amount, fee_coin)| {
                            (
                                lot.amount as f64 / filled_amount as f64 * fee_amount,
                                fee_coin,
                            )
                        });
                        disposed_lots.push(DisposedLot {
                            lot,
                            when,
                            price: Some(price),
                            decimal_price: None,
                            kind: LotDisposalKind::Usd {
                                exchange,
                                pair: pair.clone(),
                                order_id: order_id.clone(),
                                fee,
                            },
                            token,
                        });
                    }
                    self.db.set("disposed-lots", &disposed_lots).unwrap();
                }

                if !cancelled_lots.is_empty() {
                    let mut deposit_account = self
                        .get_account(deposit_address, token)
                        .ok_or(DbError::AccountDoesNotExist(deposit_address, token))?;

                    deposit_account.merge_lots(cancelled_lots);
                    self.update_account(deposit_account)?;
                }
            }
        }
        self.auto_save(true)
    }

    pub fn record_disposal(
        &mut self,
        from_address: Pubkey,
        token: MaybeToken,
        amount: u64,
        description: String,
        when: NaiveDate,
        decimal_price: Decimal,
    ) -> DbResult<Vec<DisposedLot>> {
        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;
        let lots = from_account.extract_lots(self, amount, None)?;
        let disposed_lots = self.record_lots_disposal(
            token,
            lots,
            LotDisposalKind::Other { description },
            when,
            decimal_price,
        )?;
        self.update_account(from_account)?; // `update_account` calls `save`...
        Ok(disposed_lots)
    }

    // The caller must call `save()`...
    fn record_lots_disposal(
        &mut self,
        token: MaybeToken,
        lots: Vec<Lot>,
        kind: LotDisposalKind,
        when: NaiveDate,
        decimal_price: Decimal,
    ) -> DbResult<Vec<DisposedLot>> {
        let mut disposed_lots = self.disposed_lots();
        for lot in lots {
            disposed_lots.push(DisposedLot {
                lot,
                when,
                price: None,
                decimal_price: Some(decimal_price),
                kind: kind.clone(),
                token,
            });
        }
        self.db.set("disposed-lots", &disposed_lots)?;
        Ok(disposed_lots)
    }

    pub fn open_orders(
        &self,
        exchange: Option<Exchange>,
        side: Option<OrderSide>,
    ) -> Vec<OpenOrder> {
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
            .filter(|order| side.is_none() || Some(order.side) == side)
            .collect()
    }

    pub fn add_account_no_save(&mut self, account: TrackedAccount) -> DbResult<()> {
        account.assert_lot_balance();

        if !self.db.lexists("accounts") {
            self.db.lcreate("accounts")?;
        }

        if self.get_account(account.address, account.token).is_some() {
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
            .get_account_position(account.address, account.token)
            .ok_or(DbError::AccountDoesNotExist(account.address, account.token))?;
        assert!(
            self.db
                .lpop::<TrackedAccount>("accounts", position)
                .is_some(),
            "Cannot update unknown account: {} ({})",
            account.address,
            account.token,
        );
        self.db.ladd("accounts", &account).unwrap();
        self.save()
    }

    fn remove_account_no_save(&mut self, address: Pubkey, token: MaybeToken) -> DbResult<()> {
        let position = self
            .get_account_position(address, token)
            .ok_or(DbError::AccountDoesNotExist(address, token))?;
        assert!(
            self.db
                .lpop::<TrackedAccount>("accounts", position)
                .is_some(),
            "Cannot remove unknown account: {}",
            address
        );
        Ok(())
    }

    pub fn remove_account(&mut self, address: Pubkey, token: MaybeToken) -> DbResult<()> {
        self.remove_account_no_save(address, token)?;
        self.save()
    }

    fn get_account_position(&self, address: Pubkey, token: MaybeToken) -> Option<usize> {
        if self.db.lexists("accounts") {
            for (position, value) in self.db.liter("accounts").enumerate() {
                if let Some(tracked_account) = value.get_item::<TrackedAccount>() {
                    if tracked_account.address == address && tracked_account.token == token {
                        return Some(position);
                    }
                }
            }
        }
        None
    }

    pub fn get_account(&self, address: Pubkey, token: MaybeToken) -> Option<TrackedAccount> {
        if !self.db.lexists("accounts") {
            None
        } else {
            self.db
                .liter("accounts")
                .filter_map(|item_iter| item_iter.get_item::<TrackedAccount>())
                .find(|tracked_account| {
                    tracked_account.address == address && tracked_account.token == token
                })
        }
    }

    /// Returns all `MaybeToken`s associated with an `address`
    pub fn get_account_tokens(&self, address: Pubkey) -> Vec<TrackedAccount> {
        if !self.db.lexists("accounts") {
            vec![]
        } else {
            self.db
                .liter("accounts")
                .filter_map(|item_iter| item_iter.get_item::<TrackedAccount>())
                .filter(|tracked_account| tracked_account.address == address)
                .collect()
        }
    }

    pub fn get_accounts(&self) -> Vec<TrackedAccount> {
        if !self.db.lexists("accounts") {
            return vec![];
        }
        self.db
            .liter("accounts")
            .filter_map(|item_iter| item_iter.get_item::<TrackedAccount>())
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
            .get_account_position(sweep_stake_account.address, MaybeToken::SOL())
            .ok_or_else(|| {
                DbError::AccountDoesNotExist(sweep_stake_account.address, MaybeToken::SOL())
            })?;
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
            token: MaybeToken::SOL(),
            description: "Transitory stake account".to_string(),
            last_update_balance: 0,
            last_update_epoch: current_epoch,
            lots: vec![],
            no_sync: None,
        })
    }

    pub fn remove_transitory_sweep_stake_address(&mut self, address: Pubkey) -> DbResult<()> {
        let token = MaybeToken::SOL();
        let _ = self.remove_account_no_save(address, token);

        let mut transitory_sweep_stake_addresses = self.get_transitory_sweep_stake_addresses();

        if !transitory_sweep_stake_addresses.contains(&address) {
            Err(DbError::AccountDoesNotExist(address, token))
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
        token: MaybeToken,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        let mut pending_transfers = self.pending_transfers();

        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;
        let _to_account = self
            .get_account(to_address, token)
            .ok_or(DbError::AccountDoesNotExist(to_address, token))?;

        pending_transfers.push(PendingTransfer {
            signature,
            last_valid_block_height,
            from_address,
            to_address,
            token,
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
        success: Option<NaiveDate>,
        track_fiat_lots: bool,
    ) -> DbResult<()> {
        let PendingTransfer {
            from_address,
            to_address,
            lots,
            token,
            ..
        } = pending_transfer;

        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;
        let mut to_account = self
            .get_account(to_address, token)
            .ok_or(DbError::AccountDoesNotExist(to_address, token))?;

        self.auto_save(false)?;

        if let Some(when) = success {
            match (token.fiat_fungible(), track_fiat_lots) {
                (false, _) | (true, true) => {
                    to_account.merge_lots(lots);
                }
                (true, false) => {
                    let _ = self.record_lots_disposal(
                        token,
                        lots,
                        LotDisposalKind::Other {
                            description: "fiat".into(),
                        },
                        when,
                        Decimal::from_f64(1.).unwrap(),
                    );
                }
            }
        } else {
            from_account.merge_lots(lots);
        }

        self.update_account(to_account)?;
        self.update_account(from_account)?;
        self.auto_save(true)
    }

    fn complete_transfer(
        &mut self,
        signature: Signature,
        success: Option<NaiveDate>,
    ) -> DbResult<()> {
        let mut pending_transfers = self.pending_transfers();

        let transfer = pending_transfers
            .iter()
            .find(|pt| pt.signature == signature)
            .ok_or(DbError::PendingTransferDoesNotExist(signature))?
            .clone();

        pending_transfers.retain(|pt| pt.signature != signature);
        self.db.set("transfers", &pending_transfers).unwrap();

        self.complete_transfer_or_deposit(transfer, success, true) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_transfer(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_transfer(signature, None)
    }

    pub fn confirm_transfer(&mut self, signature: Signature, when: NaiveDate) -> DbResult<()> {
        self.complete_transfer(signature, Some(when))
    }

    pub fn pending_transfers(&self) -> Vec<PendingTransfer> {
        self.db.get("transfers").unwrap_or_default()
    }

    pub fn disposed_lots(&self) -> Vec<DisposedLot> {
        let mut disposed_lots: Vec<DisposedLot> = self.db.get("disposed-lots").unwrap_or_default();
        disposed_lots.sort_by_key(|lot| lot.when);
        disposed_lots
    }

    pub fn swap_lots(&mut self, lot_number1: usize, lot_number2: usize) -> DbResult<()> {
        self.auto_save(false)?;

        let mut disposed_lot = self
            .disposed_lots()
            .into_iter()
            .filter(|dl| [lot_number1, lot_number2].contains(&dl.lot.lot_number))
            .collect::<Vec<_>>();

        if disposed_lot.len() == 2 {
            return Err(DbError::LotSwapFailed("Both lots are disposed".into()));
        }
        let disposed_lot = disposed_lot.drain(..).next();

        let mut tracked_accounts = vec![];
        for account in self.get_accounts().into_iter() {
            let lots = account
                .lots
                .iter()
                .filter(|lot| [lot_number1, lot_number2].contains(&lot.lot_number))
                .cloned()
                .collect::<Vec<_>>();
            if lots.len() == 2 {
                return Err(DbError::LotSwapFailed(format!(
                    "Both lots are in the same account: {}",
                    account.address
                )));
            }
            if let Some(lot) = lots.get(0) {
                let mut account = account.clone();
                account.remove_lot(lot.lot_number);
                tracked_accounts.push((lot.clone(), account));
            }
        }

        if let Some(mut disposed_lot) = disposed_lot {
            if tracked_accounts.len() != 1 {
                return Err(DbError::LotSwapFailed("Unknown lot".into()));
            }

            let (mut lot2, mut account2) = tracked_accounts.pop().unwrap();

            if account2.token != disposed_lot.token {
                return Err(DbError::LotSwapFailed(format!(
                    "Token mismatch ({} != {})",
                    account2.token, disposed_lot.token
                )));
            }

            if lot2.acquisition.when >= disposed_lot.when {
                return Err(DbError::LotSwapFailed(format!(
                    "Lot {} was acquired after disposal of lot {}",
                    lot2.lot_number, disposed_lot.lot.lot_number,
                )));
            }

            let mut disposed_lots = self
                .disposed_lots()
                .into_iter()
                .filter(|dl| disposed_lot.lot.lot_number != dl.lot.lot_number)
                .collect::<Vec<_>>();

            std::mem::swap(&mut disposed_lot.lot.lot_number, &mut lot2.lot_number);

            #[allow(clippy::comparison_chain)]
            if disposed_lot.lot.amount < lot2.amount {
                let mut lot2_split = lot2.clone();
                lot2_split.amount -= disposed_lot.lot.amount;
                account2.lots.push(lot2_split); // TODO: merge_or_add

                lot2.lot_number = self.next_lot_number();
                lot2.amount = disposed_lot.lot.amount;
            } else if lot2.amount < disposed_lot.lot.amount {
                let mut disposed_lot_split = disposed_lot.clone();
                disposed_lot_split.lot.lot_number = self.next_lot_number();
                disposed_lot_split.lot.amount -= lot2.amount;

                disposed_lot.lot.amount = lot2.amount;
            }

            account2.merge_or_add_lot(disposed_lot.lot);
            disposed_lot.lot = lot2;
            disposed_lots.push(disposed_lot);

            self.db.set("disposed-lots", &disposed_lots)?;
            self.update_account(account2)?;
        } else {
            if tracked_accounts.len() != 2 {
                return Err(DbError::LotSwapFailed("Unknown lot".into()));
            }

            let (mut lot1, mut account1) = tracked_accounts.pop().unwrap();
            let (mut lot2, mut account2) = tracked_accounts.pop().unwrap();

            if account2.token != account1.token {
                return Err(DbError::LotSwapFailed(format!(
                    "Token mismatch ({} != {})",
                    account2.token, account1.token
                )));
            }

            std::mem::swap(&mut lot1.lot_number, &mut lot2.lot_number);

            #[allow(clippy::comparison_chain)]
            if lot1.amount < lot2.amount {
                let mut lot2_split = lot2.clone();
                lot2_split.amount -= lot1.amount;
                account2.merge_or_add_lot(lot2_split);

                lot2.lot_number = self.next_lot_number();
                lot2.amount = lot1.amount;
            } else if lot2.amount < lot1.amount {
                let mut lot1_split = lot1.clone();
                lot1_split.amount -= lot2.amount;
                account1.merge_or_add_lot(lot1_split);

                lot1.lot_number = self.next_lot_number();
                lot1.amount = lot2.amount;
            }

            account1.merge_or_add_lot(lot2);
            account2.merge_or_add_lot(lot1);
            self.update_account(account1)?;
            self.update_account(account2)?;
        }

        self.auto_save(true)
    }

    pub fn delete_lot(&mut self, lot_number: usize) -> DbResult<()> {
        let mut account = self
            .get_accounts()
            .into_iter()
            .find(|tracked_account| {
                tracked_account
                    .lots
                    .iter()
                    .any(|lot| lot.lot_number == lot_number)
            })
            .ok_or_else(|| DbError::LotDeleteFailed(format!("Unknown lot: {}", lot_number)))?;

        let lot = account
            .lots
            .iter()
            .find(|lot| lot.lot_number == lot_number)
            .cloned()
            .unwrap();

        account.remove_lot(lot_number);
        account.last_update_balance -= lot.amount;

        self.update_account(account)
    }

    pub fn move_lot(&mut self, lot_number: usize, to_address: Pubkey) -> DbResult<()> {
        self.auto_save(false)?;

        let mut from_account = self
            .get_accounts()
            .into_iter()
            .find(|tracked_account| {
                tracked_account
                    .lots
                    .iter()
                    .any(|lot| lot.lot_number == lot_number)
            })
            .ok_or_else(|| DbError::LotMoveFailed(format!("Unknown lot: {}", lot_number)))?;

        let mut to_account = self
            .get_accounts()
            .into_iter()
            .find(|tracked_account| {
                tracked_account.address == to_address && tracked_account.token == from_account.token
            })
            .ok_or_else(|| {
                DbError::LotMoveFailed(format!("Unknown destination account: {}", to_address))
            })?;

        if to_account.token != from_account.token {
            return Err(DbError::LotMoveFailed(format!(
                "Token mismatch ({} != {})",
                to_account.token, from_account.token
            )));
        }

        let lot = from_account
            .lots
            .iter()
            .find(|lot| lot.lot_number == lot_number)
            .cloned()
            .unwrap();

        from_account.remove_lot(lot_number);
        to_account.last_update_balance += lot.amount;
        from_account.last_update_balance -= lot.amount;
        to_account.merge_or_add_lot(lot);

        self.update_account(to_account)?;
        self.update_account(from_account)?;

        self.auto_save(true)
    }

    pub fn import_db(&mut self, other_db: Self) -> DbResult<()> {
        if other_db.pending_deposits(None).len()
            + other_db.pending_swaps().len()
            + other_db.pending_withdrawals(None).len()
            + other_db.pending_transfers().len()
            + other_db.open_orders(None, None).len()
            > 0
        {
            return Err(DbError::ImportFailed(
                "Unable to import database with pending operations".into(),
            ));
        }

        self.auto_save(false)?;
        let other_accounts = other_db.get_accounts();
        for mut other_account in other_accounts {
            for lot in other_account.lots.iter_mut() {
                lot.lot_number = self.next_lot_number();
            }
            self.add_account(other_account)?;
        }

        let mut disposed_lots = self.disposed_lots();
        let other_disposed_lots = other_db.disposed_lots();
        for mut other_disposed_lot in other_disposed_lots {
            other_disposed_lot.lot.lot_number = self.next_lot_number();
            disposed_lots.push(other_disposed_lot);
        }
        self.db.set("disposed-lots", &disposed_lots)?;

        self.auto_save(true)?;
        Ok(())
    }
}
