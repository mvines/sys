use {
    crate::{field_as_string, metrics::MetricsConfig},
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
        collections::{HashMap, HashSet},
        fmt, fs, io,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
    strum::{EnumString, IntoStaticStr},
    sys::{exchange::*, token::*},
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Io: {0}")]
    Io(#[from] io::Error),

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

    let legacy_db_filename = db_path.join("◎.db");
    let credentials_db_filename = db_path.join("🤐.db");
    let data_filename = db_path.join("data.json");

    let credentials_db = if credentials_db_filename.exists() {
        PickleDb::load_json(credentials_db_filename, PickleDbDumpPolicy::DumpUponRequest)?
    } else {
        PickleDb::new_json(credentials_db_filename, PickleDbDumpPolicy::DumpUponRequest)
    };

    let data = if data_filename.exists() {
        DbData::load(&data_filename)?
    } else if legacy_db_filename.exists() {
        let db = PickleDb::load_json(&legacy_db_filename, PickleDbDumpPolicy::NeverDump)?;
        DbData::import_legacy_db(&db)
    } else {
        DbData::default()
    };

    Ok(Db {
        data,
        data_filename,
        credentials_db,
        auto_save: true,
    })
}

pub struct Db {
    credentials_db: PickleDb,
    data: DbData,
    data_filename: PathBuf,
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
    pub fee: u64,    // in same lamports/tokens as `amount`

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
    pub from_token: MaybeToken,
    #[serde(with = "field_as_string")]
    pub to_address: Pubkey,
    pub to_token: MaybeToken,

    pub lots: Vec<Lot>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

    pub lot_selection_method: LotSelectionMethod,
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

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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
                write!(f, "epoch {epoch} reward (slot {slot})")
            }
            LotAcquistionKind::Transaction { signature, .. } => write!(f, "{signature}"),
            LotAcquistionKind::Exchange {
                exchange,
                pair,
                order_id,
            } => write!(f, "{exchange:?} {pair}, order {order_id}"),
            LotAcquistionKind::Fiat => {
                write!(f, "post tax")
            }
            LotAcquistionKind::NotAvailable => {
                write!(f, "other income")
            }
            LotAcquistionKind::Swap {
                token,
                signature,
                /*amount*/
                ..
            } => {
                /*
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
                */
                write!(f, "Swap from {token}, {signature}")
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

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, EnumString, IntoStaticStr)]
pub enum LotSelectionMethod {
    #[strum(serialize = "fifo")]
    FirstInFirstOut,
    #[strum(serialize = "lifo")]
    LastInFirstOut,
    #[strum(serialize = "lowest-basis")]
    LowestBasis,
    #[strum(serialize = "highest-basis")]
    HighestBasis,
}

pub const POSSIBLE_LOT_SELECTION_METHOD_VALUES: &[&str] =
    &["fifo", "lifo", "lowest-basis", "highest-basis"];

impl Default for LotSelectionMethod {
    fn default() -> Self {
        Self::FirstInFirstOut
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
    WithdrawalFee {
        exchange: Exchange,
        tag: String,
    },
}

impl LotDisposalKind {
    pub fn fee(&self) -> Option<&(f64, String)> {
        match self {
            LotDisposalKind::Usd { fee, .. } => fee.as_ref(),
            LotDisposalKind::Other { .. }
            | LotDisposalKind::Swap { .. }
            | LotDisposalKind::WithdrawalFee { .. }
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
                    Some((amount, coin)) if *amount > 0. => format!(" (fee: {amount} {coin})"),
                    _ => "".into(),
                }
            ),
            LotDisposalKind::Other { description } => write!(f, "{description}"),
            LotDisposalKind::WithdrawalFee { exchange, tag } => {
                write!(f, "{exchange} withdrawal fee [{tag}])")
            }
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
                    write!(f, "Swap to {token}, {signature}")
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
    mut lots: Vec<Lot>,
    amount: u64,
    lot_selection_method: LotSelectionMethod,
    lot_numbers: Option<HashSet<usize>>,
) -> (Vec<Lot>, Vec<Lot>) {
    let mut extracted_lots = vec![];
    let mut remaining_lots = vec![];

    match lot_selection_method {
        LotSelectionMethod::FirstInFirstOut => {
            lots.sort_by(|a, b| a.acquisition.when.cmp(&b.acquisition.when));
            if !lots.is_empty() {
                // Assume the oldest lot is the rent-reserve. Extract it as the last resort
                let first_lot = lots.remove(0);
                lots.push(first_lot);
            }
        }
        LotSelectionMethod::LastInFirstOut => {
            lots.sort_by(|a, b| b.acquisition.when.cmp(&a.acquisition.when))
        }
        LotSelectionMethod::LowestBasis => {
            lots.sort_by(|a, b| a.acquisition.price().cmp(&b.acquisition.price()))
        }
        LotSelectionMethod::HighestBasis => {
            lots.sort_by(|a, b| b.acquisition.price().cmp(&a.acquisition.price()))
        }
    }

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
            "Lot balance mismatch: {self:?}"
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
        lot_selection_method: LotSelectionMethod,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<Vec<Lot>> {
        self.assert_lot_balance();

        let mut lots = std::mem::take(&mut self.lots);
        lots.sort_by_key(|lot| lot.acquisition.when);

        let balance: u64 = lots.iter().map(|lot| lot.amount).sum();
        if balance < amount {
            return Err(DbError::AccountHasInsufficientBalance(self.address));
        }

        let (extracted_lots, remaining_lots) =
            split_lots(db, lots, amount, lot_selection_method, lot_numbers);

        self.lots = remaining_lots;
        self.last_update_balance -= amount;
        self.assert_lot_balance();
        Ok(extracted_lots)
    }

    fn merge_lots(&mut self, lots: Vec<Lot>) {
        let mut amount = 0;
        for lot in lots {
            amount += lot.amount;
            if let Some(existing_lot) = self
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

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SweepStakeAccount {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
    pub stake_authority: PathBuf,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct TransitorySweepStake {
    #[serde(with = "field_as_string")]
    pub address: Pubkey,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TaxRate {
    pub income: f64,
    pub short_term_gain: f64,
    pub long_term_gain: f64,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ValidatorCreditScore {
    #[serde(with = "field_as_string")]
    pub vote_account: Pubkey,
    pub credits: u64,
}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct DbData {
    next_lot_number: usize,
    accounts: Vec<TrackedAccount>,
    open_orders: Vec<OpenOrder>,
    disposed_lots: Vec<DisposedLot>,
    pending_deposits: Vec<PendingDeposit>,
    pending_withdrawals: Vec<PendingWithdrawal>,
    pending_transfers: Vec<PendingTransfer>,
    pending_swaps: Vec<PendingSwap>,
    sweep_stake_account: Option<SweepStakeAccount>,
    transitory_sweep_stake_accounts: Vec<TransitorySweepStake>,
    tax_rate: Option<TaxRate>,
    validator_credit_scores: Option<HashMap<Epoch, Vec<ValidatorCreditScore>>>,
}

impl DbData {
    fn import_legacy_db(db: &PickleDb) -> Self {
        Self {
            next_lot_number: db.get::<usize>("next_lot_number").unwrap_or(0),
            accounts: db
                .liter("accounts")
                .filter_map(|item_iter| item_iter.get_item())
                .collect(),
            open_orders: db.get("orders").unwrap_or_default(),
            disposed_lots: db.get("disposed-lots").unwrap_or_default(),
            pending_deposits: db
                .lexists("deposits")
                .then(|| {
                    db.liter("deposits")
                        .filter_map(|item_iter| item_iter.get_item())
                        .collect()
                })
                .unwrap_or_default(),
            pending_withdrawals: db
                .lexists("withdrawals")
                .then(|| {
                    db.liter("withdrawals")
                        .filter_map(|item_iter| item_iter.get_item())
                        .collect()
                })
                .unwrap_or_default(),
            pending_transfers: db
                .lexists("transfers")
                .then(|| {
                    db.liter("transfers")
                        .filter_map(|item_iter| item_iter.get_item())
                        .collect()
                })
                .unwrap_or_default(),
            pending_swaps: db
                .lexists("swaps")
                .then(|| {
                    db.liter("swaps")
                        .filter_map(|item_iter| item_iter.get_item())
                        .collect()
                })
                .unwrap_or_default(),
            sweep_stake_account: db.get("sweep-stake-account"),
            transitory_sweep_stake_accounts: db
                .get("transitory-sweep-stake-accounts")
                .unwrap_or_default(),
            tax_rate: None,
            validator_credit_scores: None,
        }
    }

    fn load(filename: &Path) -> io::Result<Self> {
        let bytes = fs::read(filename)?;

        serde_json::from_str(std::str::from_utf8(&bytes).expect("invalid utf8")).map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("JSON parse failed: {err:?}"))
        })
    }

    fn save(&self, filename: &Path) -> io::Result<()> {
        let bytes = serde_json::to_string_pretty(self)?.into_bytes();

        let temp_filename = format!(
            "{}.temp.{}",
            filename.display(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        fs::write(&temp_filename, bytes)?;
        fs::rename(temp_filename, filename)?;

        Ok(())
    }
}

impl Db {
    pub fn set_exchange_credentials(
        &mut self,
        exchange: Exchange,
        exchange_credentials: ExchangeCredentials,
    ) -> DbResult<()> {
        self.clear_exchange_credentials(exchange)?;

        self.credentials_db
            .set(&format!("{exchange:?}"), &exchange_credentials)
            .unwrap();

        Ok(self.credentials_db.dump()?)
    }

    pub fn get_exchange_credentials(&self, exchange: Exchange) -> Option<ExchangeCredentials> {
        self.credentials_db.get(&format!("{exchange:?}"))
    }

    pub fn clear_exchange_credentials(&mut self, exchange: Exchange) -> DbResult<()> {
        if self.get_exchange_credentials(exchange).is_some() {
            self.credentials_db.rem(&format!("{exchange:?}")).ok();
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

    pub fn set_metrics_config(&mut self, metrics_config: MetricsConfig) -> DbResult<()> {
        self.clear_metrics_config()?;

        self.credentials_db
            .set("influxdb", &metrics_config)
            .unwrap();

        Ok(self.credentials_db.dump()?)
    }

    pub fn get_metrics_config(&self) -> Option<MetricsConfig> {
        self.credentials_db.get("influxdb")
    }

    pub fn clear_metrics_config(&mut self) -> DbResult<()> {
        if self.get_metrics_config().is_some() {
            self.credentials_db.rem("influxdb").ok();
            self.credentials_db.dump()?;
        }
        Ok(())
    }

    fn auto_save(&mut self, auto_save: bool) -> DbResult<()> {
        self.auto_save = auto_save;
        self.save()
    }

    fn save(&mut self) -> DbResult<()> {
        if self.auto_save {
            self.data.save(&self.data_filename)?;
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
        lot_selection_method: LotSelectionMethod,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
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
                from_token: token,
                to_address: deposit_address,
                to_token: token,
                lots: from_account.extract_lots(self, amount, lot_selection_method, lot_numbers)?,
            },
        };
        self.data.pending_deposits.push(deposit);
        self.update_account(from_account) // `update_account` calls `save`...
    }

    fn complete_deposit(
        &mut self,
        signature: Signature,
        success: Option<NaiveDate>,
    ) -> DbResult<()> {
        let PendingDeposit { transfer, .. } = self
            .data
            .pending_deposits
            .iter()
            .find(|pd| pd.transfer.signature == signature)
            .ok_or(DbError::PendingDepositDoesNotExist(signature))?
            .clone();

        self.data
            .pending_deposits
            .retain(|pd| pd.transfer.signature != signature);
        self.complete_transfer_or_deposit(transfer, success, false) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_deposit(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_deposit(signature, None)
    }

    pub fn confirm_deposit(&mut self, signature: Signature, when: NaiveDate) -> DbResult<()> {
        self.complete_deposit(signature, Some(when))
    }

    // Careful!
    pub fn drop_deposit(&mut self, signature: Signature) -> DbResult<()> {
        let _ = self
            .data
            .pending_deposits
            .iter()
            .find(|pd| pd.transfer.signature == signature)
            .ok_or(DbError::PendingDepositDoesNotExist(signature))?;
        self.data
            .pending_deposits
            .retain(|pd| pd.transfer.signature != signature);
        self.save()
    }

    pub fn pending_deposits(&self, exchange: Option<Exchange>) -> Vec<PendingDeposit> {
        self.data
            .pending_deposits
            .iter()
            .filter(|pending_deposit| {
                if let Some(exchange) = exchange {
                    pending_deposit.exchange == exchange
                } else {
                    true
                }
            })
            .cloned()
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
        lot_selection_method: LotSelectionMethod,
    ) -> DbResult<()> {
        let _ = self
            .get_account(address, from_token)
            .ok_or(DbError::AccountDoesNotExist(address, from_token))?;

        self.data.pending_swaps.push(PendingSwap {
            signature,
            last_valid_block_height,
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
            lot_selection_method,
        });
        self.save()
    }

    fn complete_swap(
        &mut self,
        signature: Signature,
        success: Option<(NaiveDate, u64, u64)>,
    ) -> DbResult<()> {
        let PendingSwap {
            signature,
            address,
            from_token,
            from_token_price,
            to_token,
            to_token_price,
            lot_selection_method,
            ..
        } = self
            .data
            .pending_swaps
            .iter()
            .find(|pd| pd.signature == signature)
            .ok_or(DbError::PendingDepositDoesNotExist(signature))?
            .clone();

        self.data
            .pending_swaps
            .retain(|pd| pd.signature != signature);

        let mut from_account = self
            .get_account(address, from_token)
            .ok_or(DbError::AccountDoesNotExist(address, from_token))?;
        let mut to_account = self
            .get_account(address, to_token)
            .ok_or(DbError::AccountDoesNotExist(address, to_token))?;

        self.auto_save(false)?;
        if let Some((when, from_amount, to_amount)) = success {
            let lots = from_account.extract_lots(self, from_amount, lot_selection_method, None)?;

            let to_amount_over_from_amount = to_amount as f64 / from_amount as f64;
            for lot in lots {
                let lot_from_amount = lot.amount as f64;
                let lot_to_amount = lot_from_amount * to_amount_over_from_amount;

                self.data.disposed_lots.push(DisposedLot {
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
        self.data.pending_swaps.clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_withdrawal(
        &mut self,
        exchange: Exchange,
        tag: String,
        token: MaybeToken,
        amount: u64,
        fee: u64,
        from_address: Pubkey,
        to_address: Pubkey,
        lot_selection_method: LotSelectionMethod,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        if self.data.pending_withdrawals.iter().any(|pw| pw.tag == tag) {
            panic!("Withdrawal tag already present in database: {tag}");
        }

        assert!(amount > fee);

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
            from_account.extract_lots(self, amount, lot_selection_method, lot_numbers)?
        };

        let withdrawal = PendingWithdrawal {
            exchange,
            tag,
            token,
            amount: amount - fee,
            fee,
            from_address,
            to_address,
            lots,
        };

        self.data.pending_withdrawals.push(withdrawal);
        self.update_account(from_account) // `update_account` calls `save`.../
    }

    // The caller must call `save()`...
    fn remove_pending_withdrawal(&mut self, tag: &str) {
        self.data.pending_withdrawals.retain(|pw| pw.tag != tag);
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
        self.remove_pending_withdrawal(&tag);

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
            exchange,
            tag,
            to_address,
            token,
            mut lots,
            fee,
            ..
        }: PendingWithdrawal,
        when: NaiveDate,
    ) -> DbResult<()> {
        self.remove_pending_withdrawal(&tag);

        if fee > 0 {
            assert!(lots[0].amount > fee); // TODO: handle a fee that's split across multiple lots
            let fee_price = lots[0].acquisition.price(); // Assume no gain/lost on the fee disposal for simplicity
            lots[0].amount -= fee;
            let fee_lot = Lot {
                lot_number: self.next_lot_number(),
                acquisition: lots[0].acquisition.clone(),
                amount: fee,
            };
            let _ = self.record_lots_disposal(
                token,
                vec![fee_lot],
                LotDisposalKind::WithdrawalFee { exchange, tag },
                when,
                fee_price,
            );
        }

        let mut to_account = self
            .get_account(to_address, token)
            .ok_or(DbError::AccountDoesNotExist(to_address, token))?;

        to_account.merge_lots(lots);
        self.update_account(to_account) // `update_account` calls `save`...
    }

    pub fn pending_withdrawals(&self, exchange: Option<Exchange>) -> Vec<PendingWithdrawal> {
        self.data
            .pending_withdrawals
            .iter()
            .filter(|pending_withdrawal| {
                if let Some(exchange) = exchange {
                    pending_withdrawal.exchange == exchange
                } else {
                    true
                }
            })
            .cloned()
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

        self.data.open_orders.push(OpenOrder {
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
        self.update_account(deposit_account) // `update_account` calls `save`...
    }

    #[allow(dead_code)]
    pub fn update_order_price(&mut self, order_id: &str, price: f64) -> DbResult<()> {
        self.data.open_orders = self
            .data
            .open_orders
            .iter()
            .map(|order| {
                let mut order = order.clone();
                if order.order_id == order_id {
                    order.price = price
                }
                order
            })
            .collect::<Vec<_>>();
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

        let OpenOrder {
            exchange,
            side,
            pair,
            order_id,
            lots,
            deposit_address,
            token,
            ..
        } = self
            .data
            .open_orders
            .iter()
            .find(|o| o.order_id == order_id)
            .ok_or_else(|| DbError::OpenOrderDoesNotExist(order_id.to_string()))?
            .clone();
        self.data.open_orders.retain(|o| o.order_id != order_id);

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

                let (filled_lots, cancelled_lots) = split_lots(
                    self,
                    lots,
                    filled_amount,
                    LotSelectionMethod::default(),
                    None,
                );

                if !filled_lots.is_empty() {
                    for lot in filled_lots {
                        // Split fee proportionally across all disposed lots
                        let fee = fee.clone().map(|(fee_amount, fee_coin)| {
                            (
                                lot.amount as f64 / filled_amount as f64 * fee_amount,
                                fee_coin,
                            )
                        });
                        self.data.disposed_lots.push(DisposedLot {
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

    #[allow(clippy::too_many_arguments)]
    pub fn record_disposal(
        &mut self,
        from_address: Pubkey,
        token: MaybeToken,
        amount: u64,
        description: String,
        when: NaiveDate,
        decimal_price: Decimal,
        lot_selection_method: LotSelectionMethod,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<Vec<DisposedLot>> {
        let mut from_account = self
            .get_account(from_address, token)
            .ok_or(DbError::AccountDoesNotExist(from_address, token))?;
        let lots = from_account.extract_lots(self, amount, lot_selection_method, lot_numbers)?;
        let disposed_lots = self.record_lots_disposal(
            token,
            lots,
            LotDisposalKind::Other { description },
            when,
            decimal_price,
        );
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
    ) -> Vec<DisposedLot> {
        let mut newly_disposed_lots = vec![];
        for lot in lots {
            let disposed_lot = DisposedLot {
                lot,
                when,
                price: None,
                decimal_price: Some(decimal_price),
                kind: kind.clone(),
                token,
            };
            self.data.disposed_lots.push(disposed_lot.clone());
            newly_disposed_lots.push(disposed_lot);
        }
        newly_disposed_lots
    }

    pub fn open_orders(
        &self,
        exchange: Option<Exchange>,
        side: Option<OrderSide>,
    ) -> Vec<OpenOrder> {
        self.data
            .open_orders
            .iter()
            .filter(|order| {
                if let Some(exchange) = exchange {
                    order.exchange == exchange
                } else {
                    true
                }
            })
            .filter(|order| side.is_none() || Some(order.side) == side)
            .cloned()
            .collect()
    }

    pub fn add_account_no_save(&mut self, account: TrackedAccount) -> DbResult<()> {
        account.assert_lot_balance();

        if self.get_account(account.address, account.token).is_some() {
            Err(DbError::AccountAlreadyExists(account.address))
        } else {
            self.data.accounts.push(account);
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
        self.data.accounts[position] = account;
        self.save()
    }

    fn remove_account_no_save(&mut self, address: Pubkey, token: MaybeToken) -> DbResult<()> {
        let position = self
            .get_account_position(address, token)
            .ok_or(DbError::AccountDoesNotExist(address, token))?;
        self.data.accounts.remove(position);
        Ok(())
    }

    pub fn remove_account(&mut self, address: Pubkey, token: MaybeToken) -> DbResult<()> {
        self.remove_account_no_save(address, token)?;
        self.save()
    }

    fn get_account_position(&self, address: Pubkey, token: MaybeToken) -> Option<usize> {
        for (position, tracked_account) in self.data.accounts.iter().enumerate() {
            if tracked_account.address == address && tracked_account.token == token {
                return Some(position);
            }
        }

        None
    }

    pub fn get_account(&self, address: Pubkey, token: MaybeToken) -> Option<TrackedAccount> {
        self.data
            .accounts
            .iter()
            .find(|tracked_account| {
                tracked_account.address == address && tracked_account.token == token
            })
            .cloned()
    }

    /// Returns all `MaybeToken`s associated with an `address`
    pub fn get_account_tokens(&self, address: Pubkey) -> Vec<TrackedAccount> {
        self.data
            .accounts
            .iter()
            .filter(|tracked_account| tracked_account.address == address)
            .cloned()
            .collect()
    }

    pub fn get_accounts(&self) -> Vec<TrackedAccount> {
        self.data.accounts.clone()
    }

    // The caller must call `save()`...
    pub fn next_lot_number(&mut self) -> usize {
        let next_lot_number = self.data.next_lot_number;
        self.data.next_lot_number += 1;
        next_lot_number
    }

    pub fn get_sweep_stake_account(&self) -> Option<SweepStakeAccount> {
        self.data.sweep_stake_account.clone()
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

        self.data.sweep_stake_account = Some(sweep_stake_account);
        self.save()
    }

    pub fn get_transitory_sweep_stake_addresses(&self) -> HashSet<Pubkey> {
        self.data
            .transitory_sweep_stake_accounts
            .iter()
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
        self.data.transitory_sweep_stake_accounts = transitory_sweep_stake_addresses
            .into_iter()
            .map(|address| TransitorySweepStake { address })
            .collect();
        self.save()
    }

    pub fn get_tax_rate(&self) -> Option<&TaxRate> {
        self.data.tax_rate.as_ref()
    }

    pub fn set_tax_rate(&mut self, tax_rate: TaxRate) -> DbResult<()> {
        self.data.tax_rate = Some(tax_rate);
        self.save()
    }

    pub fn contains_validator_credit_scores(&self, epoch: Epoch) -> bool {
        self.data
            .validator_credit_scores
            .as_ref()
            .and_then(|vcs| vcs.get(&epoch))
            .is_some()
    }

    pub fn get_validator_credit_scores(&self, epoch: Epoch) -> Vec<ValidatorCreditScore> {
        self.data
            .validator_credit_scores
            .as_ref()
            .and_then(|vcs| vcs.get(&epoch))
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_validator_credit_scores(
        &mut self,
        epoch: Epoch,
        validator_credit_scores: Vec<ValidatorCreditScore>,
    ) -> DbResult<()> {
        if self.data.validator_credit_scores.is_none() {
            self.data.validator_credit_scores = Some(HashMap::default());
        }

        *self
            .data
            .validator_credit_scores
            .as_mut()
            .unwrap()
            .entry(epoch)
            .or_default() = validator_credit_scores;

        // Retain the last 10 epochs
        self.data
            .validator_credit_scores
            .as_mut()
            .unwrap()
            .retain(|k, _| *k >= epoch.saturating_sub(10));

        self.save()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_transfer(
        &mut self,
        signature: Signature,
        last_valid_block_height: u64,
        amount: Option<u64>, // None = all
        from_address: Pubkey,
        from_token: MaybeToken,
        to_address: Pubkey,
        to_token: MaybeToken,
        lot_selection_method: LotSelectionMethod,
        lot_numbers: Option<HashSet<usize>>,
    ) -> DbResult<()> {
        assert_eq!(from_token.mint(), to_token.mint());

        let mut pending_transfers = self.pending_transfers();

        let mut from_account = self
            .get_account(from_address, from_token)
            .ok_or(DbError::AccountDoesNotExist(from_address, from_token))?;
        let _to_account = self
            .get_account(to_address, to_token)
            .ok_or(DbError::AccountDoesNotExist(to_address, to_token))?;

        pending_transfers.push(PendingTransfer {
            signature,
            last_valid_block_height,
            from_address,
            from_token,
            to_address,
            to_token,
            lots: from_account.extract_lots(
                self,
                amount.unwrap_or(from_account.last_update_balance),
                lot_selection_method,
                lot_numbers,
            )?,
        });

        self.data.pending_transfers = pending_transfers;
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
            from_token,
            to_address,
            to_token,
            lots,
            ..
        } = pending_transfer;

        let mut from_account = self
            .get_account(from_address, from_token)
            .ok_or(DbError::AccountDoesNotExist(from_address, from_token))?;
        let mut to_account = self
            .get_account(to_address, to_token)
            .ok_or(DbError::AccountDoesNotExist(to_address, to_token))?;

        self.auto_save(false)?;

        if let Some(when) = success {
            assert_eq!(from_token.fiat_fungible(), to_token.fiat_fungible());

            match (from_token.fiat_fungible(), track_fiat_lots) {
                (false, _) | (true, true) => {
                    to_account.merge_lots(lots);
                }
                (true, false) => {
                    let _ = self.record_lots_disposal(
                        from_token,
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
        self.data.pending_transfers = pending_transfers;

        self.complete_transfer_or_deposit(transfer, success, true) // `complete_transfer_or_deposit` calls `save`...
    }

    pub fn cancel_transfer(&mut self, signature: Signature) -> DbResult<()> {
        self.complete_transfer(signature, None)
    }

    pub fn confirm_transfer(&mut self, signature: Signature, when: NaiveDate) -> DbResult<()> {
        self.complete_transfer(signature, Some(when))
    }

    pub fn pending_transfers(&self) -> Vec<PendingTransfer> {
        self.data.pending_transfers.clone()
    }

    pub fn disposed_lots(&self) -> Vec<DisposedLot> {
        let mut disposed_lots = self.data.disposed_lots.clone();
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

            if account2.token != disposed_lot.token
                && [account2.token, disposed_lot.token]
                    .iter()
                    .filter(|t| t.is_sol() || t.token() == Some(Token::wSOL))
                    .count()
                    != 2
            {
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

            self.data.disposed_lots = disposed_lots;
            self.update_account(account2)?;
        } else {
            if tracked_accounts.len() != 2 {
                return Err(DbError::LotSwapFailed("Unknown lot".into()));
            }

            let (mut lot1, mut account1) = tracked_accounts.pop().unwrap();
            let (mut lot2, mut account2) = tracked_accounts.pop().unwrap();

            if account2.token != account1.token
                && [account1.token, account2.token]
                    .iter()
                    .filter(|t| t.is_sol() || t.token() == Some(Token::wSOL))
                    .count()
                    != 2
            {
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
            .ok_or_else(|| DbError::LotDeleteFailed(format!("Unknown lot: {lot_number}")))?;

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
            .ok_or_else(|| DbError::LotMoveFailed(format!("Unknown lot: {lot_number}")))?;

        let mut to_account = self
            .get_accounts()
            .into_iter()
            .find(|tracked_account| {
                tracked_account.address == to_address && tracked_account.token == from_account.token
            })
            .ok_or_else(|| {
                DbError::LotMoveFailed(format!("Unknown destination account: {to_address}"))
            })?;

        if from_account.address == to_account.address {
            return Err(DbError::LotMoveFailed(format!(
                "Destination and source accounts are the same: {} ({})",
                to_account.address, to_account.token
            )));
        }

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

        self.data.disposed_lots = disposed_lots;
        self.auto_save(true)?;
        Ok(())
    }
}
