pub enum Amount {
    Half,
    All,
    Exact(u64),
}

impl Amount {
    pub fn unwrap_or(self, all_amount: u64) -> u64 {
        match self {
            Self::All => all_amount,
            Self::Half => all_amount / 2,
            Self::Exact(exact) => exact,
        }
    }

    pub fn unwrap_or_else<F>(self, f: F) -> u64
    where
        F: std::ops::FnOnce() -> u64,
    {
        if let Self::Exact(exact) = self {
            exact
        } else {
            f()
        }
    }
}
