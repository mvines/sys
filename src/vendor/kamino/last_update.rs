#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct LastUpdate {
    slot: u64,
    stale: u8,
    price_status: u8,
    placeholder: [u8; 6],
}
