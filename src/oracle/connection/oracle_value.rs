#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct OracleValue {
    pub(crate) bytes: [u8],
}

impl OracleValue {
    pub fn new(bytes: &[u8]) -> &Self {
        unsafe { &*(bytes as *const [u8] as *const Self) }
    }
}
