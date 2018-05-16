#[allow(missing_debug_implementations, missing_copy_implementations)]
pub struct OracleValue {
    pub bytes: Vec<u8>,
}

impl OracleValue {
    pub fn new() -> Self {
        OracleValue{
            bytes: Vec::new(),
        }
    }
}