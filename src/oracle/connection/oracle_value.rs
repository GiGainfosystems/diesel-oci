#[derive(Debug)]
pub struct OracleValue<'a> {
    pub(crate) bytes: &'a [u8],
}

impl<'a> OracleValue<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}
