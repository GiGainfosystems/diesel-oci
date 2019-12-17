use oracle::types::OciDataType;

#[derive(Debug)]
pub struct OracleValue<'a> {
    pub(crate) bytes: &'a [u8],
    tpe: OciDataType,
}

impl<'a> OracleValue<'a> {
    pub fn new(bytes: &'a [u8], tpe: OciDataType) -> Self {
        Self { bytes, tpe }
    }

    pub fn datatype(&self) -> OciDataType {
        self.tpe
    }
}
