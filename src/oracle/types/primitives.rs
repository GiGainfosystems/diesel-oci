use std::error::Error;

use diesel::deserialize::FromSql;
use diesel::result::Error as DieselError;
use diesel::sql_types::*;
use oracle::backend::Oracle;
use std::ffi::CStr;

use super::super::connection::OracleValue;

impl FromSql<Text, Oracle> for String {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let pos = bytes
            .bytes
            .iter()
            .position(|&b| b == 0)
            .ok_or(Box::new(DieselError::DeserializationError(
                "Expected at least one null byte".into(),
            )) as Box<Error + Send + Sync>)?;
        Ok(CStr::from_bytes_with_nul(&bytes.bytes[..=pos])?
            .to_str()?
            .to_owned())
    }
}
