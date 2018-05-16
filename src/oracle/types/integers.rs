
use std::error::Error;
use std::io::Write;

use diesel::sql_types::*;
use diesel::deserialize::FromSql;
use diesel::serialize::{ToSql, IsNull, Output};

use oracle::backend::Oracle;

use std::ffi::{CString, CStr};
use std::os::raw::c_char;

use super::super::connection::OracleValue;

use byteorder::ReadBytesExt;
use diesel::backend::*;

pub type FromSqlResult<T> = Result<T, ErrorType>;
pub type ErrorType = Box<Error + Send + Sync>;
pub type ToSqlResult = FromSqlResult<IsNull>;

impl FromSql<BigInt, Oracle> for i64 {
    fn from_sql(bytes: Option<&OracleValue>) -> FromSqlResult<Self> {
        let mut bytes = not_none!(bytes);
        let mut bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 8,
            "Received more than 8 bytes decoding i64. \
             Was an expression of a different type misidentified as BigInteger?"
        );
        debug_assert!(
            bytes.len() >= 8,
            "Received fewer than 8 bytes decoding i64. \
             Was an Integer expression misidentified as BigInteger?"
        );
        bytes.as_slice()
            .read_i64::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}