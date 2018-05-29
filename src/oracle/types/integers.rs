
use std::error::Error;

use diesel::sql_types::*;
use diesel::deserialize::FromSql;

use oracle::backend::Oracle;

use super::super::connection::OracleValue;

use byteorder::ReadBytesExt;
use diesel::backend::*;

pub type FromSqlResult<T> = Result<T, ErrorType>;
pub type ErrorType = Box<Error + Send + Sync>;

impl FromSql<BigInt, Oracle> for i64 {
    fn from_sql(bytes: Option<&OracleValue>) -> FromSqlResult<Self> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
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

impl FromSql<Integer, Oracle> for i32 {
    fn from_sql(bytes: Option<&OracleValue>) -> FromSqlResult<Self> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 4,
            "Received more than 4 bytes decoding i32. \
             Was an expression of a different type misidentified as Integer?"
        );
        debug_assert!(
            bytes.len() >= 4,
            "Received fewer than 4 bytes decoding i32. \
             Was an Integer expression misidentified as Integer?"
        );
        bytes.as_slice()
            .read_i32::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}

impl FromSql<SmallInt, Oracle> for i16 {
    fn from_sql(bytes: Option<&OracleValue>) -> FromSqlResult<Self> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 2,
            "Received more than 2 bytes decoding i16. \
             Was an expression of a different type misidentified as SmallInteger?"
        );
        debug_assert!(
            bytes.len() >= 2,
            "Received fewer than 2 bytes decoding i16. \
             Was an Integer expression misidentified as SmallInteger?"
        );
        bytes.as_slice()
            .read_i16::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}