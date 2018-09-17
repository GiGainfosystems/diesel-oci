use std::error::Error;

use diesel::deserialize::FromSql;
use diesel::sql_types::*;

use oracle::backend::Oracle;

use super::super::connection::OracleValue;

use byteorder::ReadBytesExt;
use diesel::backend::*;

impl FromSql<Double, Oracle> for f64 {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let mut bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 8,
            "Received more than 8 bytes while decoding \
             an f64. Was a numeric accidentally marked as double?"
        );
        bytes
            .read_f64::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}

impl FromSql<Float, Oracle> for f32 {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let mut bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 4,
            "Received more than 4 bytes while decoding \
             an f32. Was a numeric accidentally marked as double?"
        );
        bytes
            .read_f32::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}
