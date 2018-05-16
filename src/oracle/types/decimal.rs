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

impl FromSql<Double, Oracle> for f64 {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let mut bytes = not_none!(bytes);
        let mut bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 8,
            "Received more than 8 bytes while decoding \
             an f64. Was a numeric accidentally marked as double?"
        );
        bytes.as_slice()
            .read_f64::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}