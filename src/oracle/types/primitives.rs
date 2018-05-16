
use std::error::Error;
use std::io::Write;

use diesel::sql_types::*;
use diesel::deserialize::FromSql;
use diesel::serialize::{ToSql, IsNull, Output};

use oracle::backend::Oracle;

use std::ffi::{CString, CStr};
use std::os::raw::c_char;

use super::super::connection::OracleValue;

impl FromSql<Text, Oracle> for String {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let mut bytes = not_none!(bytes);
        let mut bytes = bytes.bytes.clone();
        let mut s = String::from("");
        for c in bytes {
            if c == 0 {
                break;
            }
            s.push(c as char);
        }
        Ok(s)
    }
}