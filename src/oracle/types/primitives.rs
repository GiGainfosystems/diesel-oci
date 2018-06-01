use std::error::Error;

use diesel::deserialize::FromSql;
use diesel::sql_types::*;

use oracle::backend::Oracle;

use super::super::connection::OracleValue;

impl FromSql<Text, Oracle> for String {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = bytes.bytes.clone();
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
