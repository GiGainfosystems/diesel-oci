use std::error::Error;

use diesel::deserialize::FromSql;
use diesel::sql_types::*;

use bigdecimal::BigDecimal;

use oracle::backend::Oracle;

use super::super::connection::OracleValue;

use byteorder::ReadBytesExt;
use diesel::backend::*;

use std::fmt;

#[derive(Debug, Clone)]
struct BigDecimalError;

// Generation of an error is completely separate from how it is displayed.
// There's no need to be concerned about cluttering complex logic with the display style.
//
// Note that we don't store any extra info about the errors. This means we can't state
// which string failed to parse without modifying our types to carry that information.
impl fmt::Display for BigDecimalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error while converting numeric to BigDecimal")
    }
}

// This is important for other errors to wrap this one.
impl Error for BigDecimalError {
    fn description(&self) -> &str {
        "Error while converting numeric to BigDecimal"
    }

    fn cause(&self) -> Option<&Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl FromSql<Double, Oracle> for f64 {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 8,
            "Received more than 8 bytes while decoding \
             an f64. Was a numeric accidentally marked as double?"
        );
        bytes
            .as_slice()
            .read_f64::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}

impl FromSql<Float, Oracle> for f32 {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 4,
            "Received more than 4 bytes while decoding \
             an f32. Was a numeric accidentally marked as double?"
        );
        bytes
            .as_slice()
            .read_f32::<<Oracle as Backend>::ByteOrder>()
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
    }
}

impl FromSql<Numeric, Oracle> for BigDecimal {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        debug_assert!(
            bytes.len() <= 8,
            "Received more than 8 bytes while decoding \
             an f64. Was a numeric accidentally marked as double?"
        );

        BigDecimal::parse_bytes(bytes.as_slice(), 10)
            .ok_or(Box::new(BigDecimalError) as Box<Error + Send + Sync>)
    }
}
