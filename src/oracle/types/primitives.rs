use crate::oracle::connection::{InnerValue, OracleValue};
use crate::oracle::Oracle;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types::*;
use std::io::Write;

impl FromSql<SmallInt, Oracle> for i16 {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <i16 as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::SmallInt(v) => Ok(v),
            _ => Err("Got invalid value for i16".into()),
        }
    }
}

impl FromSql<Integer, Oracle> for i32 {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Integer(i) => Ok(i),
            _ => Err("Got invalid value for i32".into()),
        }
    }
}

impl FromSql<BigInt, Oracle> for i64 {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::BigInt(i) => Ok(i),
            _ => Err("Got invalid value for i64".into()),
        }
    }
}

impl FromSql<Float, Oracle> for f32 {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Float(f) => Ok(f),
            _ => Err("Got invalid value for f32".into()),
        }
    }
}

impl FromSql<Double, Oracle> for f64 {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Double(f) => Ok(f),
            _ => Err("Got invalid value for f64".into()),
        }
    }
}

impl FromSql<Text, Oracle> for String {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Text(s) => Ok(s),
            _ => Err("Got invalid value for text".into()),
        }
    }
}

impl FromSql<Binary, Oracle> for Vec<u8> {
    fn from_sql(raw: OracleValue<'_>) -> deserialize::Result<Self> {
        match raw.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Binary(b) => Ok(b),
            _ => Err("Got invalid value for binary".into()),
        }
    }
}

impl FromSql<Bool, Oracle> for bool {
    fn from_sql(bytes: OracleValue<'_>) -> deserialize::Result<Self> {
        FromSql::<SmallInt, Oracle>::from_sql(bytes).map(|v: i16| v != 0)
    }
}

impl ToSql<Bool, Oracle> for bool {
    fn to_sql<W: Write>(&self, out: &mut serialize::Output<W, Oracle>) -> serialize::Result {
        if *self {
            out.write(&[1])?;
        } else {
            out.write(&[0])?;
        }
        Ok(serialize::IsNull::No)
    }
}
