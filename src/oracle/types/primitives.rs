use crate::oracle::connection::bind_collector::BindValue;
use crate::oracle::connection::{InnerValue, OracleValue};
use crate::oracle::Oracle;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types::*;

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

impl ToSql<SmallInt, Oracle> for i16 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Borrowed(self));
        Ok(serialize::IsNull::No)
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

impl ToSql<Integer, Oracle> for i32 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Borrowed(self));
        Ok(serialize::IsNull::No)
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

impl ToSql<BigInt, Oracle> for i64 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Borrowed(self));
        Ok(serialize::IsNull::No)
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

struct BinaryFloatWrapper(f32);

impl oracle::sql_type::ToSql for BinaryFloatWrapper {
    fn oratype(&self, _conn: &oracle::Connection) -> oracle::Result<oracle::sql_type::OracleType> {
        Ok(oracle::sql_type::OracleType::BinaryFloat)
    }

    fn to_sql(&self, val: &mut oracle::SqlValue) -> oracle::Result<()> {
        val.set(&self.0)?;
        Ok(())
    }
}

impl ToSql<Float, Oracle> for f32 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Owned(Box::new(BinaryFloatWrapper(*self))));
        Ok(serialize::IsNull::No)
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

struct BinaryDoubleWrapper(f64);

impl oracle::sql_type::ToSql for BinaryDoubleWrapper {
    fn oratype(&self, _conn: &oracle::Connection) -> oracle::Result<oracle::sql_type::OracleType> {
        Ok(oracle::sql_type::OracleType::BinaryDouble)
    }

    fn to_sql(&self, val: &mut oracle::SqlValue) -> oracle::Result<()> {
        val.set(&self.0)?;
        Ok(())
    }
}

impl ToSql<Double, Oracle> for f64 {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Owned(Box::new(BinaryDoubleWrapper(*self))));
        Ok(serialize::IsNull::No)
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

impl ToSql<Text, Oracle> for str {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Owned(Box::new(self.to_owned())));
        Ok(serialize::IsNull::No)
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

impl ToSql<Binary, Oracle> for [u8] {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Owned(Box::new(self.to_owned())));
        Ok(serialize::IsNull::No)
    }
}

impl FromSql<Bool, Oracle> for bool {
    fn from_sql(bytes: OracleValue<'_>) -> deserialize::Result<Self> {
        FromSql::<SmallInt, Oracle>::from_sql(bytes).map(|v: i16| v != 0)
    }
}

impl ToSql<Bool, Oracle> for bool {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Oracle>) -> serialize::Result {
        out.set_value(BindValue::Owned(if *self {
            Box::new(1)
        } else {
            Box::new(0)
        }));
        Ok(serialize::IsNull::No)
    }
}
