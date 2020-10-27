#[cfg(feature = "dynamic-schema")]
extern crate diesel_dynamic_schema;

use super::backend::*;
use super::connection::OracleValue;
use byteorder::{ByteOrder, NativeEndian};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::*;
use std::error::Error;
use std::hash::Hash;
use std::io::Write;
use std::str;

mod primitives;

#[derive(Clone, Copy)]
pub struct OciTypeMetadata {
    pub(crate) tpe: OciDataType,
    pub(crate) handler: fn(Vec<u8>) -> Box<dyn oracle::sql_type::ToSql>,
}

impl PartialEq for OciTypeMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.tpe.eq(&other.tpe)
    }
}

impl Eq for OciTypeMetadata {}

impl Hash for OciTypeMetadata {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.tpe.hash(hasher)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OciDataType {
    Bool,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Text,
    Binary,
    Date,
    Time,
    Timestamp,
}

fn i16_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    if bytes.is_empty() {
        Box::new(oracle::sql_type::OracleType::Number(0, 0))
    } else {
        let v = NativeEndian::read_i16(&bytes);
        Box::new(v)
    }
}

fn i32_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    if bytes.is_empty() {
        Box::new(oracle::sql_type::OracleType::Number(0, 0))
    } else {
        let v = NativeEndian::read_i32(&bytes);
        Box::new(v)
    }
}

fn i64_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    if bytes.is_empty() {
        Box::new(oracle::sql_type::OracleType::Number(0, 0))
    } else {
        let v = NativeEndian::read_i64(&bytes);
        Box::new(v)
    }
}

fn f32_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    if bytes.is_empty() {
        Box::new(oracle::sql_type::OracleType::Number(0, 0))
    } else {
        let v = NativeEndian::read_f32(&bytes);
        Box::new(v)
    }
}

struct BinaryDoubleHandler(f64);

impl oracle::sql_type::ToSql for BinaryDoubleHandler {
    fn oratype(
        &self,
        conn: &oracle::Connection,
    ) -> Result<oracle::sql_type::OracleType, oracle::Error> {
        Ok(oracle::sql_type::OracleType::BinaryDouble)
    }

    fn to_sql(&self, v: &mut oracle::SqlValue) -> std::result::Result<(), oracle::Error> {
        v.set(&self.0)
    }
}

fn f64_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    if bytes.is_empty() {
        Box::new(oracle::sql_type::OracleType::BinaryDouble)
    } else {
        let v = NativeEndian::read_f64(&bytes);
        Box::new(BinaryDoubleHandler(v))
    }
}

fn string_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    Box::new(String::from_utf8(bytes).expect("Parse does not fail"))
}

fn blob_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    Box::new(bytes)
}

fn bool_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    Box::new(bytes[0])
}

fn foo(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    todo!()
}

impl HasSqlType<SmallInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::SmallInt,
            handler: i16_handler,
        }
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Integer,
            handler: i32_handler,
        }
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::BigInt,
            handler: i64_handler,
        }
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Float,
            handler: f32_handler,
        }
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Double,
            handler: f64_handler,
        }
    }
}

impl HasSqlType<Text> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Text,
            handler: string_handler,
        }
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Binary,
            handler: blob_handler,
        }
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Time,
            handler: foo,
        }
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Timestamp,
            #[cfg(feature = "chrono")]
            handler: self::chrono_date_time::timestamp_handler,
        }
    }
}

impl HasSqlType<Bool> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Bool,
            handler: bool_handler,
        }
    }
}

#[cfg(feature = "dynamic-schema")]
mod dynamic_schema_impls {

    use super::diesel_dynamic_schema::dynamic_value::{Any, DynamicRow, NamedField};
    use crate::oracle::Oracle;
    use diesel::deserialize::{self, FromSql, QueryableByName};
    use diesel::expression::QueryMetadata;
    use diesel::row::NamedRow;

    impl<I> QueryableByName<Oracle> for DynamicRow<I>
    where
        I: FromSql<Any, Oracle>,
    {
        fn build<'a>(row: &impl NamedRow<'a, Oracle>) -> deserialize::Result<Self> {
            Self::from_row(row)
        }
    }

    impl<I> QueryableByName<Oracle> for DynamicRow<NamedField<Option<I>>>
    where
        I: FromSql<Any, Oracle>,
    {
        fn build<'a>(row: &impl NamedRow<'a, Oracle>) -> deserialize::Result<Self> {
            Self::from_nullable_row(row)
        }
    }

    impl QueryMetadata<Any> for Oracle {
        fn row_metadata(_lookup: &Self::MetadataLookup, out: &mut Vec<Option<Self::TypeMetadata>>) {
            out.push(None)
        }
    }
}

#[cfg(feature = "chrono-time")]
mod chrono_date_time;
