use super::backend::*;
use super::connection::OracleValue;
use byteorder::WriteBytesExt;
use diesel::backend::*;
use diesel::deserialize::FromSql;
use diesel::serialize::{IsNull, Output, ToSql};
use diesel::sql_types::*;
use oci_sys as ffi;
use std::error::Error;
use std::io::Write;

pub type FromSqlResult<T> = Result<T, ErrorType>;
pub type ErrorType = Box<Error + Send + Sync>;
pub type ToSqlResult = FromSqlResult<IsNull>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

impl OciDataType {
    pub fn is_text(&self) -> bool {
        match *self {
            OciDataType::Text => true,
            _ => false,
        }
    }

    pub fn bind_type(&self) -> u32 {
        use self::OciDataType::*;
        match *self {
            Bool => ffi::SQLT_INT,
            SmallInt => ffi::SQLT_INT,
            Integer => ffi::SQLT_INT,
            BigInt => ffi::SQLT_INT,
            Float => ffi::SQLT_IBFLOAT,
            Double => ffi::SQLT_IBDOUBLE,
            Text => ffi::SQLT_CHR,
            Binary => ffi::SQLT_BIN,
            Date | Time | Timestamp => ffi::SQLT_DAT,
        }
    }

    pub fn define_type(&self) -> u32 {
        use self::OciDataType::*;
        match *self {
            Text => ffi::SQLT_STR,
            _ => self.bind_type(),
        }
    }

    pub fn byte_size(&self) -> usize {
        use self::OciDataType::*;
        match *self {
            Bool => 2,
            SmallInt => 2,
            Integer => 4,
            BigInt => 8,
            Float => 4,
            Double => 8,
            Text => 2_000_000,
            Binary => 88,
            Date | Time | Timestamp => 7,
        }
    }
}

macro_rules! not_none {
    ($bytes:expr) => {
        match $bytes {
            Some(bytes) => bytes,
            None => panic!("Unexpected null for non-null column"),
        }
    };
}

impl HasSqlType<SmallInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::SmallInt
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Integer
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::BigInt
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Float
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Double
    }
}

impl HasSqlType<Numeric> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        panic!("currently not supported")
    }
}

impl HasSqlType<Text> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Text
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Binary
    }
}

impl HasSqlType<Date> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Time
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Timestamp
    }
}

impl HasSqlType<Bool> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Bool
    }
}

impl FromSql<Bool, Oracle> for bool {
    fn from_sql(bytes: Option<&OracleValue>) -> FromSqlResult<Self> {
        FromSql::<SmallInt, Oracle>::from_sql(bytes).map(|v: i16| v != 0)
    }
}

impl ToSql<Bool, Oracle> for bool {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Oracle>) -> ToSqlResult {
        out.write_i16::<<Oracle as Backend>::ByteOrder>(if *self { 1 } else { 0 })
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as ErrorType)
    }
}

#[cfg(feature = "chrono-time")]
mod chrono_date_time;

mod decimal;
mod integers;
mod primitives;
