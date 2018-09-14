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
    Number {
        bind_type: u32,
        define_type: u32,
        size: usize,
    },
    Float {
        bind_type: u32,
        define_type: u32,
        size: usize,
    },
    Text {
        bind_type: u32,
        define_type: u32,
    },
    Date {
        bind_type: u32,
        define_type: u32,
        size: usize,
    },
    Blob {
        bind_type: u32,
        define_type: u32,
    },
}

impl OciDataType {
    pub fn is_text(&self) -> bool {
        match *self {
            OciDataType::Text { .. } => true,
            _ => false,
        }
    }

    pub fn bind_type(&self) -> u32 {
        use self::OciDataType::*;
        match *self {
            Float { bind_type, .. }
            | Text { bind_type, .. }
            | Number { bind_type, .. }
            | Date { bind_type, .. }
            | Blob { bind_type, .. } => bind_type,
        }
    }

    pub fn define_type(&self) -> u32 {
        use self::OciDataType::*;
        match *self {
            Float { define_type, .. }
            | Text { define_type, .. }
            | Number { define_type, .. }
            | Date { define_type, .. }
            | Blob { define_type, .. } => define_type,
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
        OciDataType::Number {
            bind_type: ffi::SQLT_INT,
            define_type: ffi::SQLT_INT,
            size: 2,
        }
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Number {
            bind_type: ffi::SQLT_INT,
            define_type: ffi::SQLT_INT,
            size: 4,
        }
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Number {
            bind_type: ffi::SQLT_INT,
            define_type: ffi::SQLT_INT,
            size: 8,
        }
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Float {
            bind_type: ffi::SQLT_BFLOAT,
            define_type: ffi::SQLT_BFLOAT,
            size: 4,
        }
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Float {
            bind_type: ffi::SQLT_BDOUBLE,
            define_type: ffi::SQLT_BDOUBLE,
            size: 8,
        }
    }
}

impl HasSqlType<Numeric> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        panic!("currently not supported")
    }
}

impl HasSqlType<Text> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Text {
            bind_type: ffi::SQLT_CHR,
            define_type: ffi::SQLT_STR,
        }
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Blob {
            bind_type: ffi::SQLT_BIN,
            define_type: ffi::SQLT_BIN,
        }
    }
}

impl HasSqlType<Date> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            bind_type: ffi::SQLT_DAT,
            define_type: ffi::SQLT_DAT,
            size: 7,
        }
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            bind_type: ffi::SQLT_DAT,
            define_type: ffi::SQLT_DAT,
            size: 7,
        }
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            bind_type: ffi::SQLT_DAT,
            define_type: ffi::SQLT_DAT,
            size: 7,
        }
    }
}

impl HasSqlType<Bool> for Oracle {
    fn metadata(lookup: &Self::MetadataLookup) -> Self::TypeMetadata {
        <Oracle as HasSqlType<SmallInt>>::metadata(lookup)
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
