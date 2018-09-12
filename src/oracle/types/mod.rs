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
        raw_type: u32,
        size: usize,
        scale: Option<i8>,
        precision: i16,
    },
    Float {
        raw_type: u32,
        size: usize,
    },
    Date {
        raw_type: u32,
        size: usize,
    },
    Blob {
        raw_type: u32,
    },
    Text {
        raw_type: u32,
    },
}

// #[allow(dead_code)]
// #[repr(u32)]
// #[derive(Clone, Copy, Debug, PartialEq, Hash, Eq)]
// pub enum OCIDataType {
//     Char = ffi::SQLT_CHR,
//     Date = ffi::SQLT_DATE,
//     Time = ffi::SQLT_TIME,
//     Timestamp = ffi::SQLT_TIMESTAMP,
//     TimestampWithTz = ffi::SQLT_TIMESTAMP_TZ,
//     TimestampWithLocalTz = ffi::SQLT_TIMESTAMP_LTZ,
//     IntervalYearToMonth = ffi::SQLT_INTERVAL_YM,
//     IntervalDayToSecond = ffi::SQLT_INTERVAL_DS,
//     Clob = ffi::SQLT_CLOB,
//     Blob = ffi::SQLT_BLOB,
//     Int = ffi::SQLT_INT,
//     Uint = ffi::SQLT_UIN,
//     Float = ffi::SQLT_FLT,
//     PackedDecimalNumber = ffi::SQLT_PDN,
//     Binary = ffi::SQLT_BIN,
//     Numeric = ffi::SQLT_NUM,
//     NamedObject = ffi::SQLT_NTY,
//     Ref = ffi::SQLT_REF,
//     OCIString = ffi::SQLT_VST,
//     NumericWithLength = ffi::SQLT_VNU,
//     BFloat = ffi::SQLT_BFLOAT,
//     BDouble = ffi::SQLT_BDOUBLE,
//     IBFloat = ffi::SQLT_IBFLOAT,
//     IBDouble = ffi::SQLT_IBDOUBLE,
//     String = ffi::SQLT_STR,
//     AnsiChar = ffi::SQLT_AFC,
//     InternDate = ffi::SQLT_DAT,
// }

impl OciDataType {
    pub fn is_text(&self) -> bool {
        match *self {
            OciDataType::Text { .. } => true,
            _ => false,
        }
    }

    pub fn as_raw(&self) -> u32 {
        use self::OciDataType::*;
        match *self {
            Float { raw_type, .. }
            | Text { raw_type, .. }
            | Number { raw_type, .. }
            | Date { raw_type, .. }
            | Blob { raw_type, .. } => raw_type,
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
            raw_type: ffi::SQLT_NUM,
            size: 2,
            scale: Some(0),
            precision: 5,
        }
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Number {
            raw_type: ffi::SQLT_NUM,
            size: 4,
            scale: Some(0),
            precision: 10,
        }
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Number {
            raw_type: ffi::SQLT_NUM,
            size: 8,
            scale: Some(0),
            precision: 19,
        }
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Float {
            raw_type: ffi::SQLT_BFLOAT,
            size: 4,
        }
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Float {
            raw_type: ffi::SQLT_BDOUBLE,
            size: 8,
        }
    }
}

impl HasSqlType<Numeric> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Number {
            raw_type: ffi::SQLT_NUM,
            size: 21,
            scale: None,
            precision: 19,
        }
    }
}

impl HasSqlType<VarChar> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Text {
            raw_type: ffi::SQLT_CHR,
        }
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Blob {
            raw_type: ffi::SQLT_BLOB,
        }
    }
}

impl HasSqlType<Date> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            raw_type: ffi::SQLT_DAT,
            size: 7,
        }
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            raw_type: ffi::SQLT_TIME,
            size: 7,
        }
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciDataType::Date {
            raw_type: ffi::SQLT_TIMESTAMP,
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
