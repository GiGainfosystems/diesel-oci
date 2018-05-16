use std::error::Error;
use std::io::Write;
use diesel::sql_types::*;
use byteorder::{NativeEndian, WriteBytesExt};
use diesel::deserialize::FromSql;
use diesel::serialize::{ToSql, IsNull, Output};
use super::backend::*;
use oci_sys as ffi;

pub type FromSqlResult<T> = Result<T, ErrorType>;
pub type ErrorType = Box<Error + Send + Sync>;
pub type ToSqlResult = FromSqlResult<IsNull>;

#[allow(dead_code)]
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Hash, Eq)]
pub enum OCIDataType {
    Char = ffi::SQLT_CHR,
    Date = ffi::SQLT_DATE,
    Time = ffi::SQLT_TIME,
    Timestamp = ffi::SQLT_TIMESTAMP,
    TimestampWithTz = ffi::SQLT_TIMESTAMP_TZ,
    TimestampWithLocalTz = ffi::SQLT_TIMESTAMP_LTZ,
    IntervalYearToMonth = ffi::SQLT_INTERVAL_YM,
    IntervalDayToSecond = ffi::SQLT_INTERVAL_DS,
    Clob = ffi::SQLT_CLOB,
    Blob = ffi::SQLT_BLOB,
    Int = ffi::SQLT_INT,
    Uint = ffi::SQLT_UIN,
    Float = ffi::SQLT_FLT,
    PackedDecimalNumber = ffi::SQLT_PDN,
    Binary = ffi::SQLT_BIN,
    Numeric = ffi::SQLT_NUM,
    NamedObject = ffi::SQLT_NTY,
    Ref = ffi::SQLT_REF,
    OCIString = ffi::SQLT_VST,
    NumericWithLength = ffi::SQLT_VNU,
    BFloat = ffi::SQLT_BFLOAT,
    BDouble = ffi::SQLT_BDOUBLE,
    IBFloat = ffi::SQLT_IBFLOAT,
    IBDouble = ffi::SQLT_IBDOUBLE,
    String = ffi::SQLT_STR,
    AnsiChar = ffi::SQLT_AFC,
    InternDate = ffi::SQLT_DAT,
}

impl OCIDataType {
    pub fn from_raw(n: u32) -> Option<OCIDataType> {
        use self::OCIDataType::*;
        match n {
            ffi::SQLT_CHR => Some(Char),
            ffi::SQLT_NUM => Some(Numeric),
            ffi::SQLT_INT => Some(Int),
            ffi::SQLT_FLT => Some(Float),
            ffi::SQLT_STR => Some(String),
            ffi::SQLT_VNU => Some(NumericWithLength),
            ffi::SQLT_PDN => Some(PackedDecimalNumber),
            ffi::SQLT_DAT => Some(InternDate),
            ffi::SQLT_BFLOAT => Some(BFloat),
            ffi::SQLT_BDOUBLE => Some(BDouble),
            ffi::SQLT_BIN => Some(Binary),
            ffi::SQLT_UIN => Some(Uint),
            ffi::SQLT_AFC => Some(AnsiChar),
            ffi::SQLT_IBFLOAT => Some(IBFloat),
            ffi::SQLT_IBDOUBLE => Some(IBDouble),
            ffi::SQLT_NTY => Some(NamedObject),
            ffi::SQLT_REF => Some(Ref),
            ffi::SQLT_CLOB => Some(Clob),
            ffi::SQLT_BLOB => Some(Blob),
            ffi::SQLT_VST => Some(OCIString),
            ffi::SQLT_DATE => Some(Date),
            ffi::SQLT_TIME => Some(Time),
            ffi::SQLT_TIMESTAMP => Some(Timestamp),
            ffi::SQLT_TIMESTAMP_TZ => Some(TimestampWithTz),
            ffi::SQLT_INTERVAL_YM => Some(IntervalYearToMonth),
            ffi::SQLT_INTERVAL_DS => Some(IntervalDayToSecond),
            ffi::SQLT_TIMESTAMP_LTZ => Some(TimestampWithLocalTz),
            _ => None,
        }
    }
}

macro_rules! not_none {
    ($bytes:expr) => {
        match $bytes {
            Some(bytes) => bytes,
            None =>
                panic!(),
                // return Err(Box::new(diesel::types::impls::option::UnexpectedNullError {
            //     msg: "Unexpected null for non-null column".to_string(),
            // })),
        }
    }
}


impl HasSqlType<SmallInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Int
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Int
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Int
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Float
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Float
    }
}

impl HasSqlType<VarChar> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Char
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Binary
    }
}

impl HasSqlType<Date> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::InternDate
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Time
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::InternDate
    }
}

impl HasSqlType<Bool> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> OCIDataType {
        OCIDataType::Int
    }
}

impl FromSql<Bool, Oracle> for bool {
    fn from_sql(bytes: Option<&[u8]>) -> FromSqlResult<Self> {
        FromSql::<Double, Oracle>::from_sql(bytes).map(|v: f64| v != 0.0)
    }
}

impl ToSql<Bool, Oracle> for bool {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Oracle>) -> ToSqlResult {
        out.write_i16::<NativeEndian>(if *self { 1 } else { 0 })
            .map(|_| IsNull::No)
            .map_err(|e| Box::new(e) as ErrorType)
    }
}


#[cfg(feature = "chrono-time")]
mod chrono_date_time;
