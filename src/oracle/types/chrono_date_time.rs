extern crate chrono_time as chrono;
use std::error::Error;
use std::io::Write;

use byteorder::{ByteOrder, NativeEndian, WriteBytesExt};
use diesel::deserialize::FromSql;
use diesel::result;
use diesel::serialize::{IsNull, Output, ToSql};
use diesel::sql_types::*;

use oracle::backend::Oracle;

use self::chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use super::super::connection::{InnerValue, OracleValue};
use super::OciDataType;
use super::OciTypeMetadata;

impl FromSql<Timestamp, Oracle> for NaiveDateTime {
    fn from_sql(bytes: OracleValue<'_>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match bytes.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Timestamp(t) => Ok(t),
            _ => Err("Invalid timestamp value".into()),
        }
    }
}

impl ToSql<Timestamp, Oracle> for NaiveDateTime {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        <_ as ToSql<Date, Oracle>>::to_sql(&self.date(), out)?;
        <_ as ToSql<Time, Oracle>>::to_sql(&self.time(), out)
    }
}

pub fn timestamp_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    let year = NativeEndian::read_i32(&bytes);
    let month = NativeEndian::read_u32(&bytes[4..]);
    let day = NativeEndian::read_u32(&bytes[8..]);
    let hour = NativeEndian::read_u32(&bytes[12..]);
    let min = NativeEndian::read_u32(&bytes[16..]);
    let sec = NativeEndian::read_u32(&bytes[20..]);
    let nano = NativeEndian::read_u32(&bytes[24..]);
    Box::new(NaiveDateTime::new(
        NaiveDate::from_ymd(year, month, day),
        NaiveTime::from_hms_nano(hour, min, sec, nano),
    ))
}

impl FromSql<Date, Oracle> for NaiveDate {
    fn from_sql(bytes: OracleValue<'_>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match bytes.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            InnerValue::Date(d) => Ok(d),
            _ => Err("Invalid value for date".into()),
        }
    }
}

impl ToSql<Date, Oracle> for NaiveDate {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        let year = self.year();
        let month = self.month();
        let day = self.day();
        out.write_i32::<NativeEndian>(year)?;
        out.write_u32::<NativeEndian>(month)?;
        out.write_u32::<NativeEndian>(day)?;

        Ok(IsNull::No)
    }
}

fn date_handler(bytes: Vec<u8>) -> Box<dyn oracle::sql_type::ToSql> {
    let year = NativeEndian::read_i32(&bytes);
    let month = NativeEndian::read_u32(&bytes[4..]);
    let day = NativeEndian::read_u32(&bytes[8..]);
    Box::new(NaiveDate::from_ymd(year, month, day))
}

impl HasSqlType<Date> for Oracle {
    fn metadata(_: &Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Date,
            handler: date_handler,
        }
    }
}

impl ToSql<Time, Oracle> for NaiveTime {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        let hours = self.hour();
        let minutes = self.minute();
        let seconds = self.second();
        let nanos = self.nanosecond();
        out.write_u32::<NativeEndian>(hours)?;
        out.write_u32::<NativeEndian>(minutes)?;
        out.write_u32::<NativeEndian>(seconds)?;
        out.write_u32::<NativeEndian>(nanos)?;
        Ok(IsNull::No)
    }
}
