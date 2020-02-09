extern crate chrono_time as chrono;
use std::error::Error;
use std::io::Write;

use diesel::deserialize::FromSql;
use diesel::result;
use diesel::serialize::{IsNull, Output, ToSql};
use diesel::sql_types::*;

use oracle::backend::Oracle;

use self::chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

use super::super::connection::OracleValue;

impl FromSql<Timestamp, Oracle> for NaiveDateTime {
    fn from_sql(bytes: Option<OracleValue<'_>>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        let sec = u32::from(bytes[6]) - 1;
        let min = u32::from(bytes[5]) - 1;
        let hr = u32::from(bytes[4]) - 1;
        let day = u32::from(bytes[3]);
        let month = u32::from(bytes[2]);
        let year = i32::from(bytes[1]);
        let century = i32::from(bytes[0]);
        if century > 100 && year > 100 {
            let d = NaiveDate::from_ymd_opt((century - 100) * 100 + year - 100, month, day).ok_or(
                Box::new(result::Error::DeserializationError(
                    "could not parse timestamp".into(),
                )),
            )?;

            Ok(d.and_hms_opt(hr, min, sec).unwrap())
        } else if century < 100 && year < 100 {
            let d = NaiveDate::from_ymd_opt(century * -100 + year, month, day).ok_or(Box::new(
                result::Error::DeserializationError("could not parse timestamp".into()),
            ))?;

            Ok(d.and_hms_opt(hr, min, sec).unwrap())
        } else {
            Err(Box::new(result::Error::DeserializationError(
                concat!(
                    "reached a unreachable state while parsing timestamp: ",
                    file!(),
                    ":",
                    line!()
                )
                .into(),
            )))
        }
    }
}

impl ToSql<Timestamp, Oracle> for NaiveDateTime {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        let year = self.year();
        if year > 0 {
            let c: u8 = (year / 100 + 100) as u8;
            let y: u8 = (year % 100 + 100) as u8;
            out.write(&[c, y])
                .map_err(|e| result::Error::SerializationError(Box::new(e)))?;
        } else {
            let c: u8 = (year / 100) as u8;
            let y: u8 = (year % 100) as u8;
            out.write(&[c, y])
                .map_err(|e| result::Error::SerializationError(Box::new(e)))?;
        }
        let mo = self.month() as u8;
        let d = self.day() as u8;
        let h = (self.hour() + 1) as u8;
        let mi = (self.minute() + 1) as u8;
        let s = (self.second() + 1) as u8;
        out.write(&[mo, d, h, mi, s])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
            .map(|_| IsNull::No)
    }
}

impl FromSql<Date, Oracle> for NaiveDate {
    fn from_sql(bytes: Option<OracleValue<'_>>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        let d = u32::from(bytes[3]);
        let mo = u32::from(bytes[2]);
        let y = i32::from(bytes[1]);
        let c = i32::from(bytes[0]);
        if c > 100 && y > 100 {
            NaiveDate::from_ymd_opt((c - 100) * 100 + y - 100, mo, d).ok_or_else(|| {
                Box::new(result::Error::DeserializationError(
                    "Unable to parse timestamp".into(),
                )) as Box<dyn Error + Send + Sync>
            })
        } else if c < 100 && y < 100 {
            NaiveDate::from_ymd_opt(c * -100 + y - 100, mo, d).ok_or_else(|| {
                Box::new(result::Error::DeserializationError(
                    "Unable to parse timestamp".into(),
                )) as _
            })
        } else {
            Err(Box::new(result::Error::DeserializationError(
                concat!(
                    "reached a unreachable state while parsing timestamp: ",
                    file!(),
                    ":",
                    line!()
                )
                .into(),
            )))
        }
    }
}

impl ToSql<Date, Oracle> for NaiveDate {
    fn to_sql<W: Write>(
        &self,
        out: &mut Output<W, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        let year = self.year();
        if year > 0 {
            let c: u8 = (year / 100 + 100) as u8;
            let y: u8 = (year % 100 + 100) as u8;
            out.write(&[c, y])
                .map_err(|e| result::Error::SerializationError(Box::new(e)))?;
        } else {
            let c: u8 = (year / 100) as u8;
            let y: u8 = (year % 100) as u8;
            out.write(&[c, y])
                .map_err(|e| result::Error::SerializationError(Box::new(e)))?;
        }
        let mo = self.month() as u8;
        let d = self.day() as u8;
        out.write(&[mo, d, 1, 1, 1])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
            .map(|_| IsNull::No)
    }
}
