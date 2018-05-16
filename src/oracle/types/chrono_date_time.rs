extern crate chrono;
use std::error::Error;
use std::io::Write;

use diesel::sql_types::*;
use diesel::deserialize::FromSql;
use diesel::serialize::{ToSql, IsNull, Output};

use oracle::backend::Oracle;

use self::chrono::{NaiveDateTime, NaiveDate, Datelike, Timelike};

use std::ffi::{CString, CStr};
use std::os::raw::c_char;

use super::super::connection::OracleValue;

impl FromSql<Timestamp, Oracle> for NaiveDateTime {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        let s = bytes[6] as u32 - 1;
        let mi = bytes[5] as u32 - 1;
        let h = bytes[4] as u32 - 1;
        let d = bytes[3] as u32;
        let mo = bytes[2] as u32;
        let y = bytes[1] as i32;
        let c = bytes[0] as i32;
        if c > 100 && y > 100 {
            // TODO: error handling
            let d = NaiveDate::from_ymd_opt((c - 100) * 100 + y - 100, mo, d).unwrap();
            // ok_or(Box::new(
            //     result::Error::DatabaseError(
            //         result::DatabaseErrorKind::__Unknown,
            //         Box::new(String::from("could not parse timestamp"))))));

            Ok(d.and_hms_opt(h, mi, s).unwrap())
        } else if c < 100 && y < 100 {
            // TODO: error handling
            let d = NaiveDate::from_ymd_opt(c * -100 + y, mo, d).unwrap();
            // .ok_or(Box::new(result::Error::DatabaseError("could not parse \
            //                                               timestamp"
            //                                                  .to_owned()))));

            Ok(d.and_hms_opt(h, mi, s).unwrap())
        } else {
            unreachable!()
        }

    }
}

impl ToSql<Timestamp, Oracle> for NaiveDateTime {
    fn to_sql<W: Write>(&self,
                        out: &mut Output<W, Oracle>)
                        -> Result<IsNull, Box<Error + Send + Sync>> {
        let year = self.year();
        if year > 0 {
            let c: u8 = (year / 100 + 100) as u8;
            let y: u8 = (year % 100 + 100) as u8;
            try!(out.write(&[c, y])
                     .map_err(|e| Box::new(e) as Box<Error + Send + Sync>));
        } else {
            let c: u8 = (year / 100) as u8;
            let y: u8 = (year % 100) as u8;
            try!(out.write(&[c, y])
                     .map_err(|e| Box::new(e) as Box<Error + Send + Sync>));
        }
        let mo = self.month() as u8;
        let d = self.day() as u8;
        let h = (self.hour() + 1) as u8;
        let mi = (self.minute() + 1) as u8;
        let s = (self.second() + 1) as u8;
        out.write(&[mo, d, h, mi, s])
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
            .map(|_| IsNull::No)

    }
}


impl FromSql<Date, Oracle> for NaiveDate {
    fn from_sql(bytes: Option<&OracleValue>) -> Result<Self, Box<Error + Send + Sync>> {
        let bytes = not_none!(bytes);
        let bytes = &bytes.bytes;
        let d = bytes[3] as u32;
        let mo = bytes[2] as u32;
        let y = bytes[1] as i32;
        let c = bytes[0] as i32;
        if c > 100 && y > 100 {
            // TODO: error handling
            Ok(NaiveDate::from_ymd_opt((c - 100) * 100 + y - 100, mo, d).unwrap())
        } else if c < 100 && y < 100 {
            // TODO: error handling
            Ok(NaiveDate::from_ymd_opt(c * -100 + y - 100, mo, d).unwrap())
        } else {
            unreachable!()
        }

    }
}

impl ToSql<Date, Oracle> for NaiveDate {
    fn to_sql<W: Write>(&self,
                        out: &mut Output<W, Oracle>)
                        -> Result<IsNull, Box<Error + Send + Sync>> {
        let year = self.year();
        if year > 0 {
            let c: u8 = (year / 100 + 100) as u8;
            let y: u8 = (year % 100 + 100) as u8;
            try!(out.write(&[c, y])
                     .map_err(|e| Box::new(e) as Box<Error + Send + Sync>));
        } else {
            let c: u8 = (year / 100) as u8;
            let y: u8 = (year % 100) as u8;
            try!(out.write(&[c, y])
                     .map_err(|e| Box::new(e) as Box<Error + Send + Sync>));
        }
        let mo = self.month() as u8;
        let d = self.day() as u8;
        out.write(&[mo, d, 1, 1, 1])
            .map_err(|e| Box::new(e) as Box<Error + Send + Sync>)
            .map(|_| IsNull::No)

    }
}
