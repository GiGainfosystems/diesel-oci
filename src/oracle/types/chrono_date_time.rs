extern crate chrono_time as chrono;
use diesel::deserialize::FromSql;
use diesel::serialize::{IsNull, Output, ToSql};
use diesel::sql_types::*;
use std::error::Error;

use crate::oracle::backend::Oracle;

use crate::oracle::connection::bind_collector::BindValue;

use self::chrono::{NaiveDate, NaiveDateTime};

use super::super::connection::{InnerValue, OracleValue};

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
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.set_value(BindValue::Borrowed(self));
        Ok(IsNull::No)
    }
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
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, Oracle>,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.set_value(BindValue::Borrowed(self));
        Ok(IsNull::No)
    }
}
