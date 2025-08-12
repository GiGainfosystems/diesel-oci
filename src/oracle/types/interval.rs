//! Types representing IntervalYM and IntervalDS

use crate::oracle::connection::bind_collector::BindValue;
use crate::oracle::connection::InnerValue;
use crate::{OciDataType, OciTypeMetadata, Oracle, OracleValue};
use diesel::deserialize::FromSql;
use diesel::serialize::{IsNull, Output, ToSql};
use diesel_derives::{AsExpression, FromSqlRow};
use oracle::sql_type::{IntervalDS, IntervalYM, OracleType};
use oracle::{Connection, SqlValue};
use std::error::Error;

/// Sql type that represent the Oracle IntervalYM type, to be used with things like the `!table` macro
///
/// https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/sql/INTERVALYM.html
pub struct SqlIntervalYM;

impl diesel::sql_types::SqlType for SqlIntervalYM {
    type IsNull = diesel::sql_types::is_nullable::NotNull;
}

impl diesel::sql_types::SingleValue for SqlIntervalYM {}
impl diesel::sql_types::HasSqlType<SqlIntervalYM> for Oracle {
    fn metadata(_: &mut ()) -> OciTypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::IntervalYM,
        }
    }
}

/// Oracle Interval with year and month information, the specifics of how many days are in a year
/// or in a month is dependent on the start and end date of the interval and is handled by Oracle internally
///
/// https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/sql/INTERVALYM.html
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = SqlIntervalYM)]
pub struct OciIntervalYM {
    /// Number of years
    pub years: i32,
    /// Number of months
    pub months: i32,
}

impl oracle::sql_type::ToSql for OciIntervalYM {
    fn oratype(&self, _: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::IntervalYM(
            (self.years.checked_ilog10().unwrap_or(0) + 1) as u8,
        ))
    }

    fn to_sql(&self, val: &mut SqlValue) -> oracle::Result<()> {
        let res = IntervalYM::new(self.years, self.months)?;
        res.to_sql(val)?;
        Ok(())
    }
}

impl oracle::sql_type::FromSql for OciIntervalYM {
    fn from_sql(val: &SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let interval_ym: IntervalYM = oracle::sql_type::FromSql::from_sql(val)?;
        Ok(OciIntervalYM {
            years: interval_ym.years(),
            months: interval_ym.months(),
        })
    }
}

impl ToSql<SqlIntervalYM, Oracle> for OciIntervalYM {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Oracle>) -> diesel::serialize::Result {
        out.set_value(BindValue::Borrowed(self));
        Ok(IsNull::No)
    }
}

impl FromSql<SqlIntervalYM, Oracle> for OciIntervalYM {
    fn from_sql(bytes: OracleValue<'_>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match bytes.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            _ => Err("Invalid value for Interval".into()),
        }
    }
}

impl std::ops::Add for OciIntervalYM {
    type Output = OciIntervalYM;

    fn add(self, rhs: Self) -> Self::Output {
        OciIntervalYM {
            years: self.years + rhs.years,
            months: self.months + rhs.months,
        }
    }
}

impl std::ops::Sub for OciIntervalYM {
    type Output = OciIntervalYM;

    fn sub(self, rhs: Self) -> Self::Output {
        OciIntervalYM {
            years: self.years - rhs.years,
            months: self.months - rhs.months,
        }
    }
}

/// Sql type that represent the Oracle INTERVALDS type, to be used with things like the `!table` macro
///
/// https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/sql/INTERVALDS.html
pub struct SqlIntervalDS;

impl diesel::sql_types::SqlType for SqlIntervalDS {
    type IsNull = diesel::sql_types::is_nullable::NotNull;
}

impl diesel::sql_types::SingleValue for SqlIntervalDS {}
impl diesel::sql_types::HasSqlType<SqlIntervalDS> for Oracle {
    fn metadata(_: &mut ()) -> OciTypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::IntervalDS,
        }
    }
}

/// Oracle Interval with day, hour, minute, second, and subsecond information, the specifics of how long
/// e.g. a day is depends on the start and end date of the interval and is handled by Oracle internally
///
/// https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/sql/INTERVALDS.html
#[derive(Debug, Clone, Copy, PartialEq, Eq, AsExpression, FromSqlRow)]
#[diesel(sql_type = SqlIntervalDS)]
pub struct OciIntervalDS {
    /// Number of days
    pub days: i32,
    /// Number of hours
    pub hours: i32,
    /// Number of minutes
    pub minutes: i32,
    /// Number of seconds
    pub seconds: i32,
    /// Number of nanoseconds
    pub nanoseconds: i32,
}

impl oracle::sql_type::ToSql for OciIntervalDS {
    fn oratype(&self, _: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::IntervalDS(
            (self.days.checked_ilog10().unwrap_or(0) + 1) as u8,
            (self.nanoseconds.checked_ilog10().unwrap_or(0) + 1) as u8,
        ))
    }

    fn to_sql(&self, val: &mut SqlValue) -> oracle::Result<()> {
        let res = IntervalDS::new(
            self.days,
            self.hours,
            self.minutes,
            self.seconds,
            self.nanoseconds,
        )?;
        res.to_sql(val)?;
        Ok(())
    }
}

impl oracle::sql_type::FromSql for OciIntervalDS {
    fn from_sql(val: &SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let interval_ds: IntervalDS = oracle::sql_type::FromSql::from_sql(val)?;
        Ok(OciIntervalDS {
            days: interval_ds.days(),
            hours: interval_ds.hours(),
            minutes: interval_ds.minutes(),
            seconds: interval_ds.seconds(),
            nanoseconds: interval_ds.nanoseconds(),
        })
    }
}

impl ToSql<SqlIntervalDS, Oracle> for OciIntervalDS {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Oracle>) -> diesel::serialize::Result {
        out.set_value(BindValue::Borrowed(self));
        Ok(IsNull::No)
    }
}

impl FromSql<SqlIntervalDS, Oracle> for OciIntervalDS {
    fn from_sql(bytes: OracleValue<'_>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        match bytes.inner {
            InnerValue::Raw { raw_value, .. } => {
                <Self as oracle::sql_type::FromSql>::from_sql(raw_value).map_err(Into::into)
            }
            _ => Err("Invalid value for Interval".into()),
        }
    }
}

impl std::ops::Add for OciIntervalDS {
    type Output = OciIntervalDS;

    fn add(self, rhs: Self) -> Self::Output {
        OciIntervalDS {
            days: self.days + rhs.days,
            hours: self.hours + rhs.hours,
            minutes: self.minutes + rhs.minutes,
            seconds: self.seconds + rhs.seconds,
            nanoseconds: self.nanoseconds + rhs.nanoseconds,
        }
    }
}

impl std::ops::Sub for OciIntervalDS {
    type Output = OciIntervalDS;

    fn sub(self, rhs: Self) -> Self::Output {
        OciIntervalDS {
            days: self.days - rhs.days,
            hours: self.hours - rhs.hours,
            minutes: self.minutes - rhs.minutes,
            seconds: self.seconds - rhs.seconds,
            nanoseconds: self.nanoseconds - rhs.nanoseconds,
        }
    }
}
