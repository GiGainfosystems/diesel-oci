use crate::oracle::types::OciDataType;

/// A unserialized value as received from the database
#[derive(Debug, Clone)]
pub struct OracleValue<'a> {
    pub(crate) inner: InnerValue<'a>,
}

#[derive(Debug, Clone)]
pub(crate) enum InnerValue<'a> {
    Raw {
        raw_value: &'a oracle::SqlValue<'a>,
        tpe: oracle::sql_type::OracleType,
    },
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Binary(Vec<u8>),
    #[cfg(feature = "chrono")]
    Date(chrono_time::NaiveDate),
    #[cfg(feature = "chrono")]
    Timestamp(chrono_time::NaiveDateTime),
}

impl<'a> OracleValue<'a> {
    pub(crate) fn new(raw_value: &'a oracle::SqlValue, tpe: oracle::sql_type::OracleType) -> Self {
        Self {
            inner: InnerValue::Raw { raw_value, tpe },
        }
    }

    /// Get the datatype of the underlying value
    pub fn value_type(&self) -> OciDataType {
        use self::InnerValue::*;

        match self.inner {
            SmallInt(_) => OciDataType::SmallInt,
            Integer(_) => OciDataType::Integer,
            BigInt(_) => OciDataType::BigInt,
            Float(_) => OciDataType::Float,
            Double(_) => OciDataType::Double,
            Text(_) => OciDataType::Text,
            Binary(_) => OciDataType::Binary,
            #[cfg(feature = "chrono")]
            Date(_) => OciDataType::Date,
            #[cfg(feature = "chrono")]
            Timestamp(_) => OciDataType::Timestamp,
            Raw {
                tpe: oracle::sql_type::OracleType::Varchar2(_),
                ..
            } => OciDataType::Text,
            Raw {
                tpe: oracle::sql_type::OracleType::NVarchar2(_),
                ..
            } => OciDataType::Text,
            Raw {
                tpe: oracle::sql_type::OracleType::Char(_),
                ..
            } => OciDataType::Text,
            Raw {
                tpe: oracle::sql_type::OracleType::NChar(_),
                ..
            } => OciDataType::Text,
            Raw {
                tpe: oracle::sql_type::OracleType::BinaryFloat,
                ..
            } => OciDataType::Float,
            Raw {
                tpe: oracle::sql_type::OracleType::BinaryDouble,
                ..
            } => OciDataType::Double,
            // Map number(prec, scale) to various integer types
            // For any scale <= 0 we apply the following mapping
            //
            // Scale -127 seems to be used to indicate that no scale has
            // been set
            //
            // *, 0 -> Unbound == Equal to 38 -> BigInt
            // 1..=5 -> SmallInt
            // 6..=10 -> Int,
            // 11..=38 -> BigInt
            //
            // Technically anything larger than prec == 19 won't fit
            // into a i64, we map it that way anyway as using NUMBER(*, scale)
            // is common as integer primary key
            //
            // https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1834
            Raw {
                tpe: oracle::sql_type::OracleType::Number(prec, scale),
                ..
            } if (1..=5).contains(&prec) && (-126..=0).contains(&scale) => OciDataType::SmallInt,
            Raw {
                tpe: oracle::sql_type::OracleType::Number(prec, scale),
                ..
            } if (6..=10).contains(&prec) && (-126..=0).contains(&scale) => OciDataType::Integer,
            Raw {
                tpe: oracle::sql_type::OracleType::Number(_prec, scale),
                ..
            } if (-126..=0).contains(&scale) => OciDataType::BigInt,
            // If we did not map NUMBER to an integer above, we just
            // use a double value
            Raw {
                tpe: oracle::sql_type::OracleType::Number(_prec, _scale),
                ..
            } => OciDataType::Double,
            Raw {
                // same as NUMBER(prec, *)
                tpe: oracle::sql_type::OracleType::Float(_),
                ..
            } => OciDataType::Double,
            Raw {
                tpe: oracle::sql_type::OracleType::Date,
                ..
            } => OciDataType::Date,
            Raw {
                tpe: oracle::sql_type::OracleType::Timestamp(_),
                ..
            } => OciDataType::Timestamp,
            Raw {
                tpe: oracle::sql_type::OracleType::CLOB,
                ..
            } => OciDataType::Text,
            Raw {
                tpe: oracle::sql_type::OracleType::BLOB,
                ..
            } => OciDataType::Binary,
            Raw {
                tpe: oracle::sql_type::OracleType::Int64,
                ..
            } => OciDataType::BigInt,
            Raw {
                tpe: oracle::sql_type::OracleType::UInt64,
                ..
            } => OciDataType::BigInt,

            Raw {
                tpe: oracle::sql_type::OracleType::BFILE,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::NCLOB,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::RefCursor,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Boolean,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Object(_),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Long,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::LongRaw,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::TimestampTZ(_),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::TimestampLTZ(_),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::IntervalDS(_, _),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::IntervalYM(_),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Rowid,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Raw(_),
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Json,
                ..
            }
            | Raw {
                tpe: oracle::sql_type::OracleType::Xml,
                ..
            } => unimplemented!(),
            // e =>
            // {
            //     #[cfg(feature = "chrono")]
            //     match e {
            //         Date(_) => OciDataType::Date,
            //         Timestamp(_) => OciDataType::Timestamp,
            //         _ => unreachable!(),
            //     }
            // }
        }
    }
}
