use oracle::types::OciDataType;

#[derive(Debug, Clone)]
pub struct OracleValue<'a> {
    pub(crate) inner: InnerValue<'a>,
}

#[derive(Debug, Clone)]
pub(crate) enum InnerValue<'a> {
    Raw {
        raw_value: &'a oracle::SqlValue,
        tpe: &'a oracle::sql_type::OracleType,
    },
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Bool(bool),
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
    pub(crate) fn new(
        raw_value: &'a oracle::SqlValue,
        tpe: &'a oracle::sql_type::OracleType,
    ) -> Self {
        Self {
            inner: InnerValue::Raw { raw_value, tpe },
        }
    }

    pub fn value_type(&self) -> OciDataType {
        use self::InnerValue::*;

        match self.inner {
            SmallInt(_) => OciDataType::SmallInt,
            Integer(_) => OciDataType::Integer,
            BigInt(_) => OciDataType::BigInt,
            Bool(_) => OciDataType::Bool,
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
            Raw {
                tpe: oracle::sql_type::OracleType::Number(prec, 0),
                ..
            } if *prec == 5 => OciDataType::SmallInt,
            Raw {
                tpe: oracle::sql_type::OracleType::Number(prec, 0),
                ..
            } if *prec == 10 => OciDataType::Integer,
            Raw {
                tpe: oracle::sql_type::OracleType::Number(prec, 0),
                ..
            } if *prec == 19 => OciDataType::BigInt,
            Raw {
                tpe: oracle::sql_type::OracleType::Number(_, _),
                ..
            } => OciDataType::Double,
            Raw {
                tpe: oracle::sql_type::OracleType::Float(_),
                ..
            } => OciDataType::Float,
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
