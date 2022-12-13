#[cfg(feature = "dynamic-schema")]
extern crate diesel_dynamic_schema;

use super::backend::*;
use diesel::sql_types::*;
use std::hash::Hash;

mod primitives;

/// Oracle specfic metadata about the type of a bind value
#[derive(Clone, Copy)]
pub struct OciTypeMetadata {
    pub(crate) tpe: OciDataType,
}

impl PartialEq for OciTypeMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.tpe.eq(&other.tpe)
    }
}

impl Eq for OciTypeMetadata {}

impl Hash for OciTypeMetadata {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.tpe.hash(hasher)
    }
}

/// A list of database side datatypes
///
/// This list closely mirrors the datatypes expected by diesel
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OciDataType {
    /// A boolean
    Bool,
    /// A 2 byte integer
    SmallInt,
    /// A 4 byte integer
    Integer,
    /// A 8 byte integer
    BigInt,
    /// A 4 byte floating point values
    Float,
    /// A 8 byte floating point value
    Double,
    /// A text value
    Text,
    /// A binary value
    Binary,
    /// A date value
    Date,
    /// A time value
    Time,
    /// A timestamp value
    Timestamp,
}

impl HasSqlType<SmallInt> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::SmallInt,
        }
    }
}

impl HasSqlType<Integer> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Integer,
        }
    }
}

impl HasSqlType<BigInt> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::BigInt,
        }
    }
}

impl HasSqlType<Float> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Float,
        }
    }
}

impl HasSqlType<Double> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Double,
        }
    }
}

impl HasSqlType<Text> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Text,
        }
    }
}

impl HasSqlType<Binary> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Binary,
        }
    }
}

impl HasSqlType<Time> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Time,
        }
    }
}

impl HasSqlType<Timestamp> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Timestamp,
        }
    }
}

impl HasSqlType<Bool> for Oracle {
    fn metadata(_: &mut Self::MetadataLookup) -> Self::TypeMetadata {
        OciTypeMetadata {
            tpe: OciDataType::Bool,
        }
    }
}

#[cfg(feature = "dynamic-schema")]
mod dynamic_schema_impls {

    use super::diesel_dynamic_schema::dynamic_value::{Any, DynamicRow, NamedField};
    use crate::oracle::Oracle;
    use diesel::deserialize::{self, FromSql, QueryableByName};
    use diesel::expression::QueryMetadata;
    use diesel::row::NamedRow;

    impl<I> QueryableByName<Oracle> for DynamicRow<I>
    where
        I: FromSql<Any, Oracle>,
    {
        fn build<'a>(row: &impl NamedRow<'a, Oracle>) -> deserialize::Result<Self> {
            Self::from_row(row)
        }
    }

    impl<I> QueryableByName<Oracle> for DynamicRow<NamedField<Option<I>>>
    where
        I: FromSql<Any, Oracle>,
    {
        fn build<'a>(row: &impl NamedRow<'a, Oracle>) -> deserialize::Result<Self> {
            Self::from_nullable_row(row)
        }
    }

    impl QueryMetadata<Any> for Oracle {
        fn row_metadata(
            _lookup: &mut Self::MetadataLookup,
            out: &mut Vec<Option<Self::TypeMetadata>>,
        ) {
            out.push(None)
        }
    }
}

#[cfg(feature = "chrono-time")]
mod chrono_date_time;
