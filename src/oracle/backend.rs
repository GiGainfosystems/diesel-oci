use diesel::backend::UsesAnsiSavepointSyntax;
use diesel::backend::*;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::sql_types::TypeMetadata;

use super::connection::bind_collector::OracleBindCollector;
use super::connection::OracleValue;
use super::query_builder::OciQueryBuilder;
use super::types::OciTypeMetadata;
use oracle::types::OciDataType;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Oracle;

impl Backend for Oracle {
    type QueryBuilder = OciQueryBuilder;
    type BindCollector = OracleBindCollector;
    type ByteOrder = byteorder::NativeEndian;
}

impl<'a> HasRawValue<'a> for Oracle {
    type RawValue = OracleValue<'a>;
}

impl TypeMetadata for Oracle {
    type TypeMetadata = OciTypeMetadata;
    type MetadataLookup = ();
}

impl UsesAnsiSavepointSyntax for Oracle {}

// TODO: check if Oracle supports this
//impl SupportsDefaultKeyword for Oracle {}
impl SupportsReturningClause for Oracle {}
