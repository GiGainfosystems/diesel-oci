use byteorder::NativeEndian;
use diesel::backend::UsesAnsiSavepointSyntax;
use diesel::backend::*;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::sql_types::TypeMetadata;
use oracle::types::OCIDataType;

use super::connection::OracleValue;
use super::query_builder::OciQueryBuilder;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Oracle;

impl Backend for Oracle {
    type QueryBuilder = OciQueryBuilder;
    type BindCollector = RawBytesBindCollector<Oracle>;
    type RawValue = OracleValue;
    type ByteOrder = NativeEndian;
}

impl TypeMetadata for Oracle {
    type TypeMetadata = OCIDataType;
    type MetadataLookup = ();
}

impl UsesAnsiSavepointSyntax for Oracle {}

// TODO: check if Oracle supports this
impl SupportsDefaultKeyword for Oracle {}

