use diesel::backend::*;
use diesel::sql_types::TypeMetadata;

use super::connection::bind_collector::OracleBindCollector;
use super::connection::OracleValue;
use super::query_builder::OciQueryBuilder;
use super::types::OciTypeMetadata;

#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub struct Oracle;

impl Backend for Oracle {
    type QueryBuilder = OciQueryBuilder;
}

impl<'a> HasBindCollector<'a> for Oracle {
    type BindCollector = OracleBindCollector<'a>;
}

impl<'a> HasRawValue<'a> for Oracle {
    type RawValue = OracleValue<'a>;
}

impl TypeMetadata for Oracle {
    type TypeMetadata = OciTypeMetadata;
    type MetadataLookup = ();
}

impl TrustedBackend for Oracle {}
impl DieselReserveSpecialization for Oracle {}

impl SqlDialect for Oracle {
    type ReturningClause = OracleReturningClause;

    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;

    type InsertWithDefaultKeyword = sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;
    type BatchInsertSupport = OracleStyleBatchInsert;
    type DefaultValueClauseForInsert = sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type EmptyFromClauseSyntax = OracleDualForEmptySelectClause;
    type ExistsSyntax = OracleExistsSyntax;

    type ArrayComparision = sql_dialect::array_comparision::AnsiSqlArrayComparison;
}

pub struct OracleStyleBatchInsert;
pub struct OracleReturningClause;
pub struct OracleDualForEmptySelectClause;
pub struct OracleExistsSyntax;
