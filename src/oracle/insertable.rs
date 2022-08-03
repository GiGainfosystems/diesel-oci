use diesel::insertable::InsertValues;
use diesel::query_builder::AstPass;
use diesel::query_builder::BatchInsert;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::ValuesClause;
use diesel::result::QueryResult;
use diesel::Table;

use super::backend::{Oracle, OracleStyleBatchInsert};

// please refer to https://stackoverflow.com/questions/39576/best-way-to-do-multi-row-insert-in-oracle
impl<V, Tab, QId, const STABLE_QUERY_ID: bool> QueryFragment<Oracle, OracleStyleBatchInsert>
    for BatchInsert<Vec<ValuesClause<V, Tab>>, Tab, QId, STABLE_QUERY_ID>
where
    V: QueryFragment<Oracle> + InsertValues<Tab, Oracle>,
    Tab: Table,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        if !STABLE_QUERY_ID {
            out.unsafe_to_cache_prepared();
        }
        let mut records = self.values.iter();
        if let Some(record) = records.next() {
            out.push_sql("(");
            record.values.column_names(out.reborrow())?;
            out.push_sql(") ");
            out.push_sql("select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        for record in records {
            out.push_sql(" union all select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        Ok(())
    }
}
