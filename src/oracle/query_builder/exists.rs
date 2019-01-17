use super::Oracle;

use diesel::expression::exists::Exists;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;

impl<T> QueryFragment<Oracle> for Exists<T>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql("CASE WHEN EXISTS (");
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(") THEN 1 ELSE 0 END FROM DUAL");
        Ok(())
    }
}
