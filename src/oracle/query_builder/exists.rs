use super::Oracle;

use diesel::expression::exists::Exists;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;

impl<T> QueryFragment<Oracle> for Exists<T>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql("1 FROM DUAL WHERE EXISTS (");
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}
