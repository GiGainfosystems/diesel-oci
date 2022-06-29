use crate::oracle::backend::{Oracle, OracleExistsSyntax};

use diesel::expression::exists::Exists;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;

impl<T> QueryFragment<Oracle, OracleExistsSyntax> for Exists<T>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        out.push_sql("CASE WHEN EXISTS (");
        self.subselect.walk_ast(out.reborrow())?;
        out.push_sql(") THEN 1 ELSE 0 END");
        Ok(())
    }
}
