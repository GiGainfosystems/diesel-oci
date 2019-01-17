use diesel::query_builder::{AstPass, QueryFragment};
/// TODO: this is currently not used, IDK why diesel still takes the original limit clause
/// coming from diesel which uses `LIMIT` syntax
use diesel::result::QueryResult;

use super::Oracle;

#[derive(Debug, Clone, Copy, QueryId)]
pub struct LimitClause<Expr>(pub Expr);

impl<Expr> QueryFragment<Oracle> for LimitClause<Expr>
where
    Expr: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql("OFFSET 0 ROWS FETCH NEXT ");
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ONLY");
        Ok(())
    }
}
