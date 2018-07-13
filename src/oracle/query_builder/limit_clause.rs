#![feature(specialization)]

/// TODO: this is currently not used, IDK why diesel still takes the original limit clause
/// coming from diesel which uses `LIMIT` syntax
use diesel::backend::Backend;
use diesel::result::QueryResult;
use diesel::query_builder::{QueryFragment, AstPass};

use super::Oracle;

#[derive(Debug, Clone, Copy, QueryId)]
pub struct LimitClause<Expr>(pub Expr);

impl<Expr> QueryFragment<Oracle> for LimitClause<Expr> where
Expr: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql("OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
        self.0.walk_ast(out.reborrow())?;
        Ok(())
    }
}