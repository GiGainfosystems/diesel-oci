use diesel::query_builder::{AstPass, QueryFragment, SelectClauseQueryFragment, SelectStatement};
use diesel::query_builder::{LimitClause, NoLimitClause, NoOffsetClause, OffsetClause};
use diesel::QueryResult;
use diesel::QuerySource;

use oracle::backend::Oracle;

impl<F, S, D, W, O, L, Of, G, LC> QueryFragment<Oracle>
    for SelectStatement<F, S, D, W, O, L, Of, G, LC>
where
    S: SelectClauseQueryFragment<F, Oracle>,
    F: QuerySource,
    F::FromClause: QueryFragment<Oracle>,
    D: QueryFragment<Oracle>,
    W: QueryFragment<Oracle>,
    O: QueryFragment<Oracle>,
    for<'a> OffsetWrapper<'a, L, Of>: QueryFragment<Oracle>,
    L: QueryFragment<Oracle>,
    Of: QueryFragment<Oracle>,
    G: QueryFragment<Oracle>,
    LC: QueryFragment<Oracle>,
{
    default fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql("SELECT ");
        self.distinct.walk_ast(out.reborrow())?;
        self.select.walk_ast(&self.from, out.reborrow())?;
        out.push_sql(" FROM ");
        self.from.from_clause().walk_ast(out.reborrow())?;
        self.where_clause.walk_ast(out.reborrow())?;
        self.group_by.walk_ast(out.reborrow())?;
        self.order.walk_ast(out.reborrow())?;
        OffsetWrapper(&self.limit, &self.offset).walk_ast(out.reborrow())?;
        self.locking.walk_ast(out.reborrow())?;
        Ok(())
    }
}

pub struct OffsetWrapper<'a, T, V>(&'a T, &'a V);

impl<'a> QueryFragment<Oracle> for OffsetWrapper<'a, NoLimitClause, NoOffsetClause> {
    fn walk_ast(&self, _out: AstPass<Oracle>) -> QueryResult<()> {
        Ok(())
    }
}

impl<'a, T> QueryFragment<Oracle> for OffsetWrapper<'a, NoLimitClause, OffsetClause<T>>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql(" OFFSET ");
        (self.1).0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ");
        Ok(())
    }
}

impl<'a, T> QueryFragment<Oracle> for OffsetWrapper<'a, LimitClause<T>, NoOffsetClause>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql(" FETCH FIRST ");
        (self.0).0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ONLY ");
        Ok(())
    }
}

impl<'a, T1, T2> QueryFragment<Oracle> for OffsetWrapper<'a, LimitClause<T1>, OffsetClause<T2>>
where
    T1: QueryFragment<Oracle>,
    T2: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_sql(" OFFSET ");
        (self.1).0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS FETCH NEXT ");
        (self.0).0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ONLY ");
        Ok(())
    }
}
