use diesel::query_builder::{AstPass, QueryFragment};
use diesel::query_builder::{BoxedLimitOffsetClause, IntoBoxedClause, LimitOffsetClause};
use diesel::query_builder::{LimitClause, NoLimitClause};
use diesel::query_builder::{NoOffsetClause, OffsetClause};
use diesel::result::QueryResult;
use oracle::Oracle;

impl QueryFragment<Oracle> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    fn walk_ast(&self, _out: AstPass<Oracle>) -> QueryResult<()> {
        Ok(())
    }
}

impl<L> QueryFragment<Oracle> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        out.push_sql(" FETCH FIRST ");
        self.limit_clause.0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ONLY ");
        Ok(())
    }
}

impl<O> QueryFragment<Oracle> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        out.push_sql(" OFFSET ");
        self.offset_clause.0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ");
        Ok(())
    }
}

impl<L, O> QueryFragment<Oracle> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<Oracle>,
    O: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        out.push_sql(" OFFSET ");
        self.offset_clause.0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS FETCH NEXT ");
        self.limit_clause.0.walk_ast(out.reborrow())?;
        out.push_sql(" ROWS ONLY ");
        Ok(())
    }
}

impl<'a> QueryFragment<Oracle> for BoxedLimitOffsetClause<'a, Oracle> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        match (self.limit.as_ref(), self.offset.as_ref()) {
            (Some(limit), Some(offset)) => {
                out.push_sql(" OFFSET ");
                offset.walk_ast(out.reborrow())?;
                out.push_sql(" ROWS FETCH NEXT ");
                limit.walk_ast(out.reborrow())?;
                out.push_sql(" ROWS ONLY ");
            }
            (Some(limit), None) => {
                out.push_sql(" FETCH FIRST ");
                limit.walk_ast(out.reborrow())?;
                out.push_sql(" ROWS ONLY ");
            }
            (None, Some(offset)) => {
                out.push_sql(" OFFSET ");
                offset.walk_ast(out.reborrow())?;
                out.push_sql(" ROWS ");
            }
            (None, None) => {}
        }
        Ok(())
    }
}

impl<'a> IntoBoxedClause<'a, Oracle> for LimitOffsetClause<NoLimitClause, NoOffsetClause> {
    type BoxedClause = BoxedLimitOffsetClause<'a, Oracle>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: None,
        }
    }
}

impl<'a, L> IntoBoxedClause<'a, Oracle> for LimitOffsetClause<LimitClause<L>, NoOffsetClause>
where
    L: QueryFragment<Oracle> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, Oracle>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause.0)),
            offset: None,
        }
    }
}

impl<'a, O> IntoBoxedClause<'a, Oracle> for LimitOffsetClause<NoLimitClause, OffsetClause<O>>
where
    O: QueryFragment<Oracle> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, Oracle>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: None,
            offset: Some(Box::new(self.offset_clause.0)),
        }
    }
}

impl<'a, L, O> IntoBoxedClause<'a, Oracle> for LimitOffsetClause<LimitClause<L>, OffsetClause<O>>
where
    L: QueryFragment<Oracle> + Send + 'a,
    O: QueryFragment<Oracle> + Send + 'a,
{
    type BoxedClause = BoxedLimitOffsetClause<'a, Oracle>;

    fn into_boxed(self) -> Self::BoxedClause {
        BoxedLimitOffsetClause {
            limit: Some(Box::new(self.limit_clause.0)),
            offset: Some(Box::new(self.offset_clause.0)),
        }
    }
}
