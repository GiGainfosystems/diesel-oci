use super::Oracle;

use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::result::QueryResult;

/// Create an alias for a given expression
///
/// This is a helper to provide aliasing support while it's not in diesel itself
/// It probably needs improvements before something like this can be merged to diesel
pub trait Alias: Sized {
    /// Create an alias with the given name
    fn alias(self, alias: String) -> As<Self>;
}

impl<T> Alias for T {
    fn alias(self, alias: String) -> As<Self> {
        As { query: self, alias }
    }
}

#[derive(Debug, Clone, QueryId)]
pub struct As<T> {
    query: T,
    alias: String,
}

use diesel::expression::Expression;
impl<T: Expression> Expression for As<T> {
    type SqlType = T::SqlType;
}

use diesel::expression::AppearsOnTable;
impl<QS, T: Expression> AppearsOnTable<QS> for As<T> {}

use diesel::expression::SelectableExpression;
impl<T, QS> SelectableExpression<QS> for As<T> where T: SelectableExpression<QS> {}

use diesel::expression::ValidGrouping;

impl<T, G> ValidGrouping<G> for As<T>
where
    T: ValidGrouping<G>,
{
    type IsAggregate = T::IsAggregate;
}

impl<T> QueryFragment<Oracle> for As<T>
where
    T: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(" ");
        out.push_identifier(&self.alias)
    }
}

impl<S> QueryFragment<Oracle, crate::oracle::backend::OracleAliasSyntax>
    for diesel::query_source::Alias<S>
where
    S: diesel::query_source::AliasSource,
    S::Target: QueryFragment<Oracle>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Oracle>) -> QueryResult<()> {
        self.source.target().walk_ast(pass.reborrow())?;
        pass.push_sql(" ");
        pass.push_identifier(S::NAME)?;
        Ok(())
    }
}
