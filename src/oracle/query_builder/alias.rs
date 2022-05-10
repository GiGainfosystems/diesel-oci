use super::Oracle;

use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;

pub trait Alias: Sized {
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
