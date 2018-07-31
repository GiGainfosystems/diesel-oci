use std::marker::PhantomData;

use diesel::expression::array_comparison::MaybeEmpty;
use diesel::expression::*;
use diesel::query_builder::*;
use diesel::result::QueryResult;

use super::Oracle;

#[derive(Debug, Copy, Clone, QueryId)]
pub struct Subselect<T, ST> {
    values: T,
    _sql_type: PhantomData<ST>,
}

impl<T, ST> Subselect<T, ST> {
    pub(crate) fn new(values: T) -> Self {
        Self {
            values,
            _sql_type: PhantomData,
        }
    }
}

impl<T: SelectQuery, ST> Expression for Subselect<T, ST> {
    type SqlType = ST;
}

impl<T, ST> MaybeEmpty for Subselect<T, ST> {
    fn is_empty(&self) -> bool {
        false
    }
}

impl<T, ST, QS> SelectableExpression<QS> for Subselect<T, ST>
    where
        Subselect<T, ST>: AppearsOnTable<QS>,
        T: ValidSubselect<QS>,
{
}

impl<T, ST, QS> AppearsOnTable<QS> for Subselect<T, ST>
    where
        Subselect<T, ST>: Expression,
        T: ValidSubselect<QS>,
{
}

impl<T, ST> NonAggregate for Subselect<T, ST> {}

impl<T, ST> QueryFragment<Oracle> for Subselect<T, ST>
    where
        T: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        self.values.walk_ast(out.reborrow())?;
        Ok(())
    }
}

pub trait ValidSubselect<QS> {}
