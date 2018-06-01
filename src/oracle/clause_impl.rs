use super::backend::Oracle;
use diesel::backend::Backend;
use diesel::query_builder::order_clause::{NoOrderClause, OrderClause};
use diesel::query_builder::{BuildQueryResult, QueryBuilder, QueryFragment};

macro_rules! simple_clause_impl {
    ($no_clause:ident, $clause:ident, $sql:expr, $DB:ty) => {
        impl QueryFragment<$DB> for $no_clause {
            fn to_sql(&self, _out: &mut <$DB as Backend>::QueryBuilder) -> BuildQueryResult {
                Ok(())
            }
        }

        impl<Expr> QueryFragment<$DB> for $clause<Expr>
        where
            $DB: Backend,
            Expr: QueryFragment<$DB>,
        {
            fn to_sql(&self, out: &mut <$DB as Backend>::QueryBuilder) -> BuildQueryResult {
                out.push_sql($sql);
                self.0.to_sql(out)
            }
        }
    };
}

simple_clause_impl!(NoOrderClause, OrderClause, " ORDER BY ", Oracle);
