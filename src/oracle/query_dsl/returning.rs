use diesel::associations::HasTable;
use diesel::query_builder::InsertStatement;
use diesel::query_builder::{AstPass, Query, QueryFragment};
use diesel::query_source::{Column, Table};
use diesel::query_dsl::RunQueryDsl;
use diesel::result::QueryResult;

use oracle::backend::Oracle;
use oracle::connection::OciConnection;

pub trait OciReturningDsl {
    type Output;

    fn oci_returning(self) -> Self::Output;
}

pub trait ReturningHelper {
    type SqlType;

    fn column_list(out: AstPass<Oracle>) -> QueryResult<()>;
    fn bind_list(out: AstPass<Oracle>) -> QueryResult<()>;
}

pub trait BindColumnList {
    fn bind_column_list(out: AstPass<Oracle>) -> QueryResult<()>;
}

#[derive(QueryId)]
pub struct OciReturningClause<T>(pub(crate) T);

impl<T> Query for OciReturningClause<T>
where
    T: ReturningHelper,
{
    type SqlType = T::SqlType;
}

impl<T> RunQueryDsl<OciConnection> for OciReturningClause<T> {}

impl<T, U> ReturningHelper for InsertStatement<T, U>
where
    T: Table + HasTable<Table = T>,
    T::AllColumns: QueryFragment<Oracle> + BindColumnList,
{
    type SqlType = T::SqlType;

    fn column_list(mut out: AstPass<Oracle>) -> QueryResult<()> {
        T::all_columns().walk_ast(out.reborrow())
    }

    fn bind_list(out: AstPass<Oracle>) -> QueryResult<()> {
        <T::AllColumns as BindColumnList>::bind_column_list(out)
    }
}

impl<T, U> OciReturningDsl for InsertStatement<T, U> {
    type Output = OciReturningClause<Self>;

    fn oci_returning(self) -> Self::Output {
        OciReturningClause(self)
    }
}

impl<T> QueryFragment<Oracle> for OciReturningClause<T>
where
    T: QueryFragment<Oracle> + ReturningHelper,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        T::column_list(out.reborrow())?;
        out.push_sql(" INTO ");
        T::bind_list(out.reborrow())?;
        Ok(())
    }
}

macro_rules!  impl_bind_column_list {
    ($(
        $Tuple:tt {
            $(($idx:tt) -> $T:ident, $ST:ident, $TT:ident,)+
        }
    )+) => {
        $(
            impl<$($T: Column,)+> BindColumnList for ($($T,)+) {
                #[allow(unused_assignments)]
                fn bind_column_list(mut out: AstPass<Oracle>) -> QueryResult<()> {
                    let mut needs_comma = false;
                    $(
                        if needs_comma {
                            out.push_sql(", ");
                        }
                        out.push_sql(":");
                        out.push_sql($T::NAME);
                        needs_comma = true;
                    )+
                    Ok(())
                }
            }
        )+
    }
}

__diesel_for_each_tuple!(impl_bind_column_list);
