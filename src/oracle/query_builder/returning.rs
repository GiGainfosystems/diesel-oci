use crate::oracle::{backend::OracleReturningClause, Oracle};
use diesel::query_builder::{AstPass, QueryFragment, ReturningClause};
use diesel::Column;

impl<Expr> QueryFragment<Oracle, OracleReturningClause> for ReturningClause<Expr>
where
    Expr: BindColumnList + QueryFragment<Oracle>,
{
    fn walk_ast<'b>(
        &'b self,
        mut out: diesel::query_builder::AstPass<'_, 'b, Oracle>,
    ) -> diesel::QueryResult<()> {
        out.push_sql(" RETURNING ");
        self.0.walk_ast(out.reborrow())?;
        out.push_sql(" INTO ");
        Expr::bind_column_list(out)?;
        Ok(())
    }
}

/// TODO
pub trait BindColumnList {
    #[doc(hidden)]
    fn bind_column_list(out: AstPass<Oracle>) -> diesel::QueryResult<()>;
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
                fn bind_column_list(mut out: AstPass<Oracle>) -> diesel::QueryResult<()> {
                    let mut needs_comma = false;
                    $(
                        if needs_comma {
                            out.push_sql(", ");
                        }
                        let placeholder = format!(":out{}", $idx);
                        out.push_sql(&placeholder);
                        needs_comma = true;
                    )+
                    Ok(())
                }
            }
        )+
    }
}

diesel_derives::__diesel_for_each_tuple!(impl_bind_column_list);
