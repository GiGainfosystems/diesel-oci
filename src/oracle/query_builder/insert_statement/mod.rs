use diesel::query_builder::QueryFragment;
use diesel::query_builder::ValuesClause;

use diesel::query_source::Table;
use diesel::insertable::InsertValues;
use diesel::query_builder::insert_statement::DefaultValues;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::backend::Backend;

impl<T, Tab, DB> QueryFragment<DB> for ValuesClause<T, Tab>
    where
        DB: Backend,
        Tab: Table,
        T: InsertValues<Tab, DB>,
        DefaultValues: QueryFragment<DB>,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        if self.values.is_noop()? {
            DefaultValues.walk_ast(out)?;
        } else {
            out.push_sql("(");
            self.values.column_names(out.reborrow())?;
            out.push_sql(") select ");
            self.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        Ok(())
    }
}