use diesel::query_builder::QueryFragment;
use diesel::query_builder::ValuesClause;

use diesel::query_source::Table;
use diesel::insertable::InsertValues;
use diesel::query_builder::insert_statement::DefaultValues;
use diesel::query_builder::AstPass;
use diesel::result::QueryResult;
use diesel::backend::Backend;

use diesel::insertable::Insertable;
use diesel::query_builder::insert_statement::InsertStatement;
use diesel::query_dsl::methods::ExecuteDsl;
use diesel::insertable::BatchInsert;
use diesel::backend::SupportsDefaultKeyword;


impl<'a, T, Tab, Inner, DB> QueryFragment<DB> for BatchInsert<'a, T, Tab>
    where
        DB: Backend + SupportsDefaultKeyword,
        &'a T: Insertable<Tab, Values = ValuesClause<Inner, Tab>>,
        ValuesClause<Inner, Tab>: QueryFragment<DB>,
        Inner: QueryFragment<DB>,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        let mut records = self.records.iter().map(Insertable::values);
        if let Some(record) = records.next() {
            record.walk_ast(out.reborrow())?;
        }
        for record in records {
            out.push_sql(" union all select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        Ok(())
    }
}