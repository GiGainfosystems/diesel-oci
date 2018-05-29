#![feature(specialization)]

//use diesel::query_builder::QueryFragment;
//use diesel::query_builder::ValuesClause;
//
//use diesel::query_builder::AstPass;
//use diesel::result::QueryResult;
//
//use diesel::insertable::Insertable;
//use diesel::insertable::BatchInsert;
//
//use super::backend::Oracle;
//
//impl<'a, T, Tab, Inner> QueryFragment<Oracle> for BatchInsert<'a, T, Tab>
//    where
//        &'a T: Insertable<Tab, Values = ValuesClause<Inner, Tab>>,
//        ValuesClause<Inner, Tab>: QueryFragment<Oracle>,
//        Inner: QueryFragment<Oracle>,
//{
//    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
//        let mut records = self.records.iter().map(Insertable::values);
//        if let Some(record) = records.next() {
//            record.walk_ast(out.reborrow())?;
//        }
//        for record in records {
//            out.push_sql(" union all select ");
//            record.values.walk_ast(out.reborrow())?;
//            out.push_sql(" from dual");
//        }
//        Ok(())
//    }
//}