use super::Oracle;

use diesel::deserialize::Queryable;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::HasSqlType;

// this cannot be done but would be the most preferred solution
//impl<T> QueryFragment<Oracle> for Exists<T>
//    where
//        T: QueryFragment<Oracle>,
//{
//    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
//        out.push_sql("SELECT 1 FROM DUAL EXISTS (");
//        self.0.walk_ast(out.reborrow())?;
//        out.push_sql(")");
//        Ok(())
//    }
//}

pub fn exists<T, U>(query: T, conn: &::oracle::connection::OciConnection) -> QueryResult<bool>
where
    T: AsQuery,
    T::Query: QueryFragment<Oracle> + QueryId,
    Oracle: HasSqlType<T::SqlType>,
    U: Queryable<T::SqlType, Oracle>,
{
    use diesel::Connection;
    // this doesn't work for oracle (is always 0)
    //let cnt = conn.execute_returning_count(&query)?;
    //if cnt > 0 {
    //    return Ok(true);
    //} else {
    //    return Ok(false);
    //}
    // TODO: I am not happy with that, but currently diesel has not other options
    // the code above would be better, need to check why it doesn't work
    let v: Vec<U> = conn.query_by_index(query)?;
    if v.len() > 0 {
        Ok(true)
    } else {
        Ok(false)
    }
    // TODO: the below needs more trait bounds, e.g. T: QueryDsL, @weiznich may look into this
    // adding T: QueryDsl as bound yields some recursion error
    //use diesel::dsl::count_star;
    //use diesel::QueryDsl;
    //let v = query.select(count_star()).load()?;
    //if v > 0 {
    //    Ok(true)
    //} else {
    //    Ok(false)
    //}
}
// we could define our own expression but then would probably need to implement everything
// here instead of using diesel (this yields some compiler errors about unfulfilled traits
//
//pub fn exists<T>(query: T) -> Exists<T> {
//    Exists(Subselect::new(query))
//}
//
//#[derive(Debug, Clone, Copy, QueryId)]
//pub struct Exists<T>(Subselect<T, ()>);
//
//impl<T> Expression for Exists<T>
//    where
//        Subselect<T, ()>: Expression,
//{
//    type SqlType = Bool;
//}
//
//impl<T> NonAggregate for Exists<T> {}
//
//impl<T> QueryFragment<Oracle> for Exists<T>
//    where
//        T: QueryFragment<Oracle>,
//{
//    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
//        out.push_sql("EXISTS (");
//        self.0.walk_ast(out.reborrow())?;
//        out.push_sql(")");
//        Ok(())
//    }
//}
//
//impl<T, QS> SelectableExpression<QS> for Exists<T>
//    where
//        Self: AppearsOnTable<QS>,
//        Subselect<T, ()>: SelectableExpression<QS>,
//{
//}
//
//impl<T, QS> AppearsOnTable<QS> for Exists<T>
//    where
//        Self: Expression,
//        Subselect<T, ()>: AppearsOnTable<QS>,
//{
//}
