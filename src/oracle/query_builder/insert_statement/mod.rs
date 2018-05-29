#![feature(specialization)]

//use diesel::query_builder::QueryFragment;
//use diesel::query_builder::ValuesClause;
//
//use diesel::query_source::Table;
//use diesel::insertable::InsertValues;
//use diesel::query_builder::insert_statement::DefaultValues;
//use diesel::query_builder::AstPass;
//use diesel::result::QueryResult;
//
//use super::Oracle;
//
//impl<T, Tab> QueryFragment<Oracle> for ValuesClause<T, Tab>
//    where
//        Tab: Table,
//        T: InsertValues<Tab, Oracle>,
//        DefaultValues: QueryFragment<Oracle>,
//{
//    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
//        if self.values.is_noop()? {
//            DefaultValues.walk_ast(out)?;
//        } else {
//            out.push_sql("(");
//            self.values.column_names(out.reborrow())?;
//            out.push_sql(") select ");
//            self.values.walk_ast(out.reborrow())?;
//            out.push_sql(" from dual");
//        }
//        Ok(())
//    }
//}



//use diesel::insertable::Insertable;
//use diesel::query_builder::insert_statement::InsertStatement;
//use diesel::query_dsl::methods::ExecuteDsl;
//use diesel::result::QueryResult;
//use diesel::query_builder::QueryFragment;

// TODO:
//    error[E0210]: type parameter `T` must be used as the type parameter for some local type (e.g. `MyStruct<T>`); only traits defined in the current crate can be implemented for a type parameter
//    --> src/oracle/query_builder.rs:54:1
//    |
//    54 | / impl<'a, T, U, Op> ExecuteDsl<OciConnection> for InsertStatement<T, &'a [U], Op>
//    55 | |     where
//    56 | |         &'a U: Insertable<T>,
//    57 | |         InsertStatement<T, <&'a U as Insertable<T>>::Values, Op>: QueryFragment<Oracle>,
//    ...  |
//    75 | |     }
//    76 | | }
//    | |_^
////#[deprecated(since = "1.2.0", note = "Use `<&'a [U] as Insertable<T>>::Values` instead")]
//impl<'a, T, U, Op> ExecuteDsl<OciConnection> for InsertStatement<T, &'a [U], Op>
//    where
//        &'a U: Insertable<T>,
//        InsertStatement<T, <&'a U as Insertable<T>>::Values, Op>: QueryFragment<Oracle>,
//        T: Copy,
//        Op: Copy,
//{
//    fn execute(query: Self, conn: &OciConnection) -> QueryResult<usize> {
//        use diesel::connection::Connection;
//        conn.transaction(|| {
//            let mut result = 0;
//            for record in query.records {
//                result += InsertStatement::new(
//                    query.target,
//                    record.values(),
//                    query.operator,
//                    query.returning,
//                ).execute(conn)?;
//            }
//            Ok(result)
//        })
//    }
//}