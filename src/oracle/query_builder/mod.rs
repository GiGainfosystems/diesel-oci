use super::backend::Oracle;

use diesel::query_builder::QueryBuilder;
use diesel::result::Error as DieselError;

use super::OciConnection;

mod insert_statement;

#[derive(Default)]
pub struct OciQueryBuilder {
    pub sql: String,
    bind_idx: u32,
}

impl OciQueryBuilder {
    pub fn new() -> Self {
        OciQueryBuilder {
            sql: String::new(),
            bind_idx: 0,
        }
    }
}

impl QueryBuilder<Oracle> for OciQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> Result<(), DieselError> {
        // TODO: check if there is a better way for escaping strings
        self.push_sql("\"");
        self.push_sql(&identifier.replace("`", "``").to_uppercase());
        self.push_sql("\"");
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        let sql = format!(":{}", self.bind_idx);
        self.push_sql(&sql);
    }

    fn finish(self) -> String {
        self.sql
    }
}

//#![feature(specialization)]

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