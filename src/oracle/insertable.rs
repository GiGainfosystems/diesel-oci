#![feature(specialization)]

// This fails to compile since we cannot specialize QueryFragement here, but we would need to this
// since Oracle demands a different syntax for batch inserts, ala `insert into t(a,b) select (1,2)
// from dual union all select (4,5) from dual`
// once https://github.com/rust-lang/rfcs/pull/2451 has been accepted and
// implemented in Rust (or diesel changed something else) we can go forward with this
// otherwise we would need to implement an own SQL parser

// error[E0119]: conflicting implementations of trait `diesel::query_builder::QueryFragment<oracle::backend::Oracle>` for type `diesel::insertable::BatchInsert<'_, _, _>`:
//  --> src/oracle/insertable.rs:21:1
//   |
//21 | / impl<'a, T, Tab, Inner> QueryFragment<Oracle> for BatchInsert<'a, T, Tab>
//22 | | where
//23 | |     &'a T: Insertable<Tab, Values = ValuesClause<Inner, Tab>>,
//24 | |     ValuesClause<Inner, Tab>: QueryFragment<Oracle>,
//...  |
//38 | |     }
//39 | | }
//   | |_^
//   |
//   = note: conflicting implementation in crate `diesel`:
//           - impl<'a, T, Tab, Inner, DB> diesel::query_builder::QueryFragment<DB> for diesel::insertable::BatchInsert<'a, T, Tab>
//             where <&'a T as diesel::Insertable<Tab>>::Values == diesel::query_builder::ValuesClause<Inner, Tab>, DB: diesel::backend::Backend, DB: diesel::backend::SupportsDefaultKeyword, &'a T: diesel::Insertable<Tab>, diesel::query_builder::ValuesClause<Inner, Tab>: diesel::query_builder::QueryFragment<DB>, Inner: diesel::query_builder::QueryFragment<DB>;

use diesel::query_builder::QueryFragment;
use diesel::query_builder::ValuesClause;

use diesel::backend::Backend;
use diesel::insertable::InsertValues;
use diesel::query_builder::insert_statement::DefaultValues;
use diesel::query_builder::AstPass;
use diesel::query_source::Table;
use diesel::result::QueryResult;

use diesel::backend::SupportsDefaultKeyword;
use diesel::insertable::BatchInsert;
use diesel::insertable::Insertable;
use diesel::query_builder::insert_statement::InsertStatement;
use diesel::query_dsl::methods::ExecuteDsl;

use super::backend::Oracle;

impl<'a, T, Tab, Inner> QueryFragment<Oracle> for BatchInsert<'a, T, Tab>
where
    &'a T: Insertable<Tab, Values = ValuesClause<Inner, Tab>>,
    ValuesClause<Inner, Tab>: QueryFragment<Oracle>,
    Inner: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
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
