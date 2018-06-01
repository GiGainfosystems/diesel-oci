use diesel::connection::StatementCache;
use diesel::connection::{Connection, MaybeCached, SimpleConnection};
use diesel::deserialize::{Queryable, QueryableByName};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::QueryId;
use diesel::query_builder::{AsQuery, QueryFragment};
use diesel::result::*;
use diesel::sql_types::HasSqlType;
use std::rc::Rc;

#[cfg(test)]
use diesel::migration::MigrationConnection;

use self::cursor::Cursor;
use self::stmt::Statement;
use self::transaction::OCITransactionManager;
use super::backend::Oracle;
mod oracle_value;
pub use self::oracle_value::OracleValue;

mod cursor;
mod raw;
mod row;
mod stmt;
mod transaction;

pub struct OciConnection {
    raw: Rc<raw::RawConnection>,
    transaction_manager: OCITransactionManager,
    statement_cache: StatementCache<Oracle, Statement>,
}

// This relies on the invariant that RawConnection or Statement are never
// leaked. If a reference to one of those was held on a different thread, this
// would not be thread safe.
// Similar to diesel::sqlite::SqliteConnection;
unsafe impl Send for OciConnection {}

#[cfg(test)]
impl MigrationConnection for OciConnection {
    #[cfg(ka)]
    const CREATE_MIGRATIONS_FUNCTION: &'static str =
        "create or replace procedure create_if_not_exists(input_sql varchar2) \
         as \
         begin \
         execute immediate input_sql; \
         exception \
         when others then \
         if sqlcode = -955 then \
         NULL; \
         else \
         raise; \
         end if; \
         end; \n ";

    const CREATE_MIGRATIONS_TABLE: &'static str = "
    declare \
    begin \
    create_if_not_exists('CREATE TABLE \"__DIESEL_SCHEMA_MIGRATIONS\" (\
         \"VERSION\" VARCHAR2(50) PRIMARY KEY NOT NULL,\
         \"RUN_ON\" TIMESTAMP with time zone DEFAULT sysdate not null\
         )'); \
        end; \n";
}

impl SimpleConnection for OciConnection {
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        let stmt = try!(Statement::prepare(&self.raw, query));
        try!(stmt.run());
        Ok(())
    }
}

impl Connection for OciConnection {
    type Backend = Oracle;
    type TransactionManager = OCITransactionManager;

    /// Establishes a new connection to the database at the given URL. The URL
    /// should be a valid connection string for a given backend. See the
    /// documentation for the specific backend for specifics.
    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let r = try!(raw::RawConnection::establish(database_url));
        let ret = OciConnection {
            raw: Rc::new(r),
            transaction_manager: OCITransactionManager::new(),
            statement_cache: StatementCache::new(),
        };
        let _k = ret.execute("alter session set sql_trace=true");
        Ok(ret)
    }

    #[doc(hidden)]
    fn execute(&self, query: &str) -> QueryResult<usize> {
        let stmt = try!(Statement::prepare(&self.raw, query));
        try!(stmt.run());
        Ok(try!(stmt.get_affected_rows()))
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let stmt = try!(self.prepare_query(source));
        try!(stmt.run());
        Ok(try!(stmt.get_affected_rows()))
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self.transaction_manager
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId,
        Self::Backend: HasSqlType<T::SqlType>,
        U: Queryable<T::SqlType, Self::Backend>,
    {
        let stmt = try!(self.prepare_query(&source.as_query()));
        let cursor: Cursor<T::SqlType, U> = try!(stmt.run_with_cursor());
        let mut ret = Vec::new();
        for el in cursor {
            ret.push(try!(el));
        }
        Ok(ret)
    }

    fn query_by_name<T, U>(&self, _source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Self::Backend> + QueryId,
        U: QueryableByName<Self::Backend>,
    {
        unimplemented!()
    }
}

impl OciConnection {
    fn prepare_query<T: QueryFragment<Oracle> + QueryId>(
        &self,
        source: &T,
    ) -> QueryResult<MaybeCached<Statement>> {
        let mut statement = try!(self.cached_prepared_statement(source));

        let mut bind_collector = RawBytesBindCollector::<Oracle>::new();
        try!(source.collect_binds(&mut bind_collector, &()));
        let metadata = bind_collector.metadata;
        let binds = bind_collector.binds;
        for (tpe, value) in metadata.into_iter().zip(binds) {
            try!(statement.bind(tpe, value));
        }

        Ok(statement)
    }

    fn cached_prepared_statement<T: QueryFragment<Oracle> + QueryId>(
        &self,
        source: &T,
    ) -> QueryResult<MaybeCached<Statement>> {
        self.statement_cache
            .cached_statement(source, &[], |sql| Statement::prepare(&self.raw, sql))
    }
}
