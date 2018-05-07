use std::rc::Rc;
use diesel::connection::{SimpleConnection, Connection, MaybeCached};
use diesel::result::*;
use diesel::query_builder::{AsQuery, QueryFragment};
use diesel::sql_types::HasSqlType;
use diesel::deserialize::{Queryable, QueryableByName};
use diesel::query_builder::QueryId;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::connection::StatementCache;
use diesel::connection::AnsiTransactionManager;
use diesel::migration::MigrationConnection;

use super::backend::Oracle;
use self::stmt::Statement;
use self::cursor::Cursor;

mod raw;
mod stmt;
mod cursor;
mod row;

pub struct OciConnection {
    raw: Rc<raw::RawConnection>,
    transaction_manager: AnsiTransactionManager,
    statement_cache: StatementCache<Oracle, Statement>,
}

// This relies on the invariant that RawConnection or Statement are never
// leaked. If a reference to one of those was held on a different thread, this
// would not be thread safe.
// Similar to diesel::sqlite::SqliteConnection;
unsafe impl Send for OciConnection {}

impl MigrationConnection for OciConnection {
    const CREATE_MIGRATIONS_TABLE: &'static str =
         "CREATE TABLE \"__DIESEL_SCHEMA_MIGRATIONS\" (\
         \"VERSION\" VARCHAR2(50) PRIMARY KEY NOT NULL,\
         \"RUN_ON\" TIMESTAMP with time zone DEFAULT sysdate not null\
         )";
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
    type TransactionManager = AnsiTransactionManager;

    /// Establishes a new connection to the database at the given URL. The URL
    /// should be a valid connection string for a given backend. See the
    /// documentation for the specific backend for specifics.
    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let r = try!(raw::RawConnection::establish(database_url));
        Ok(OciConnection {
               raw: Rc::new(r),
               transaction_manager: AnsiTransactionManager::new(),
               statement_cache: StatementCache::new(),
           })
    }


    #[doc(hidden)]
    fn execute(&self, query: &str) -> QueryResult<usize> {
        let stmt = try!(Statement::prepare(&self.raw, query));
        try!(stmt.run());
        Ok(try!(stmt.get_affected_rows()))
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
        where T: QueryFragment<Self::Backend> + QueryId
    {
        let stmt = try!(self.prepare_query(source));
        try!(stmt.run());
        Ok(try!(stmt.get_affected_rows()))
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self.transaction_manager
    }

    fn query_by_index<T, U>(&self, source: T) -> QueryResult<Vec<U>>
        where T: AsQuery,
              T::Query: QueryFragment<Self::Backend> + QueryId,
              Self::Backend: HasSqlType<T::SqlType>,
              U: Queryable<T::SqlType, Self::Backend>
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
            U: QueryableByName<Self::Backend>
    {
        unimplemented!()
    }


}

impl OciConnection {
    fn prepare_query<T: QueryFragment<Oracle> + QueryId>(&self,
                                                         source: &T)
                                                         -> QueryResult<MaybeCached<Statement>> {
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

    fn cached_prepared_statement<T: QueryFragment<Oracle> + QueryId>
        (&self,
         source: &T)
         -> QueryResult<MaybeCached<Statement>> {
        self.statement_cache
            .cached_statement(source, &[], |sql| Statement::prepare(&self.raw, sql))
    }
}
