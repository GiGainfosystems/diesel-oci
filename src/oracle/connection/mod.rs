use std::cell::Cell;
use std::rc::Rc;

use diesel::connection::StatementCache;
use diesel::connection::{Connection, MaybeCached, SimpleConnection, TransactionManager};
use diesel::deserialize::{Queryable, QueryableByName};
use diesel::migration::MigrationConnection;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::QueryId;
use diesel::query_builder::{AsQuery, QueryFragment};
use diesel::result::*;
use diesel::sql_types::HasSqlType;

use self::cursor::{Cursor, NamedCursor};
use self::stmt::Statement;
use self::transaction::OCITransactionManager;
use super::backend::Oracle;

mod oracle_value;
pub use self::oracle_value::OracleValue;

mod bind_context;
mod cursor;
mod raw;
mod row;
mod stmt;
mod transaction;

pub struct OciConnection {
    raw: Rc<raw::RawConnection>,
    transaction_manager: OCITransactionManager,
    statement_cache: StatementCache<Oracle, Statement>,
    has_open_test_transaction: Cell<bool>,
}

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

// This relies on the invariant that RawConnection or Statement are never
// leaked. If a reference to one of those was held on a different thread, this
// would not be thread safe.
// Similar to diesel::sqlite::SqliteConnection;
unsafe impl Send for OciConnection {}

impl SimpleConnection for OciConnection {
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        let mut stmt = try!(Statement::prepare(&self.raw, query));
        try!(stmt.run(self.auto_commit(), &[]));
        stmt.bind_index = 0;
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
            has_open_test_transaction: Cell::new(false),
        };
        Ok(ret)
    }

    /// Creates a transaction that will never be committed. This is useful for
    /// tests. Panics if called while inside of a transaction.
    fn begin_test_transaction(&self) -> QueryResult<()> {
        let transaction_manager = self.transaction_manager();
        assert_eq!(transaction_manager.get_transaction_depth(), 0);
        self.has_open_test_transaction.set(true);
        transaction_manager.begin_transaction(self)
    }

    #[doc(hidden)]
    fn execute(&self, query: &str) -> QueryResult<usize> {
        let mut stmt = try!(Statement::prepare(&self.raw, query));
        try!(stmt.run(self.auto_commit(), &[]));
        stmt.bind_index = 0;
        Ok(try!(stmt.get_affected_rows()))
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        // TODO: FIXME: this always returns 0 whereas the code looks proper
        let mut stmt = try!(self.prepare_query(source));
        try!(stmt.run(self.auto_commit(), &[]));
        stmt.bind_index = 0;
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
        let mut stmt = self.prepare_query(&source.as_query())?;
        let mut metadata = Vec::new();
        // TODO: FIXME: Georg will check if this can get un-deprecated.
        #[allow(deprecated)]
        Oracle::row_metadata(&mut metadata, &());
        let cursor: Cursor<T::SqlType, U> = stmt.run_with_cursor(self.auto_commit(), metadata)?;
        cursor.collect()
    }

    fn query_by_name<T, U>(&self, source: &T) -> QueryResult<Vec<U>>
    where
        T: QueryFragment<Self::Backend> + QueryId,
        U: QueryableByName<Self::Backend>,
    {
        let mut stmt = self.prepare_query(&source)?;
        let mut metadata = Vec::new();
        stmt.get_metadata(&mut metadata)?;
        let mut cursor: NamedCursor = stmt.run_with_named_cursor(self.auto_commit(), metadata)?;
        cursor.collect()
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

    fn auto_commit(&self) -> bool {
        self.transaction_manager.get_transaction_depth() == 0
    }
}

impl Drop for OciConnection {
    fn drop(&mut self) {
        if self.has_open_test_transaction.get() {
            let tm = self.transaction_manager();
            tm.rollback_transaction(&self)
                .expect("This return Ok() for all paths anyway");
        }
    }
}
