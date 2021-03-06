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
use super::backend::{HasSqlTypeExt, Oracle};
use diesel::RunQueryDsl;

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
    fn setup(&self) -> QueryResult<usize> {
        diesel::sql_query(include_str!("define_create_if_not_exists.sql")).execute(self)?;
        diesel::sql_query(include_str!("create_migration_table.sql")).execute(self)
    }
}

// This relies on the invariant that RawConnection or Statement are never
// leaked. If a reference to one of those was held on a different thread, this
// would not be thread safe.
// Similar to diesel::sqlite::SqliteConnection;
unsafe impl Send for OciConnection {}

impl SimpleConnection for OciConnection {
    fn batch_execute(&self, query: &str) -> QueryResult<()> {
        let mut stmt = Statement::prepare(&self.raw, query)?;
        stmt.run(self.auto_commit(), &[])?;
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
        let r = raw::RawConnection::establish(database_url)?;
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
        let mut stmt = Statement::prepare(&self.raw, query)?;
        stmt.run(self.auto_commit(), &[])?;
        stmt.bind_index = 0;
        Ok(stmt.get_affected_rows()?)
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        // TODO: FIXME: this always returns 0 whereas the code looks proper
        let mut stmt = self.prepare_query(source)?;
        stmt.run(self.auto_commit(), &[])?;
        stmt.bind_index = 0;
        Ok(stmt.get_affected_rows()?)
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
        Oracle::oci_row_metadata(&mut metadata);
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
        let mut statement = self.cached_prepared_statement(source)?;

        let mut bind_collector = RawBytesBindCollector::<Oracle>::new();
        source.collect_binds(&mut bind_collector, &())?;
        let metadata = bind_collector.metadata;
        let binds = bind_collector.binds;
        for (tpe, value) in metadata.into_iter().zip(binds) {
            let tpe = tpe.ok_or_else(|| diesel::result::Error::QueryBuilderError(
                "Input binds need type information".into(),
            ))?;
            statement.bind(tpe, value)?;
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

#[cfg(feature = "r2d2")]
use diesel::r2d2::R2D2Connection;

#[cfg(feature = "r2d2")]
impl R2D2Connection for OciConnection {
    fn ping(&self) -> QueryResult<()> {
        self.execute("SELECT 1 FROM DUAL").map(|_| ())
    }
}
