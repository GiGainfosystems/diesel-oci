use diesel::connection::TransactionManager;
use diesel::backend::UsesAnsiSavepointSyntax;
use diesel::connection::{Connection, SimpleConnection};
use diesel::result::QueryResult;
use std::cell::Cell;
use oci_sys as ffi;
use super::OciConnection;

/// An implementation of `TransactionManager` which can be used for oracle
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub struct OCITransactionManager {
    transaction_depth: Cell<i32>,
}

impl OCITransactionManager {
    /// Create a new transaction manager
    #[allow(dead_code)]
    pub fn new() -> Self {
        OCITransactionManager::default()
    }

    fn change_transaction_depth(&self, by: i32, query: QueryResult<()>) -> QueryResult<()> {
        if query.is_ok() {
            self.transaction_depth
                .set(self.transaction_depth.get() + by)
        }
        query
    }

    /// Begin a transaction with custom SQL
    ///
    /// This is used by connections to implement more complex transaction APIs
    /// to set things such as isolation levels.
    /// Returns an error if already inside of a transaction.
    #[allow(dead_code)]
    pub fn begin_transaction_sql<Conn>(&self, conn: &Conn, sql: &str) -> QueryResult<()>
        where
            Conn: SimpleConnection
    {
        use diesel::result::Error::AlreadyInTransaction;

        if self.transaction_depth.get() == 0 {
            self.change_transaction_depth(1, conn.batch_execute(sql))
        } else {
            Err(AlreadyInTransaction)
        }
    }
}

impl TransactionManager<OciConnection> for OCITransactionManager
{
    fn begin_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        self.change_transaction_depth(
            1,
            if transaction_depth == 0 {
                let status = unsafe {
                    ffi::OCITransStart(
                        conn.raw.service_handle,
                        conn.raw.env.error_handle,
                        0,
                        ffi::OCI_TRANS_NEW
                    )
                };
                Ok(())
            } else {
                conn.batch_execute(&format!("SAVEPOINT diesel_savepoint_{}", transaction_depth))
            },
        )

    }

    fn rollback_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        self.change_transaction_depth(
            -1,
            if transaction_depth == 1 {
                let status = unsafe {
                    ffi::OCITransRollback(
                        conn.raw.service_handle,
                        conn.raw.env.error_handle,
                        ffi::OCI_DEFAULT
                    )
                };
                Ok(())
            } else {
                conn.batch_execute(&format!(
                    "ROLLBACK TO SAVEPOINT diesel_savepoint_{}",
                    transaction_depth - 1
                ))
            },
        )
    }

    fn commit_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        self.change_transaction_depth(
            -1,
            if transaction_depth <= 1 {
                let status = unsafe {
                    ffi::OCITransCommit(
                        conn.raw.service_handle,
                        conn.raw.env.error_handle,
                        ffi::OCI_DEFAULT
                    )
                };
                Ok(())
            } else {
                conn.batch_execute(&format!(
                    "COMMIT diesel_savepoint_{}",
                    transaction_depth - 1
                ))
            },
        )
    }

    fn get_transaction_depth(&self) -> u32 {
        self.transaction_depth.get() as u32
    }
}