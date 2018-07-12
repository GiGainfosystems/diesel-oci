use super::OciConnection;
use diesel::connection::SimpleConnection;
use diesel::connection::TransactionManager;
use diesel::result::QueryResult;
use oci_sys as ffi;
use std::cell::Cell;

/// An implementation of `TransactionManager` which can be used for oracle
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub struct OCITransactionManager {
    transaction_depth: Cell<i32>,
}

impl OCITransactionManager {
    /// Create a new transaction manager
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
        Conn: SimpleConnection,
    {
        use diesel::result::Error::AlreadyInTransaction;

        if self.transaction_depth.get() == 0 {
            self.change_transaction_depth(1, conn.batch_execute(sql))
        } else {
            Err(AlreadyInTransaction)
        }
    }
}

impl TransactionManager<OciConnection> for OCITransactionManager {
    fn begin_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        let query = if transaction_depth == 0 {
            let _status = unsafe {
                ffi::OCITransStart(
                    conn.raw.service_handle,
                    conn.raw.env.error_handle,
                    0,
                    ffi::OCI_TRANS_NEW,
                )
            };
            Ok(())
        } else {
            conn.batch_execute(&format!("SAVEPOINT diesel_savepoint_{}", transaction_depth))
        };
        self.change_transaction_depth(1, query)
    }

    fn rollback_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        // DDL will never rolled back: https://asktom.oracle.com/pls/apex/f?p=100:11:0::::P11_QUESTION_ID:9532421900346923086
        // all preceding DML will be commited with a DDL statement !!!
        // c.f. https://docs.oracle.com/cd/E25054_01/server.1111/e25789/transact.htm#sthref1318
        let transaction_depth = self.transaction_depth.get();
        let query = if transaction_depth == 1 {
            let _status = unsafe {
                ffi::OCITransRollback(
                    conn.raw.service_handle,
                    conn.raw.env.error_handle,
                    ffi::OCI_DEFAULT,
                )
            };
            Ok(())
        } else {
            conn.batch_execute(&format!(
                "ROLLBACK TO SAVEPOINT diesel_savepoint_{}",
                transaction_depth - 1
            ))
        };
        self.change_transaction_depth(-1, query)
    }

    fn commit_transaction(&self, conn: &OciConnection) -> QueryResult<()> {
        let transaction_depth = self.transaction_depth.get();
        let query = if transaction_depth <= 1 {
            let _status = unsafe {
                ffi::OCITransCommit(
                    conn.raw.service_handle,
                    conn.raw.env.error_handle,
                    ffi::OCI_DEFAULT,
                )
            };
            Ok(())
        } else {
            conn.batch_execute(&format!(
                "COMMIT",
                transaction_depth - 1
            ))
        };
        self.change_transaction_depth(-1, query)
    }

    fn get_transaction_depth(&self) -> u32 {
        self.transaction_depth.get() as u32
    }
}
