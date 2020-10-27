use super::ErrorHelper;
use super::OciConnection;
use diesel::connection::SimpleConnection;
use diesel::connection::TransactionManager;
use diesel::result::QueryResult;
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
            conn.raw.borrow_mut().set_autocommit(false);
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
            let res = conn
                .raw
                .borrow()
                .rollback()
                .map_err(ErrorHelper::from)
                .map_err(Into::into);

            conn.raw.borrow_mut().set_autocommit(true);
            res
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
        // since oracle doesn't support nested transactions we only commit the outmost transaction
        // and not every inner transaction; if the outmost transaction fails everything will be
        // rolled back, every inner transaction can fail, but no be committed since it doesn't make
        // sense to commit the inner ones
        if transaction_depth <= 1 {
            conn.raw.borrow().commit().map_err(ErrorHelper::from)?;
            conn.raw.borrow_mut().set_autocommit(true);
        };
        self.change_transaction_depth(-1, Ok(()))
    }

    fn get_transaction_depth(&self) -> u32 {
        self.transaction_depth.get() as u32
    }
}
