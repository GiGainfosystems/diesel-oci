use std::num::NonZeroU32;

use super::ErrorHelper;
use super::OciConnection;
use diesel::connection::SimpleConnection;
use diesel::connection::TransactionDepthChange;
use diesel::connection::TransactionManager;
use diesel::connection::TransactionManagerStatus;
use diesel::result::QueryResult;

/// An implementation of `TransactionManager` which can be used for oracle
#[allow(missing_debug_implementations)]
#[derive(Default)]
pub struct OCITransactionManager {
    pub(super) status: TransactionManagerStatus,
}

impl OCITransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        OCITransactionManager::default()
    }

    fn change_transaction_depth(
        conn: &mut OciConnection,
        by: TransactionDepthChange,
    ) -> QueryResult<()> {
        match Self::transaction_manager_status_mut(conn) {
            TransactionManagerStatus::Valid(ref mut v) => v.change_transaction_depth(by),
            TransactionManagerStatus::InError => {
                Err(diesel::result::Error::BrokenTransactionManager)
            }
        }
    }

    /// Begin a transaction with custom SQL
    ///
    /// This is used by connections to implement more complex transaction APIs
    /// to set things such as isolation levels.
    /// Returns an error if already inside of a transaction.
    #[allow(dead_code)]
    pub fn begin_transaction_sql(conn: &mut OciConnection, sql: &str) -> QueryResult<()> {
        use diesel::result::Error::AlreadyInTransaction;

        if Self::get_transaction_depth(conn)?.is_none() {
            conn.batch_execute(sql)?;
            Self::change_transaction_depth(conn, TransactionDepthChange::IncreaseDepth)
        } else {
            Err(AlreadyInTransaction)
        }
    }

    fn get_transaction_depth(conn: &mut OciConnection) -> QueryResult<Option<NonZeroU32>> {
        Self::transaction_manager_status_mut(conn).transaction_depth()
    }
}

impl TransactionManager<OciConnection> for OCITransactionManager {
    type TransactionStateData = Self;

    fn begin_transaction(conn: &mut OciConnection) -> QueryResult<()> {
        let transaction_depth = Self::get_transaction_depth(conn)?;
        match transaction_depth {
            None => {
                conn.raw.set_autocommit(false);
                Ok(())
            }
            Some(d) => conn.batch_execute(&format!("SAVEPOINT diesel_savepoint_{}", d)),
        }?;
        Self::change_transaction_depth(conn, TransactionDepthChange::IncreaseDepth)
    }

    fn rollback_transaction(conn: &mut OciConnection) -> QueryResult<()> {
        // DDL will never rolled back: https://asktom.oracle.com/pls/apex/f?p=100:11:0::::P11_QUESTION_ID:9532421900346923086
        // all preceding DML will be commited with a DDL statement !!!
        // c.f. https://docs.oracle.com/cd/E25054_01/server.1111/e25789/transact.htm#sthref1318
        let transaction_depth = Self::get_transaction_depth(conn)?;
        let mut mark_as_broken = false;
        match transaction_depth.map(|d| d.into()) {
            Some(1) => {
                let res = conn
                    .raw
                    .rollback()
                    .map_err(ErrorHelper::from)
                    .map_err(Into::into);
                if res.is_err() {
                    mark_as_broken = true;
                }

                conn.raw.set_autocommit(true);
                res
            }
            Some(d) => {
                conn.batch_execute(&format!("ROLLBACK TO SAVEPOINT diesel_savepoint_{}", d - 1))
            }
            None => Err(diesel::result::Error::NotInTransaction),
        }?;
        let res = Self::change_transaction_depth(conn, TransactionDepthChange::DecreaseDepth);
        if mark_as_broken {
            let status = Self::transaction_manager_status_mut(conn);
            *status = diesel::connection::TransactionManagerStatus::InError;
        }
        res
    }

    fn commit_transaction(conn: &mut OciConnection) -> QueryResult<()> {
        let transaction_depth = Self::get_transaction_depth(conn)?;
        // since oracle doesn't support nested transactions we only commit the outmost transaction
        // and not every inner transaction; if the outmost transaction fails everything will be
        // rolled back, every inner transaction can fail, but no be committed since it doesn't make
        // sense to commit the inner ones
        match transaction_depth.map(Into::into) {
            Some(1) => {
                if let Err(e) = conn.raw.commit().map_err(ErrorHelper::from) {
                    let status = Self::transaction_manager_status_mut(conn);
                    *status = diesel::connection::TransactionManagerStatus::InError;
                    return Err(e.into());
                }
                conn.raw.set_autocommit(true);
            }
            Some(_) => {
                // Do nothing for savepoints
            }
            None => return Err(diesel::result::Error::NotInTransaction),
        }
        Self::change_transaction_depth(conn, TransactionDepthChange::DecreaseDepth)
    }

    fn transaction_manager_status_mut(
        conn: &mut OciConnection,
    ) -> &mut diesel::connection::TransactionManagerStatus {
        &mut conn.transaction_manager.status
    }
}
