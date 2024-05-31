use std::fmt::Write;
use std::marker::PhantomData;
use std::rc::Rc;

use self::bind_collector::OracleBindCollector;
use self::row::OciRow;
use self::transaction::OCITransactionManager;
use super::backend::Oracle;
use super::query_builder::OciQueryBuilder;
use super::OciDataType;
use crate::oracle::connection::stmt_iter::RowIter;
use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;
use diesel::connection::{Connection, SimpleConnection, TransactionManager};
use diesel::connection::{LoadConnection, MultiConnectionHelper};
use diesel::deserialize::FromSql;
use diesel::expression::QueryMetadata;
use diesel::insertable::{CanInsertInSingleQuery, InsertValues};
use diesel::internal::derives::multiconnection::ConnectionSealed;
use diesel::migration::MigrationConnection;
use diesel::query_builder::{AsQuery, BatchInsert, QueryBuilder, QueryFragment};
use diesel::query_builder::{InsertStatement, QueryId, ValuesClause};
use diesel::sql_types::HasSqlType;
use diesel::RunQueryDsl;
use diesel::{result::*, Table};
use oracle::ErrorKind;

mod oracle_value;
pub(crate) use self::oracle_value::InnerValue;
pub use self::oracle_value::OracleValue;

pub(crate) mod bind_collector;
mod row;
mod stmt_iter;
mod transaction;

/// Connections for the Oracle backend. The following connection url schema is supported:
///
/// `oracle://user:password@host:[port]/database`
///
/// where:
///
///  * `user` is your username
///  * `password` is the corresponding password
///  * `host` is the hostname/ip address of the database server
///  * `port` is an optional port number
///  * `database` is your database name
///
/// # Supported loading model implementations
///
/// * [`DefaultLoadingMode`]
///
/// As `OciConnection` only supports a single loading mode implementation
/// it is **not required** to explicitly specify a loading mode
/// when calling [`RunQueryDsl::load_iter()`] or [`LoadConnection::load`]
///
/// [`RunQueryDsl::load_iter()`]: diesel::query_dsl::RunQueryDsl::load_iter
///
/// ## DefaultLoadingMode
///
/// `OciConnection` only supports a single loading mode, which internally loads
/// all values at once.
///
/// ```no_run
/// # use diesel_oci::OciConnection;
/// # use diesel::prelude::*;
/// #
/// # fn establish_connection() -> OciConnection {
/// #    OciConnection::establish("…").unwrap()
/// # }
/// #
/// # table! {
/// #    users {
/// #        id -> Integer,
/// #        name -> Text,
/// #    }
/// # }
/// #
/// # fn main() {
/// #     run_test().unwrap();
/// # }
/// #
/// # fn run_test() -> QueryResult<()> {
/// #     use self::users;
/// #     let connection = &mut establish_connection();
/// use diesel::connection::DefaultLoadingMode;
/// { // scope to restrict the lifetime of the iterator
///     let iter1 = users::table.load_iter::<(i32, String), DefaultLoadingMode>(connection)?;
///
///     for r in iter1 {
///         let (id, name) = r?;
///         println!("Id: {} Name: {}", id, name);
///     }
/// }
///
/// // works without specifying the loading mode
/// let iter2 = users::table.load_iter::<(i32, String), _>(connection)?;
///
/// for r in iter2 {
///     let (id, name) = r?;
///     println!("Id: {} Name: {}", id, name);
/// }
/// #   Ok(())
/// # }
/// ```
///
/// This mode does support creating
/// multiple iterators using the same connection.
///
/// ```no_run
/// # use diesel_oci::OciConnection;
/// # use diesel::prelude::*;
/// #
/// # fn establish_connection() -> OciConnection {
/// #    OciConnection::establish("…").unwrap()
/// # }
/// #
/// # table! {
/// #    users {
/// #        id -> Integer,
/// #        name -> Text,
/// #    }
/// # }
/// #
/// # fn main() {
/// #     run_test().unwrap();
/// # }
/// #
/// # fn run_test() -> QueryResult<()> {
/// #     use self::users;
/// #     let connection = &mut establish_connection();
/// use diesel::connection::DefaultLoadingMode;
///
/// let iter1 = users::table.load_iter::<(i32, String), DefaultLoadingMode>(connection)?;
/// let iter2 = users::table.load_iter::<(i32, String), DefaultLoadingMode>(connection)?;
///
/// for r in iter1 {
///     let (id, name) = r?;
///     println!("Id: {} Name: {}", id, name);
/// }
///
/// for r in iter2 {
///     let (id, name) = r?;
///     println!("Id: {} Name: {}", id, name);
/// }
/// #   Ok(())
/// # }
/// ```
pub struct OciConnection {
    raw: oracle::Connection,
    transaction_manager: OCITransactionManager,
    instrumentation: Option<Box<dyn Instrumentation>>,
}

struct ErrorHelper(oracle::Error);

impl From<oracle::Error> for ErrorHelper {
    fn from(e: oracle::Error) -> Self {
        Self(e)
    }
}

impl From<ErrorHelper> for diesel::result::Error {
    fn from(ErrorHelper(e): ErrorHelper) -> Self {
        match (e.kind(), e.db_error()) {
            (ErrorKind::OciError, _) => {
                // TODO: better handling here
                diesel::result::Error::QueryBuilderError(e.into())
            }
            (ErrorKind::DpiError, _) => {
                // TODO: better handling here
                diesel::result::Error::QueryBuilderError(e.into())
            }
            (ErrorKind::NullValue, _) => diesel::result::Error::DeserializationError(
                diesel::result::UnexpectedNullError.into(),
            ),
            (ErrorKind::ParseError, _) => diesel::result::Error::SerializationError(e.into()),
            (ErrorKind::OutOfRange | ErrorKind::InvalidTypeConversion, _) => {
                diesel::result::Error::DeserializationError(e.into())
            }
            (ErrorKind::InvalidBindIndex, Some(e)) => diesel::result::Error::QueryBuilderError(
                format!("Invalid bind with index: {}", e).into(),
            ),
            (ErrorKind::InvalidBindName, Some(e)) => diesel::result::Error::QueryBuilderError(
                format!("Invalid bind with name: {}", e).into(),
            ),
            (ErrorKind::InvalidColumnIndex, _) => diesel::result::Error::DeserializationError(
                diesel::result::UnexpectedEndOfRow.into(),
            ),
            (ErrorKind::InvalidColumnName, _) => diesel::result::Error::DeserializationError(
                format!("Invalid column name: {}", e).into(),
            ),
            (ErrorKind::InvalidAttributeName, _) => diesel::result::Error::QueryBuilderError(
                format!("Invalid attribute name: {}", e).into(),
            ),
            (ErrorKind::InvalidOperation, _) => {
                diesel::result::Error::QueryBuilderError(format!("Invalid operation: {}", e).into())
            }
            (ErrorKind::UninitializedBindValue, _) => {
                diesel::result::Error::QueryBuilderError("Uninitialized bind value".into())
            }
            (ErrorKind::NoDataFound, _) => diesel::result::Error::NotFound,
            (ErrorKind::InternalError, _) => diesel::result::Error::QueryBuilderError(e.into()),
            (ErrorKind::BatchErrors, _) => {
                diesel::result::Error::QueryBuilderError("Batch error".into())
            }
            _ => unimplemented!(),
        }
    }
}

impl ConnectionSealed for OciConnection {}
impl MultiConnectionHelper for OciConnection {
    fn to_any<'a>(
        lookup: &mut <Self::Backend as diesel::sql_types::TypeMetadata>::MetadataLookup,
    ) -> &mut (dyn std::any::Any + 'a) {
        lookup
    }

    fn from_any(
        lookup: &mut dyn std::any::Any,
    ) -> Option<&mut <Self::Backend as diesel::sql_types::TypeMetadata>::MetadataLookup> {
        lookup.downcast_mut()
    }
}

impl MigrationConnection for OciConnection {
    fn setup(&mut self) -> QueryResult<usize> {
        diesel::sql_query(include_str!("define_create_if_not_exists.sql")).execute(self)?;
        diesel::sql_query(include_str!("create_migration_table.sql")).execute(self)
    }
}

// TODO: check this
// This relies on the invariant that RawConnection or Statement are never
// leaked. If a reference to one of those was held on a different thread, this
// would not be thread safe.
// Similar to diesel::sqlite::SqliteConnection;
unsafe impl Send for OciConnection {}

impl SimpleConnection for OciConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(
                &diesel::connection::StrQueryHelper::new(query),
            ));
        let r = self
            .raw
            .execute(query, &[])
            .map_err(ErrorHelper::from)
            .map_err(Into::into)
            .map(|_| ());
        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &diesel::connection::StrQueryHelper::new(query),
                r.as_ref().err(),
            ));

        r
    }
}

impl Connection for OciConnection {
    type Backend = Oracle;
    type TransactionManager = OCITransactionManager;

    /// Establishes a new connection to the database at the given URL. The URL
    /// should be a valid connection string for a given backend. See the
    /// documentation for the specific backend for specifics.
    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let mut instrumentation = diesel::connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::start_establish_connection(
            database_url,
        ));
        let raw = Self::inner_establish(database_url);
        instrumentation.on_connection_event(InstrumentationEvent::finish_establish_connection(
            database_url,
            raw.as_ref().err(),
        ));

        Ok(Self {
            raw: raw?,
            transaction_manager: OCITransactionManager::new(),
            instrumentation,
        })
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&diesel::debug_query(
                source,
            )));
        let res = self.inner_executing_returning_count(source);
        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &diesel::debug_query(source),
                res.as_ref().err(),
            ));
        res
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.transaction_manager
    }

    fn begin_test_transaction(&mut self) -> QueryResult<()> {
        match Self::TransactionManager::transaction_manager_status_mut(self) {
            diesel::connection::TransactionManagerStatus::Valid(valid_status) => {
                assert_eq!(None, valid_status.transaction_depth())
            }
            diesel::connection::TransactionManagerStatus::InError => {
                panic!("Transaction manager in error")
            }
        };
        Self::TransactionManager::begin_transaction(self)?;
        self.transaction_manager.is_test_transaction = true;
        Ok(())
    }

    fn instrumentation(&mut self) -> &mut dyn diesel::connection::Instrumentation {
        &mut self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl diesel::connection::Instrumentation) {
        self.instrumentation = Some(Box::new(instrumentation));
    }
}

impl LoadConnection for OciConnection {
    type Cursor<'conn, 'query> = RowIter;
    type Row<'conn, 'query> = OciRow;

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> QueryResult<RowIter>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let query = source.as_query();
        self.instrumentation
            .on_connection_event(InstrumentationEvent::start_query(&diesel::debug_query(
                &query,
            )));
        let res = self.with_prepared_statement(&query, |mut stmt, bind_collector| {
            if stmt.is_query() {
                let binds = bind_collector
                    .binds
                    .iter()
                    .map(|(n, b)| (n as &str, &**b))
                    .collect::<Vec<_>>();
                let result_set = stmt.query_named(&binds).map_err(ErrorHelper::from)?;
                let column_infos = Rc::new(result_set.column_info().to_owned());
                let rows = result_set
                    .map(|row| {
                        Ok::<_, diesel::result::Error>(OciRow::new(
                            row.map_err(ErrorHelper)?,
                            column_infos.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RowIter::new(rows))
            } else if stmt.is_returning() {
                Self::load_from_is_returning(stmt, bind_collector)
            } else {
                unreachable!()
            }
        });
        self.instrumentation
            .on_connection_event(InstrumentationEvent::finish_query(
                &diesel::debug_query(&query),
                res.as_ref().err(),
            ));
        res
    }
}

struct ReturningClauseFromSqlHelper<T, ST>(T, PhantomData<ST>);

impl<T, ST> oracle::sql_type::FromSql for ReturningClauseFromSqlHelper<T, ST>
where
    T: FromSql<ST, Oracle>,
    Oracle: HasSqlType<ST>,
{
    fn from_sql(val: &oracle::SqlValue) -> oracle::Result<Self>
    where
        Self: Sized,
    {
        let tpe = val.oracle_type()?;
        let oracle_value = OracleValue::new(val, tpe.clone());
        Ok(ReturningClauseFromSqlHelper(
            T::from_sql(oracle_value).unwrap(),
            PhantomData,
        ))
    }
}

impl OciConnection {
    fn with_prepared_statement<'conn, 'query, T, R>(
        &'conn mut self,
        query: &T,
        callback: impl FnOnce(oracle::Statement, OracleBindCollector) -> QueryResult<R>,
    ) -> Result<R, Error>
    where
        T: QueryFragment<Oracle> + QueryId + 'query,
    {
        let mut qb = OciQueryBuilder::default();
        query.to_sql(&mut qb, &Oracle)?;
        let query_string = qb.finish();
        let is_safe_to_cache = query.is_safe_to_cache_prepared(&Oracle)?;
        let mut stmt = self.raw.statement(&query_string);
        if !is_safe_to_cache {
            stmt.exclude_from_cache();
        }
        let stmt = stmt.build().map_err(ErrorHelper::from)?;
        let mut bind_collector = OracleBindCollector::default();
        query.collect_binds(&mut bind_collector, &mut (), &Oracle)?;
        callback(stmt, bind_collector)
    }

    fn load_from_is_returning<ST>(
        mut stmt: oracle::Statement,
        bind_collector: bind_collector::OracleBindCollector,
    ) -> QueryResult<RowIter>
    where
        Oracle: QueryMetadata<ST>,
    {
        let mut binds = bind_collector
            .binds
            .iter()
            .map(|(n, b)| (n as &str, &**b))
            .collect::<Vec<_>>();

        let return_count = stmt.bind_count() - binds.len();
        let mut metadata: Vec<Option<crate::oracle::types::OciTypeMetadata>> = Vec::new();
        Oracle::row_metadata(&mut (), &mut metadata);
        debug_assert!(metadata.len() == return_count);
        let other_binds = metadata
            .iter()
            .enumerate()
            .map(|(id, m)| {
                let m = m.as_ref().expect("Returning queries need to be typed");
                let tpe = match m.tpe {
                    OciDataType::Bool => oracle::sql_type::OracleType::Number(5, 0),
                    OciDataType::SmallInt => oracle::sql_type::OracleType::Number(5, 0),
                    OciDataType::Integer => oracle::sql_type::OracleType::Number(10, 0),
                    OciDataType::BigInt => oracle::sql_type::OracleType::Number(19, 0),
                    OciDataType::Float => oracle::sql_type::OracleType::Number(19, 0),
                    OciDataType::Double => oracle::sql_type::OracleType::BinaryDouble,
                    OciDataType::Text => oracle::sql_type::OracleType::NVarchar2(2_000_000),
                    OciDataType::Binary => oracle::sql_type::OracleType::Raw(2_000_000),
                    OciDataType::Date => oracle::sql_type::OracleType::Timestamp(0),
                    OciDataType::Time => oracle::sql_type::OracleType::Timestamp(0),
                    OciDataType::Timestamp => oracle::sql_type::OracleType::Timestamp(0),
                };
                (format!("out{}", id), tpe)
            })
            .collect::<Vec<_>>();

        for (n, b) in &other_binds {
            binds.push((n, &*b));
        }

        stmt.execute_named(&binds).map_err(ErrorHelper::from)?;

        let row_count = stmt.row_count().map_err(ErrorHelper::from)?;

        let mut data = (0..row_count)
            .map(|_| Vec::with_capacity(metadata.len()))
            .collect::<Vec<_>>();

        for (idx, m) in metadata.iter().enumerate() {
            let idx = &format!("out{}", idx) as &str;
            match m.as_ref().unwrap().tpe {
                OciDataType::Bool => {
                    for (idx, v) in (stmt.returned_values::<_, Option<i16>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::SmallInt(v),
                        }));
                    }
                }
                OciDataType::SmallInt => {
                    for (idx, v) in (stmt.returned_values::<_, Option<i16>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::SmallInt(v),
                        }));
                    }
                }
                OciDataType::Integer => {
                    for (idx, v) in (stmt.returned_values::<_, Option<i32>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Integer(v),
                        }));
                    }
                }
                OciDataType::BigInt => {
                    for (idx, v) in (stmt.returned_values::<_, Option<i64>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::BigInt(v),
                        }));
                    }
                }
                OciDataType::Float => {
                    for (idx, v) in (stmt.returned_values::<_, Option<f32>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Float(v),
                        }));
                    }
                }
                OciDataType::Double => {
                    for (idx, v) in (stmt.returned_values::<_, Option<f64>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Double(v),
                        }));
                    }
                }
                OciDataType::Text => {
                    for (idx, v) in stmt
                        .returned_values::<_, Option<String>>(idx)
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Text(v),
                        }));
                    }
                }
                OciDataType::Binary => {
                    for (idx, v) in (stmt.returned_values::<_, Option<Vec<u8>>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Binary(v),
                        }));
                    }
                }
                #[cfg(feature = "chrono")]
                OciDataType::Date => {
                    for (idx, v) in (stmt.returned_values::<_, Option<chrono_time::NaiveDate>>(idx))
                        .map_err(ErrorHelper::from)?
                        .into_iter()
                        .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Date(v),
                        }));
                    }
                }
                #[cfg(feature = "chrono")]
                OciDataType::Timestamp => {
                    for (idx, v) in (stmt
                        .returned_values::<_, Option<chrono_time::NaiveDateTime>>(idx))
                    .map_err(ErrorHelper::from)?
                    .into_iter()
                    .enumerate()
                    {
                        data[idx].push(v.map(|v| OracleValue {
                            inner: InnerValue::Timestamp(v),
                        }));
                    }
                }
                _ => unimplemented!(),
            }
        }
        let data = data.into_iter().map(OciRow::new_from_value).collect();
        Ok(RowIter::new(data))
    }

    pub(crate) fn batch_insert<T, V, QId, Op, const STATIC_QUERY_ID: bool>(
        &mut self,
        stmt: InsertStatement<T, BatchInsert<Vec<ValuesClause<V, T>>, T, QId, STATIC_QUERY_ID>, Op>,
    ) -> diesel::QueryResult<usize>
    where
        T: Table + Copy + QueryId + 'static,
        T::FromClause: QueryFragment<Oracle>,
        Op: Copy + QueryId + QueryFragment<Oracle>,
        V: InsertValues<Oracle, T> + CanInsertInSingleQuery<Oracle> + QueryId,
    {
        let record_count = stmt.records.values.len();
        let mut record_iter = stmt.records.values.iter().map(|records| {
            InsertStatement::new(stmt.target, records, stmt.operator, stmt.returning)
        });

        if let Some(first_record) = record_iter.next() {
            self.instrumentation
                .on_connection_event(InstrumentationEvent::start_query(&diesel::debug_query(
                    &first_record,
                )));
            let res = self.inner_batch_insert(&first_record, record_count, record_iter);
            self.instrumentation
                .on_connection_event(InstrumentationEvent::finish_query(
                    &diesel::debug_query(&first_record),
                    res.as_ref().err(),
                ));
            res
        } else {
            Ok(0)
        }
    }

    fn inner_batch_insert<Q>(
        &mut self,
        first_record: &Q,
        record_count: usize,
        record_iter: impl Iterator<Item = Q>,
    ) -> Result<usize, Error>
    where
        Q: QueryFragment<Oracle>,
    {
        let mut qb = OciQueryBuilder::default();
        first_record.to_sql(&mut qb, &Oracle)?;
        let query_string = qb.finish();
        let mut batch = self
            .raw
            .batch(&query_string, record_count)
            .build()
            .map_err(ErrorHelper::from)?;

        bind_params_to_batch(first_record, &mut batch)?;
        for record in record_iter {
            bind_params_to_batch(&record, &mut batch)?;
        }
        batch.execute().map_err(ErrorHelper::from)?;
        Ok(record_count)
    }

    fn inner_establish(database_url: &str) -> Result<oracle::Connection, ConnectionError> {
        let url = url::Url::parse(database_url)
            .map_err(|_| ConnectionError::InvalidConnectionUrl("Invalid url".into()))?;
        if url.scheme() != "oracle" {
            return Err(ConnectionError::InvalidConnectionUrl(format!(
                "Got a unsupported url scheme: {}",
                url.scheme()
            )));
        }
        let user = url.username();
        if user.is_empty() {
            return Err(ConnectionError::InvalidConnectionUrl(
                "Username not set".into(),
            ));
        }
        let user = match percent_encoding::percent_decode_str(url.username()).decode_utf8() {
            Ok(username) => username,
            Err(_e) => {
                return Err(ConnectionError::InvalidConnectionUrl(
                    "Username could not be percent decoded".into(),
                ))
            }
        };
        let password = url
            .password()
            .ok_or_else(|| ConnectionError::InvalidConnectionUrl("Password not set".into()))?;
        let host = url
            .host_str()
            .ok_or_else(|| ConnectionError::InvalidConnectionUrl("Hostname not set".into()))?;
        let port = url.port();
        let path = url.path();
        let mut url = host.to_owned();
        if let Some(port) = port {
            write!(url, ":{}", port).expect("Write to string does not fail");
        }
        url += path;
        let mut raw = oracle::Connection::connect(user, password, url)
            .map_err(ErrorHelper::from)
            .map_err(|e| ConnectionError::CouldntSetupConfiguration(e.into()))?;
        raw.set_autocommit(true);
        Ok(raw)
    }

    fn inner_executing_returning_count<T>(&mut self, source: &T) -> Result<usize, Error>
    where
        T: QueryFragment<Oracle> + QueryId,
    {
        let mut qb = OciQueryBuilder::default();

        source.to_sql(&mut qb, &Oracle)?;

        let conn = &self.raw;
        let sql = qb.finish();
        let mut stmt = conn.statement(&sql);
        if !source.is_safe_to_cache_prepared(&Oracle)? {
            stmt.exclude_from_cache();
        }
        let mut stmt = stmt.build().map_err(ErrorHelper::from)?;
        let mut bind_collector = OracleBindCollector::default();

        source.collect_binds(&mut bind_collector, &mut (), &Oracle)?;
        let binds = bind_collector
            .binds
            .iter()
            .map(|(n, b)| -> (&str, &dyn oracle::sql_type::ToSql) {
                (n as &str, std::ops::Deref::deref(b))
            })
            .collect::<Vec<_>>();

        if stmt.is_query() {
            stmt.query_named(&binds).map_err(ErrorHelper::from)?;
        } else {
            stmt.execute_named(&binds).map_err(ErrorHelper::from)?;
        }

        Ok(stmt.row_count().map_err(ErrorHelper::from)? as usize)
    }
}

fn bind_params_to_batch(
    record: &impl QueryFragment<Oracle>,
    batch: &mut oracle::Batch,
) -> Result<(), Error> {
    let mut bind_collector = OracleBindCollector::default();
    record.collect_binds(&mut bind_collector, &mut (), &Oracle)?;
    let binds = bind_collector
        .binds
        .iter()
        .map(|(n, b)| (n as &str, &**b))
        .collect::<Vec<_>>();
    batch.append_row_named(&binds).map_err(ErrorHelper::from)?;
    Ok(())
}

impl Drop for OciConnection {
    fn drop(&mut self) {}
}

#[cfg(feature = "r2d2")]
use diesel::r2d2::R2D2Connection;

#[cfg(feature = "r2d2")]
impl R2D2Connection for OciConnection {
    fn ping(&mut self) -> QueryResult<()> {
        diesel::sql_query("SELECT 1 FROM DUAL")
            .execute(self)
            .map(|_| ())
    }

    fn is_broken(&mut self) -> bool {
        // consider this connection as broken
        // if the transaction manager is in an error state,
        // contains an open transaction or the connection itself
        // reports an open transaction
        matches!(self.transaction_manager.status.transaction_depth(), Err(_))
            || (matches!(
                self.transaction_manager.status.transaction_depth(),
                Ok(Some(_))
            ) || self
                .raw
                .oci_attr::<oracle::oci_attr::TransactionInProgress>()
                .unwrap_or(true))
                && !self.transaction_manager.is_test_transaction
    }
}
