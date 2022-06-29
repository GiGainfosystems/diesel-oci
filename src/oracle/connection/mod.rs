use std::marker::PhantomData;
use std::rc::Rc;

use crate::oracle::connection::stmt_iter::RowIter;

use self::bind_collector::OracleBindCollector;
use self::row::OciRow;
use self::transaction::OCITransactionManager;
use super::backend::Oracle;
use super::query_builder::OciQueryBuilder;
use super::OciDataType;
use diesel::connection::ConnectionGatWorkaround;
use diesel::connection::{Connection, SimpleConnection, TransactionManager};
use diesel::deserialize::FromSql;
use diesel::expression::QueryMetadata;
use diesel::migration::MigrationConnection;
use diesel::query_builder::QueryId;
use diesel::query_builder::{AsQuery, QueryBuilder, QueryFragment};
use diesel::result::*;
use diesel::sql_types::HasSqlType;
use diesel::RunQueryDsl;

mod oracle_value;
pub(crate) use self::oracle_value::InnerValue;
pub use self::oracle_value::OracleValue;

pub(crate) mod bind_collector;
mod row;
mod stmt_iter;
mod transaction;

pub struct OciConnection {
    raw: oracle::Connection,
    transaction_manager: OCITransactionManager,
}

struct ErrorHelper(oracle::Error);

impl From<oracle::Error> for ErrorHelper {
    fn from(e: oracle::Error) -> Self {
        Self(e)
    }
}

impl From<ErrorHelper> for diesel::result::Error {
    fn from(ErrorHelper(e): ErrorHelper) -> Self {
        match e {
            oracle::Error::OciError(_) => {
                // TODO: better handling here
                diesel::result::Error::QueryBuilderError(e.into())
            }
            oracle::Error::DpiError(_) => {
                // TODO: better handling here
                diesel::result::Error::QueryBuilderError(e.into())
            }
            oracle::Error::NullValue => diesel::result::Error::DeserializationError(
                diesel::result::UnexpectedNullError.into(),
            ),
            oracle::Error::ParseError(e) => diesel::result::Error::SerializationError(e),
            oracle::Error::OutOfRange(e) => diesel::result::Error::DeserializationError(e.into()),
            oracle::Error::InvalidTypeConversion(from, to) => {
                diesel::result::Error::DeserializationError(
                    format!("Cannot convert from {} to {}", from, to).into(),
                )
            }
            oracle::Error::InvalidBindIndex(e) => diesel::result::Error::QueryBuilderError(
                format!("Invalid bind with index: {}", e).into(),
            ),
            oracle::Error::InvalidBindName(e) => diesel::result::Error::QueryBuilderError(
                format!("Invalid bind with name: {}", e).into(),
            ),
            oracle::Error::InvalidColumnIndex(_) => diesel::result::Error::DeserializationError(
                diesel::result::UnexpectedEndOfRow.into(),
            ),
            oracle::Error::InvalidColumnName(c) => diesel::result::Error::DeserializationError(
                format!("Invalid column name: {}", c).into(),
            ),
            oracle::Error::InvalidAttributeName(e) => diesel::result::Error::QueryBuilderError(
                format!("Invalid attribute name: {}", e).into(),
            ),
            oracle::Error::InvalidOperation(e) => {
                diesel::result::Error::QueryBuilderError(format!("Invalid operation: {}", e).into())
            }
            oracle::Error::UninitializedBindValue => {
                diesel::result::Error::QueryBuilderError("Uninitialized bind value".into())
            }
            oracle::Error::NoDataFound => diesel::result::Error::NotFound,
            oracle::Error::InternalError(e) => diesel::result::Error::QueryBuilderError(e.into()),
            oracle::Error::BatchErrors(_e) => {
                diesel::result::Error::QueryBuilderError("Batch error".into())
            }
        }
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
        self.raw.execute(&query, &[]).map_err(ErrorHelper::from)?;
        Ok(())
    }
}

impl<'conn, 'query> ConnectionGatWorkaround<'conn, 'query, Oracle> for OciConnection {
    type Cursor = RowIter;
    type Row = OciRow;
}

impl Connection for OciConnection {
    type Backend = Oracle;
    type TransactionManager = OCITransactionManager;

    /// Establishes a new connection to the database at the given URL. The URL
    /// should be a valid connection string for a given backend. See the
    /// documentation for the specific backend for specifics.
    fn establish(database_url: &str) -> ConnectionResult<Self> {
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
            url += &format!(":{}", port);
        }
        url += path;

        let mut raw = oracle::Connection::connect(user, password, url)
            .map_err(ErrorHelper::from)
            .map_err(|e| ConnectionError::CouldntSetupConfiguration(e.into()))?;

        raw.set_autocommit(true);

        Ok(Self {
            raw,
            transaction_manager: OCITransactionManager::new(),
        })
    }

    #[doc(hidden)]
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let mut qb = OciQueryBuilder::default();

        source.to_sql(&mut qb, &Oracle)?;

        let conn = &self.raw;

        let mut stmt = conn.prepare(&qb.finish(), &[]).map_err(ErrorHelper::from)?;
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

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<<Self as ConnectionGatWorkaround<'conn, 'query, Oracle>>::Cursor>
    where
        T: AsQuery,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
        Self::Backend: QueryMetadata<T::SqlType>,
    {
        let query = source.as_query();

        self.with_prepared_statement(query, |mut stmt, bind_collector| {
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
        })
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.transaction_manager
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
        query: T,
        callback: impl FnOnce(oracle::Statement<'conn>, OracleBindCollector) -> QueryResult<R>,
    ) -> Result<R, Error>
    where
        T: QueryFragment<Oracle> + QueryId + 'query,
    {
        let mut qb = OciQueryBuilder::default();
        query.to_sql(&mut qb, &Oracle)?;
        let conn = &self.raw;
        let stmt = conn
            .statement(&qb.finish())
            .build()
            .map_err(ErrorHelper::from)?;
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
            binds.push((&n, &*b));
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
        let data = data
            .into_iter()
            .map(|row| OciRow::new_from_value(row))
            .collect();
        Ok(RowIter::new(data))
    }
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
        match self.transaction_manager.status.transaction_depth() {
            // all transactions are closed
            // so we don't consider this connection broken
            Ok(None) => false,
            // The transaction manager is in an error state
            // or contains an open transaction
            // Therefore we consider this connection broken
            Err(_) | Ok(Some(_)) => true,
        }
    }
}
