//! Provides types and functions releated to working with the Oracle backend
pub(crate) mod backend;
pub(crate) mod connection;
pub(crate) mod insertable;
pub(crate) mod query_builder;
pub(crate) mod query_dsl;
pub(crate) mod types;

#[doc(inline)]
pub use self::backend::Oracle;
#[doc(inline)]
pub use self::connection::OciConnection;
#[doc(inline)]
pub use self::connection::OracleValue;
#[doc(inline)]
pub use self::types::OciDataType;
