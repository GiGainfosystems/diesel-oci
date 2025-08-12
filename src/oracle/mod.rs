//! Provides types and functions related to working with Oracle
//!
//! This module contains a diesel backend and connection implementation for
//! Oracle databases

pub(crate) mod backend;
pub(crate) mod connection;
pub(crate) mod insertable;
/// Oracle specific query builder implementation
pub mod query_builder;
pub(crate) mod query_dsl;
pub(crate) mod types;

pub use self::backend::Oracle;
pub use self::connection::{OciConnection, OracleValue};
pub use self::types::{
    OciDataType, OciIntervalDS, OciIntervalYM, OciTypeMetadata, SqlIntervalDS, SqlIntervalYM,
};
