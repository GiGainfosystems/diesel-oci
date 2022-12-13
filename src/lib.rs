#![warn(missing_docs)]
//! The Oracle Diesel Backend
//!
//! This crate only implements an oracle backend and connection for Diesel.
//! To use diesel features, you must import it.
//!
//! ```no_run
//! // Import diesel
//! use diesel::prelude::*;
//! // Import the oracle connection type
//! use diesel_oci::OciConnection;
//!
//! table! {
//!    users {
//!        id -> Integer,
//!        name -> Text,
//!    }
//! }
//!
//! # fn run_test() -> Result<(), Box<dyn std::error::Error>> {
//! // establish a connection
//! let mut conn = OciConnection::establish("oracle://user:secret@127.0.0.1/MY_DB")?;
//!
//! // use the connection similary to any other diesel connection
//! let res = users::table.load::<(i32, String)>(&mut conn)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Feature flags
//!
//! * `chrono` Enables support for the `chrono` crate
//! * `r2d2` Enables support for r2d2 connection pooling
//! * `dynamic-schema` Enables support for diesel-dynamic-schema

pub mod oracle;

#[doc(inline)]
pub use crate::oracle::*;

#[cfg(test)]
mod test;

#[cfg(test)]
mod logger;
