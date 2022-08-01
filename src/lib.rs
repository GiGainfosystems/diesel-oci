//! This crate provides a diesel backend and connection implementation for oracle
//!
//! To use this crate you need to depend on diesel and diesel_oci at the same time.
//!
//! ```
//! use diesel::prelude::*;
//! use diesel_oci::OciConnection;
//!
//! diesel::table! {
//!    users(id) {
//!        id -> Integer,
//!        name -> Text,
//!    }
//! }
//!
//! let mut conn = OciConnection::establish("oracle://…")?;
//!
//! let users_from_db = users::table.load::<(i32, String)>(&mut conn)?;
//! ```
//!
//! For using diesel refer to the [diesel documentation].
//!
//! [diesel documentation](https://docs.diesel.rs/2.0.x/diesel/index.html)
#![warn(missing_docs)]

pub mod oracle;

#[doc(inline)]
pub use self::oracle::connection::OciConnection;

#[cfg(test)]
mod test;

#[cfg(test)]
mod logger;
