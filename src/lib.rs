#![feature(exclusive_range_pattern)]
#[macro_use]
extern crate diesel;
extern crate bigdecimal;
extern crate byteorder;
extern crate libc;
#[macro_use]
extern crate log;
extern crate oci_sys;
pub mod oracle;

extern crate num;
#[macro_use]
extern crate num_derive;

#[cfg(test)]
mod test;

#[cfg(test)]
mod logger;
