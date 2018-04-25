use std::marker::PhantomData;
use diesel::deserialize::{Queryable, FromSqlRow};
use diesel::sql_types::HasSqlType;
use diesel::result::QueryResult;
use diesel::result::Error::DeserializationError;
use oci_sys as ffi;

use super::stmt::Statement;
use super::super::backend::Oracle;
use super::row::OciRow;


pub struct Field {
    inner: *mut ffi::OCIDefine,
    buffer: Vec<u8>,
    null_indicator: Box<i16>,
}

impl Field {
    pub fn new(raw: *mut ffi::OCIDefine, buffer: Vec<u8>, indicator: Box<i16>) -> Field {
        Field {
            inner: raw,
            buffer: buffer,
            null_indicator: indicator,
        }
    }

    pub fn is_null(&self) -> bool {
        *self.null_indicator == -1
    }
}

impl Drop for Field {
    fn drop(&mut self) {
        unsafe {
            ffi::OCIHandleFree(self.inner as *mut _, ffi::OCI_HTYPE_DEFINE);
        }
    }
}

pub struct Cursor<'a, ST, T> {
    stmt: &'a Statement,
    _marker: PhantomData<(ST, T)>,
    results: Vec<Field>,
    current_row: u32,
}

impl<'a, ST, T> Cursor<'a, ST, T> {
    pub fn new(stmt: &'a Statement, binds: Vec<Field>) -> Cursor<'a, ST, T> {
        Cursor {
            stmt: stmt,
            _marker: PhantomData,
            results: binds,
            current_row: 0,
        }
    }
}

impl<'a, ST, T> Iterator for Cursor<'a, ST, T>
    where Oracle: HasSqlType<ST>,
          T: Queryable<ST, Oracle>
{
    type Item = QueryResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        //        println!("before fetch");
        unsafe {
            ffi::OCIStmtFetch2(self.stmt.inner_statement,
                               self.stmt.connection.env.error_handle,
                               1,
                               ffi::OCI_FETCH_NEXT as u16,
                               0,
                               ffi::OCI_DEFAULT);
        }

        //        println!("after fetch");
        self.current_row += 1;
        let mut row = OciRow::new(self.results
                                      .iter()
                                      .map(|r| &r.buffer[..])
                                      .collect::<Vec<&[u8]>>(),
                                  self.results.iter().map(|r| r.is_null()).collect());
        let value = T::Row::build_from_row(&mut row)
            .map(T::build)
            .map_err(DeserializationError);
        Some(value)

    }
}
