use std::marker::PhantomData;
use diesel::deserialize::{Queryable, FromSqlRow};
use diesel::sql_types::HasSqlType;
use diesel::result::QueryResult;
use diesel::result::Error::DeserializationError;
use oci_sys as ffi;

use super::stmt::Statement;
use super::super::backend::Oracle;
use super::row::OciRow;
use super::super::types::OCIDataType;


pub struct Field {
    inner: *mut ffi::OCIDefine,
    buffer: Vec<u8>,
    null_indicator: Box<i16>,
    typ: OCIDataType,
}

impl Field {
    pub fn new(raw: *mut ffi::OCIDefine, buffer: Vec<u8>, indicator: Box<i16>, typ: OCIDataType) -> Field {
        Field {
            inner: raw,
            buffer: buffer,
            null_indicator: indicator,
            typ,
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
        unsafe {
            let status = ffi::OCIStmtFetch2(self.stmt.inner_statement,
                               self.stmt.connection.env.error_handle,
                               1,
                               ffi::OCI_FETCH_NEXT as u16,
                               0,
                               ffi::OCI_DEFAULT);
            if let Some(err) = Statement::check_error(self.stmt.connection.env.error_handle, status) {
                return Some(Err(err));
            }
            if status as u32 == ffi::OCI_NO_DATA {
                return None;
            }
        }

        self.current_row += 1;
        let null_indicators = self.results.iter().map(|r| r.is_null()).collect();
        let mut row = OciRow::new(self.results
                                      .iter_mut()
                                      .map(|r: &mut Field| {
                                          &r.buffer[..]
                                      })
                                      .collect::<Vec<&[u8]>>(),
                                  null_indicators);
        let value = T::Row::build_from_row(&mut row)
            .map(T::build)
            .map_err(DeserializationError);
        Some(value)

    }
}
