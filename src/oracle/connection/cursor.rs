use diesel::deserialize::FromSqlRow;
use diesel::result::Error::DeserializationError;
use diesel::result::QueryResult;
use oci_sys as ffi;
use std::marker::PhantomData;

use super::row::OciRow;
use super::stmt::Statement;
use oracle::backend::Oracle;
use oracle::types::OciDataType;

pub struct Field {
    inner: *mut ffi::OCIDefine,
    buffer: Vec<u8>,
    null_indicator: Box<i16>,
    #[allow(dead_code)]
    typ: OciDataType,
    name: String,
}

impl Field {
    pub fn new(
        raw: *mut ffi::OCIDefine,
        buffer: Vec<u8>,
        indicator: Box<i16>,
        typ: OciDataType,
        name: String,
    ) -> Field {
        Field {
            inner: raw,
            buffer,
            null_indicator: indicator,
            typ,
            name,
        }
    }

    pub fn is_null(&self) -> bool {
        *self.null_indicator == -1
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn datatype(&self) -> OciDataType {
        self.typ
    }
}

impl Drop for Field {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                ffi::OCIHandleFree(self.inner as *mut _, ffi::OCI_HTYPE_DEFINE);
            }
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
            stmt,
            _marker: PhantomData,
            results: binds,
            current_row: 0,
        }
    }
}

impl<'a, ST, T> Iterator for Cursor<'a, ST, T>
where
    T: FromSqlRow<ST, Oracle>,
{
    type Item = QueryResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.stmt.is_returning {
            unsafe {
                let status = ffi::OCIStmtFetch2(
                    self.stmt.inner_statement,
                    self.stmt.connection.env.error_handle,
                    1,
                    ffi::OCI_FETCH_NEXT as u16,
                    0,
                    ffi::OCI_DEFAULT,
                );
                if let Some(err) =
                    Statement::check_error(self.stmt.connection.env.error_handle, status).err()
                {
                    debug!("{:?}", self.stmt.mysql);
                    return Some(Err(err));
                }
                if status as u32 == ffi::OCI_NO_DATA {
                    return None;
                }
            }
        } else if self.current_row > 0 {
            return None;
        }

        self.current_row += 1;
        let row = OciRow::new(&self.results);
        let value = T::build_from_row(&row).map_err(DeserializationError);
        Some(value)
    }
}
