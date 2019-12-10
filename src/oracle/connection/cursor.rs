use diesel::deserialize::{FromSqlRow, Queryable, QueryableByName};
use diesel::result::Error::DeserializationError;
use diesel::result::QueryResult;
use diesel::sql_types::HasSqlType;
use oci_sys as ffi;
use std::collections::HashMap;
use std::marker::PhantomData;

use super::row::{NamedOciRow, OciRow};
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
    Oracle: HasSqlType<ST>,
    T: Queryable<ST, Oracle>,
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
        let mut row = OciRow::new(&self.results);
        let value = T::Row::build_from_row(&mut row)
            .map(T::build)
            .map_err(DeserializationError);
        Some(value)
    }
}

pub struct NamedCursor<'a> {
    stmt: &'a Statement,
    results: Vec<Field>,
    lut: HashMap<String, usize>,
}

impl<'a> NamedCursor<'a> {
    pub fn new(stmt: &'a Statement, binds: Vec<Field>) -> NamedCursor<'a> {
        let lut = binds
            .iter()
            .enumerate()
            .map(|(i, b)| (b.name.clone(), i))
            .collect();
        NamedCursor {
            stmt,
            results: binds,
            lut,
        }
    }

    pub fn collect<T>(&mut self) -> QueryResult<Vec<T>>
    where
        T: QueryableByName<Oracle>,
    {
        let mut status = ffi::OCI_SUCCESS as i32;
        let mut ret = Vec::new();
        while status as u32 != ffi::OCI_NO_DATA {
            unsafe {
                status = ffi::OCIStmtFetch2(
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
                    return Err(err);
                }
                if status as u32 == ffi::OCI_NO_DATA {
                    break;
                }
            }
            let null_indicators = self.results.iter().map(|r| r.is_null()).collect();
            let row = NamedOciRow::new(
                self.results
                    .iter_mut()
                    .map(|r: &mut Field| &r.buffer[..])
                    .collect::<Vec<&[u8]>>(),
                null_indicators,
                &self.lut,
            );

            ret.push(T::build(&row).map_err(DeserializationError)?);
        }

        Ok(ret)
    }
}
