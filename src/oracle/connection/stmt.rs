use diesel::result::*;
use oci_sys as ffi;
use libc;
use std::ptr;
use std::ffi::CString;
use super::raw::RawConnection;
use super::cursor::{Cursor, Field};
use oracle::types::OCIDataType;
use std::rc::Rc;


pub struct Statement {
    pub connection: Rc<RawConnection>,
    pub inner_statement: *mut ffi::OCIStmt,
    bind_index: libc::c_uint,
    is_select: bool,
}

impl Statement {
    pub fn prepare(raw_connection: &Rc<RawConnection>, sql: &str) -> QueryResult<Self> {
        println!("prepare statment {}", sql);
        let stmt = unsafe {
            let mut stmt: *mut ffi::OCIStmt = ptr::null_mut();
            ffi::OCIStmtPrepare2(raw_connection.service_handle,
                                 &mut stmt,
                                 raw_connection.env.error_handle,
                                 sql.as_ptr(),
                                 sql.len() as u32,
                                 ptr::null(),
                                 0,
                                 ffi::OCI_NTV_SYNTAX,
                                 ffi::OCI_DEFAULT);
            stmt
        };
        Ok(Statement {
               connection: raw_connection.clone(),
               inner_statement: stmt,
               bind_index: 0,
               is_select: sql.contains("SELECT"),
           })
    }

    pub fn run(&self) -> QueryResult<()> {
        let iters = if self.is_select { 0 } else { 1 };
        unsafe {
            ffi::OCIStmtExecute(self.connection.service_handle,
                                self.inner_statement,
                                self.connection.env.error_handle,
                                iters,
                                0,
                                ptr::null(),
                                ptr::null_mut(),
                                ffi::OCI_DEFAULT);
        }
        Ok(())
    }

    pub fn get_affected_rows(&self) -> QueryResult<usize> {
        let mut affected_rows: u32 = 0;
        unsafe {
            ffi::OCIAttrGet(self.inner_statement as *const _,
                            ffi::OCI_HTYPE_STMT,
                            (&mut affected_rows as *mut u32) as *mut _,
                            &mut 0,
                            ffi::OCI_ATTR_ROW_COUNT,
                            self.connection.env.error_handle);
        }
        Ok(affected_rows as usize)
    }

    pub fn run_with_cursor<ST, T>(&self) -> QueryResult<Cursor<ST, T>> {
        try!(self.run());
        let mut col_count: u32 = 0;
        unsafe {
            ffi::OCIAttrGet(self.inner_statement as *const _,
                            ffi::OCI_HTYPE_STMT,
                            (&mut col_count as *mut u32) as *mut _,
                            &mut 0,
                            ffi::OCI_ATTR_PARAM_COUNT,
                            self.connection.env.error_handle);
        }
        let mut fields = Vec::<Field>::with_capacity(col_count as usize);
        for i in 0..col_count as usize {
            let col_number = i + 1;
            let col_handle = unsafe {
                let mut parameter_descriptor: *mut ffi::OCIStmt = ptr::null_mut();
                ffi::OCIParamGet(self.inner_statement as *const _,
                                 ffi::OCI_HTYPE_STMT,
                                 self.connection.env.error_handle,
                                 (&mut parameter_descriptor as *mut *mut ffi::OCIStmt) as *mut _,
                                 col_number as u32);
                parameter_descriptor
            };

            let mut tpe: u32 = 0;
            let mut len_type: u32 = 0;
            let mut tpe_size: u32 = 0;
            unsafe {
                ffi::OCIAttrGet(col_handle as *mut _,
                                ffi::OCI_DTYPE_PARAM,
                                (&mut tpe as *mut u32) as *mut _,
                                &mut 0,
                                ffi::OCI_ATTR_DATA_TYPE,
                                self.connection.env.error_handle);
                ffi::OCIAttrGet(col_handle as *mut _,
                                ffi::OCI_DTYPE_PARAM,
                                (&mut len_type as *mut u32) as *mut _,
                                &mut 0,
                                ffi::OCI_ATTR_CHAR_USED,
                                self.connection.env.error_handle);
                let attr = if len_type != 0 {
                    ffi::OCI_ATTR_CHAR_SIZE
                } else {
                    ffi::OCI_ATTR_DATA_SIZE
                };
                ffi::OCIAttrGet(col_handle as *mut _,
                                ffi::OCI_DTYPE_PARAM,
                                (&mut tpe_size as *mut u32) as *mut _,
                                &mut 0,
                                attr,
                                self.connection.env.error_handle);
            }

            if let Some(tpe) = ::oracle::types::OCIDataType::from_raw(tpe) {
                use oracle::types::OCIDataType;
                let (tpe, tpe_size) = match tpe {
                    // Maybe we should check the exact size of the
                    // numeric and then do this conversion only in some cases
                    OCIDataType::Numeric => (OCIDataType::BDouble, 8),
                    OCIDataType::IBDouble => (OCIDataType::BDouble, 8),
                    OCIDataType::IBFloat => (OCIDataType::BFloat, 4),
                    OCIDataType::Char => (OCIDataType::String, tpe_size),
                    OCIDataType::Timestamp => (OCIDataType::InternDate, 7),
                    _ => (tpe, tpe_size),

                };
                info!("{} -> {:?}", tpe_size, tpe);
                let mut v = Vec::with_capacity(tpe_size as usize);
                v.resize(tpe_size as usize, 0);
                let mut null_indicator: Box<i16> = Box::new(0);
                let def = unsafe {
                    let mut def = ptr::null_mut();
                    ffi::OCIDefineByPos(self.inner_statement,
                                        &mut def,
                                        self.connection.env.error_handle,
                                        col_number as u32,
                                        v.as_ptr() as *mut _,
                                        v.len() as i32,
                                        tpe as libc::c_ushort,
                                        (&mut *null_indicator as *mut i16) as *mut _,
                                        ptr::null_mut(),
                                        ptr::null_mut(),
                                        ffi::OCI_DEFAULT);
                    def

                };
                fields.push(Field::new(def, v, null_indicator));
            } else {
                return Err(Error::DatabaseError(DatabaseErrorKind::__Unknown,
                                                Box::new(format!("unknown type {}", tpe))));
            }



        }
        Ok(Cursor::new(self, fields))
    }

    pub fn bind(&mut self, tpe: OCIDataType, value: Option<Vec<u8>>) -> QueryResult<()> {
        self.bind_index += 1;
        let (buf, size): (*mut _, i32) = match (tpe, value) {
            (_, None) => (ptr::null_mut(), 0),
            (OCIDataType::OCIString, Some(value)) => {
                let s = CString::new(::std::str::from_utf8(&value).unwrap()).unwrap();
                (s.as_ptr() as *mut _, s.as_bytes().len() as i32)
            }
            (_, Some(value)) => (value.as_ptr() as *mut _, value.len() as i32),
        };
        unsafe {
            ffi::OCIBindByPos(self.inner_statement,
                              &mut ptr::null_mut(),
                              self.connection.env.error_handle,
                              self.bind_index,
                              buf,
                              size,
                              tpe as libc::c_ushort,
                              ptr::null_mut(),
                              ptr::null_mut(),
                              ptr::null_mut(),
                              0,
                              ptr::null_mut(),
                              ffi::OCI_DEFAULT);
        }
        Ok(())

    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            ffi::OCIStmtRelease(self.inner_statement,
                                self.connection.env.error_handle,
                                ptr::null(),
                                0,
                                ffi::OCI_DEFAULT);
        }
    }
}
