use diesel::result::*;
use diesel::result::Error;
use oci_sys as ffi;
use libc;
use std::ptr;
use std::ffi::{CString, CStr};
use super::raw::RawConnection;
use super::cursor::{Cursor, Field};
use oracle::types::OCIDataType;
use std::rc::Rc;
use std::os::raw::{c_char, c_void, c_int};


pub struct Statement {
    pub connection: Rc<RawConnection>,
    pub inner_statement: *mut ffi::OCIStmt,
    bind_index: libc::c_uint,
    is_select: bool,
    /// TODO: resolve double frees
    // we may introduce double free's here
    // c.f. https://docs.oracle.com/cd/B10501_01/appdev.920/a96584/oci15r30.htm
    // [...] An address of a bind handle which is implicitly allocated by this call. The bind
    // handle maintains all the bind information for this particular input value. The handle is
    // freed implicitly when the statement handle is deallocated. [...]
    binds: Vec<Box<ffi::OCIBind>>,
    buffers: Vec<Box<c_void>>,
    sizes: Vec<i32>,
}

const IS_NULL: ffi::OCIInd = -1;
const IS_NOT_NULL: ffi::OCIInd = 0;

impl Statement {
    pub fn prepare(raw_connection: &Rc<RawConnection>, sql: &str) -> QueryResult<Self> {
        println!("prepare statement {}", sql);
        let stmt = unsafe {
            let mut stmt: *mut ffi::OCIStmt = ptr::null_mut();
            let status = ffi::OCIStmtPrepare2(raw_connection.service_handle,
                                 &mut stmt,
                                 raw_connection.env.error_handle,
                                 sql.as_ptr(),
                                 sql.len() as u32,
                                 ptr::null(),
                                 0,
                                 ffi::OCI_NTV_SYNTAX,
                                 ffi::OCI_DEFAULT);

            if let Some(err) = Self::check_error(raw_connection.env.error_handle, status) {
                return Err(err);
            }

            // for create statements we need to run OCIStmtPrepare2 twice
            // c.f. https://docs.oracle.com/database/121/LNOCI/oci17msc001.htm#LNOCI17165
            // "To reexecute a DDL statement, you must prepare the statement again using OCIStmtPrepare2()."
            if let Some(u) = sql.to_string().find("CREATE") {
                if u < 10 {
                    let status = ffi::OCIStmtPrepare2(raw_connection.service_handle,
                                                      &mut stmt,
                                                      raw_connection.env.error_handle,
                                                      sql.as_ptr(),
                                                      sql.len() as u32,
                                                      ptr::null(),
                                                      0,
                                                      ffi::OCI_NTV_SYNTAX,
                                                      ffi::OCI_DEFAULT);

                    if let Some(err) = Self::check_error(raw_connection.env.error_handle, status) {
                        return Err(err);
                    }
                }
            }

            stmt
        };
        Ok(Statement {
               connection: raw_connection.clone(),
               inner_statement: stmt,
               bind_index: 0,
               is_select: sql.contains("SELECT"),
                binds: Vec::with_capacity(20),
                buffers: Vec::with_capacity(20),
                sizes: Vec::with_capacity(20),
           })
    }

    pub fn check_error(error_handle: *mut ffi::OCIError, status: i32) -> Option<Error> {

        // c.f. https://github.com/Mingun/rust-oci/blob/2e0f2acb35066b5f510b46826937a634017cda5d/src/ffi/mod.rs#L102

        let mut errbuf: Vec<u8> = Vec::with_capacity(3072);
        let mut errcode : c_int = 0;

        match status {
            ffi::OCI_ERROR => {
                unsafe {
                    let res = ffi::OCIErrorGet(error_handle as *mut c_void,
                                1,
                                ptr::null_mut(),
                                     &mut errcode,
                                     errbuf.as_mut_ptr(),
                                               errbuf.capacity() as u32,
                                ffi::OCI_HTYPE_ERROR);

                    if res == (ffi::OCI_NO_DATA as i32) {
                        return None;
                    }

                    let msg = CStr::from_ptr(errbuf.as_ptr() as *const c_char);
                    errbuf.set_len(msg.to_bytes().len());
                }


                Some(Error::DatabaseError(DatabaseErrorKind::UnableToSendCommand,
                                          Box::new(format!("OCI_ERROR {:?}", String::from_utf8(errbuf).expect("Invalid UTF-8 from OCIErrorGet") ))))
            },
            ffi::OCI_INVALID_HANDLE => Some(Error::DatabaseError(DatabaseErrorKind::UnableToSendCommand,
                                                                 Box::new(format!("OCI_INVALID_HANDLE {:?}", errbuf)))),
            _ => None,
        }
    }

    pub fn run(&self) -> QueryResult<()> {
        let iters = if self.is_select { 0 } else { 1 };
        unsafe {
            let status = ffi::OCIStmtExecute(self.connection.service_handle,
                                self.inner_statement,
                                self.connection.env.error_handle,
                                iters,
                                0,
                                ptr::null(),
                                ptr::null_mut(),
                                ffi::OCI_DEFAULT);
            if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                return Err(err);
            }
        }
        Ok(())
    }

    pub fn get_affected_rows(&self) -> QueryResult<usize> {
        let mut affected_rows: u32 = 0;
        unsafe {
            let status = ffi::OCIAttrGet(self.inner_statement as *const _,
                            ffi::OCI_HTYPE_STMT,
                            (&mut affected_rows as *mut u32) as *mut _,
                            &mut 0,
                            ffi::OCI_ATTR_ROW_COUNT,
                            self.connection.env.error_handle);
            if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                return Err(err);
            }
        }
        Ok(affected_rows as usize)
    }

    pub fn run_with_cursor<ST, T>(&self) -> QueryResult<Cursor<ST, T>> {
        try!(self.run());
        let mut col_count: u32 = 0;
        unsafe {
            let status = ffi::OCIAttrGet(self.inner_statement as *const _,
                            ffi::OCI_HTYPE_STMT,
                            (&mut col_count as *mut u32) as *mut _,
                            &mut 0,
                            ffi::OCI_ATTR_PARAM_COUNT,
                            self.connection.env.error_handle);

            if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                return Err(err);
            }

        }
        let mut fields = Vec::<Field>::with_capacity(col_count as usize);
        for i in 0..col_count as usize {
            let col_number = i + 1;
            let col_handle = unsafe {
                let mut parameter_descriptor: *mut ffi::OCIStmt = ptr::null_mut();
                let status = ffi::OCIParamGet(self.inner_statement as *const _,
                                 ffi::OCI_HTYPE_STMT,
                                 self.connection.env.error_handle,
                                 (&mut parameter_descriptor as *mut *mut ffi::OCIStmt) as *mut _,
                                 col_number as u32);
                if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                    return Err(err);
                }
                parameter_descriptor
            };

            let mut tpe: u32 = 0;
            let mut len_type: u32 = 0;
            let mut tpe_size: u32 = 0;
            unsafe {
                let status = ffi::OCIAttrGet(col_handle as *mut _,
                                ffi::OCI_DTYPE_PARAM,
                                (&mut tpe as *mut u32) as *mut _,
                                &mut 0,
                                ffi::OCI_ATTR_DATA_TYPE,
                                self.connection.env.error_handle);
                if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                    return Err(err);
                }


                match tpe {
                    ffi::SQLT_INT | ffi::SQLT_UIN => {
                        tpe_size = 8;
                    },
                    ffi::SQLT_NUM => {
                        let mut attributesize = 16u32; //sb2
                        let mut scale = 0i8;
                        let mut precision = 0i16;
                        let status = ffi::OCIAttrGet(col_handle as *mut _,
                        ffi::OCI_DTYPE_PARAM,
                         (&mut precision as *mut i16) as *mut _,
                         &mut attributesize as *mut u32,
                        ffi::OCI_ATTR_PRECISION,
                                                 self.connection.env.error_handle);
                        if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                            return Err(err);
                        }
                        let mut attributesize = 8u32; // sb1
                        let status = ffi::OCIAttrGet(col_handle as *mut _,
                        ffi::OCI_DTYPE_PARAM,
                         (&mut scale as *mut i8) as *mut _,
                        &mut attributesize as *mut u32,
                        ffi::OCI_ATTR_SCALE,
                        self.connection.env.error_handle);
                        if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                            return Err(err);
                        }
                        if scale == 0 && precision == 1 {
                            tpe_size = 8;
                        }
                        else
                        {
                            tpe_size = 8;
                        }
                    }
                    ffi::SQLT_FLT | ffi::SQLT_BFLOAT | ffi::SQLT_BDOUBLE | ffi::SQLT_LNG => {
                        tpe_size= 8;

                    },
                    ffi::SQLT_CHR | ffi::SQLT_VCS | ffi::SQLT_LVC | ffi::SQLT_AFC | ffi::SQLT_VST | ffi::SQLT_ODT | ffi::SQLT_DATE | ffi::SQLT_TIMESTAMP | ffi::SQLT_TIMESTAMP_TZ | ffi::SQLT_TIMESTAMP_LTZ => {
                        let mut length = 0u32;
                        let status =  ffi::OCIAttrGet(col_handle as *mut _, ffi::OCI_DTYPE_PARAM, (&mut tpe_size as *mut u32) as *mut _, &mut length as *mut u32, ffi::OCI_ATTR_CHAR_SIZE,
                                                      self.connection.env.error_handle  );
                        if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                            return Err(err);
                        }
                        //tpe_size -= 1;
                    }
                    _ => {
                        panic!("Shit")
                    }

                }
            }

            if let Some(tpe) = ::oracle::types::OCIDataType::from_raw(tpe) {
                let mut v = Vec::with_capacity(tpe_size as usize);
                v.resize(tpe_size as usize, 0);
                let mut null_indicator: Box<i16> = Box::new(0);
                let def = unsafe {
                    let mut def = ptr::null_mut();
                    let status = ffi::OCIDefineByPos(self.inner_statement,
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
                    if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                        return Err(err);
                    }
                    def

                };
                fields.push(Field::new(def, v, null_indicator, tpe));
            } else {
                return Err(Error::DatabaseError(DatabaseErrorKind::__Unknown,
                                                Box::new(format!("unknown type {}", tpe))));
            }



        }
        Ok(Cursor::new(self, fields))
    }

    pub fn bind(&mut self, tpe: OCIDataType, value: Option<Vec<u8>>) -> QueryResult<()> {
        self.bind_index += 1;
        let mut bndp = ptr::null_mut() as *mut ffi::OCIBind;
        let mut is_null: ffi::OCIInd = 0;
        // otherwise the string will be deleted before reaching OCIBindByPos
        let mut s = CString::new("").unwrap();
        let (buf, size): (*mut c_void, i32) = match (tpe, value) {
            (_, None) => {
                is_null = -1;
                (ptr::null_mut(), 0)
            },
            (OCIDataType::OCIString, Some(value)) | (OCIDataType::String, Some(value)) | (OCIDataType::Char, Some(value)) | (OCIDataType::Clob, Some(value)) => {
                s = CString::new(::std::str::from_utf8(&value).unwrap()).unwrap();
                let len = s.as_bytes_with_nul().len();
                (s.into_raw() as *mut c_void, len as i32)
            },
            (_, Some(mut value)) => {
                (value.as_mut_ptr() as *mut c_void, value.len() as i32)
            },
        };
        unsafe {
            let status = ffi::OCIBindByPos(self.inner_statement,
                              &mut bndp,
                              self.connection.env.error_handle,
                              self.bind_index,
                              buf as *mut c_void,
                              size,
                               //tpe as libc::c_ushort,
                              ffi::SQLT_CHR as u16,
                              IS_NOT_NULL as *mut ffi::OCIInd as *mut c_void,
                              ptr::null_mut(),
                              ptr::null_mut(),
                              0,
                              ptr::null_mut(),
                              ffi::OCI_DEFAULT);
            self.buffers.push(Box::from_raw(buf));
            self.sizes.push(size);

            if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                return Err(err);
            }

            if tpe == OCIDataType::Char {
                unsafe {
                    let mut cs_id = self.connection.env.cs_id;
                    ffi::OCIAttrSet(bndp as *mut c_void,
                                    ffi::OCI_HTYPE_BIND,
                                    &mut cs_id as *mut u16 as *mut c_void,
                                    0,
                                    ffi::OCI_ATTR_CHARSET_ID,
                                    self.connection.env.error_handle);
                }
            }
            self.binds.push(Box::from_raw(bndp));
        }
        Ok(())

    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            let status = ffi::OCIStmtRelease(self.inner_statement,
                                self.connection.env.error_handle,
                                ptr::null(),
                                0,
                                ffi::OCI_DEFAULT);
            if let Some(err) = Self::check_error(self.connection.env.error_handle, status) {
                println!("{:?}", err);
            }
        }
    }
}
