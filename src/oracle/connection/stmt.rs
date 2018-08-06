use super::cursor::{Cursor, Field};
use super::raw::RawConnection;
use diesel::result::Error;
use diesel::result::*;
use libc;
use oci_sys as ffi;
use oracle::types::OCIDataType;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::rc::Rc;

pub struct Statement {
    pub connection: Rc<RawConnection>,
    pub inner_statement: *mut ffi::OCIStmt,
    bind_index: libc::c_uint,
    is_select: bool,
    buffers: Vec<Box<[u8]>>,
    sizes: Vec<i32>,
    indicators: Vec<Box<ffi::OCIInd>>,
    pub mysql: String,
}

const NUM_ELEMENTS: usize = 20;

impl Statement {
    pub fn prepare(raw_connection: &Rc<RawConnection>, sql: &str) -> QueryResult<Self> {
        let mut mysql = sql.to_string();
        // TODO: this can go wrong: `UPDATE table SET k='LIMIT';`
        if let Some(pos) = mysql.find("LIMIT") {
            let mut limit_clause = mysql.split_off(pos);
            let place_holder = limit_clause.split_off(String::from("LIMIT ").len());
            mysql = mysql + &format!("OFFSET 0 ROWS FETCH NEXT {} ROWS ONLY", place_holder);
        }

        let stmt = unsafe {
            let mut stmt: *mut ffi::OCIStmt = ptr::null_mut();
            let status = ffi::OCIStmtPrepare2(
                raw_connection.service_handle,
                &mut stmt,
                raw_connection.env.error_handle,
                mysql.as_ptr(),
                mysql.len() as u32,
                ptr::null(),
                0,
                ffi::OCI_NTV_SYNTAX,
                ffi::OCI_DEFAULT,
            );

            Self::check_error_sql(
                raw_connection.env.error_handle,
                status,
                &mysql,
                "PREPARING STMT".to_string(),
            )?;

            // for create statements we need to run OCIStmtPrepare2 twice
            // c.f. https://docs.oracle.com/database/121/LNOCI/oci17msc001.htm#LNOCI17165
            // "To reexecute a DDL statement, you must prepare the statement again using OCIStmtPrepare2()."
            if let Some(u) = mysql.to_string().find("CREATE") {
                if u < 10 {
                    let status = ffi::OCIStmtPrepare2(
                        raw_connection.service_handle,
                        &mut stmt,
                        raw_connection.env.error_handle,
                        mysql.as_ptr(),
                        mysql.len() as u32,
                        ptr::null(),
                        0,
                        ffi::OCI_NTV_SYNTAX,
                        ffi::OCI_DEFAULT,
                    );

                    Self::check_error_sql(
                        raw_connection.env.error_handle,
                        status,
                        &mysql,
                        "PREPARING STMT 2".to_string(),
                    )?;
                }
            }

            stmt
        };
        Ok(Statement {
            connection: raw_connection.clone(),
            inner_statement: stmt,
            bind_index: 0,
            // TODO: this can go wrong: `UPDATE table SET k='select';` OR
            // ```
            //            CREATE OR REPLACE FORCE VIEW full_bounding_boxes(id, o_c1, o_c2, o_c3, u_c1, u_c2, u_c3, v_c1, v_c2, v_c3, w_c1, w_c2, w_c3)
            //            AS
            //            SELECT bbox.id as id,
            //            o.c1 as o_c1, o.c2 as o_c2, o.c3 as o_c3,
            //            u.c1 as u_c1, u.c2 as u_c2, u.c3 as u_c3,
            //            v.c1 as v_c1, v.c2 as v_c2, v.c3 as v_c3,
            //            w.c1 as w_c1, w.c2 as w_c2, w.c3 as w_c3
            //            FROM bounding_boxes bbox
            //            INNER JOIN geo_points o ON bbox.o = o.id
            //            INNER JOIN geo_points u ON bbox.u = u.id
            //            INNER JOIN geo_points v ON bbox.v = v.id
            //            INNER JOIN geo_points w ON bbox.w = w.id
            // ```
            is_select: sql.starts_with("SELECT") || sql.starts_with("select"),
            buffers: Vec::with_capacity(NUM_ELEMENTS),
            sizes: Vec::with_capacity(NUM_ELEMENTS),
            indicators: Vec::with_capacity(NUM_ELEMENTS),
            mysql,
        })
    }

    pub fn check_error(error_handle: *mut ffi::OCIError, status: i32) -> Result<(), Error> {
        match status {
            ffi::OCI_ERROR => {
                // c.f. https://github.com/Mingun/rust-oci/blob/2e0f2acb35066b5f510b46826937a634017cda5d/src/ffi/mod.rs#L102
                // ffi::OCI_ERROR_MAXMSG_SIZE2 is 3072
                let mut errbuf: Vec<u8> = vec![0; ffi::OCI_ERROR_MAXMSG_SIZE2 as usize + 1];
                let mut errcode: c_int = 0;

                unsafe {
                    let res = ffi::OCIErrorGet(
                        error_handle as *mut c_void,
                        1,
                        ptr::null_mut(),
                        &mut errcode,
                        errbuf.as_mut_ptr(),
                        errbuf.len() as u32,
                        ffi::OCI_HTYPE_ERROR,
                    );

                    if res == (ffi::OCI_NO_DATA as i32) {
                        return Ok(());
                    }

                    let nul_byte_pos = errbuf
                        .iter()
                        .position(|&b| b == 0)
                        .expect("Expected at least one null byte");
                    errbuf.resize(nul_byte_pos, 0);
                }

                Err(Error::DatabaseError(
                    DatabaseErrorKind::UnableToSendCommand,
                    Box::new(format!(
                        "OCI_ERROR {:?}",
                        String::from_utf8(errbuf).expect("Invalid UTF-8 from OCIErrorGet")
                    )),
                ))
            }
            ffi::OCI_INVALID_HANDLE => Err(Error::DatabaseError(
                DatabaseErrorKind::UnableToSendCommand,
                Box::new(format!("OCI_INVALID_HANDLE {:?}", status)),
            )),
            _ => Ok(()),
        }
    }

    pub fn check_error_sql(
        error_handle: *mut ffi::OCIError,
        status: i32,
        sql: &String,
        action: String,
    ) -> Result<(), Error> {
        let check = Self::check_error(error_handle, status);
        if check.is_err() {
            println!("{:?} while {:?}", sql, action);
        }
        check
    }

    pub fn run(&mut self) -> QueryResult<()> {
        let iters = if self.is_select { 0 } else { 1 };
        unsafe {
            let status = ffi::OCIStmtExecute(
                self.connection.service_handle,
                self.inner_statement,
                self.connection.env.error_handle,
                iters,
                0,
                ptr::null(),
                ptr::null_mut(),
                ffi::OCI_DEFAULT,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "EXECUTING STMT".to_string(),
            )?;
        }
        // TODO: since we have a statement cache, which I couldn't find to turn off,
        // we need to reset the bind_index once we executed the statement
        self.bind_index = 0;
        Ok(())
    }

    pub fn get_affected_rows(&self) -> QueryResult<usize> {
        let mut affected_rows: u32 = 0;
        unsafe {
            let status = ffi::OCIAttrGet(
                self.inner_statement as *const _,
                ffi::OCI_HTYPE_STMT,
                (&mut affected_rows as *mut u32) as *mut _,
                &mut 0,
                ffi::OCI_ATTR_ROW_COUNT,
                self.connection.env.error_handle,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "GET AFFECTED ROWS".to_string(),
            )?;
        }
        Ok(affected_rows as usize)
    }

    fn get_column_count(&self) -> QueryResult<u32> {
        let mut col_count: u32 = 0;
        unsafe {
            let status = ffi::OCIAttrGet(
                self.inner_statement as *const _,
                ffi::OCI_HTYPE_STMT,
                (&mut col_count as *mut u32) as *mut _,
                &mut 0,
                ffi::OCI_ATTR_PARAM_COUNT,
                self.connection.env.error_handle,
            );

            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "GET NUM COLS".to_string(),
            )?;
        }
        Ok(col_count)
    }

    fn get_attr_type_and_size(&self, col_handle: *mut ffi::OCIStmt) -> QueryResult<(u32, u32)> {
        let mut tpe: u32 = 0;
        let mut tpe_size: u32 = 0;
        unsafe {
            let status = ffi::OCIAttrGet(
                col_handle as *mut _,
                ffi::OCI_DTYPE_PARAM,
                (&mut tpe as *mut u32) as *mut _,
                &mut 0,
                ffi::OCI_ATTR_DATA_TYPE,
                self.connection.env.error_handle,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "RETRIEVING TYPE".to_string(),
            )?;

            // c.f. https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/data-types.html#GUID-7DA48B90-07C7-41A7-BC57-D8F358A4EEBE
            match tpe {
                ffi::SQLT_INT | ffi::SQLT_UIN => {
                    tpe_size = 8;
                }
                ffi::SQLT_NUM => {
                    let mut attributesize = 16u32; //sb2
                    let mut scale = 0i8;
                    let mut precision = 0i16;
                    let status = ffi::OCIAttrGet(
                        col_handle as *mut _,
                        ffi::OCI_DTYPE_PARAM,
                        (&mut precision as *mut i16) as *mut _,
                        &mut attributesize as *mut u32,
                        ffi::OCI_ATTR_PRECISION,
                        self.connection.env.error_handle,
                    );
                    Self::check_error_sql(
                        self.connection.env.error_handle,
                        status,
                        &self.mysql,
                        "RETRIEVING PRECISION".to_string(),
                    )?;
                    let mut attributesize = 8u32; // sb1
                    let status = ffi::OCIAttrGet(
                        col_handle as *mut _,
                        ffi::OCI_DTYPE_PARAM,
                        (&mut scale as *mut i8) as *mut _,
                        &mut attributesize as *mut u32,
                        ffi::OCI_ATTR_SCALE,
                        self.connection.env.error_handle,
                    );
                    Self::check_error_sql(
                        self.connection.env.error_handle,
                        status,
                        &self.mysql,
                        "RETRIEVING SCALE".to_string(),
                    )?;
                    if scale == 0 {
                        tpe_size = match precision {
                            5 => 2,  // number(5) -> smallint
                            10 => 4, // number(10) -> int
                            19 => 8, // number(19) -> bigint
                            _ => 21, // number(38) -> consume_all
                        };
                        tpe = ffi::SQLT_INT;
                    } else {
                        tpe = ffi::SQLT_FLT;
                        tpe_size = 8;
                    }
                }
                ffi::SQLT_BDOUBLE | ffi::SQLT_LNG | ffi::SQLT_IBDOUBLE => {
                    tpe_size = 8;
                    tpe = ffi::SQLT_BDOUBLE;
                }
                ffi::SQLT_FLT | ffi::SQLT_BFLOAT | ffi::SQLT_IBFLOAT => {
                    tpe_size = 4;
                    tpe = ffi::SQLT_BFLOAT;
                }
                ffi::SQLT_CHR | ffi::SQLT_VCS | ffi::SQLT_LVC | ffi::SQLT_AFC | ffi::SQLT_VST => {
                    let mut length = 0u32;
                    let status = ffi::OCIAttrGet(
                        col_handle as *mut _,
                        ffi::OCI_DTYPE_PARAM,
                        (&mut tpe_size as *mut u32) as *mut _,
                        &mut length as *mut u32,
                        ffi::OCI_ATTR_CHAR_SIZE,
                        self.connection.env.error_handle,
                    );
                    Self::check_error_sql(
                        self.connection.env.error_handle,
                        status,
                        &self.mysql,
                        "RETRIEVING LENGTH".to_string(),
                    )?;
                    //tpe_size += 1;
                    tpe = ffi::SQLT_STR;
                }
                ffi::SQLT_ODT
                | ffi::SQLT_DATE
                | ffi::SQLT_TIMESTAMP
                | ffi::SQLT_TIMESTAMP_TZ
                | ffi::SQLT_TIMESTAMP_LTZ => {
                    // DATE is 7 bytes, c.f. https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/data-types.html#GUID-7DA48B90-07C7-41A7-BC57-D8F358A4EEBE
                    tpe = ffi::SQLT_DAT;
                    tpe_size = 7;
                }
                ffi::SQLT_BLOB => {
                    tpe = ffi::SQLT_BIN;
                    // this just fits GST's current password hashing settings, if they are changed
                    // we need to change the size here
                    // TODO: FIXME: find a away to read the size of a BLOB
                    tpe_size = 88;
                }
                ffi::SQLT_CLOB => {
                    // just read two GB
                    tpe_size = 2_000_000_000;
                    tpe = ffi::SQLT_STR;
                }
                _ => {
                    return Err(Error::DatabaseError(
                        DatabaseErrorKind::__Unknown,
                        Box::new(format!("unsupported type {}", tpe)),
                    ))
                }
            }
        }
        Ok((tpe, tpe_size))
    }

    pub fn define(
        &self,
        fields: &mut Vec<Field>,
        tpe: u32,
        tpe_size: u32,
        col_number: usize,
    ) -> QueryResult<()> {
        let mut v = Vec::with_capacity(tpe_size as usize);
        v.resize(tpe_size as usize, 0);
        let mut null_indicator: Box<i16> = Box::new(-1);
        let def = unsafe {
            let mut def = ptr::null_mut();
            let status = ffi::OCIDefineByPos(
                self.inner_statement,
                &mut def,
                self.connection.env.error_handle,
                col_number as u32,
                v.as_ptr() as *mut _,
                v.len() as i32,
                tpe as libc::c_ushort,
                &mut *null_indicator as *mut i16 as *mut c_void,
                ptr::null_mut(),
                ptr::null_mut(),
                ffi::OCI_DEFAULT,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "DEFINING".to_string(),
            )?;
            def
        };
        if let Some(tpe) = ::oracle::types::OCIDataType::from_raw(tpe) {
            fields.push(Field::new(def, v, null_indicator, tpe));
        } else {
            return Err(Error::DatabaseError(
                DatabaseErrorKind::__Unknown,
                Box::new(format!("unsupported type {}", tpe)),
            ));
        }

        Ok(())
    }

    fn define_column(&self, mut fields: &mut Vec<Field>, col_number: usize) -> QueryResult<()> {
        let col_handle = unsafe {
            let mut parameter_descriptor: *mut ffi::OCIStmt = ptr::null_mut();
            let status = ffi::OCIParamGet(
                self.inner_statement as *const _,
                ffi::OCI_HTYPE_STMT,
                self.connection.env.error_handle,
                (&mut parameter_descriptor as *mut *mut ffi::OCIStmt) as *mut _,
                col_number as u32,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "RETRIEVING COL HANDLE".to_string(),
            )?;
            parameter_descriptor
        };

        let (tpe, tpe_size): (u32, u32) = self.get_attr_type_and_size(col_handle)?;

        self.define(&mut fields, tpe, tpe_size, col_number)?;
        Ok(())
    }

    fn define_all_columns(&self) -> QueryResult<Vec<Field>> {
        let col_count = self.get_column_count()?;
        let mut fields = Vec::<Field>::with_capacity(col_count as usize);
        for i in 0..col_count as usize {
            let col_number = i + 1;
            self.define_column(&mut fields, col_number)?;
        }
        Ok(fields)
    }

    pub fn run_with_cursor<ST, T>(&mut self) -> QueryResult<Cursor<ST, T>> {
        self.run()?;
        let fields = self.define_all_columns()?;

        Ok(Cursor::new(self, fields))
    }

    pub fn bind(&mut self, tpe: OCIDataType, value: Option<Vec<u8>>) -> QueryResult<()> {
        self.bind_index += 1;
        let mut bndp = ptr::null_mut() as *mut ffi::OCIBind;
        let mut is_null = false;
        // using a box here otherwise the string will be deleted before
        // reaching OCIBindByPos
        let (mut buf, size): (Box<[u8]>, i32) = if let Some(mut value) = value {
            let len = value.len() as i32;
            (value.into_boxed_slice(), len)
        } else {
            is_null = true;
            (Vec::new().into_boxed_slice(), 0)
        };
        let mut nullind: Box<ffi::OCIInd> = if is_null { Box::new(-1) } else { Box::new(0) };

        unsafe {
            let status = ffi::OCIBindByPos(
                self.inner_statement,
                &mut bndp,
                self.connection.env.error_handle,
                self.bind_index,
                buf.as_mut_ptr() as *mut c_void,
                buf.len() as i32,
                if size == 4 && tpe == OCIDataType::Float {
                    ffi::SQLT_BFLOAT as u16
                } else {
                    tpe.to_raw() as u16
                },
                &mut *nullind as *mut i16 as *mut c_void,
                ptr::null_mut(),
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                ffi::OCI_DEFAULT,
            );

            self.buffers.push(buf);
            self.sizes.push(size);
            self.indicators.push(nullind);

            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "BINDING".to_string(),
            )?;

            if tpe == OCIDataType::Char {
                let mut cs_id = self.connection.env.cs_id;
                ffi::OCIAttrSet(
                    bndp as *mut c_void,
                    ffi::OCI_HTYPE_BIND,
                    &mut cs_id as *mut u16 as *mut c_void,
                    0,
                    ffi::OCI_ATTR_CHARSET_ID,
                    self.connection.env.error_handle,
                );
            }
        }
        Ok(())
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            let status = ffi::OCIStmtRelease(
                self.inner_statement,
                self.connection.env.error_handle,
                ptr::null(),
                0,
                ffi::OCI_DEFAULT,
            );
            if let Some(err) = Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "DROPPING STMT".to_string(),
            ).err()
            {
                println!("{:?}", err);
            }
        }
    }
}
