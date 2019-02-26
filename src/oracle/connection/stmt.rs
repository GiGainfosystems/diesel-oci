use super::bind_context::BindContext;
use super::cursor::{Cursor, Field, NamedCursor};
use super::raw::RawConnection;
use diesel::result::Error;
use diesel::result::*;
use libc;
use oci_sys as ffi;
use oracle::types::OciDataType;
use std::os::raw::{c_int, c_void};
use std::ptr;
use std::rc::Rc;

pub struct Statement {
    pub connection: Rc<RawConnection>,
    pub inner_statement: *mut ffi::OCIStmt,
    pub bind_index: libc::c_uint,
    is_select: bool,
    pub is_returning: bool,
    buffers: Vec<Box<[u8]>>,
    sizes: Vec<i32>,
    indicators: Vec<Box<ffi::OCIInd>>,
    pub(crate) mysql: String,
    pub(crate) returning_contexts: Vec<BindContext>,
}

const NUM_ELEMENTS: usize = 40;

impl Statement {
    pub fn prepare(raw_connection: &Rc<RawConnection>, sql: &str) -> QueryResult<Self> {
        let mut mysql = sql.to_string();
        // TODO: this can go wrong: `UPDATE table SET k='LIMIT';`
        if let Some(pos) = mysql.find("LIMIT") {
            let mut limit_clause = mysql.split_off(pos);
            let place_holder = limit_clause.split_off(String::from("LIMIT ").len());
            mysql = mysql + &format!("OFFSET 0 ROWS FETCH NEXT {} ROWS ONLY", place_holder);
        }
        // TODO: this is bad, things will break
        let is_returning =
            (sql.starts_with("INSERT") || sql.starts_with("insert")) && sql.contains("RETURNING");
        debug!("SQL Statement {}", mysql);
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
                "PREPARING STMT",
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
                        "PREPARING STMT 2",
                    )?;
                }
            }
            debug!("Executing {:?}", mysql);
            stmt
        };
        Ok(Statement {
            connection: raw_connection.clone(),
            inner_statement: stmt,
            bind_index: 0,
            // TODO: this can go wrong, since where is also `WITH` and other SQL structures. before
            // there was `sql.contains("SELECT")||sql.contains("select") which might fails on the
            // following queries (meaning they will be identified as select clause even if they are
            // not: `UPDATE table SET k='select';` OR
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
            is_returning,
            buffers: Vec::with_capacity(NUM_ELEMENTS),
            sizes: Vec::with_capacity(NUM_ELEMENTS),
            indicators: Vec::with_capacity(NUM_ELEMENTS),
            mysql,
            returning_contexts: Vec::new(),
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
        action: &str,
    ) -> Result<(), Error> {
        let check = Self::check_error(error_handle, status);
        if check.is_err() {
            debug!("{:?} while {:?}", sql, action);
        }
        check
    }

    pub fn run(&mut self, auto_commit: bool, metadata: &[OciDataType]) -> QueryResult<()> {
        let iters = if self.is_select { 0 } else { 1 };

        if self.is_returning {
            self.returning_contexts.reserve_exact(metadata.len());
            for tpe in metadata {
                self.bind_index += 1;
                self.returning_contexts
                    .push(BindContext::new(self.connection.env.error_handle, tpe));
                let octx = self
                    .returning_contexts
                    .last_mut()
                    .expect("We pushed it above");
                let mut bndp = ptr::null_mut() as *mut ffi::OCIBind;

                unsafe {
                    // read https://docs.oracle.com/database/121/LNOCI/oci16rel003.htm#LNOCI153
                    // read it again, then you will understand why the parameters are set like that
                    // make sure to read it again
                    // otherwise you may enter the ORA-03106: fatal two-task communication protocol error-hell
                    let status = ffi::OCIBindByPos(
                        self.inner_statement,
                        &mut bndp,
                        self.connection.env.error_handle,
                        self.bind_index,
                        ptr::null_mut(),
                        NUM_ELEMENTS as i32,
                        tpe.bind_type() as u16,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                        0,
                        ptr::null_mut(),
                        ffi::OCI_DATA_AT_EXEC,
                    );

                    Self::check_error_sql(
                        self.connection.env.error_handle,
                        status,
                        &self.mysql,
                        "RETURNING BINDING",
                    )?;

                    if tpe.is_text() {
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
                // bind_dynamic

                // TODO: this was more less taken from:
                // https://github.com/Mingun/rust-oci/blob/2b06c2564cf529db6b9cafa9eea3f764fb981f27/src/stmt/mod.rs
                // https://github.com/Mingun/rust-oci/blob/2b06c2564cf529db6b9cafa9eea3f764fb981f27/src/ffi/native/bind.rs
                // we need to get this to compile and "just" define the callback properly
                let mut ictx = BindContext::new(self.connection.env.error_handle, tpe);

                unsafe {
                    ffi::OCIBindDynamic(
                        bndp,
                        self.connection.env.error_handle,
                        &mut ictx as *mut _ as *mut c_void, // this can be a number
                        Some(super::bind_context::empty_data),
                        octx as *mut _ as *mut c_void, // this can be a number
                        Some(super::bind_context::on_receive_data),
                    )
                };
            }
        }
        let mode = if !self.is_select && auto_commit {
            ffi::OCI_COMMIT_ON_SUCCESS
        } else {
            ffi::OCI_DEFAULT
        };
        unsafe {
            let status = ffi::OCIStmtExecute(
                self.connection.service_handle,
                self.inner_statement,
                self.connection.env.error_handle,
                iters,
                0,
                ptr::null(),
                ptr::null_mut(),
                mode,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "EXECUTING STMT",
            )?;
        }
        // the bind index is required to start by zero. if a statement is
        // executed more than once we need to reset the index here
        //self.bind_index = 0;
        Ok(())
    }

    fn run_describe(&mut self) -> QueryResult<()> {
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
                ffi::OCI_DESCRIBE_ONLY,
            );
            Self::check_error_sql(
                self.connection.env.error_handle,
                status,
                &self.mysql,
                "EXECUTING DESCRIBE STMT",
            )?;
        }
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
                "GET AFFECTED ROWS",
            )?;
        }
        Ok(affected_rows as usize)
    }

    fn get_column_param(&self, col_number: u32) -> QueryResult<*mut ffi::OCIParam> {
        let mut col_param: *mut ffi::OCIParam = ptr::null_mut();
        let status = unsafe {
            ffi::OCIParamGet(
                self.inner_statement as *const _,
                ffi::OCI_HTYPE_STMT,
                self.connection.env.error_handle,
                (&mut col_param as *mut *mut ffi::OCIParam) as *mut _,
                col_number,
            )
        };
        Self::check_error_sql(
            self.connection.env.error_handle,
            status,
            &self.mysql,
            "RETRIEVING PARAM HANDLE",
        )?;
        Ok(col_param)
    }

    fn get_column_data_type(&self, col_param: *mut ffi::OCIParam) -> QueryResult<u32> {
        let mut column_type: u32 = 0;
        let status = unsafe {
            ffi::OCIAttrGet(
                col_param as *mut _,
                ffi::OCI_DTYPE_PARAM,
                (&mut column_type as *mut u32) as *mut _,
                ptr::null_mut(),
                ffi::OCI_ATTR_DATA_TYPE,
                self.connection.env.error_handle,
            )
        };
        Self::check_error_sql(
            self.connection.env.error_handle,
            status,
            &self.mysql,
            "RETRIEVING DATA_TYPE",
        )?;
        Ok(column_type)
    }

    fn get_column_name(&self, col_param: *mut ffi::OCIParam) -> QueryResult<String> {
        use std::slice;
        use std::str;

        let mut column_name: String = String::from("");
        column_name.reserve_exact(20);
        let mut col_n = ptr::null_mut() as *mut u8;
        let mut len: u32 = 0;
        let status = unsafe {
            ffi::OCIAttrGet(
                col_param as *mut _,
                ffi::OCI_DTYPE_PARAM,
                &mut col_n as *mut *mut u8 as *mut _,
                (&mut len as *mut u32) as *mut _,
                ffi::OCI_ATTR_NAME,
                self.connection.env.error_handle,
            )
        };
        Self::check_error_sql(
            self.connection.env.error_handle,
            status,
            &self.mysql,
            "RETRIEVING COLUMN_NAME",
        )?;
        let s = unsafe { slice::from_raw_parts(col_n, len as usize) };
        let n = str::from_utf8(s).map_err(|_| {
            Error::DatabaseError(
                DatabaseErrorKind::UnableToSendCommand,
                Box::new(String::from("Invalid UTF-8 from OCIAttrGet")),
            )
        })?;
        Ok(n.to_string())
    }

    fn get_column_char_size(&self, col_param: *mut ffi::OCIParam) -> QueryResult<u32> {
        let mut type_size: u32 = 0;
        let status = unsafe {
            ffi::OCIAttrGet(
                col_param as *mut _,
                ffi::OCI_DTYPE_PARAM,
                (&mut type_size as *mut u32) as *mut _,
                ptr::null_mut(),
                ffi::OCI_ATTR_CHAR_SIZE,
                self.connection.env.error_handle,
            )
        };
        Self::check_error_sql(
            self.connection.env.error_handle,
            status,
            &self.mysql,
            "RETRIEVING CHAR_SIZE",
        )?;
        Ok(type_size)
    }

    fn get_define_buffer_size(
        &self,
        col_param: *mut ffi::OCIParam,
        col_type: OciDataType,
    ) -> QueryResult<usize> {
        // TODO: FIXME: proper CLOB and BLOB handling

        // Improvement for text:
        //
        // We can check the column type and see if it is varchar.
        // If yes, we use the column char size as buffer size.
        // Otherwise we use the default size.
        match col_type {
            OciDataType::Text => {
                let column_type = self.get_column_data_type(col_param)?;
                match column_type {
                    ffi::SQLT_CHR => {
                        let char_size = self.get_column_char_size(col_param)?;
                        // + 1 accounts for the extra 0 byte we need,
                        // because we define with SQLT_STR i.e. 0-terminated string.
                        Ok((char_size + 1) as usize)
                    }
                    _ => Ok(col_type.byte_size()),
                }
            }
            _ => Ok(col_type.byte_size()),
        }
    }

    fn define_column(&self, col_number: usize, col_type: OciDataType) -> QueryResult<Field> {
        let param = self.get_column_param(col_number as u32)?;
        let buf_size = self.get_define_buffer_size(param, col_type)?;

        let buf = vec![0; buf_size as usize];
        let mut null_indicator: Box<i16> = Box::new(-1);
        let mut define_handle = ptr::null_mut();
        let status = unsafe {
            ffi::OCIDefineByPos(
                self.inner_statement,
                &mut define_handle,
                self.connection.env.error_handle,
                col_number as u32,
                buf.as_ptr() as *mut _,
                buf.len() as i32,
                col_type.define_type() as libc::c_ushort,
                &mut *null_indicator as *mut i16 as *mut c_void,
                ptr::null_mut(),
                ptr::null_mut(),
                ffi::OCI_DEFAULT,
            )
        };
        Self::check_error_sql(
            self.connection.env.error_handle,
            status,
            &self.mysql,
            "DEFINING",
        )?;

        if col_type == OciDataType::Text {
            let mut cs_id = self.connection.env.cs_id;
            unsafe {
                ffi::OCIAttrSet(
                    define_handle as *mut c_void,
                    ffi::OCI_HTYPE_DEFINE,
                    &mut cs_id as *mut u16 as *mut c_void,
                    0,
                    ffi::OCI_ATTR_CHARSET_ID,
                    self.connection.env.error_handle,
                );
            }
        }
        let name = self.get_column_name(param)?;

        Ok(Field::new(
            define_handle,
            buf,
            null_indicator,
            col_type,
            name,
        ))
    }

    fn define_all_columns(&self, row: &[OciDataType]) -> QueryResult<Vec<Field>> {
        row.iter()
            .enumerate()
            .map(|(i, tpe)| self.define_column(i + 1, *tpe))
            .collect()
    }

    pub fn run_with_cursor<ST, T>(
        &mut self,
        auto_commit: bool,
        metadata: Vec<OciDataType>,
    ) -> QueryResult<Cursor<ST, T>> {
        self.run(auto_commit, &metadata)?;
        self.bind_index = 0;
        if self.is_returning {
            let fields = self
                .returning_contexts
                .iter()
                .zip(metadata.into_iter())
                .map(|(buffer, tpe)| {
                    let null_indicator: Box<i16> = Box::new(buffer.is_null);
                    Field::new(
                        ptr::null_mut(),
                        buffer.store.to_owned(),
                        null_indicator,
                        tpe,
                        String::from(""),
                    )
                })
                .collect();
            Ok(Cursor::new(self, fields))
        } else {
            let fields = self.define_all_columns(&metadata)?;
            Ok(Cursor::new(self, fields))
        }
    }

    pub fn get_metadata(&mut self, metadata: &mut Vec<OciDataType>) {
        let desc = self.run_describe();
        if desc.is_ok() {
            let mut cnt = 1;
            let mut param = self.get_column_param(cnt);
            while param.is_ok() {
                let data_type =
                    self.get_column_data_type(param.expect("We test a line before that it is Ok"));
                if data_type.is_ok() {
                    metadata.push(OciDataType::from_sqlt(
                        data_type.expect("We test a line before that it is Ok"),
                    ));
                }
                cnt = cnt + 1;
                param = self.get_column_param(cnt);
            }
        } else {
            debug!("{:?}", desc.err());
        }
    }

    pub fn run_with_named_cursor(
        &mut self,
        auto_commit: bool,
        metadata: Vec<OciDataType>,
    ) -> QueryResult<NamedCursor> {
        self.run(auto_commit, &metadata)?;
        self.bind_index = 0;
        if self.is_returning {
            let fields = self
                .returning_contexts
                .iter()
                .zip(metadata.into_iter())
                .map(|(buffer, tpe)| {
                    let null_indicator: Box<i16> = Box::new(buffer.is_null);
                    Field::new(
                        ptr::null_mut(),
                        buffer.store.to_owned(),
                        null_indicator,
                        tpe,
                        String::from(""),
                    )
                })
                .collect();
            Ok(NamedCursor::new(self, fields))
        } else {
            let fields = self.define_all_columns(&metadata)?;
            Ok(NamedCursor::new(self, fields))
        }
    }

    pub fn bind(&mut self, tpe: OciDataType, value: Option<Vec<u8>>) -> QueryResult<()> {
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
                tpe.bind_type() as u16,
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
                "BINDING",
            )?;

            if tpe.is_text() {
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
                "DROPPING STMT",
            )
            .err()
            {
                debug!("{:?}", err);
            }
        }
    }
}
