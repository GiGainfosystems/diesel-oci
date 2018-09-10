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
    pub is_returning: bool,
    pub affected_table: String,
    buffers: Vec<Box<[u8]>>,
    sizes: Vec<i32>,
    indicators: Vec<Box<ffi::OCIInd>>,
    pub(crate) mysql: String,
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
        let mut affected_table = "".to_string();
        let mut is_returning = false;
        if let Some(pos) = mysql.find("RETURNING") {
            is_returning = true;
//            mysql = mysql + &format!(" into ");
            // determine the affected table, which is stupid, but works for `insert into #table ...` pretty well and is used as proof of concept
            let mysqlcopy = mysql.clone();
            let keywords : Vec<&str> = mysqlcopy.split(' ').collect();
            affected_table = format!("{}", keywords[2]);

            // we clone since we need the original statement
            let mut _fields = mysql.split_off(pos + String::from("RETURNING").len());
            // now that's just a shortcut to count the `,` which now come
//            let single_fields : Vec<&str> = fields.split(',').collect();
//            for i in 0..single_fields.len() {
//                if i > 0 {
//                    mysql = mysql + &format!(",");
//                }
//                mysql = mysql + &format!(":out{}", i);
//            }
            mysql = mysql + &format!(" rowidtochar(rowid) into :out1");
        }
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
            affected_table,
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
        action: &str,
    ) -> Result<(), Error> {
        let check = Self::check_error(error_handle, status);
        if check.is_err() {
            debug!("{:?} while {:?}", sql, action);
        }
        check
    }

    pub fn run(&mut self, auto_commit: bool) -> QueryResult<()> {
        let iters = if self.is_select { 0 } else { 1 };
        if self.is_returning {
            self.bind_index += 1;
            let tpe = OCIDataType::Char;
            let mut bndp = ptr::null_mut() as *mut ffi::OCIBind;
            let is_null = true;
            // using a box here otherwise the string will be deleted before
            // reaching OCIBindByPos
            let (mut buf, size): (Box<[u8]>, i32) = (Vec::new().into_boxed_slice(), 0);
            let mut nullind: Box<ffi::OCIInd> = if is_null { Box::new(-1) } else { Box::new(0) };
            unsafe {
                let status = ffi::OCIBindByPos(
                    self.inner_statement,
                    &mut bndp,
                    self.connection.env.error_handle,
                    self.bind_index,
                    buf.as_mut_ptr() as *mut c_void,
                    buf.len() as i32,
                    tpe.to_raw() as u16,
                    &mut *nullind as *mut i16 as *mut c_void,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    0,
                    ptr::null_mut(),
                    ffi::OCI_DATA_AT_EXEC,
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
            // bind_dynamic

            // TODO: this was more less taken from:
            // https://github.com/Mingun/rust-oci/blob/2b06c2564cf529db6b9cafa9eea3f764fb981f27/src/stmt/mod.rs
            // https://github.com/Mingun/rust-oci/blob/2b06c2564cf529db6b9cafa9eea3f764fb981f27/src/ffi/native/bind.rs
            // we need to get this to compile and "just" define the callback properly
            let mut ictx = BindContext::new(move |_, _v, iter, index| {
                debug!("in call back iter {}, index {}", iter, index);
                               //let is_null = match func(iter, index).as_db() {
                               //    Some(slice) => { v.extend_from_slice(slice); false },
                               //    None => true,
                               //};
                               //(is_null, false)
                (false, false)
            }, &self);
            let mut octx = BindContext::new(move |_, v, iter, index| {
                debug!("out call back iter {}, index {}", iter, index);
                debug!("{:?}", v);
                //let is_null = match func(iter, index).as_db() {
                //    Some(slice) => { v.extend_from_slice(slice); false },
                //    None => true,
                //};
                //(is_null, false)
                (false, false)
            }, &self);
            unsafe {
                ffi::OCIBindDynamic(
                    bndp,
                    self.connection.env.error_handle,
                    &mut ictx as *mut _ as *mut c_void, // this can be a number
                    Some(cbf_no_data),
                    &mut octx as *mut _ as *mut c_void,// this can be a number
                    Some(cbf_get_data)
                )
            };




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
                "GET AFFECTED ROWS",
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
                "GET NUM COLS",
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
                "RETRIEVING TYPE",
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
                        "RETRIEVING PRECISION",
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
                        "RETRIEVING SCALE",
                    )?;
                    if scale == 0 {
                        tpe_size = match precision {
                            1..=5 => 2,   // number(5) -> smallint
                            6..=10 => 4,  // number(10) -> int
                            11..=19 => 8, // number(19) -> bigint
                            _ => 21,      // number(38) -> consume_all // TODO: use numeric(diesel)
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
                        "RETRIEVING LENGTH",
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
                    // TODO: FIXME: do proper LOB Handling here
                    // if we set below 2_000_000_000 oracle will deny the binding with
                    // ORA-01062: unable to allocate memory for define buffer
                    // just read two MB
                    tpe_size = 2_000_000;
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
                "DEFINING",
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
                "RETRIEVING COL HANDLE",
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

    pub fn run_with_cursor<ST, T>(&mut self, auto_commit: bool) -> QueryResult<Cursor<ST, T>> {

        self.run(auto_commit)?;
        if self.is_returning {
            // TODO: this needs to read from last bind/field and create
            // a custom cursor which has one row and one column (the rowid)

            unimplemented!("");

        } else {
            let fields = self.define_all_columns()?;
            Ok(Cursor::new(self, fields))
        }
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
                "BINDING",
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
                "DROPPING STMT",
            ).err()
            {
                debug!("{:?}", err);
            }
        }
    }
}



use std::mem;
use std::fmt;

pub type InBindFn<'f> = FnMut(&mut ffi::OCIBind, &mut Vec<u8>, u32, u32) -> (bool, bool) + 'f;

pub struct BindContext<'a> {
    /// Функция, предоставляющая данные для связанных переменных
    func: Box<InBindFn<'a>>,
    /// Место, где хранятся данные для связанной переменной, возвращенные замыканием, пока не будет
    /// вызван метод `execute`.
    store: Vec<u8>,
    /// Место для указания адреса в памяти, в котором хранится признак `NULL`-а в связанной переменной.
    /// По странной прихоти API требует указать адрес переменной, в которой хранится признак `NULL`-а,
    /// а не просто заполнить выходной параметр в функции обратного вызова.
    is_null: ffi::OCIInd,
    return_code: u16,
    return_len: u32,
    stmt: &'a Statement,
}
impl<'a> BindContext<'a> {
    pub fn new<F>(f: F, stmt: &'a Statement) -> Self
        where F: FnMut(&mut ffi::OCIBind, &mut Vec<u8>, u32, u32) -> (bool, bool) + 'a
    {
        BindContext {
            func: Box::new(f),
            store: Vec::new(),
            is_null: 0,
            return_code: 0,
            return_len: 0,
            stmt,
        }
    }
}
impl<'a> fmt::Debug for BindContext<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BindContext")
            .field("func", &(&self.func as *const _))
            .field("store", &self.store)
            .field("is_null", &self.is_null)
            .finish()
    }
}

// c.f. https://github.com/dongyongzhi/android_work/blob/adcaec07b3a7dd64b98763645522972387c67e73/xvapl(sql)/oci/samples/cdemodr1.c#L1038
pub extern "C" fn cbf_no_data(_ictxp: *mut c_void,
                              _bindp: *mut ffi::OCIBind,
                              _iter: u32,
                              _index: u32,
                              bufpp: *mut *mut c_void,
                              alenp: *mut u32,
                              piecep: *mut u8,
                              indpp: *mut *mut c_void) -> i32 {
    unsafe {
        *bufpp = ptr::null_mut();
        *alenp = 0;
        *indpp = ptr::null_mut();
        *piecep = ffi::OCI_ONE_PIECE as u8;
    }

    ffi::OCI_CONTINUE
}
// c.f. https://github.com/dongyongzhi/android_work/blob/adcaec07b3a7dd64b98763645522972387c67e73/xvapl(sql)/oci/samples/cdemodr1.c#L1038
pub unsafe extern "C" fn cbf_get_data(octxp: *mut c_void,
                               bindp: *mut ffi::OCIBind,
                               iter: u32,
                               index: u32,
                               bufpp: *mut *mut c_void,
                               alenp: *mut *mut u32,
                               piecep: *mut u8,
                               indpp: *mut *mut c_void,
                               rcodepp: *mut *mut u16) -> i32 {
    debug!("we are in the callback");
    // This is the callback function that is called to receive the OUT
    // bind values for the bind variables in the RETURNING clause
    let ctx: &mut BindContext = mem::transmute(octxp);
    let handle = &mut *bindp;
    // For each iteration the OCI_ATTR_ROWS_RETURNED tells us the number
    // of rows returned in that iteration.  So we can use this information
    // to dynamically allocate storage for all the returned rows for that
    // bind.

    let mut rows : u32 = 0;

    if index == 0 {
        let status = ffi::OCIAttrGet(
            bindp as *const _,
            ffi::OCI_HTYPE_BIND,
            (&mut rows as *mut u32) as *mut _,
            &mut 4, //sizeof(ub4),
            ffi::OCI_ATTR_ROWS_RETURNED,
            ctx.stmt.connection.env.error_handle,
        );

        let err = Statement::check_error_sql(
            ctx.stmt.connection.env.error_handle,
            status,
            &ctx.stmt.mysql,
            "GET ROWS RETURNED",
        );
        if err.is_err() {
            debug!("{:?}", err.err());
            return ffi::OCI_ERROR;
        }
    }

    // Provide the address of the storage where the data is to be returned
    const ELEM : usize = 8;
    ctx.store = Vec::with_capacity(ELEM);
    ctx.store.resize(ELEM, 0);

    debug!("vec len: {}", ctx.store.len());
    *bufpp = ctx.store.as_ptr() as *mut _;

    *piecep = ffi::OCI_ONE_PIECE as u8;

    // provide address of the storage where the indicator will be returned
    *indpp = &mut ctx.is_null as *mut _ as *mut c_void;

    // provide address of the storage where the return code  will be returned
    *rcodepp = &mut ctx.return_code as *mut _;

    // provide address of the storage where the actual length  will be
    // returned
    ctx.return_len = ctx.store.len() as u32;
    *alenp = &mut ctx.return_len as *mut _;

    let (_is_null, _res) = (ctx.func)(handle, &mut ctx.store, iter, index);

    ffi::OCI_CONTINUE
}