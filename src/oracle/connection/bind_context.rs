use std::fmt;
use std::os::raw::c_void;
use std::ptr;

use oci_sys as ffi;

use oracle::types::OciDataType;

use super::stmt::Statement;

pub struct BindContext {
    pub(crate) store: Vec<u8>,
    is_null: ffi::OCIInd,
    return_code: u16,
    return_len: u32,
    error_handle: *mut ffi::OCIError,
}
impl BindContext {
    pub fn new(error_handle: *mut ffi::OCIError, tpe: &OciDataType) -> Self {
        // TODO: FIXME: proper CLOB and BLOB handling
        let size = tpe.byte_size();
        BindContext {
            store: Vec::new(),
            is_null: 0,
            return_code: 0,
            return_len: size as u32,
            error_handle,
        }
    }
}
impl fmt::Debug for BindContext {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("BindContext")
            .field("store", &self.store)
            .field("is_null", &self.is_null)
            .finish()
    }
}

// c.f. https://github.com/dongyongzhi/android_work/blob/adcaec07b3a7dd64b98763645522972387c67e73/xvapl(sql)/oci/samples/cdemodr1.c#L1038
pub extern "C" fn empty_data(
    _ictxp: *mut c_void,
    _bindp: *mut ffi::OCIBind,
    _iter: u32,
    _index: u32,
    bufpp: *mut *mut c_void,
    alenp: *mut u32,
    piecep: *mut u8,
    indpp: *mut *mut c_void,
) -> i32 {
    unsafe {
        *bufpp = ptr::null_mut();
        *alenp = 0;
        *indpp = ptr::null_mut();
        *piecep = ffi::OCI_ONE_PIECE as u8;
    }

    ffi::OCI_CONTINUE
}
// c.f. https://github.com/dongyongzhi/android_work/blob/adcaec07b3a7dd64b98763645522972387c67e73/xvapl(sql)/oci/samples/cdemodr1.c#L1038
// This is the callback function that is called to receive the OUT
// bind values for the bind variables in the RETURNING clause
pub unsafe extern "C" fn on_receive_data(
    octxp: *mut c_void,
    bindp: *mut ffi::OCIBind,
    _iter: u32,
    index: u32,
    bufpp: *mut *mut c_void,
    alenp: *mut *mut u32,
    piecep: *mut u8,
    indpp: *mut *mut c_void,
    rcodepp: *mut *mut u16,
) -> i32 {
    let ctx: &mut BindContext = &mut *(octxp as *mut _);
    // For each iteration the OCI_ATTR_ROWS_RETURNED tells us the number
    // of rows returned in that iteration.  So we can use this information
    // to dynamically allocate storage for all the returned rows for that
    // bind.

    let mut rows: u32 = 0;

    if index == 0 {
        let status = ffi::OCIAttrGet(
            bindp as *const _,
            ffi::OCI_HTYPE_BIND,
            (&mut rows as *mut u32) as *mut _,
            ptr::null_mut(),
            ffi::OCI_ATTR_ROWS_RETURNED,
            ctx.error_handle,
        );

        let err = Statement::check_error_sql(
            ctx.error_handle,
            status,
            &"returning rowid".to_string(),
            "GET ROWS RETURNED",
        );
        if err.is_err() {
            debug!("{:?}", err.err());
            return ffi::OCI_ERROR;
        }
    }

    // TODO: somehow find a solution to allocate the right buffer size here
    // maybe just matching on the requested output type?
    ctx.store.resize(ctx.return_len as usize, 0);

    // Provide the address of the storage where the data is to be returned
    *bufpp = ctx.store.as_ptr() as *mut _;

    *piecep = ffi::OCI_ONE_PIECE as u8;

    // provide address of the storage where the indicator will be returned
    *indpp = &mut ctx.is_null as *mut _ as *mut c_void;

    // provide address of the storage where the return code  will be returned
    *rcodepp = &mut ctx.return_code as *mut _;

    // provide address of the storage where the actual length  will be
    // returned
    *alenp = &mut ctx.return_len as *mut _;

    ffi::OCI_CONTINUE
}
