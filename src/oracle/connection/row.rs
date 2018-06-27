use super::super::backend::Oracle;
use diesel::row::Row;

use super::oracle_value::OracleValue;

pub struct OciRow<'a> {
    buf: Vec<&'a [u8]>,
    is_null: Vec<bool>,
    col_idx: usize,
    val: OracleValue,
}

impl<'a> OciRow<'a> {
    pub fn new(row_buf: Vec<&'a [u8]>, is_null: Vec<bool>) -> Self {
        OciRow {
            buf: row_buf,
            is_null,
            col_idx: 0,
            val: OracleValue::new(),
        }
    }
}

impl<'a> Row<Oracle> for OciRow<'a> {
    fn take(&mut self) -> Option<&OracleValue> {
        let ret = if self.col_idx < self.buf.len() {
            if self.is_null[self.col_idx] {
                None
            } else {
                // TODO: find a better way to create vec from slice
                self.val.bytes.clear();
                self.val.bytes.extend_from_slice(self.buf[self.col_idx]);
                Some(&self.val)
            }
        } else {
            None
        };
        self.col_idx += 1;
        ret
    }

    fn next_is_null(&self, count: usize) -> bool {
        (0..count).all(|i| self.is_null[i + self.col_idx])
    }
}
