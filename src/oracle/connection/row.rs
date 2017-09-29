use super::super::backend::Oracle;
use diesel::row::Row;

pub struct OciRow<'a> {
    buf: Vec<&'a [u8]>,
    is_null: Vec<bool>,
    col_idx: usize,
}

impl<'a> OciRow<'a> {
    pub fn new(row_buf: Vec<&'a [u8]>, is_null: Vec<bool>) -> Self {
        OciRow {
            buf: row_buf,
            is_null: is_null,
            col_idx: 0,
        }
    }
}

impl<'a> Row<Oracle> for OciRow<'a> {
    fn take(&mut self) -> Option<&[u8]> {
        let ret = if self.col_idx < self.buf.len() {
            Some(self.buf[self.col_idx])
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
