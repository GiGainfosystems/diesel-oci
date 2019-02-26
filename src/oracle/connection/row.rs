use super::super::backend::Oracle;
use diesel::row::{Row, NamedRow};

use super::oracle_value::OracleValue;

pub struct OciRow<'a> {
    buf: Vec<&'a [u8]>,
    is_null: Vec<bool>,
    col_idx: usize,
}

impl<'a> OciRow<'a> {
    pub fn new(row_buf: Vec<&'a [u8]>, is_null: Vec<bool>) -> Self {
        OciRow {
            buf: row_buf,
            is_null,
            col_idx: 0,
        }
    }
}

impl<'a> Row<Oracle> for OciRow<'a> {
    fn take(&mut self) -> Option<&OracleValue> {
        let ret = if self.col_idx < self.buf.len() {
            if self.is_null[self.col_idx] {
                None
            } else {
                Some(OracleValue::new(self.buf[self.col_idx]))
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

use std::collections::HashMap;
pub struct NamedOciRow<'a> {
    buf: Vec<&'a [u8]>,
    is_null: Vec<bool>,
    lut: HashMap<String, usize>,
}

impl<'a> NamedOciRow<'a> {
    pub fn new(row_buf: Vec<&'a [u8]>, is_null: Vec<bool>, lut: HashMap<String, usize>) -> Self {
        NamedOciRow {
            buf: row_buf,
            is_null,
            lut,
        }
    }
}

impl<'a> NamedRow<Oracle> for NamedOciRow<'a> {
    fn index_of(&self, column_name: &str) -> Option<usize> {
        self.lut.get(column_name).map(|ci| *ci as usize)
    }
    fn get_raw_value(&self, index: usize) -> Option<&OracleValue> {
        let ret = if index < self.buf.len() {
            if self.is_null[index] {
                None
            } else {
                Some(OracleValue::new(self.buf[index]))
            }
        } else {
            None
        };
        ret
    }
}
