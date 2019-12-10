use super::cursor::Field;
use diesel::row::{NamedRow, Row};
use oracle::backend::Oracle;
use std::collections::HashMap;

use super::oracle_value::OracleValue;

pub struct OciRow<'a> {
    binds: &'a [Field],
    col_idx: usize,
}

impl<'a> OciRow<'a> {
    pub fn new(binds: &'a [Field]) -> Self {
        OciRow { col_idx: 0, binds }
    }
}

impl<'a> Row<Oracle> for OciRow<'a> {
    fn take(&'_ mut self) -> Option<OracleValue<'_>> {
        let ret = if self.col_idx < self.binds.len() {
            if self.binds[self.col_idx].is_null() {
                None
            } else {
                Some(OracleValue::new(self.binds[self.col_idx].buffer()))
            }
        } else {
            None
        };
        self.col_idx += 1;
        ret
    }

    fn next_is_null(&self, count: usize) -> bool {
        (0..count).all(|i| self.binds[i + self.col_idx].is_null())
    }

    fn column_count(&self) -> usize {
        self.binds.len()
    }

    fn column_name(&self) -> Option<&str> {
        Some(self.binds[self.col_idx].name())
    }
}

pub struct NamedOciRow<'a> {
    buf: Vec<&'a [u8]>,
    is_null: Vec<bool>,
    lut: &'a HashMap<String, usize>,
}

impl<'a> NamedOciRow<'a> {
    pub fn new(
        row_buf: Vec<&'a [u8]>,
        is_null: Vec<bool>,
        lut: &'a HashMap<String, usize>,
    ) -> Self {
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

    fn get_raw_value(&self, index: usize) -> Option<OracleValue<'_>> {
        if index < self.buf.len() {
            if self.is_null[index] {
                None
            } else {
                Some(OracleValue::new(self.buf[index]))
            }
        } else {
            None
        }
    }
}
