use super::cursor::Field;
use diesel::row::{self, Row, RowIndex};
use oracle::backend::Oracle;

use super::oracle_value::OracleValue;

pub struct OciRow<'a> {
    binds: &'a [Field],
}

impl<'a> OciRow<'a> {
    pub fn new(binds: &'a [Field]) -> Self {
        OciRow { binds }
    }
}

impl<'a> RowIndex<usize> for OciRow<'a> {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.binds.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a, 'b> RowIndex<&'a str> for OciRow<'b> {
    fn idx(&self, field_name: &'a str) -> Option<usize> {
        self.binds
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == field_name)
            .map(|(idx, _)| idx)
    }
}

impl<'a> Row<'a, Oracle> for OciRow<'a> {
    type Field = OciField<'a>;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.binds.len()
    }

    fn get<I>(&self, idx: I) -> Option<Self::Field>
    where
        Self: diesel::row::RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(OciField(&self.binds[idx]))
    }

    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> diesel::row::PartialRow<Self::InnerPartialRow> {
        diesel::row::PartialRow::new(self, range)
    }
}

pub struct OciField<'a>(&'a Field);

impl<'a> row::Field<'a, Oracle> for OciField<'a> {
    fn field_name(&self) -> Option<&'a str> {
        Some(self.0.name())
    }

    fn value(&self) -> Option<diesel::backend::RawValue<'a, Oracle>> {
        if self.0.is_null() {
            None
        } else {
            Some(OracleValue::new(self.0.buffer(), self.0.datatype()))
        }
    }

    fn is_null(&self) -> bool {
        self.0.is_null()
    }
}
