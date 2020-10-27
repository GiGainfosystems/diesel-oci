use crate::oracle::backend::Oracle;
use diesel::row::{self, Row, RowIndex};

use super::oracle_value::OracleValue;

pub struct OciRow<'a> {
    values: Vec<Option<OracleValue<'a>>>,
    column_infos: &'a [oracle::ColumnInfo],
}

impl<'a> OciRow<'a> {
    pub fn new(values: &'a [oracle::SqlValue], column_infos: &'a [oracle::ColumnInfo]) -> Self {
        let values = values
            .iter()
            .zip(column_infos)
            .map(|(v, c)| {
                if v.is_null().unwrap_or(true) {
                    None
                } else {
                    Some(OracleValue::new(v, c.oracle_type()))
                }
            })
            .collect();
        OciRow {
            values,
            column_infos,
        }
    }

    pub fn new_from_value(values: Vec<Option<OracleValue<'a>>>) -> Self {
        Self {
            values,
            column_infos: &[],
        }
    }
}

impl<'a> RowIndex<usize> for OciRow<'a> {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.values.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a, 'b> RowIndex<&'a str> for OciRow<'b> {
    fn idx(&self, field_name: &'a str) -> Option<usize> {
        self.column_infos
            .iter()
            .enumerate()
            .find(|(_, c)| c.name() == field_name)
            .map(|(idx, _)| idx)
    }
}

impl<'a> Row<'a, Oracle> for OciRow<'a> {
    type Field = OciField<'a>;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.values.len()
    }

    fn get<I>(&self, idx: I) -> Option<Self::Field>
    where
        Self: diesel::row::RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(OciField {
            field_value: self.values[idx].clone(),
            column_info: self.column_infos.get(idx),
        })
    }

    fn partial_row(
        &self,
        range: std::ops::Range<usize>,
    ) -> diesel::row::PartialRow<Self::InnerPartialRow> {
        diesel::row::PartialRow::new(self, range)
    }
}

pub struct OciField<'a> {
    field_value: Option<OracleValue<'a>>,
    column_info: Option<&'a oracle::ColumnInfo>,
}

impl<'a> row::Field<'a, Oracle> for OciField<'a> {
    fn field_name(&self) -> Option<&'a str> {
        self.column_info.map(|c| c.name())
    }

    fn value(&self) -> Option<diesel::backend::RawValue<'a, Oracle>> {
        self.field_value.clone()
    }

    fn is_null(&self) -> bool {
        self.field_value.is_none()
    }
}
