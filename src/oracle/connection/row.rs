use std::rc::Rc;

use crate::oracle::backend::Oracle;
use diesel::row::{self, Row, RowIndex, RowSealed};

use super::oracle_value::OracleValue;

pub struct OciRow {
    row: InnerOciRow,
    column_infos: Rc<Vec<oracle::ColumnInfo>>,
}

enum InnerOciRow {
    Row(oracle::Row),
    Values(Vec<Option<OracleValue<'static>>>),
}

impl OciRow {
    pub fn new(row: oracle::Row, column_infos: Rc<Vec<oracle::ColumnInfo>>) -> Self {
        OciRow {
            row: InnerOciRow::Row(row),
            column_infos,
        }
    }

    pub fn new_from_value(values: Vec<Option<OracleValue<'static>>>) -> Self {
        Self {
            row: InnerOciRow::Values(values),
            column_infos: Rc::new(Vec::new()),
        }
    }
}

impl RowIndex<usize> for OciRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        if idx < self.row.len() {
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a> RowIndex<&'a str> for OciRow {
    fn idx(&self, field_name: &'a str) -> Option<usize> {
        self.column_infos
            .iter()
            .enumerate()
            .find(|(_, c)| c.name() == field_name)
            .map(|(idx, _)| idx)
    }
}

impl RowSealed for OciRow {}

impl<'a> Row<'a, Oracle> for OciRow {
    type InnerPartialRow = Self;
    type Field<'f>
        = OciField<'f>
    where
        'a: 'f,
        Self: 'f;

    fn field_count(&self) -> usize {
        self.row.len()
    }

    fn get<'row, I>(&'row self, idx: I) -> Option<OciField<'row>>
    where
        'a: 'row,
        Self: diesel::row::RowIndex<I>,
    {
        let idx = self.idx(idx)?;
        Some(OciField {
            field_value: self.row.value_at(idx, &self.column_infos),
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

    fn value(&self) -> Option<OracleValue<'_>> {
        self.field_value.clone()
    }

    fn is_null(&self) -> bool {
        self.field_value.is_none()
    }
}

impl InnerOciRow {
    fn value_at(&self, idx: usize, col_infos: &[oracle::ColumnInfo]) -> Option<OracleValue<'_>> {
        match self {
            InnerOciRow::Row(row) => {
                let sql = &row.sql_values()[idx];
                if sql.is_null().unwrap_or(true) {
                    None
                } else {
                    let tpe = col_infos[idx].oracle_type().clone();
                    Some(OracleValue::new(sql, tpe))
                }
            }
            InnerOciRow::Values(ref v) => v[idx].clone(),
        }
    }

    fn len(&self) -> usize {
        match self {
            InnerOciRow::Row(row) => row.sql_values().len(),
            InnerOciRow::Values(v) => v.len(),
        }
    }
}
