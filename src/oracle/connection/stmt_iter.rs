use diesel::QueryResult;

use super::row::OciRow;

pub struct RowIter {
    rows: Vec<OciRow>,
}

impl RowIter {
    pub(super) fn new(mut rows: Vec<OciRow>) -> Self {
        rows.reverse();
        Self { rows }
    }
}

impl Iterator for RowIter {
    type Item = QueryResult<OciRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.pop().map(Ok)
    }
}
