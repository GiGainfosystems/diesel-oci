use diesel::expression::{AppearsOnTable, Expression};
use diesel::insertable::{
    BatchInsert, CanInsertInSingleQuery, ColumnInsertValue, InsertValues, Insertable,
    OwnedBatchInsert,
};
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::ValuesClause;
use diesel::query_source::Column;
use diesel::result::QueryResult;
use diesel::Table;

use super::backend::Oracle;

// please refer to https://stackoverflow.com/questions/39576/best-way-to-do-multi-row-insert-in-oracle

impl<'a, T, Tab, Inner> QueryFragment<Oracle> for BatchInsert<'a, T, Tab>
where
    &'a T: Insertable<Tab, Values = ValuesClause<Inner, Tab>>,
    ValuesClause<Inner, Tab>: QueryFragment<Oracle>,
    Inner: QueryFragment<Oracle> + InsertValues<Tab, Oracle>,
    Tab: Table,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        let mut records = self.records.iter().map(Insertable::values);
        if let Some(record) = records.next() {
            out.push_sql("(");
            record.values.column_names(out.reborrow())?;
            out.push_sql(") ");
            out.push_sql("select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        for record in records {
            out.push_sql(" union all select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        Ok(())
    }
}

impl<Tab, Inner> QueryFragment<Oracle> for OwnedBatchInsert<ValuesClause<Inner, Tab>>
where
    ValuesClause<Inner, Tab>: QueryFragment<Oracle>,
    Inner: QueryFragment<Oracle> + InsertValues<Tab, Oracle>,
    Tab: Table,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        let mut records = self.values.iter();
        if let Some(record) = records.next() {
            out.push_sql("(");
            record.values.column_names(out.reborrow())?;
            out.push_sql(") ");
            out.push_sql("select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        for record in records {
            out.push_sql(" union all select ");
            record.values.walk_ast(out.reborrow())?;
            out.push_sql(" from dual");
        }
        Ok(())
    }
}

impl<'a, T, Tab> CanInsertInSingleQuery<Oracle> for BatchInsert<'a, T, Tab> {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(self.records.len())
    }
}

impl<T> CanInsertInSingleQuery<Oracle> for OwnedBatchInsert<T> {
    fn rows_to_insert(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

impl<Col, Expr> QueryFragment<Oracle> for ColumnInsertValue<Col, Expr>
where
    Expr: QueryFragment<Oracle>,
{
    fn walk_ast(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        if let ColumnInsertValue::Expression(_, ref value) = *self {
            value.walk_ast(out.reborrow())?;
        } else {
            out.push_sql("DEFAULT");
        }
        Ok(())
    }
}

impl<Col, Expr> InsertValues<Col::Table, Oracle> for ColumnInsertValue<Col, Expr>
where
    Col: Column,
    Expr: Expression<SqlType = Col::SqlType> + AppearsOnTable<()>,
    Self: QueryFragment<Oracle>,
{
    fn column_names(&self, mut out: AstPass<Oracle>) -> QueryResult<()> {
        out.push_identifier(Col::NAME)?;
        Ok(())
    }
}
