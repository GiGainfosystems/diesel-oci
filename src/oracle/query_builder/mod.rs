use super::backend::Oracle;

use diesel::query_builder::QueryBuilder;
use diesel::result::Error as DieselError;

mod insert_statement;

#[derive(Default)]
pub struct OciQueryBuilder {
    pub sql: String,
    bind_idx: u32,
}

impl OciQueryBuilder {
    pub fn new() -> Self {
        OciQueryBuilder {
            sql: String::new(),
            bind_idx: 0,
        }
    }
}

impl QueryBuilder<Oracle> for OciQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> Result<(), DieselError> {
        // TODO: check if there is a better way for escaping strings
        self.push_sql("\"");
        self.push_sql(&identifier.replace("`", "``").to_uppercase());
        self.push_sql("\"");
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        let sql = format!(":{}", self.bind_idx);
        self.push_sql(&sql);
    }

    fn finish(self) -> String {
        self.sql
    }
}



