use crate::oracle::types::OciTypeMetadata;
use crate::oracle::OciDataType;
use crate::oracle::Oracle;
use diesel::query_builder::BindCollector;
use diesel::sql_types::HasSqlType;
use std::io::{self, Write};

pub struct OracleBindCollector {
    pub(crate) binds: Vec<(String, Box<dyn oracle::sql_type::ToSql>)>,
}

impl Default for OracleBindCollector {
    fn default() -> Self {
        Self { binds: Vec::new() }
    }
}

impl BindCollector<Oracle> for OracleBindCollector {
    fn push_bound_value<T, U>(&mut self, bind: &U, metadata_lookup: &()) -> diesel::QueryResult<()>
    where
        Oracle: HasSqlType<T>,
        U: diesel::serialize::ToSql<T, Oracle>,
    {
        let OciTypeMetadata { tpe: ty, handler } = Oracle::metadata(metadata_lookup);

        let out: Vec<u8> = Vec::new();

        let mut out = diesel::serialize::Output::<_, Oracle>::new(out, metadata_lookup);

        bind.to_sql(&mut out).unwrap();
        let len = self.binds.len();

        self.binds
            .push((format!("in{}", len), handler(out.into_inner())));

        Ok(())
    }
}
