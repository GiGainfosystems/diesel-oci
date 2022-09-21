use crate::oracle::types::OciTypeMetadata;
use crate::oracle::OciDataType;
use crate::oracle::Oracle;
use diesel::query_builder::BindCollector;
use diesel::sql_types::HasSqlType;
use std::ops::Deref;

#[derive(Default)]
pub struct OracleBindCollector<'a> {
    pub(crate) binds: Vec<(String, BindValue<'a>)>,
}

pub enum BindValue<'a> {
    Owned(Box<dyn oracle::sql_type::ToSql>),
    Borrowed(&'a dyn oracle::sql_type::ToSql),
    NotSet(OciDataType),
}

impl<'a> Deref for BindValue<'a> {
    type Target = dyn oracle::sql_type::ToSql + 'a;

    fn deref(&self) -> &Self::Target {
        match self {
            BindValue::Owned(b) => &**b,
            BindValue::Borrowed(b) => *b,
            BindValue::NotSet(d) => default_value(d),
        }
    }
}

fn default_value(d: &'_ OciDataType) -> &'static dyn oracle::sql_type::ToSql {
    match d {
        OciDataType::Bool | OciDataType::SmallInt | OciDataType::Integer | OciDataType::BigInt => {
            &oracle::sql_type::OracleType::Number(0, 0)
        }
        OciDataType::Float => &oracle::sql_type::OracleType::BinaryFloat,
        OciDataType::Double => &oracle::sql_type::OracleType::BinaryDouble,
        OciDataType::Text => &oracle::sql_type::OracleType::Varchar2(0),
        OciDataType::Binary => &oracle::sql_type::OracleType::BLOB,
        OciDataType::Date => &oracle::sql_type::OracleType::Date,
        OciDataType::Time => unimplemented!("No time support in the oracle crate yet"),
        OciDataType::Timestamp => &oracle::sql_type::OracleType::Timestamp(0),
    }
}

impl<'a> BindCollector<'a, Oracle> for OracleBindCollector<'a> {
    type Buffer = BindValue<'a>;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'a U,
        metadata_lookup: &mut (),
    ) -> diesel::QueryResult<()>
    where
        Oracle: HasSqlType<T>,
        U: diesel::serialize::ToSql<T, Oracle> + 'a,
    {
        let OciTypeMetadata { tpe: ty } = Oracle::metadata(metadata_lookup);

        let out = {
            let out = BindValue::NotSet(ty);
            let mut out = diesel::serialize::Output::<Oracle>::new(out, metadata_lookup);

            bind.to_sql(&mut out).unwrap();
            out.into_inner()
        };
        let len = self.binds.len();

        self.binds.push((format!("in{}", len), out));

        Ok(())
    }
}
