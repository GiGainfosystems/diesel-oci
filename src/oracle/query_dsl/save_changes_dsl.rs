use diesel::associations::HasTable;
use diesel::associations::Identifiable;
use diesel::dsl::Find;
use diesel::dsl::Update;
use diesel::query_builder::{AsChangeset, AsQuery, IntoUpdateTarget};
use diesel::query_dsl::methods::{ExecuteDsl, FindDsl};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;

use crate::oracle::connection::OciConnection;
use diesel::query_dsl::UpdateAndFetchResults;

impl<'query, Changes, Output> UpdateAndFetchResults<Changes, Output> for OciConnection
where
    Changes: Copy + Identifiable,
    Changes: AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget,
    Changes::Table: FindDsl<Changes::Id>,
    Update<Changes, Changes>: ExecuteDsl<OciConnection> + AsQuery,
    Find<Changes::Table, Changes::Id>: LoadQuery<'query, OciConnection, Output>,
{
    fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output> {
        diesel::update(changeset).set(changeset).execute(self)?;
        Changes::table().find(changeset.id()).get_result(self)
    }
}
