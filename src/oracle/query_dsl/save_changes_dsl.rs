use diesel::associations::HasTable;
use diesel::associations::Identifiable;
use diesel::dsl::Find;
use diesel::dsl::Update;
use diesel::query_builder::{AsChangeset, IntoUpdateTarget};
use diesel::query_dsl::methods::{ExecuteDsl, FindDsl};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;

use super::super::connection::OciConnection;

use diesel::query_builder::functions::update;
use diesel::query_dsl::UpdateAndFetchResults;

impl<Changes, Output> UpdateAndFetchResults<Changes, Output> for OciConnection
where
    Changes: Copy + Identifiable,
    Changes: AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget,
    Changes::Table: FindDsl<Changes::Id>,
    Update<Changes, Changes>: ExecuteDsl<OciConnection>,
    Find<Changes::Table, Changes::Id>: LoadQuery<OciConnection, Output>,
{
    fn update_and_fetch(&self, changeset: Changes) -> QueryResult<Output> {
        update(changeset).set(changeset).execute(self)?;
        Changes::table().find(changeset.id()).get_result(self)
    }
}
