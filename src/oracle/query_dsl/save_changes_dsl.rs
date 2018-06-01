use diesel::associations::HasTable;
use diesel::associations::Identifiable;
use diesel::dsl::Find;
use diesel::dsl::Update;
use diesel::query_builder::{AsChangeset, IntoUpdateTarget};
use diesel::query_dsl::methods::{ExecuteDsl, FindDsl};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::QueryResult;

use super::super::OciConnection;

use diesel::query_builder::functions::update;
use diesel::query_dsl::save_changes_dsl::InternalSaveChangesDsl;

impl<T, U> InternalSaveChangesDsl<OciConnection, U> for T
where
    T: Copy + Identifiable,
    T: AsChangeset<Target = <T as HasTable>::Table> + IntoUpdateTarget,
    T::Table: FindDsl<T::Id>,
    Update<T, T>: ExecuteDsl<OciConnection>,
    Find<T::Table, T::Id>: LoadQuery<OciConnection, U>,
{
    fn internal_save_changes(self, conn: &OciConnection) -> QueryResult<U> {
        update(self).set(self).execute(conn)?;
        T::table().find(self.id()).get_result(conn)
    }
}
