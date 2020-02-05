extern crate chrono_time as chrono;
extern crate dotenv;
use self::chrono::{NaiveDateTime, Utc};
use self::dotenv::dotenv;
use super::oracle::connection::OciConnection;
use diesel::deserialize::{self, FromSql};
use diesel::result::Error;
use diesel::serialize::{self, ToSql};
use diesel::sql_types::SmallInt;
use diesel::Connection;
use diesel::RunQueryDsl;
use num::FromPrimitive;
use oracle::backend::Oracle;
use oracle::connection::OracleValue;
use oracle::query_dsl::OciReturningDsl;
use std::env;
use std::error::Error as StdError;
use std::io::Write;

fn init_testing() -> OciConnection {
    use super::logger::init;
    init();
    let database_url = database_url_from_env("OCI_DATABASE_URL");
    OciConnection::establish(&database_url).expect("No connection, no test!")
}

#[allow(dead_code)]
fn connection() -> OciConnection {
    let database_url = database_url_from_env("OCI_DATABASE_URL");
    OciConnection::establish(&database_url).unwrap()
}

fn database_url_from_env(backend_specific_env_var: &str) -> String {
    dotenv().ok();
    match env::var(backend_specific_env_var) {
        Ok(val) => val,
        _ => env::var("OCI_DATABASE_URL")
            .expect("OCI_DATABASE_URL must be set in order to run tests"),
    }
}

const CREATE_TEST_TABLE: &str = "CREATE TABLE test (\
                                 ID NUMBER(38), \
                                 TST_CHR VARCHAR(50),\
                                 TST_NUM NUMBER(38)\
                                 )";

const DROP_TEST_TABLE: &str = "DROP TABLE test";

const TEST_VARCHAR: &str = "'blabla'";

const CREATE_GST_TYPE_TABLE: &'static str = "CREATE TABLE gst_types (\
        big NUMBER(19),
        big2 NUMBER(19),
        small NUMBER(5),
        normal NUMBER(10),
        tz timestamp default sysdate,
        text clob,
        byte blob,
        d binary_double,
        r binary_float,
        v VARCHAR2(50)
    )";

macro_rules! assert_result {
    ($r:expr) => {{
        assert!($r.is_ok() && !$r.is_err(), format!("{:?}", $r.err()));
    }};
}

table! {
     test {
         id -> Nullable<BigInt>,
         TST_CHR -> Nullable<Text>,
         TST_NUM -> Nullable<BigInt>,
     }
}

table! {
    gst_types (big) {
        big -> Nullable<BigInt>,
        big2 -> Nullable<BigInt>,
        small -> Nullable<SmallInt>,
        normal -> Nullable<Integer>,
        tz -> Nullable<Timestamp>,
        text -> Nullable<VarChar>,
        byte -> Nullable<Binary>,
        d -> Nullable<Double>,
        r -> Nullable<Float>,
        v -> Nullable<VarChar>,
    }
}

const DROP_DIESEL_TABLE: &str = "DROP TABLE \"__DIESEL_SCHEMA_MIGRATIONS\"";

const CREATE_DIESEL_MIGRATIONS_TABLE: &str =
    "CREATE TABLE \"__DIESEL_SCHEMA_MIGRATIONS\" (\
     VERSION VARCHAR(50) PRIMARY KEY NOT NULL,\
     RUN_ON TIMESTAMP with time zone DEFAULT sysdate not null\
     )";

table! {
    __diesel_schema_migrations (version) {
        version -> VarChar,
        run_on -> Timestamp,
    }
}

fn create_test_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

fn create_gst_types_table(conn: &OciConnection) {
    drop_table(&conn, "GST_TYPES");
    let ret = conn.execute(CREATE_GST_TYPE_TABLE);
    assert_result!(ret);
}

fn drop_test_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(DROP_TEST_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

fn drop_diesel_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(DROP_DIESEL_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

#[allow(dead_code)]
fn execute_sql_or_rollback(conn: &OciConnection, sql: &str, rollback_sql: &str) -> usize {
    let ret = conn.execute(&*sql);
    if ret.is_err() {
        let inner = conn.execute(&*rollback_sql);
        assert_result!(inner)
    }
    assert_result!(ret);
    ret.unwrap()
}

fn clean_test(conn: &OciConnection) {
    let sql = "SELECT * FROM test";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let _ret = drop_test_table(conn);
    }
    let sql = "SELECT * FROM \"__DIESEL_SCHEMA_MIGRATIONS\"";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let _ret = drop_diesel_table(conn);
    }
}

fn drop_table(conn: &OciConnection, tbl: &str) {
    let sql = format!("SELECT * FROM {:?}", tbl);
    let sql = sql.replace("\"", "");
    let ret = conn.execute(&sql);
    if ret.is_ok() {
        let sql = format!("drop table {:?}", tbl);
        let _ret = conn.execute(&sql);
    }
}

#[test]
fn connect() {
    let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&database_url);

    assert_result!(conn);
}

#[test]
fn transaction_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 1);
}

#[test]
fn transaction_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn transaction_nested_rollback_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);
            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn transaction_nested_commit_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);
            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 2);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 2);
}

#[test]
fn transaction_nested_commit_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);
            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 1);
}

#[test]
fn transaction_nested_rollback_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);
            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 2);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn create_table() {
    let conn = init_testing();

    clean_test(&conn);

    let _u = create_test_table(&conn);
    let _u = drop_test_table(&conn);
}

#[test]
fn test_diesel_migration() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_DIESEL_MIGRATIONS_TABLE);
    assert_result!(ret);

    use self::__diesel_schema_migrations::dsl::*;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    let expected = vec!["00000000000000", "20151219180527", "20160107090901"];
    let migrations = expected.iter().map(|m| version.eq(*m)).collect::<Vec<_>>();

    let ret = ::diesel::insert_into(__diesel_schema_migrations)
        .values(&migrations)
        .execute(&conn);
    assert_result!(ret);

    let _already_run: HashSet<String> =
        self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
            .select(version)
            .load(&conn)
            .map(FromIterator::from_iter)
            .unwrap();

    let ret = self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
        .select(version)
        .load(&conn);
    let already_run: HashSet<String> = ret.map(FromIterator::from_iter).unwrap();

    let pending_migrations: Vec<_> = expected
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    assert_eq!(pending_migrations.len(), 0);
}

#[cfg(this_test_doesnt_work)]
#[test]
fn test_multi_insert() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_DIESEL_MIGRATIONS_TABLE);
    assert_result!(ret);

    use self::__diesel_schema_migrations::dsl::*;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    let migrations = vec![
        version.eq("00000000000000"),
        version.eq("20160107090901"),
        version.eq("20151219180527"),
    ];

    let ret = ::diesel::insert_into(__diesel_schema_migrations)
        .values(&migrations)
        .execute(&conn);

    assert_result!(ret);

    let migrations = vec!["00000000000000", "20160107090901", "20151219180527"];

    let already_run: HashSet<String> =
        self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
            .select(version)
            .order(version)
            .load(&conn)
            .map(FromIterator::from_iter)
            .unwrap();

    let pending_migrations: Vec<_> = migrations
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    assert_eq!(pending_migrations.len(), 0);
}

#[allow(dead_code)]
enum Way {
    Diesel,
    Native,
}

#[derive(Queryable, Clone, PartialEq)]
pub struct GSTTypes {
    pub big: Option<i64>,
    pub big2: Option<i64>,
    pub small: Option<i16>,
    pub normal: Option<i32>,
    pub tz: Option<NaiveDateTime>,
    pub text: Option<String>,
    pub byte: Option<Vec<u8>>,
    pub d: Option<f64>,
    pub r: Option<f32>,
    pub v: Option<String>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Insertable)]
#[table_name = "gst_types"]
pub struct Newgst_types {
    pub big: Option<i64>,
    pub big2: Option<i64>,
    pub small: Option<i16>,
    pub normal: Option<i32>,
    pub tz: Option<NaiveDateTime>,
    pub text: Option<String>,
    pub byte: Option<Vec<u8>>,
    pub d: Option<f64>,
    pub r: Option<f32>,
    pub v: Option<String>,
}

impl Newgst_types {
    pub fn new(
        big: Option<i64>,
        big2: Option<i64>,
        small: Option<i16>,
        normal: Option<i32>,
        text: Option<String>,
        byte: Option<Vec<u8>>,
        d: Option<f64>,
        r: Option<f32>,
        v: Option<String>,
    ) -> Newgst_types {
        Newgst_types {
            big,
            big2,
            small,
            normal,
            tz: None,
            text,
            byte,
            d,
            r,
            v,
        }
    }
}

#[test]
fn gst_compat() {
    // bigint -2^63 to 2^63-1 http://wiki.ispirer.com/sqlways/postgresql/data-types/bigint // 12 byte
    // smallint -2^15 to 2^15-1 http://wiki.ispirer.com/sqlways/postgresql/data-types/smallint
    // timestamp
    // text
    // int -2147483647 2147483647 -2^31 to 2^31-1 http://wiki.ispirer.com/sqlways/postgresql/data-types/int
    // varchar
    // bytea
    // double precision 1E-307 to 1E+308 http://wiki.ispirer.com/sqlways/postgresql/data-types/double_precision
    // boolean
    // jsonb
    // real	1E-37 to 1E+37 http://wiki.ispirer.com/sqlways/postgresql/data-types/real

    // https://docs.oracle.com/cd/B19306_01/gateways.102/b14270/apa.htm

    let conn = init_testing();
    create_gst_types_table(&conn);

    use self::gst_types::columns::{big, big2, byte, d, normal, r, small, v};
    use self::gst_types::dsl::gst_types;
    use diesel::dsl::sql;
    use diesel::sql_types::{BigInt, Double, Float, Integer, SmallInt, Text};
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use std::{i16, i32, i64};

    let way_to_try = Way::Diesel;
    match way_to_try {
        Way::Native => {
            let neg_base: i64 = -2;
            let base: i128 = 2;

            let sqls = format!(
                "INSERT INTO gst_types ({}) VALUES ({},{},{},{},{}d,{},{})",
                "big, small, normal, text, d, r, v",
                neg_base.pow(63),
                neg_base.pow(15),
                neg_base.pow(31),
                "'text'",
                "1e-307",
                "1e-37",
                "'test'"
            );
            let ret = conn.execute(&*sqls);
            assert_result!(ret);
            let sqls = format!(
                "INSERT INTO gst_types ({}) VALUES ({},{},{},{},{}d,{},{})",
                "big, small, normal, text, d, r, v",
                base.pow(63) - 1,
                base.pow(15) - 1,
                base.pow(31) - 1,
                "'text'",
                "1e308",
                "1e37",
                "'test'"
            );
            let ret = conn.execute(&*sqls);
            assert_result!(ret);

            let sqls = "SELECT big, small, normal, d, r, v from gst_types";
            let ret = sql::<(BigInt, SmallInt, Integer, Double, Float, Text)>(sqls).load::<(
                i64,
                i16,
                i32,
                f64,
                f32,
                String,
            )>(
                &conn
            );
            assert_result!(ret);
            let val = ret.unwrap();
            assert_eq!(val.len(), 3);

            assert_eq!(val[0].0, i64::MIN);
            assert_eq!(val[1].0, i64::MAX);
            assert_eq!(val[0].1, i16::MIN);
            assert_eq!(val[1].1, i16::MAX);
            assert_eq!(val[0].2, i32::MIN);
            assert_eq!(val[1].2, i32::MAX);
            assert_eq!(val[0].3, 1e-307f64);
            assert_eq!(val[1].3, 1e308f64);
            assert_eq!(val[0].4, 1e-37f32);
            assert_eq!(val[1].4, 1e37f32);
            assert_eq!(val[0].5, "test");
            assert_eq!(val[1].5, "test");
        }
        Way::Diesel => {
            let mut bin: Vec<u8> = Vec::new();
            for i in 0..88 {
                bin.push(i as u8 % 128u8);
            }

            let _new_row = Newgst_types::new(
                Some(i64::MIN),
                Some(i64::MAX),
                Some(i16::MIN),
                Some(i32::MAX),
                Some("T".to_string()),
                Some(bin),
                Some(1e-307f64),
                Some(1e-37f32),
                Some("Te".to_string()),
            );

            //            let new_row = ::diesel::insert_into(gst_types)
            //                .values(&new_row)
            //                .get_results::<GSTTypes>(&conn);
            //            assert_result!(new_row);

            let mut bin: Vec<u8> = Vec::new();
            for i in 0..88 {
                bin.push(i as u8 % 128u8);
            }

            let new_row = (
                big.eq(i64::MIN),
                big2.eq(i64::MIN),
                small.eq(i16::MIN),
                normal.eq(i32::MIN),
                v.eq("t"),
                d.eq(1e-307f64),
                r.eq(1e-37f32),
                byte.eq(bin),
            );
            let ret = ::diesel::insert_into(gst_types)
                .values(&new_row)
                .execute(&conn);
            assert_result!(ret);

            let new_row = (
                big.eq(i64::MAX),
                big2.eq(i64::MAX),
                small.eq(i16::MAX),
                normal.eq(i32::MAX),
                v.eq("test"),
                d.eq(1e308f64),
                r.eq(1e37f32),
            );
            let ret = ::diesel::insert_into(gst_types)
                .values(&new_row)
                .execute(&conn);
            assert_result!(ret);

            let ret = ::diesel::insert_into(gst_types)
                .values(big.eq::<Option<i64>>(None))
                .execute(&conn);
            assert_result!(ret);

            let ret: Result<
                Vec<(
                    Option<i64>,
                    Option<i16>,
                    Option<i32>,
                    Option<f64>,
                    Option<f32>,
                    Option<String>,
                    Option<Vec<u8>>,
                )>,
                Error,
            > = gst_types
                .select((big, small, normal, d, r, v, byte))
                .load(&conn);
            assert_result!(ret);
            let val = ret.unwrap();
            assert_eq!(val.len(), 3);
            // value should not be null
            assert_ne!(val[0].0, None);
            assert_eq!(val[0].0, Some(i64::MIN));
            assert_ne!(val[1].0, None);
            assert_eq!(val[1].0, Some(i64::MAX));
            assert_ne!(val[0].1, None);
            assert_eq!(val[0].1, Some(i16::MIN));
            assert_ne!(val[1].1, None);
            assert_eq!(val[1].1, Some(i16::MAX));
            assert_ne!(val[0].2, None);
            assert_eq!(val[0].2, Some(i32::MIN));
            assert_ne!(val[1].2, None);
            assert_eq!(val[1].2, Some(i32::MAX));
            assert_ne!(val[0].3, None);
            assert_eq!(val[0].3, Some(1e-307f64));
            assert_ne!(val[1].3, None);
            assert_eq!(val[1].3, Some(1e308f64));
            assert_ne!(val[0].4, None);
            assert_eq!(val[0].4, Some(1e-37f32));
            assert_ne!(val[1].4, None);
            assert_eq!(val[1].4, Some(1e37f32));
            assert_ne!(val[0].5, None);
            assert_eq!(val[0].5, Some("t".to_string()));
            assert_ne!(val[1].5, None);
            assert_eq!(val[1].5, Some("test".to_string()));
            assert_eq!(val[2].0, None);
            assert_eq!(val[2].1, None);
            assert_eq!(val[2].2, None);
            assert_eq!(val[2].3, None);
            assert_eq!(val[2].4, None);
            assert_eq!(val[2].5, None);

            assert_ne!(val[0].6, None);
            assert_eq!(val[1].6, None);
            assert_eq!(val[2].6, None);

            let ret: Result<
                Vec<(
                    Option<i64>,
                    Option<i16>,
                    Option<i32>,
                    Option<f64>,
                    Option<f32>,
                    Option<String>,
                    Option<Vec<u8>>,
                )>,
                Error,
            > = gst_types
                .filter(big.eq(i64::MAX))
                .select((big, small, normal, d, r, v, byte))
                .load(&conn);
            assert_result!(ret);
        }
    }
}

table! {
    /// all elements which have been created
    elements {
        /// a unique identifier
        id -> BigInt,
        /// a label to help the user
        label -> Text,
        /// an arbitrary comment which can be NULL
        comment -> Nullable<Text>,
        /// the user which created the element
        owner_id -> Integer,
        /// to which level does the element belong
        level_id -> SmallInt,
    }

}

table! {
    /// a table containing the nodelinks, either to an element or feature
    node_links {
        /// unique identifier
        id -> BigInt,
        /// reference id which can be either an element or feature
        ref_id -> BigInt,
        /// describing what kind of link we have above
        target_type -> SmallInt,
        /// the parent of the above nodelink id
        parent_id -> BigInt,
        /// the user who created the nodelink
        owner_id -> Integer,
    }
}

joinable!(node_links -> elements(ref_id));

table! {
    /// all the property descriptions which have been created
    element_properties {
        /// a unique identifier
        id -> BigInt,
        /// the property type which is constrained on database side
        /// to be either bool, int, double, text
        property_type -> SmallInt,
        /// name of the property
        name -> Text,
        /// user which created the property
        owner_id -> Integer,
    }
}

table! {
    /// The table containing the special granted rights for moma elements
    element_rights {
        /// The id of the additional right
        ///
        /// This column is currently used to determine which right applies in
        /// case of multiple alternatives. The newest right, that one with the
        /// biggest id will determine the right for a given  moma element
        id -> BigInt,
        /// The corresponding moma elements id
        element_id -> BigInt,
        /// The corresponding group id
        group_id -> Nullable<Integer>,
        /// The corresponding user id
        user_id -> Nullable<Integer>,
        /// The granted right
        access_level -> SmallInt,
    }
}

table! {
    /// The table containing the special granted rights for element properties (moma)
    element_property_rights {
        /// The id of the additional right
        ///
        /// This column is currently used to determine which right applies in
        /// case of multiple alternatives. The newest right, that one with the
        /// biggest id will determine the right for a given element property (moma)
        id -> BigInt,
        /// The corresponding element property (moma) id
        element_property_id -> BigInt,
        /// The corresponding group id
        group_id -> Nullable<Integer>,
        /// The corresponding user id
        user_id -> Nullable<Integer>,
        /// The granted right
        access_level -> SmallInt,
    }
}

allow_tables_to_appear_in_same_query!(elements, element_rights, node_links);

#[test]
fn moma_elem() {
    let conn = init_testing();

    use diesel::BoolExpressionMethods;
    use diesel::ExpressionMethods;
    use diesel::GroupByDsl;
    use diesel::QueryDsl;

    let groupby = (
        elements::id,
        elements::label,
        elements::comment,
        elements::owner_id,
        elements::level_id,
    );

    let ret = conn.execute("alter session set \"_optimizer_reduce_groupby_key\" = false");
    assert_result!(ret);

    let ret = conn.begin_test_transaction();
    assert_result!(ret);

    let k: Result<Vec<(i64, String, Option<String>, i32, i16, i64)>, _> = elements::table
        .left_join(node_links::table)
        .group_by(groupby)
        .filter(elements::level_id.eq(1))
        .filter(elements::owner_id.eq(22))
        .filter(
            node_links::target_type
                .eq(0)
                .or(node_links::target_type.is_null()),
        )
        //.filter(::diesel::dsl::sql("1=1 GROUP BY ELEMENTS.ID, ELEMENTS.LABEL, ELEMENTS.\"COMMENT\", ELEMENTS.OWNER_ID, ELEMENTS.LEVEL_ID"))
        .select((
            elements::id,
            elements::label,
            elements::comment,
            elements::owner_id,
            elements::level_id,
            ::diesel::dsl::sql::<::diesel::sql_types::BigInt>(
                "CAST(COUNT(node_links.parent_id) as NUMBER(19))",
            ),
        ))
        .order(elements::label.asc())
        .load(&conn);
    assert_result!(k);

    //let tm = conn.transaction_manager();
    //let ret = tm.rollback_transaction(&conn);
    //assert_result!(ret);
}

#[test]
fn limit() {
    let conn = init_testing();
    create_gst_types_table(&conn);

    use self::gst_types::columns::{big, big2, byte, d, normal, r, small, v};
    use self::gst_types::dsl::gst_types;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use std::{i16, i32, i64};

    let mut bin: Vec<u8> = Vec::new();
    for i in 0..310 {
        bin.push(i as u8 % 128u8);
    }

    let _new_row = Newgst_types::new(
        Some(i64::MIN),
        Some(i64::MAX),
        Some(i16::MIN),
        Some(i32::MAX),
        Some("T".to_string()),
        Some(bin),
        Some(1e-307f64),
        Some(1e-37f32),
        Some("Te".to_string()),
    );

    //            let new_row = ::diesel::insert_into(gst_types)
    //                .values(&new_row)
    //                .get_results::<GSTTypes>(&conn);
    //            assert_result!(new_row);

    let mut bin: Vec<u8> = Vec::new();
    for i in 0..88 {
        bin.push(i as u8 % 128u8);
    }

    let new_row = (
        big.eq(i64::MIN),
        big2.eq(i64::MIN),
        small.eq(i16::MIN),
        normal.eq(i32::MIN),
        v.eq("t"),
        d.eq(1e-307f64),
        r.eq(1e-37f32),
        byte.eq(bin),
    );
    let ret = ::diesel::insert_into(gst_types)
        .values(&new_row)
        .execute(&conn);
    assert_result!(ret);

    let new_row = (
        big.eq(i64::MAX),
        big2.eq(i64::MAX),
        small.eq(i16::MAX),
        normal.eq(i32::MAX),
        v.eq("test"),
        d.eq(1e308f64),
        r.eq(1e37f32),
    );
    let ret = ::diesel::insert_into(gst_types)
        .values(&new_row)
        .execute(&conn);
    assert_result!(ret);

    let ret = ::diesel::insert_into(gst_types)
        .values(big.eq::<Option<i64>>(None))
        .execute(&conn);
    assert_result!(ret);

    let ret: Result<
        (
            Option<i64>,
            Option<i16>,
            Option<i32>,
            Option<f64>,
            Option<f32>,
            Option<String>,
            Option<Vec<u8>>,
        ),
        Error,
    > = gst_types
        .select((big, small, normal, d, r, v, byte))
        .first(&conn);
    assert_result!(ret);
}

#[derive(Debug)]
pub struct InvalidEnumValueError<T>(pub T);

impl<T> ::std::error::Error for InvalidEnumValueError<T>
where
    T: ::std::fmt::Display + ::std::fmt::Debug,
{
    fn description(&self) -> &str {
        "Invalid enum value"
    }
}

impl<T> ::std::fmt::Display for InvalidEnumValueError<T>
where
    T: ::std::fmt::Display,
{
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "Invalid enum value {}", self.0)
    }
}

pub fn make_err<E>(e: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(e)
}

/// The type of a coordinate system
#[derive(FromPrimitive, Debug, PartialEq, Clone, Copy, FromSqlRow, AsExpression)]
#[sql_type = "SmallInt"]
pub enum CoordinateSystemType {
    /// The coordinate system is cartesian,
    /// that means there are 3 perpendicular axes
    Cartesian = 0,
    /// The coordinate system is spherical, which means
    /// there are two angle-axes(latitude, longitude) and one even axis (height)
    Spherical = 1,
    /// The coordinate system is cylindrical, which means
    /// there are one angle-axis and two even axes
    Cylindrical = 2,
}

impl ToSql<SmallInt, Oracle> for CoordinateSystemType {
    fn to_sql<W: Write>(&self, out: &mut serialize::Output<W, Oracle>) -> serialize::Result {
        <i16 as ToSql<SmallInt, _>>::to_sql(&(*self as i16), out)
    }
}

impl FromSql<SmallInt, Oracle> for CoordinateSystemType {
    fn from_sql(bytes: Option<OracleValue<'_>>) -> deserialize::Result<Self> {
        let value = <i16 as FromSql<SmallInt, Oracle>>::from_sql(bytes)?;
        CoordinateSystemType::from_i16(value).ok_or_else(|| {
            error!("Invalid value for coordinate system type found: {}", value);
            make_err(InvalidEnumValueError(value))
        })
    }
}

table! {
    /// A database table containing all coordinate system definitions
    coordinate_system_descriptions {
        /// A unique id for a coordinate system
        id -> Integer,
        /// The name of the coordinate system
        name -> Text,
        /// The label applied to the first axis
        c1_label -> Text,
        /// Is the first axis reversed, or not
        c1_reversed -> Bool,
        /// The unit of the first axis, if avaible
        c1_unit -> Nullable<Text>,
        /// The label applied to the second axis
        c2_label -> Text,
        /// Is the second axis reversed, or not
        c2_reversed -> Bool,
        /// The unit of the second axis, if avaible
        c2_unit -> Nullable<Text>,
        /// The label applied to the third axis
        c3_label -> Text,
        /// Is the third axis reversed, or not
        c3_reversed -> Bool,
        /// The unit of the third axis, if avaible
        c3_unit -> Nullable<Text>,
        /// The type of the coordinate system
        srs_type -> SmallInt,
    }
}

/// A struct representing a coordinate system definition from the database
#[derive(Queryable, Debug, Clone, PartialEq)]
pub struct CoordinateSystemDescription {
    /// The unique id of the coordinate system definition
    pub id: i32,
    name: String,
    c1_label: String,
    c1_reversed: bool,
    c1_unit: Option<String>,
    c2_label: String,
    c2_reversed: bool,
    c2_unit: Option<String>,
    c3_label: String,
    c3_reversed: bool,
    c3_unit: Option<String>,
    srs_type: CoordinateSystemType,
}

#[test]
fn coordinatesys() {
    let conn = init_testing();

    use self::coordinate_system_descriptions::columns::{
        c1_label, c1_reversed, c1_unit, c2_label, c2_reversed, c2_unit, c3_label, c3_reversed,
        c3_unit, id, name, srs_type,
    };
    use self::coordinate_system_descriptions::dsl::coordinate_system_descriptions;
    use diesel::QueryDsl;

    let coord: Result<
        Vec<(
            i32,
            String,
            String,
            bool,
            Option<String>,
            String,
            bool,
            Option<String>,
            String,
            bool,
            Option<String>,
            CoordinateSystemType,
        )>,
        _,
    > = coordinate_system_descriptions
        .select((
            id,
            name,
            c1_label,
            c1_reversed,
            c1_unit,
            c2_label,
            c2_reversed,
            c2_unit,
            c3_label,
            c3_reversed,
            c3_unit,
            srs_type,
        ))
        .load(&conn);

    assert_result!(coord);
}

table! {
    t1 {
        id -> Integer,
        name -> Nullable<Text>,
        bol -> Bool,
        t2 -> Text,
        bin -> Binary,
        si -> SmallInt,
    }
}

table! {
    t2 {
        id -> Integer,
        name -> Text,
    }
}

joinable!(t1 -> t2(id));
allow_tables_to_appear_in_same_query!(t1, t2);

#[test]
fn ambigious_col_names() {
    const CREATE_T1: &'static str = "CREATE TABLE t1 (\
            id NUMBER(10),
            name VARCHAR2(50),
            bol NUMBER(5) DEFAULT 0 NOT NULL,
            t2 VARCHAR2(50),
            bin blob,
            si NUMBER(5)
     )";
    const CREATE_T2: &'static str = "CREATE TABLE t2 (\
            id NUMBER(10),
            name VARCHAR2(50)
     )";

    let conn = init_testing();

    drop_table(&conn, "T1");
    drop_table(&conn, "T2");

    let ret = conn.execute(CREATE_T1);
    assert_result!(ret);
    let ret = conn.execute(CREATE_T2);
    assert_result!(ret);

    use self::t1;
    use self::t2;
    use diesel::ExpressionMethods;
    use diesel::JoinOnDsl;
    use diesel::QueryDsl;
    use oracle::query_builder::Alias;

    let mut bin: Vec<u8> = Vec::new();
    for i in 0..88 {
        bin.push(i as u8 % 128u8);
    }

    let new_row = (
        t1::id.eq(1),
        t1::name.eq("test1"),
        t1::bol.eq(true),
        t1::t2.eq("tryme "),
        t1::bin.eq(bin),
        t1::si.eq(2),
    );
    let ret = ::diesel::insert_into(t1::table)
        .values(&new_row)
        .execute(&conn);
    assert_result!(ret);
    let new_row = (t2::id.eq(1), t2::name.eq("test2"));
    let ret = ::diesel::insert_into(t2::table)
        .values(&new_row)
        .execute(&conn);
    assert_result!(ret);

    let col = t1::name.alias("da".to_string());
    let ambigious: Result<(i32, i32, Option<String>, String, bool, String, Vec<u8>, i16), _> =
        t1::table
            .filter(t2::id.eq(1))
            .inner_join(t2::table.on(t1::id.eq(t2::id)))
            .select((
                t1::id,
                t2::id.alias("a".to_string()),
                col,
                t2::name,
                t1::bol,
                t1::t2,
                t1::bin,
                t1::si,
            ))
            .limit(1) // this is the crucial part!
            .first(&conn);

    assert_result!(ambigious);
}

table! {
    ts {
        id -> Integer,
        tis -> Timestamp,
    }
}

#[test]
fn timestamp() {
    const CREATE_TS: &'static str = "CREATE TABLE TS (\
            id NUMBER(10),
            tis TIMESTAMP
     )";

    let conn = init_testing();

    drop_table(&conn, "TS");

    let ret = conn.execute(CREATE_TS);
    assert_result!(ret);

    use self::ts;
    use diesel::ExpressionMethods;

    let n = Utc::now().naive_utc();

    let new_row = (ts::id.eq(1), ts::tis.eq(n));
    let query = ::diesel::insert_into(ts::table).values(&new_row);
    let ret = query.execute(&conn);
    assert_result!(ret);

    let ret: Result<Vec<(i32, NaiveDateTime)>, _> = ts::table.load(&conn);
    assert_result!(ret);
}

table! {
    clobber {
        id -> Integer,
        tiss -> Text,
        tis -> Text,
    }
}

#[test]
fn clob() {
    const CREATE_CLOBBER: &'static str = "CREATE TABLE CLOBBER (\
            id NUMBER(10),
            tiss VARCHAR2(50),
            tis CLOB
     )";

    let conn = init_testing();

    drop_table(&conn, "CLOBBER");

    let ret = conn.execute(CREATE_CLOBBER);
    assert_result!(ret);

    use self::clobber;
    use diesel::ExpressionMethods;

    let new_row = (
        clobber::id.eq(1),
        clobber::tiss.eq("This is a varcharThis is a varchar"),
        clobber::tis.eq("This is a test"),
    );
    let query = ::diesel::insert_into(clobber::table).values(&new_row);
    let ret = query.execute(&conn);
    assert_result!(ret);

    let ret: Result<Vec<(i32, String, String)>, _> = clobber::table.load(&conn);
    assert_result!(ret);
}

table! {
    props {
        id -> Integer,
        is_based -> Nullable<Bool>,
    }
}

table! {
    properties{
        id -> BigInt,
        name -> Text,
        is_vertex_based -> Bool,
        property_type -> SmallInt,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        feature_class -> BigInt,
    }
}

#[derive(FromPrimitive, Debug, PartialEq, Clone, Copy, Eq, Hash, FromSqlRow, AsExpression)]
#[sql_type = "SmallInt"]
pub enum PropertyDataType {
    Int = 0,
    Float = 1,
    Bool = 2,
    String = 3,
}

impl ToSql<SmallInt, Oracle> for PropertyDataType {
    fn to_sql<W: Write>(&self, out: &mut serialize::Output<W, Oracle>) -> serialize::Result {
        <i16 as ToSql<SmallInt, _>>::to_sql(&(*self as i16), out)
    }
}

impl FromSql<SmallInt, Oracle> for PropertyDataType {
    fn from_sql(bytes: Option<OracleValue<'_>>) -> deserialize::Result<Self> {
        let value = <i16 as FromSql<SmallInt, Oracle>>::from_sql(bytes)?;
        PropertyDataType::from_i16(value).ok_or_else(|| {
            error!("Invalid value for property data type found: {}", value);
            make_err(InvalidEnumValueError(value))
        })
    }
}

#[derive(PartialEq, Hash, Eq, Debug, Clone, Queryable, Associations, Identifiable)]
#[table_name = "properties"]
pub struct Property {
    pub id: i64,
    pub name: String,
    pub is_vertex_based: bool,
    pub property_type: PropertyDataType,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub feature_class: i64,
}

#[test]
fn props() {
    let conn = init_testing();
    use self::properties::dsl::*;
    use diesel::debug_query;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    let ids = 4;
    let query = properties.filter(feature_class.eq(ids));
    let dbg = debug_query::<Oracle, _>(&query);
    println!("{:?}", dbg.to_string());
    let ret: Result<Vec<Property>, _> = query.load(&conn);
    assert_result!(ret);

    let query = properties.filter(feature_class.eq(ids));
    let dbg = debug_query::<Oracle, _>(&query);
    println!("{:?}", dbg.to_string());
    let ret: Result<Vec<Property>, _> = query.load(&conn);
    assert_result!(ret);

    let query = properties.filter(feature_class.eq(ids));
    let dbg = debug_query::<Oracle, _>(&query);
    println!("{:?}", dbg.to_string());
    let ret: Result<Vec<Property>, _> = query.load(&conn);
    assert_result!(ret);

    let query = properties.filter(feature_class.eq(ids));
    let dbg = debug_query::<Oracle, _>(&query);
    println!("{:?}", dbg.to_string());
    let ret: Result<Vec<Property>, _> = query.load(&conn);
    assert_result!(ret);
}

#[test]
fn props_orig() {
    const CREATE_TESTT: &'static str = "CREATE TABLE PROPS (\
            id NUMBER(10),
            is_based NUMBER(5)
     )";

    let conn = init_testing();

    drop_table(&conn, "PROPS");

    let ret = conn.execute(CREATE_TESTT);
    assert_result!(ret);

    use self::props;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    let new_row = (props::id.eq(1), props::is_based.eq(true));
    let query = ::diesel::insert_into(props::table).values(&new_row);
    let ret = query.execute(&conn);
    assert_result!(ret);

    let new_row = (props::id.eq(2), props::is_based.eq(false));
    let query = ::diesel::insert_into(props::table).values(&new_row);
    let ret = query.execute(&conn);
    assert_result!(ret);

    let new_row = (props::id.eq(3),);
    let query = ::diesel::insert_into(props::table).values(&new_row);
    let ret = query.execute(&conn);
    assert_result!(ret);

    let ret: Result<Vec<(i32, Option<bool>)>, _> = props::table.load(&conn);
    assert_result!(ret);
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 3);
    assert_eq!(ret[0].1, Some(true));
    assert_eq!(ret[1].1, Some(false));
    assert_eq!(ret[2].1, None);

    let ret: Result<Vec<(i32, Option<bool>)>, _> = props::table.filter(props::id.eq(2)).load(&conn);
    assert_result!(ret);
    let ret = ret.unwrap();
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0].1, Some(false));
}

table! {
    /// all tables
    all_tables (owner, table_name) {
        /// owner
        owner -> Text,
        /// table name
        table_name -> Text,
    }
}

#[test]
fn systable() {
    let conn = init_testing();

    use self::all_tables;

    let ret: Result<Vec<(String, String)>, _> = all_tables::table.load(&conn);
    assert_result!(ret);
    let _ = ret.unwrap();
}

#[test]
fn exists() {
    let conn = init_testing();

    use self::all_tables;

    use diesel::dsl::exists;
    use diesel::query_dsl::filter_dsl::FilterDsl;
    use diesel::query_dsl::select_dsl::SelectDsl;
    use diesel::ExpressionMethods;

    let ret = diesel::select(exists(
        all_tables::table
            .filter(all_tables::table_name.eq("GEOMETRIES"))
            .select(all_tables::owner),
    ))
    .get_result::<bool>(&conn);
    assert_result!(ret);
    let ret = ret.unwrap(); // has been asserted before ;)
    assert_eq!(ret, true);

    let ret = diesel::select(exists(
        all_tables::table
            .filter(all_tables::owner.eq("dieasel"))
            .select(all_tables::owner),
    ))
    .get_result::<bool>(&conn);
    assert_result!(ret);
    let ret = ret.unwrap(); // has been asserted before ;)
    assert_eq!(ret, false);
}

#[test]
fn transaction_nested_rollback_rollback_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<i32, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Err(Error::NotFound)
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn transaction_nested_rollback_rollback_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<_, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Ok(())
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 3);

            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn transaction_nested_commit_commit_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<_, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Ok(())
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 3);

            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 3);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 3);
}

#[test]
fn transaction_nested_commit_commit_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<i32, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Err(Error::NotFound)
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 2);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 2);
}

#[test]
fn transaction_nested_commit_rollback_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<i32, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Err(Error::NotFound)
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 1);
}

#[test]
fn transaction_nested_rollback_commit_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<_, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Ok(())
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 3);

            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 3);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn transaction_nested_commit_rollback_commit() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<i32, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<_, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Ok(())
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 3);

            Err(Error::NotFound)
        });
        assert!(out_inner.is_err() && !out_inner.is_ok(), "What :shrug:?");
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 1);
}

#[test]
fn transaction_nested_rollback_commit_rollback() {
    let conn = init_testing();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);

        let out_inner = conn.transaction::<_, Error, _>(|| {
            let sql_inner = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
            let _ret_inner = conn.execute(&*sql_inner)?;
            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            let _inner_inner = conn.transaction::<i32, Error, _>(|| {
                let sql_inner_inner =
                    format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
                let _ret_inner_inner = conn.execute(&*sql_inner_inner)?;
                let ret_inner_inner =
                    self::test::dsl::test
                        .load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
                assert_eq!(ret_inner_inner.len(), 3);
                Err(Error::NotFound)
            });

            let ret_inner =
                self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
            assert_eq!(ret_inner.len(), 2);

            Ok(())
        });
        assert_result!(out_inner);
        let ret =
            self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 2);

        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

const CREATE_GEOMETRIES: &str =
    "CREATE TABLE geometries( \
     id NUMBER(19) GENERATED BY DEFAULT as IDENTITY(START with 1 INCREMENT by 1) PRIMARY KEY, \
     name VARCHAR2(2000) NOT NULL, \
     geometry_type NUMBER(5) NOT NULL, \
     created_at TIMESTAMP DEFAULT sysdate NOT NULL, \
     updated_at TIMESTAMP DEFAULT sysdate NOT NULL, \
     bbox NUMBER(19) NOT NULL, \
     origin NUMBER(19) NOT NULL, \
     feature_class NUMBER(19) NOT NULL, \
     CONSTRAINT feature_name_uniq_in_fc UNIQUE (feature_class, name) \
     )";

const DROP_GEOMETRIES: &str = "drop table geometries cascade constraints";

table! {
    geometries {
        id -> BigInt,
        name -> Text,
        geometry_type -> SmallInt,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        bbox -> BigInt,
        origin -> BigInt,
        feature_class -> BigInt,
    }
}

#[test]
fn updateing_unique_constraint() {
    let conn = init_testing();

    clean_test(&conn);

    let sql = "SELECT * FROM geometries";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let _ret = conn.execute(DROP_GEOMETRIES);
    }
    let ret = conn.execute(CREATE_GEOMETRIES);
    assert_result!(ret);

    use self::geometries;
    use diesel::query_dsl::filter_dsl::FindDsl;
    use diesel::ExpressionMethods;

    let n = Utc::now().naive_utc();
    let n2 = Utc::now().naive_utc();
    let new_row = (
        geometries::name.eq("test"),
        geometries::geometry_type.eq(1),
        geometries::created_at.eq(n),
        geometries::updated_at.eq(n2),
        geometries::bbox.eq(1),
        geometries::origin.eq(1),
        geometries::feature_class.eq(1),
    );
    let ret = ::diesel::insert_into(geometries::table)
        .values(new_row)
        .execute(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap(), 1);

    let new_row = (
        geometries::name.eq("testa"),
        geometries::geometry_type.eq(1),
        geometries::created_at.eq(n),
        geometries::updated_at.eq(n2),
        geometries::bbox.eq(1),
        geometries::origin.eq(1),
        geometries::feature_class.eq(1),
    );
    let ret = ::diesel::insert_into(geometries::table)
        .values(new_row)
        .execute(&conn);
    assert_result!(ret);

    let new_name = "test";
    let ret = ::diesel::update(geometries::table.find(1))
        .set(geometries::name.eq(new_name))
        .execute(&conn);
    assert_result!(ret);
}

#[test]
fn insert_returning() {
    let conn = init_testing();

    clean_test(&conn);

    let sql = "SELECT * FROM geometries";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let _ret = conn.execute(DROP_GEOMETRIES);
    }
    let ret = conn.execute(CREATE_GEOMETRIES);
    assert_result!(ret);

    use self::geometries;
    use diesel::ExpressionMethods;

    let n = Utc::now().naive_utc();
    let n2 = Utc::now().naive_utc();
    let new_row = (
        geometries::name.eq("test"),
        geometries::geometry_type.eq(1),
        geometries::created_at.eq(n),
        geometries::updated_at.eq(n2),
        geometries::bbox.eq(1),
        geometries::origin.eq(1),
        geometries::feature_class.eq(1),
    );
    use diesel::QueryResult;

    let ret: QueryResult<(
        i64,
        String,
        i16,
        NaiveDateTime,
        NaiveDateTime,
        i64,
        i64,
        i64,
    )> = ::diesel::insert_into(geometries::table)
        .values(new_row)
        .oci_returning()
        .get_result(&conn);
    assert_result!(ret);
    let ret = ret.unwrap(); // asserted above ;)
    assert_eq!(ret.1, "test");
    assert_eq!(ret.2, 1);
    // just don't compare the dates, they are off by a tenth of a second:
    //   left: `2018-09-10T23:34:43`,
    // right: `2018-09-10T23:34:43.856998961`', src/test/mod.rs:1998:5
    //assert_eq!(ret.3, n);
    //assert_eq!(ret.4, n2);
    assert_eq!(ret.5, 1);
    assert_eq!(ret.6, 1);
    assert_eq!(ret.7, 1);
}

#[test]
fn insert_returning_with_nulls() {
    use self::test;
    use diesel::ExpressionMethods;
    let conn = init_testing();
    clean_test(&conn);
    create_test_table(&conn);
    type ResultType = ::diesel::QueryResult<(Option<i64>, Option<String>, Option<i64>)>;
    let result: ResultType = ::diesel::insert_into(test::table)
        .values(test::id.eq(1))
        .oci_returning()
        .get_result(&conn);
    assert_result!(result);
    let result = result.unwrap();
    assert_eq!(result.0, Some(1));
    assert_eq!(result.1, None);
    assert_eq!(result.2, None);
    drop_test_table(&conn);
}

#[test]
fn umlauts() {
    let conn = init_testing();

    clean_test(&conn);

    use self::test::columns::TST_CHR;
    use self::test::dsl::test;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    let mut v = Vec::new();
    v.push(String::from("äöüß"));
    v.push(String::from("السلام عليكم"));
    v.push(String::from("Dobrý den"));
    v.push(String::from("Hello"));
    v.push(String::from("שָׁלוֹם"));
    v.push(String::from("नमस्ते"));
    v.push(String::from("こんにちは"));
    v.push(String::from("안녕하세요"));
    v.push(String::from("你好"));
    v.push(String::from("Olá"));
    v.push(String::from("Здравствуйте"));
    v.push(String::from("Hola"));
    v.push(String::from("🎉🦀"));
    for hello in &v {
        let ret = ::diesel::insert_into(test)
            .values(TST_CHR.eq(&hello))
            .execute(&conn);
        assert_result!(ret);
    }

    let ret: Result<Vec<Option<String>>, _> = self::test::dsl::test.select(TST_CHR).load(&conn);
    assert_result!(ret);
    let ret = ret.unwrap();
    assert_eq!(ret.len(), v.len());
    for (i, r) in ret.iter().enumerate() {
        assert!(r.is_some());
        let tst_chr = r.clone().unwrap();
        assert_eq!(tst_chr, v[i]);
    }
}

use diesel::sql_types::Nullable;
use diesel::sql_types::Text;
#[derive(QueryableByName)]
#[allow(non_snake_case)]
struct FooAliased {
    #[column_name = "foo"]
    #[sql_type = "Nullable<Text>"]
    TST_CHR: Option<String>,
}

#[test]
fn use_named_queries_aliased() {
    let conn = init_testing();

    clean_test(&conn);

    use self::test::columns::TST_CHR;
    use self::test::dsl::test;
    use diesel::sql_query;
    use diesel::ExpressionMethods;

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    let mut v = Vec::new();
    v.push(String::from("äöüß"));
    v.push(String::from("السلام عليكم"));
    v.push(String::from("Dobrý den"));
    v.push(String::from("Hello"));
    v.push(String::from("שָׁלוֹם"));
    v.push(String::from("नमस्ते"));
    v.push(String::from("こんにちは"));
    v.push(String::from("안녕하세요"));
    v.push(String::from("你好"));
    v.push(String::from("Olá"));
    v.push(String::from("Здравствуйте"));
    v.push(String::from("Hola"));
    v.push(String::from("🎉🦀"));
    for hello in &v {
        let ret = ::diesel::insert_into(test)
            .values(TST_CHR.eq(&hello))
            .execute(&conn);
        assert_result!(ret);
    }

    let ret = sql_query("SELECT TST_CHR \"foo\" FROM test").load::<FooAliased>(&conn);

    assert_result!(ret);
    let ret = ret.unwrap();
    assert_eq!(ret.len(), v.len());
    for (i, r) in ret.iter().enumerate() {
        assert!(r.TST_CHR.is_some());
        let tst_chr = r.TST_CHR.clone().unwrap();
        assert_eq!(tst_chr, v[i]);
    }
}

#[derive(QueryableByName)]
#[table_name = "test"]
#[allow(non_snake_case)]
struct Foo {
    TST_CHR: Option<String>,
}

#[test]
fn use_named_queries() {
    let conn = init_testing();

    clean_test(&conn);

    use self::test::columns::TST_CHR;
    use self::test::dsl::test;
    use diesel::sql_query;
    use diesel::ExpressionMethods;

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    let mut v = Vec::new();
    v.push(String::from("äöüß"));
    v.push(String::from("السلام عليكم"));
    v.push(String::from("Dobrý den"));
    v.push(String::from("Hello"));
    v.push(String::from("שָׁלוֹם"));
    v.push(String::from("नमस्ते"));
    v.push(String::from("こんにちは"));
    v.push(String::from("안녕하세요"));
    v.push(String::from("你好"));
    v.push(String::from("Olá"));
    v.push(String::from("Здравствуйте"));
    v.push(String::from("Hola"));
    v.push(String::from("🎉🦀"));
    for hello in &v {
        let ret = ::diesel::insert_into(test)
            .values(TST_CHR.eq(&hello))
            .execute(&conn);
        assert_result!(ret);
    }

    let ret = sql_query("SELECT TST_CHR FROM test").load::<Foo>(&conn);

    assert_result!(ret);
    let ret = ret.unwrap();
    assert_eq!(ret.len(), v.len());
    for (i, r) in ret.iter().enumerate() {
        assert!(r.TST_CHR.is_some());
        let tst_chr = r.TST_CHR.clone().unwrap();
        assert_eq!(tst_chr, v[i]);
    }
}

#[test]
fn insert_returning_gst_types() {
    let conn = init_testing();
    create_gst_types_table(&conn);
    let big_val = 42i64;
    let big2_val = 420i64;
    let small_val = 5i16;
    let normal_val = 25i32;
    let v_val = "test".to_string();
    let d_val = 42.12345f64;
    let r_val = 1.23f32;
    let tz_val = Utc::now().naive_utc();
    let text_val = "Some longer text".to_string();
    let mut byte_val: Vec<u8> = Vec::new();
    for i in 0..88 {
        byte_val.push(i as u8 % 128u8);
    }

    let new_row = Newgst_types{
        big: Some(big_val),
        big2: Some(big2_val),
        small: Some(small_val),
        normal: Some(normal_val),
        v: Some(v_val.clone()),
        d: Some(d_val),
        r: Some(r_val),
        byte: Some(byte_val.clone()),
        text: Some(text_val.clone()),
        tz: Some(tz_val),
    };
    let result = ::diesel::insert_into(gst_types::table)
        .values(&new_row)
        .oci_returning()
        .get_result::<GSTTypes>(&conn)
        .unwrap();
    assert_eq!(result.big, Some(big_val));
    assert_eq!(result.big2, Some(big2_val));
    assert_eq!(result.small, Some(small_val));
    assert_eq!(result.normal, Some(normal_val));
    assert_eq!(result.v, Some(v_val));
    assert_eq!(result.d, Some(d_val));
    assert_eq!(result.r, Some(r_val));
    assert_eq!(result.byte, Some(byte_val));
    assert_eq!(result.text, Some(text_val));
    // No tz test, because we don't store the subsec part.
}

#[cfg(feature = "dynamic-schema")]
mod dynamic_select;
