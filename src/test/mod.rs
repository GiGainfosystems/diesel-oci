extern crate dotenv;

use super::oracle::connection::OciConnection;
use self::dotenv::dotenv;
use diesel::result::Error;
use diesel::Connection;
use diesel::RunQueryDsl;
use std::env;

#[cfg(ka)]
use diesel::migration::MigrationConnection;

#[cfg(ka)]
impl MigrationConnection for OciConnection {
    #[cfg(ka)]
    const CREATE_MIGRATIONS_FUNCTION: &'static str =
        "create or replace procedure create_if_not_exists(input_sql varchar2) \
         as \
         begin \
         execute immediate input_sql; \
         exception \
         when others then \
         if sqlcode = -955 then \
         NULL; \
         else \
         raise; \
         end if; \
         end; \n ";

    const CREATE_MIGRATIONS_TABLE: &'static str = "
    declare \
    begin \
    create_if_not_exists('CREATE TABLE \"__DIESEL_SCHEMA_MIGRATIONS\" (\
         \"VERSION\" VARCHAR2(50) PRIMARY KEY NOT NULL,\
         \"RUN_ON\" TIMESTAMP with time zone DEFAULT sysdate not null\
         )'); \
        end; \n";
}

#[allow(dead_code)]
fn connection() -> OciConnection {
    let database_url = database_url_from_env("OCI_DATABASE_URL");
    OciConnection::establish(&database_url).unwrap()
}

#[allow(dead_code)]
fn database_url_from_env(backend_specific_env_var: &str) -> String {
    dotenv().ok();
    match env::var(backend_specific_env_var) {
        Ok(val) => {
            println!(r#"cargo:rustc-cfg=feature="backend_specific_database_url""#);
            val
        }
        _ => env::var("DATABASE_URL").expect("DATABASE_URL must be set in order to run tests"),
    }
}


const DB_URL: &str = "oci://\"diesel\"/diesel@//192.168.2.81:1521/orcl";

const CREATE_TEST_TABLE: &str = "CREATE TABLE test (\
                                 ID NUMBER(38), \
                                 TST_CHR VARCHAR(50),\
                                 TST_NUM NUMBER(38)\
                                 )";


const DROP_TEST_TABLE: &str = "DROP TABLE test";

const TEST_VARCHAR: &str = "'blabla'";

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
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL);

    assert_result!(conn);
}

#[test]
fn transaction_commit() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<_, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
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
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    let out = conn.transaction::<i32, Error, _>(|| {
        let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let _ret = conn.execute(&*sql)?;
        let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(Option<i64>, Option<String>, Option<i64>)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn create_table() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let _u = create_test_table(&conn);
    let _u = drop_test_table(&conn);
}

#[test]
fn test_diesel_migration() {
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_DIESEL_MIGRATIONS_TABLE);
    assert_result!(ret);

    use self::__diesel_schema_migrations::dsl::*;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use std::collections::HashSet;
    use std::iter::FromIterator;

    let migrations = vec!["00000000000000", "20151219180527", "20160107090901"];

    for mig in &migrations {
        let ret = ::diesel::insert_into(__diesel_schema_migrations)
            .values(&version.eq(mig))
            .execute(&conn);
        assert_result!(ret);
    }

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

    let pending_migrations: Vec<_> = migrations
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    assert_eq!(pending_migrations.len(), 0);
}

#[cfg(this_test_doesnt_work)]
#[test]
fn test_multi_insert() {
    let conn = OciConnection::establish(&DB_URL).unwrap();

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

extern crate chrono;
use self::chrono::NaiveDateTime;


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

#[derive(Debug, Insertable)]
#[table_name="gst_types"]
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
    pub fn new(    big: Option<i64>,
    big2: Option<i64>,
    small: Option<i16>,
    normal: Option<i32>,
    text: Option<String>,
    byte: Option<Vec<u8>>,
    d: Option<f64>,
    r: Option<f32>,
    v: Option<String>) -> Newgst_types {
        Newgst_types {
            big,
            big2 ,
            small ,
            normal ,
            tz: None,
            text ,
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

    let conn = OciConnection::establish(&DB_URL).unwrap();

    drop_table(&conn, "GST_TYPES");

    let ret = conn.execute(CREATE_GST_TYPE_TABLE);
    assert_result!(ret);

    use self::gst_types::columns::{big, big2, d, normal, r, small, v, byte};
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

            let mut bin : Vec<u8> = Vec::new();
            for i in 0..154 {
                bin.push(i as u8 % 128u8);
            }


            let new_row = Newgst_types::new(
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

            let new_row = ::diesel::insert_into(gst_types)
                .values(&new_row)
                .get_results::<GSTTypes>(&conn);
            assert_result!(new_row);

            let mut bin : Vec<u8> = Vec::new();
            for i in 0..154 {
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
                    Option<Vec<u8>>
                )>,
                Error,
            > = gst_types.select((
                big,
                small,
                normal,
                d,
                r,
                v,
                byte)).load(&conn);
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
        }
    }
}
