extern crate dotenv;

mod backend;
mod query_builder;
pub mod connection;
mod types;
mod query_dsl;
mod insertable;

use self::dotenv::dotenv;
use self::connection::OciConnection;
use diesel::{Connection, ConnectionError};
use diesel::result::Error;
use std::{env};
use diesel::RunQueryDsl;

fn connection() -> OciConnection {
    let database_url = database_url_from_env("OCI_DATABASE_URL");
    OciConnection::establish(&database_url).unwrap()
}


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


const DB_URL: &'static str = "oci://\"diesel\"/diesel@//192.168.2.81:1521/orcl";

const CREATE_TEST_TABLE: &'static str =
    "CREATE TABLE TEST (\
         ID NUMBER(38), \
         TST_CHR VARCHAR(50),\
         TST_NUM NUMBER(38)\
         )";

const DROP_TEST_TABLE: &'static str =
    "DROP TABLE TEST";

const INSERT_TEMPLATE: &'static str =
    "INSERT INTO TEST ({}) VALUES ({})";

const TEST_VARCHAR: &'static str =
    "'blabla'";

//fn assert_result(r: Result<T>) {
//    assert!(r.is_ok() && !r.is_err(), format!("{:?}", r.err()));
//}

use diesel::sql_types::{Integer, Text, BigInt};

macro_rules! assert_result {
    ($r:expr) => ({
        assert!($r.is_ok() && !$r.is_err(), format!("{:?}", $r.err()));
    })

}

table! {
     TEST {
         id -> BigInt,
         TST_CHR -> Text,
         TST_NUM -> BigInt,
     }
}

const DROP_DIESEL_TABLE: &'static str =
    "DROP TABLE \"__DIESEL_SCHEMA_MIGRATIONS\"";

const CREATE_DIESEL_MIGRATIONS_TABLE: &'static str =
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

fn execute_sql_or_rollback(conn: &OciConnection, sql: String, rollback_sql: String) -> usize {
    let ret = conn.execute(&*sql);
    if ret.is_err() {
        let inner = conn.execute(&*rollback_sql);
        assert_result!(inner)
    }
    assert_result!(ret);
    ret.unwrap()
}


fn clean_test(conn: &OciConnection) {

    let sql = "SELECT * FROM TEST";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let ret = drop_test_table(conn);
    }
    let sql = "SELECT * FROM \"__DIESEL_SCHEMA_MIGRATIONS\"";
    let ret = conn.execute(sql);
    if ret.is_ok() {
        let ret = drop_diesel_table(conn);
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
    let out = conn.transaction::<_, Error, _>(| | {
        let sql = format!("INSERT INTO TEST ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let ret = conn.execute(&*sql)?;
        let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn);
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
    let out = conn.transaction::<i32, Error, _>(| | {
        let sql = format!("INSERT INTO TEST ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
        let ret = conn.execute(&*sql)?;
        let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn);
    assert_result!(ret);
    assert_eq!(ret.unwrap().len(), 0);
}

#[test]
fn create_table() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    // drop the table immediately
    let ret = conn.execute(DROP_TEST_TABLE);
    assert_result!(ret);
}

#[test]
fn insert_string() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    let sql = format!("INSERT INTO TEST ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
    let ret = conn.execute(&*sql);
    assert_result!(ret);

    let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn);
    assert_result!(ret);
    let ret = ret.unwrap();
    assert_ne!(ret.len(), 0);


    // drop the table immediately
    let ret = conn.execute(DROP_TEST_TABLE);
    assert_result!(ret);
}

#[test]
fn insert_string_diesel_way() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    use self::TEST::dsl::*;
    use diesel::ExpressionMethods;

    let ret = ::diesel::insert_into(TEST)
        .values(&TST_CHR.eq(TEST_VARCHAR))
        .execute(&conn);

    assert_result!(ret);

    use diesel::QueryDsl;

    //let ret = self::TEST::dsl::TEST.load::<(i64, String, i64)>(&conn);
    let ret = self::TEST::dsl::TEST.select(TST_CHR).load::<String>(&conn);
    assert_result!(ret);
    let ret = ret.unwrap();
    assert_ne!(ret.len(), 0);
    assert_eq!(TEST_VARCHAR, ret[0]);


    // drop the table immediately
    let ret = conn.execute(DROP_TEST_TABLE);
    assert_result!(ret);
}

#[test]
fn test_diesel_migration() {
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_DIESEL_MIGRATIONS_TABLE);
    assert_result!(ret);

    use self::__diesel_schema_migrations::dsl::*;
    use diesel::QueryDsl;
    use diesel::ExpressionMethods;
    use std::iter::FromIterator;
    use diesel::QueryResult;
    use std::collections::HashSet;

    let migrations = vec!["00000000000000", "20151219180527", "20160107090901"];

    for mig in &migrations {
        let ret = ::diesel::insert_into(__diesel_schema_migrations)
            .values(&version.eq(mig))
            .execute(&conn);
        assert_result!(ret);
    }

    let already_run: HashSet<String> = self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
        .select(version)
        .load(&conn)
        .map(FromIterator::from_iter).unwrap();

    let ret = self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
            .select(version)
            .load(&conn);
    let already_run: HashSet<String> = ret.map(FromIterator::from_iter).unwrap();


    println!("migrations: {:?}", migrations);
    println!("already_run: {:?}", already_run);

    let mut pending_migrations: Vec<_> = migrations
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    println!("pending_migrations: {:?}", pending_migrations);

    assert_eq!(pending_migrations.len(), 0);
}

#[test]
fn test_multi_insert() {
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_DIESEL_MIGRATIONS_TABLE);
    assert_result!(ret);

    use self::__diesel_schema_migrations::dsl::*;
    use diesel::QueryDsl;
    use diesel::ExpressionMethods;
    use std::iter::FromIterator;
    use diesel::QueryResult;
    use std::collections::HashSet;

    let migrations = vec![version.eq("00000000000000"),
                          version.eq("20160107090901"),
                          version.eq("20151219180527")];


    let ret = ::diesel::insert_into(__diesel_schema_migrations)
        .values(&migrations)
        .execute(&conn);

    assert_result!(ret);

    let migrations = vec!["00000000000000",
                          "20160107090901",
                          "20151219180527"];

    let already_run: HashSet<String> = self::__diesel_schema_migrations::dsl::__diesel_schema_migrations
        .select(version)
        .order(version)
        .load(&conn)
        .map(FromIterator::from_iter).unwrap();


    println!("migrations: {:?}", migrations);
    println!("already_run: {:?}", already_run);

    let mut pending_migrations: Vec<_> = migrations
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    println!("already_run: {:?}", already_run);
    println!("pending_migrations: {:?}", pending_migrations);

    assert_eq!(pending_migrations.len(), 0);
}