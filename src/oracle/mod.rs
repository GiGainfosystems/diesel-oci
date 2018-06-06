extern crate dotenv;

mod backend;
pub mod connection;
mod query_builder;
mod types;

use self::connection::OciConnection;
use self::dotenv::dotenv;
use diesel::Connection;
use std::env;
#[cfg(test)]
use diesel::result::Error;
#[cfg(test)]
use diesel::RunQueryDsl;

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

#[allow(dead_code)]
const DB_URL: &'static str = "oci://\"diesel\"/diesel@//192.168.2.81:1521/orcl";

const CREATE_TEST_TABLE: &'static str = "CREATE TABLE test (\
                                         ID NUMBER(38), \
                                         TST_CHR VARCHAR(50),\
                                         TST_NUM NUMBER(38)\
                                         )";

#[allow(dead_code)]
const DROP_TEST_TABLE: &'static str = "DROP TABLE test";

#[allow(dead_code)]
const INSERT_TEMPLATE: &'static str = "INSERT INTO test ({}) VALUES ({})";

#[allow(dead_code)]
const TEST_VARCHAR: &'static str = "'blabla'";

//fn assert_result(r: Result<T>) {
//    assert!(r.is_ok() && !r.is_err(), format!("{:?}", r.err()));
//}

macro_rules! assert_result {
    ($r:expr) => {{
        assert!($r.is_ok() && !$r.is_err(), format!("{:?}", $r.err()));
    }};
}

table! {
     test {
         id -> BigInt,
         TST_CHR -> Text,
         TST_NUM -> BigInt,
     }
}

#[allow(dead_code)]
const DROP_DIESEL_TABLE: &'static str = "DROP TABLE \"__DIESEL_SCHEMA_MIGRATIONS\"";

#[allow(dead_code)]
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

#[allow(dead_code)]
fn create_test_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

#[allow(dead_code)]
fn drop_test_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(DROP_TEST_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

#[allow(dead_code)]
fn drop_diesel_table(conn: &OciConnection) -> usize {
    let ret = conn.execute(DROP_DIESEL_TABLE);
    assert_result!(ret);
    ret.unwrap()
}

#[allow(dead_code)]
fn execute_sql_or_rollback(conn: &OciConnection, sql: String, rollback_sql: String) -> usize {
    let ret = conn.execute(&*sql);
    if ret.is_err() {
        let inner = conn.execute(&*rollback_sql);
        assert_result!(inner)
    }
    assert_result!(ret);
    ret.unwrap()
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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
        let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Ok(())
    });
    assert_result!(out);
    let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn);
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
        let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn)?;
        assert_eq!(ret.len(), 1);
        Err(Error::NotFound)
    });
    assert!(out.is_err() && !out.is_ok(), "What :shrug:?");
    let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn);
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
fn insert_string() {
    //let database_url = database_url_from_env("OCI_DATABASE_URL");
    let conn = OciConnection::establish(&DB_URL).unwrap();

    clean_test(&conn);

    let ret = conn.execute(CREATE_TEST_TABLE);
    assert_result!(ret);

    let sql = format!("INSERT INTO test ({}) VALUES ({})", "TST_CHR", TEST_VARCHAR);
    let ret = conn.execute(&*sql);
    assert_result!(ret);

    let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn);
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

    use self::test::dsl::*;
    use diesel::ExpressionMethods;

    let ret = ::diesel::insert_into(test)
        .values(&TST_CHR.eq(TEST_VARCHAR))
        .execute(&conn);

    assert_result!(ret);

    use diesel::QueryDsl;

    //let ret = self::test::dsl::test.load::<(i64, String, i64)>(&conn);
    let ret = self::test::dsl::test.select(TST_CHR).load::<String>(&conn);
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

    println!("migrations: {:?}", migrations);
    println!("already_run: {:?}", already_run);

    let pending_migrations: Vec<_> = migrations
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

    println!("migrations: {:?}", migrations);
    println!("already_run: {:?}", already_run);

    let pending_migrations: Vec<_> = migrations
        .into_iter()
        .filter(|m| !already_run.contains(&m.to_string()))
        .collect();

    println!("already_run: {:?}", already_run);
    println!("pending_migrations: {:?}", pending_migrations);

    assert_eq!(pending_migrations.len(), 0);
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
    const CREATE_GST_TYPE_TABLE: &'static str =
        "CREATE TABLE gst_types (\
            big NUMBER(19),
            small NUMBER(5),
            normal NUMBER(10),
            tz timestamp default sysdate,
            text clob,
            byte blob,
            d binary_double,
            r binary_float,
            v VARCHAR2(50) NOT NULL
     )";

    const CREATE_GST_TYPE_TABLE2: &'static str =
        "CREATE TABLE gst_types (\
            big NUMBER(19),
            small smallint,
            normal integer,
            tz timestamp default sysdate,
            text clob,
            byte blob,
            d binary_double,
            r binary_float,
            v VARCHAR2(50) NOT NULL
     )";



    let conn = OciConnection::establish(&DB_URL).unwrap();

    drop_table(&conn, "GST_TYPES");

    let ret = conn.execute(CREATE_GST_TYPE_TABLE);
    assert_result!(ret);

    let neg_base: i64 = -2;
    let base: i128 = 2;

    let sqls = format!("INSERT INTO gst_types ({}) VALUES ({},{},{},{},{}d,{},{})", "big, small, normal, text, d, r, v", neg_base.pow(63), neg_base.pow(15), neg_base.pow(31), "'text'", "1e-307", "1e-37", "'test'");
    let ret = conn.execute(&*sqls);
    assert_result!(ret);
    let sqls = format!("INSERT INTO gst_types ({}) VALUES ({},{},{},{},{}d,{},{})", "big, small, normal, text, d, r, v", base.pow(63)-1, base.pow(15)-1, base.pow(31)-1, "'text'", "1e308", "1e37", "'test'");
    let ret = conn.execute(&*sqls);
    assert_result!(ret);


    use diesel::sql_types::{BigInt, SmallInt, Integer, Text, Double, Float, VarChar};
    use diesel::dsl::sql;
    //let sqls = "SELECT big, small, normal, text, d, r, v from gst_types";
    //let r = sql::<(BigInt, SmallInt, Integer, Text, Double, Float, VarChar),>(sqls).load::<(i64, i16, i32, String, f64, f32, String)>(&conn);
    let sqls = "SELECT big, small, normal, d, r, v from gst_types";
    let r = sql::<(BigInt, SmallInt, Integer, Double, Float, Text),>(sqls).load::<(i64, i16, i32, f64, f32, String)>(&conn);
    assert_result!(r);
    let v = r.unwrap();
    assert_eq!(v[0].0, neg_base.pow(63));
    assert_eq!(v[1].0, (base.pow(63)-1) as i64);
    assert_eq!(v[0].1, neg_base.pow(15) as i16);
    assert_eq!(v[1].1, (base.pow(15)-1) as i16);
    assert_eq!(v[0].2, neg_base.pow(31) as i32);
    assert_eq!(v[1].2, (base.pow(31)-1) as i32);
    assert_eq!(v[0].3, 1e-307f64);
    assert_eq!(v[1].3, 1e308f64);
    assert_eq!(v[0].4, 1e-37f32);
    assert_eq!(v[1].4, 1e37f32);
    assert_eq!(v[0].5, "test");
    assert_eq!(v[1].5, "test");

}
