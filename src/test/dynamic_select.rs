extern crate diesel_dynamic_schema;
use self::diesel_dynamic_schema::dynamic_value::*;
use self::diesel_dynamic_schema::DynamicSelectClause;
use crate::oracle::{OciDataType, Oracle, OracleValue};
use diesel::deserialize::*;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;

#[derive(PartialEq, Debug)]
enum MyDynamicValue {
    String(String),
    Integer(i32),
    Null,
}

impl FromSql<Any, Oracle> for MyDynamicValue {
    fn from_sql(value: OracleValue) -> Result<Self> {
        match value.value_type() {
            OciDataType::Integer => {
                <i32 as FromSql<Integer, Oracle>>::from_sql(value).map(MyDynamicValue::Integer)
            }
            OciDataType::Text => {
                <String as FromSql<Text, Oracle>>::from_sql(value).map(MyDynamicValue::String)
            }
            e => Err(format!("Unknown data type: {:?}", e).into()),
        }
    }

    fn from_nullable_sql(value: Option<OracleValue>) -> Result<Self> {
        if let Some(value) = value {
            Self::from_sql(value)
        } else {
            Ok(MyDynamicValue::Null)
        }
    }
}

#[test]
fn dynamic_query() {
    let mut connection = super::init_testing();
    let _ = sql_query("DROP TABLE my_users").execute(&mut connection);
    sql_query("CREATE TABLE my_users (id NUMBER(10) NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL, hair_color VARCHAR(50))")
        .execute(&mut connection)
        .unwrap();
    sql_query(
        "INSERT ALL
    INTO my_users (id, name) VALUES (3, 'Sean')
    INTO my_users (id, name) VALUES (2, 'Tess')
SELECT * FROM DUAL",
    )
    .execute(&mut connection)
    .unwrap();

    let users = diesel_dynamic_schema::table("my_users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let hair_color = users.column::<Nullable<Text>, _>("hair_color");

    let mut select = DynamicSelectClause::new();

    select.add_field(id);
    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<NamedField<MyDynamicValue>>> =
        users.select(select).load(&mut connection).unwrap();

    assert_eq!(
        actual_data[0]["NAME"],
        MyDynamicValue::String("Sean".into())
    );
    assert_eq!(
        actual_data[0][1],
        NamedField {
            name: "NAME".into(),
            value: MyDynamicValue::String("Sean".into())
        }
    );
    assert_eq!(
        actual_data[1]["NAME"],
        MyDynamicValue::String("Tess".into())
    );
    assert_eq!(
        actual_data[1][1],
        NamedField {
            name: "NAME".into(),
            value: MyDynamicValue::String("Tess".into())
        }
    );
    assert_eq!(actual_data[0]["HAIR_COLOR"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[0][2],
        NamedField {
            name: "HAIR_COLOR".into(),
            value: MyDynamicValue::Null
        }
    );
    assert_eq!(actual_data[1]["HAIR_COLOR"], MyDynamicValue::Null);
    assert_eq!(
        actual_data[1][2],
        NamedField {
            name: "HAIR_COLOR".into(),
            value: MyDynamicValue::Null
        }
    );

    let mut select = DynamicSelectClause::new();

    select.add_field(id);
    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<MyDynamicValue>> =
        users.select(select).load(&mut connection).unwrap();

    assert_eq!(actual_data[0][1], MyDynamicValue::String("Sean".into()));
    assert_eq!(actual_data[1][1], MyDynamicValue::String("Tess".into()));
    assert_eq!(actual_data[0][2], MyDynamicValue::Null);
    assert_eq!(actual_data[1][2], MyDynamicValue::Null);
}

#[test]
fn mixed_value_query() {
    use diesel::dsl::sql;
    let mut connection = super::init_testing();
    let _ = sql_query("DROP TABLE my_users").execute(&mut connection);
    sql_query("CREATE TABLE my_users (id NUMBER(10) NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL, hair_color VARCHAR(50))")
        .execute(&mut connection)
        .unwrap();

    sql_query(
        "INSERT ALL
    INTO my_users (id, name, hair_color) VALUES (42, 'Sean', 'black')
    INTO my_users (id, name, hair_color) VALUES (43, 'Tess', 'black')
SELECT * FROM DUAL",
    )
    .execute(&mut connection)
    .unwrap();

    let users = diesel_dynamic_schema::table("my_users");
    let id = users.column::<Integer, _>("id");

    let (id, row) = users
        .select((id, sql::<Untyped>("name, hair_color")))
        .first::<(i32, DynamicRow<NamedField<MyDynamicValue>>)>(&mut connection)
        .unwrap();

    assert_eq!(id, 42);
    assert_eq!(row["NAME"], MyDynamicValue::String("Sean".into()));
    assert_eq!(row["HAIR_COLOR"], MyDynamicValue::String("black".into()));
}
