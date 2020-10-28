extern crate diesel_dynamic_schema;
use self::diesel_dynamic_schema::dynamic_value::*;
use self::diesel_dynamic_schema::DynamicSelectClause;
use diesel::deserialize::*;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::*;
use oracle::{OciDataType, Oracle, OracleValue};

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
    let connection = super::init_testing();
    let _ = sql_query("DROP TABLE users").execute(&connection);
    sql_query("CREATE TABLE users (id NUMBER(10) NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL, hair_color VARCHAR(50))")
        .execute(&connection)
        .unwrap();
    sql_query(
        "INSERT ALL
    INTO users (id, name) VALUES (3, 'Sean')
    INTO users (id, name) VALUES (2, 'Tess')
SELECT * FROM DUAL",
    )
    .execute(&connection)
    .unwrap();

    let users = diesel_dynamic_schema::table("users");
    let id = users.column::<Integer, _>("id");
    let name = users.column::<Text, _>("name");
    let hair_color = users.column::<Nullable<Text>, _>("hair_color");

    let mut select = DynamicSelectClause::new();

    select.add_field(id);
    select.add_field(name);
    select.add_field(hair_color);

    let actual_data: Vec<DynamicRow<NamedField<MyDynamicValue>>> =
        users.select(select).load(&connection).unwrap();

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
        users.select(select).load(&connection).unwrap();

    assert_eq!(actual_data[0][1], MyDynamicValue::String("Sean".into()));
    assert_eq!(actual_data[1][1], MyDynamicValue::String("Tess".into()));
    assert_eq!(actual_data[0][2], MyDynamicValue::Null);
    assert_eq!(actual_data[1][2], MyDynamicValue::Null);
}
