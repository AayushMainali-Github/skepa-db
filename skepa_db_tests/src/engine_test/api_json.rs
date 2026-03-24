use super::*;
use serde_json::json;

#[test]
fn test_query_result_select_serializes_to_json() {
    let mut db = test_db();
    db.execute("create table users (id int, name text, age int)")
        .unwrap();
    db.execute(r#"insert into users values (1, "ram", 20)"#)
        .unwrap();

    let result = db.execute("select * from users order by id asc").unwrap();
    let value = serde_json::to_value(&result).unwrap();

    assert_eq!(value["Select"]["schema"]["columns"][0]["name"], "id");
    assert_eq!(value["Select"]["schema"]["columns"][1]["name"], "name");
    assert_eq!(value["Select"]["rows"], json!([[1, "ram", 20]]));
    assert_eq!(value["Select"]["stats"]["rows_returned"], 1);
    assert_eq!(
        value["Select"]["stats"]["rows_affected"],
        serde_json::Value::Null
    );
}

#[test]
fn test_query_result_mutation_serializes_to_json() {
    let mut db = test_db();
    db.execute("create table users (id int, email text)")
        .unwrap();

    let result = db
        .execute(r#"insert into users values (1, "a@x.com")"#)
        .unwrap();
    let value = serde_json::to_value(&result).unwrap();

    assert_eq!(
        value,
        json!({
            "Mutation": {
                "message": "inserted 1 row into users",
                "rows_affected": 1,
                "stats": {
                    "rows_returned": null,
                    "rows_affected": 1
                }
            }
        })
    );
}
