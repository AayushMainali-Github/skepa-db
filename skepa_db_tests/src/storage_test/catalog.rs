use super::*;

#[test]
fn catalog_save_load_roundtrip() {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    dtype: DataType::Int,
                    primary_key: false,
                    unique: false,
                    not_null: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    dtype: DataType::Text,
                    primary_key: false,
                    unique: false,
                    not_null: false,
                },
            ],
            vec![],
        )
        .unwrap();

    let path = temp_dir("catalog_roundtrip");
    std::fs::create_dir_all(&path).unwrap();
    let catalog_path = path.join("catalog.json");
    catalog.save_to_path(&catalog_path).unwrap();

    let loaded = Catalog::load_from_path(&catalog_path).unwrap();
    let schema = loaded.schema("users").unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[1].name, "name");
}

#[test]
fn catalog_load_missing_file_is_empty() {
    let path = temp_dir("catalog_missing").join("missing_catalog.json");
    let loaded = Catalog::load_from_path(&path).unwrap();
    assert!(loaded.schema("users").is_err());
}

#[test]
fn catalog_load_malformed_json_errors() {
    let path = temp_dir("catalog_malformed");
    std::fs::create_dir_all(&path).unwrap();
    let catalog_path = path.join("catalog.json");
    std::fs::write(&catalog_path, "{ bad json").unwrap();
    let err = Catalog::load_from_path(&catalog_path).unwrap_err();
    assert!(err.to_lowercase().contains("malformed catalog json"));
}

#[test]
fn catalog_save_load_roundtrip_with_constraints() {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    dtype: DataType::Int,
                    primary_key: true,
                    unique: false,
                    not_null: true,
                },
                ColumnDef {
                    name: "email".to_string(),
                    dtype: DataType::Text,
                    primary_key: false,
                    unique: true,
                    not_null: false,
                },
            ],
            vec![],
        )
        .unwrap();
    catalog
        .create_table(
            "posts".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    dtype: DataType::Int,
                    primary_key: true,
                    unique: false,
                    not_null: true,
                },
                ColumnDef {
                    name: "user_id".to_string(),
                    dtype: DataType::Int,
                    primary_key: false,
                    unique: false,
                    not_null: false,
                },
            ],
            vec![],
        )
        .unwrap();
    catalog
        .add_unique_constraint("posts", vec!["user_id".to_string()])
        .unwrap();
    catalog
        .add_secondary_index("posts", vec!["user_id".to_string()])
        .unwrap();
    catalog
        .add_foreign_key_constraint(
            "posts",
            skepa_db_core::storage::schema::ForeignKeyDef {
                columns: vec!["user_id".to_string()],
                ref_table: "users".to_string(),
                ref_columns: vec!["id".to_string()],
                on_delete: skepa_db_core::parser::command::ForeignKeyAction::SetNull,
                on_update: skepa_db_core::parser::command::ForeignKeyAction::Cascade,
            },
        )
        .unwrap();

    let path = temp_dir("catalog_constraints_roundtrip");
    std::fs::create_dir_all(&path).unwrap();
    let catalog_path = path.join("catalog.json");
    catalog.save_to_path(&catalog_path).unwrap();
    let loaded = Catalog::load_from_path(&catalog_path).unwrap();

    let posts = loaded.schema("posts").unwrap();
    assert_eq!(posts.primary_key, vec!["id".to_string()]);
    assert_eq!(posts.unique_constraints, vec![vec!["user_id".to_string()]]);
    assert_eq!(posts.secondary_indexes, vec![vec!["user_id".to_string()]]);
    assert_eq!(posts.foreign_keys.len(), 1);
    assert_eq!(posts.foreign_keys[0].columns, vec!["user_id".to_string()]);
    assert_eq!(posts.foreign_keys[0].ref_table, "users");
}
