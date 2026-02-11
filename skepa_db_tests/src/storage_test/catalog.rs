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


