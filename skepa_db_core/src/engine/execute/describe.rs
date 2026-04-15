fn handle_describe(table: String, catalog: &Catalog) -> Result<QueryResult, String> {
    let table_schema = catalog.schema(&table)?;
    let out_schema = Schema::new(vec![
        Column {
            name: "column".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: true,
            default: None,
        },
        Column {
            name: "type".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: true,
            default: None,
        },
        Column {
            name: "primary_key".to_string(),
            dtype: DataType::Bool,
            primary_key: false,
            unique: false,
            not_null: true,
            default: None,
        },
        Column {
            name: "unique".to_string(),
            dtype: DataType::Bool,
            primary_key: false,
            unique: false,
            not_null: true,
            default: None,
        },
        Column {
            name: "not_null".to_string(),
            dtype: DataType::Bool,
            primary_key: false,
            unique: false,
            not_null: true,
            default: None,
        },
        Column {
            name: "default".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: false,
            default: None,
        },
        Column {
            name: "indexes".to_string(),
            dtype: DataType::Text,
            primary_key: false,
            unique: false,
            not_null: false,
            default: None,
        },
    ]);

    let mut rows: Vec<Row> = Vec::new();
    for column in &table_schema.columns {
        rows.push(vec![
            Value::Text(column.name.clone()),
            Value::Text(datatype_to_string(&column.dtype)),
            Value::Bool(table_schema.primary_key.iter().any(|name| name == &column.name)),
            Value::Bool(column.unique || column_is_in_unique_constraint(table_schema, &column.name)),
            Value::Bool(column.not_null),
            column
                .default
                .as_ref()
                .map(|value| Value::Text(value.clone()))
                .unwrap_or(Value::Null),
            Value::Text(indexes_for_column(table_schema, &column.name).join(",")),
        ]);
    }

    Ok(QueryResult::select(out_schema, rows))
}

fn column_is_in_unique_constraint(schema: &Schema, column_name: &str) -> bool {
    schema
        .unique_constraints
        .iter()
        .any(|columns| columns.len() == 1 && columns[0] == column_name)
}

fn indexes_for_column(schema: &Schema, column_name: &str) -> Vec<String> {
    schema
        .secondary_indexes
        .iter()
        .filter(|columns| columns.iter().any(|column| column == column_name))
        .map(|columns| columns.join("+"))
        .collect()
}

fn datatype_to_string(dtype: &DataType) -> String {
    match dtype {
        DataType::Bool => "bool".to_string(),
        DataType::Int => "int".to_string(),
        DataType::BigInt => "bigint".to_string(),
        DataType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        DataType::VarChar(size) => format!("varchar({size})"),
        DataType::Text => "text".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Timestamp => "timestamp".to_string(),
        DataType::Uuid => "uuid".to_string(),
        DataType::Json => "json".to_string(),
        DataType::Blob => "blob".to_string(),
    }
}
