pub fn execute_command(
    cmd: Command,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    match cmd {
        Command::Create {
            table,
            columns,
            table_constraints,
        } => handle_create(table, columns, table_constraints, catalog, storage),
        Command::CreateIndex { table, columns } => {
            handle_create_index(table, columns, catalog, storage)
        }
        Command::DropIndex { table, columns } => handle_drop_index(table, columns, catalog, storage),
        Command::Alter { table, action } => handle_alter(table, action, catalog, storage),
        Command::Insert { table, values } => handle_insert(table, values, catalog, storage),
        Command::Update {
            table,
            assignments,
            filter,
        } => handle_update(table, assignments, filter, catalog, storage),
        Command::Delete { table, filter } => handle_delete(table, filter, catalog, storage),
        Command::Select {
            table,
            distinct,
            join,
            columns,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
        } => handle_select(table, distinct, join, columns, filter, group_by, having, order_by, limit, offset, catalog, storage),
        Command::Begin | Command::Commit | Command::Rollback => {
            Err("Transaction control is handled by Database".to_string())
        }
    }
}
