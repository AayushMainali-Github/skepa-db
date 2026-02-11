use super::*;

#[test]
fn parse_alter_add_unique() {
    let cmd = parse("alter table t add unique(a,b)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_unique() {
    let cmd = parse("alter table t drop unique(a)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_add_foreign_key() {
    let cmd = parse(
        "alter table c add foreign key(a) references p(id) on delete cascade on update no action",
    )
    .unwrap();
    match cmd {
        Command::Alter { table, action } => {
            assert_eq!(table, "c");
            match action {
                skepa_db_core::parser::command::AlterAction::AddForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                } => {
                    assert_eq!(columns, vec!["a".to_string()]);
                    assert_eq!(ref_table, "p");
                    assert_eq!(ref_columns, vec!["id".to_string()]);
                    assert_eq!(
                        on_delete,
                        skepa_db_core::parser::command::ForeignKeyAction::Cascade
                    );
                    assert_eq!(
                        on_update,
                        skepa_db_core::parser::command::ForeignKeyAction::NoAction
                    );
                }
                _ => panic!("Expected add foreign key action"),
            }
        }
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_add_foreign_key_accepts_on_update_before_on_delete() {
    let cmd = parse(
        "alter table c add foreign key(a) references p(id) on update set null on delete restrict",
    )
    .unwrap();
    match cmd {
        Command::Alter { action, .. } => match action {
            skepa_db_core::parser::command::AlterAction::AddForeignKey {
                on_delete,
                on_update,
                ..
            } => {
                assert_eq!(
                    on_update,
                    skepa_db_core::parser::command::ForeignKeyAction::SetNull
                );
                assert_eq!(
                    on_delete,
                    skepa_db_core::parser::command::ForeignKeyAction::Restrict
                );
            }
            _ => panic!("Expected add foreign key action"),
        },
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_foreign_key() {
    let cmd = parse("alter table c drop foreign key(a) references p(id)").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "c"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_set_not_null() {
    let cmd = parse("alter table t alter column a set not null").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_drop_not_null() {
    let cmd = parse("alter table t alter column a drop not null").unwrap();
    match cmd {
        Command::Alter { table, .. } => assert_eq!(table, "t"),
        _ => panic!("Expected Alter command"),
    }
}

#[test]
fn parse_alter_add_unique_empty_cols_errors() {
    assert!(parse("alter table users add unique()") .is_err());
}

#[test]
fn parse_alter_drop_unique_empty_cols_errors() {
    assert!(parse("alter table users drop unique()") .is_err());
}

#[test]
fn parse_alter_add_fk_missing_reference_cols_errors() {
    assert!(parse("alter table c add foreign key (pid) references p") .is_err());
}

