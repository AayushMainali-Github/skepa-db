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
        Command::Alter { table, .. } => assert_eq!(table, "c"),
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

