use skepa_db_core::parser::command::{Command, CompareOp, JoinType, WhereClause};
use skepa_db_core::parser::parser::parse;
use skepa_db_core::types::datatype::DataType;

fn pred(clause: &WhereClause) -> &skepa_db_core::parser::command::Predicate {
    match clause {
        WhereClause::Predicate(p) => p,
        _ => panic!("expected predicate where-clause"),
    }
}

mod create;
mod alter;
mod dml;
mod select;
mod tokenizer;
mod tx;
mod misc;
