use skepa_db_core::Database;
use skepa_db_core::parser::parser::parse;
use std::io::{self, Write};

fn main() {
    let mut db = Database::open("./mydb");

    println!("skepa_db_cli (type 'help' or 'exit')");

    loop {
        print!("db> ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err() {
            println!("Failed to read input");
            continue;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
            break;
        }

        if input.eq_ignore_ascii_case("help") {
            println!("Commands:");
            println!("  parse <cmd>   -> show parsed Command (debug)");
            println!("  begin | commit | rollback");
            println!("  create table <table> (<col> <type> [primary key|unique|not null], ..., [primary key(<col,...>)], [unique(<col,...>)])");
            println!("  insert into <table> values (<v1>, <v2>, ...)");
            println!("  update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>");
            println!("  delete from <table> where <column> <op> <value>");
            println!("  select <col1,col2|*> from <table> [where <column> <op> <value>]");
            println!("  where ops: =|eq|>|gt|<|lt|>=|gte|<=|lte|like");
            println!("  like uses '*' and '?' wildcards, e.g. \"ra*\", \"*ir\", \"*av*\", \"r?m\"");
            println!("  exit|quit     -> quit");
            continue;
        }

        if let Some(rest) = input.strip_prefix("parse ") {
            match parse(rest) {
                Ok(cmd) => println!("Parsed as: {cmd:?}"),
                Err(e) => eprintln!("Parse error: {e}"),
            }
            continue;
        }

        match db.execute(input) {
            Ok(out) => println!("{out}"),
            Err(err) => println!("{err}"),
        }
    }
}
