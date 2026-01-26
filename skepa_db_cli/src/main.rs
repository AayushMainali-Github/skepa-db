use skepa_db_core::Database;
use skepa_db_core::parser::parser::parse;
use std::io::{self, Write};


fn main(){
    let mut db = Database::open("./mydb");

    println!("skepa_db_cli (type 'help' or 'exit)");

    loop {
        print!("db> ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        if io::stdin().read_line(&mut line).is_err(){
            println!("Failed to read input");
            continue;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit"){
            break;
        }

        if input.eq_ignore_ascii_case("help"){
            println!("Commands:");
            println!("  parse <cmd>   -> show parsed Command (debug)");
            println!("  exit|quit     -> quit");
            println!("  help          -> help");
            println!("  (anything else is executed)");
            continue;
        }

        // ---- PARSE DEBUG MODE ----
        if let Some(rest) = input.strip_prefix("parse ") {
            match parse(rest) {
                Ok(cmd) => println!("Parsed as: {cmd:?}"),
                Err(e) => eprintln!("Parse error: {e}"),
            }
            continue;
        }

        // ---- NORMAL EXECUTION MODE ----
        match db.execute(input) {
            Ok(out) => println!("{out}"),
            Err(err) => println!("{err}"),
        }
    }
}