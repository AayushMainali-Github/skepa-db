use skepa_db_core::Database;
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
            println!("Commands: anything -> ok, exit, help");
            continue;
        }

        match db.execute(input) {
            Ok(out) => println!("{out}"),
            Err(err) => println!("{err}"),
        }
    }
}