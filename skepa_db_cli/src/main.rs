use anyhow::{Context, Result, bail};
use skepa_db_core::Database;
use skepa_db_core::config::DbConfig;
use skepa_db_core::parser::parser::parse;
use skepa_db_core::query_result::QueryResult;
use skepa_db_core::types::value::value_to_string;
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
enum CommandMode {
    Shell,
    Execute { sql: String },
}

#[derive(Debug, Clone)]
struct CliConfig {
    mode: CommandMode,
    db_path: PathBuf,
    remote_url: Option<String>,
}

fn render_query_result(result: &QueryResult) -> String {
    match result {
        QueryResult::Select { schema, rows, .. } => {
            let header = schema
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join("\t");

            if rows.is_empty() {
                return header;
            }

            let row_lines = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(value_to_string)
                        .collect::<Vec<_>>()
                        .join("\t")
                })
                .collect::<Vec<_>>()
                .join("\n");

            format!("{header}\n{row_lines}")
        }
        QueryResult::Mutation { message, .. } => message.clone(),
        QueryResult::SchemaChange { message, .. } => message.clone(),
        QueryResult::Transaction { message, .. } => message.clone(),
    }
}

fn print_help() {
    println!("Commands:");
    println!("  skepa_db_cli shell [--db-path <path>] [--remote <url>]");
    println!("  skepa_db_cli execute <sql> [--db-path <path>] [--remote <url>]");
    println!("  parse <cmd>   -> show parsed Command (debug) in shell mode");
    println!("  begin | commit | rollback");
    println!(
        "  create table <table> (<col> <type> [primary key|unique|not null], ..., [primary key(<col,...>)], [unique(<col,...>)], [foreign key(<col,...>) references <table>(<col,...>) [on delete restrict|cascade|set null|no action] [on update restrict|cascade|set null|no action]])"
    );
    println!("  alter table <table> add unique(<col,...>)");
    println!("  alter table <table> drop unique(<col,...>)");
    println!(
        "  alter table <table> add foreign key(<col,...>) references <table>(<col,...>) [on delete ...] [on update ...]"
    );
    println!("  alter table <table> drop foreign key(<col,...>) references <table>(<col,...>)");
    println!("  alter table <table> alter column <col> set not null");
    println!("  alter table <table> alter column <col> drop not null");
    println!("  create index on <table> (<col,...>)");
    println!("  drop index on <table> (<col,...>)");
    println!("  insert into <table> values (<v1>, <v2>, ...)");
    println!(
        "  update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
    );
    println!("  delete from <table> where <column> <op> <value>");
    println!(
        "  select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>]"
    );
    println!("  where ops: =|eq|>|gt|<|lt|>=|gte|<=|lte|like");
    println!("  like uses '*' and '?' wildcards, e.g. \"ra*\", \"*ir\", \"*av*\", \"r?m\"");
    println!("  exit|quit     -> quit");
}

fn parse_cli_config() -> Result<CliConfig> {
    let mut args = env::args().skip(1).peekable();
    let mut db_path = PathBuf::from("./mydb");
    let mut remote_url = None;
    let mut mode = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db-path" => {
                let value = args.next().context("missing value for --db-path")?;
                db_path = PathBuf::from(value);
            }
            "--remote" => {
                let value = args.next().context("missing value for --remote")?;
                remote_url = Some(value);
            }
            "shell" => {
                if mode.is_some() {
                    bail!("command already specified");
                }
                mode = Some(CommandMode::Shell);
            }
            "execute" => {
                if mode.is_some() {
                    bail!("command already specified");
                }
                let sql_parts = args.collect::<Vec<_>>();
                if sql_parts.is_empty() {
                    bail!("missing sql for execute");
                }
                mode = Some(CommandMode::Execute {
                    sql: sql_parts.join(" "),
                });
                break;
            }
            "help" | "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                if mode.is_none() {
                    mode = Some(CommandMode::Execute {
                        sql: std::iter::once(other.to_string())
                            .chain(args)
                            .collect::<Vec<_>>()
                            .join(" "),
                    });
                    break;
                }
                bail!("unknown argument: {other}");
            }
        }
    }

    Ok(CliConfig {
        mode: mode.unwrap_or(CommandMode::Shell),
        db_path,
        remote_url,
    })
}

fn execute_embedded(db: &mut Database, sql: &str) -> Result<QueryResult> {
    db.execute(sql).map_err(Into::into)
}

fn run_embedded_shell(config: &CliConfig) -> Result<()> {
    let mut db = Database::open(DbConfig::new(config.db_path.clone()))
        .with_context(|| format!("failed to open database at {}", config.db_path.display()))?;

    println!("skepa_db_cli (type 'help' or 'exit')");

    loop {
        print!("db> ");
        io::stdout().flush().context("failed to flush prompt")?;

        let mut line = String::new();
        if io::stdin()
            .read_line(&mut line)
            .context("failed to read input")?
            == 0
        {
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
            break;
        }

        if input.eq_ignore_ascii_case("help") {
            print_help();
            continue;
        }

        if let Some(rest) = input.strip_prefix("parse ") {
            match parse(rest) {
                Ok(cmd) => println!("Parsed as: {cmd:?}"),
                Err(error) => eprintln!("Parse error: {error}"),
            }
            continue;
        }

        match execute_embedded(&mut db, input) {
            Ok(result) => println!("{}", render_query_result(&result)),
            Err(error) => eprintln!("{error}"),
        }
    }

    Ok(())
}

fn run_embedded_execute(config: &CliConfig, sql: &str) -> Result<()> {
    let mut db = Database::open(DbConfig::new(config.db_path.clone()))
        .with_context(|| format!("failed to open database at {}", config.db_path.display()))?;
    let result = execute_embedded(&mut db, sql)?;
    println!("{}", render_query_result(&result));
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = parse_cli_config()?;

    if let Some(remote_url) = &config.remote_url {
        bail!("remote mode is not implemented yet: {remote_url}");
    }

    match &config.mode {
        CommandMode::Shell => run_embedded_shell(&config),
        CommandMode::Execute { sql } => run_embedded_execute(&config, sql),
    }
}
