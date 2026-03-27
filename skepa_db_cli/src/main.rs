use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use skepa_db_core::Database;
use skepa_db_core::config::DbConfig;
use skepa_db_core::execution_stats::ExecutionStats;
use skepa_db_core::parser::parser::parse;
use skepa_db_core::query_result::QueryResult;
use skepa_db_core::storage::Schema;
use skepa_db_core::types::Row;
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

#[derive(Debug, Serialize)]
struct ExecuteRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct ExecuteResponse {
    ok: bool,
    request_id: u64,
    result: ApiQueryResult,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    ok: bool,
    request_id: u64,
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ApiQueryResult {
    Select {
        schema: Schema,
        rows: Vec<Row>,
        stats: ExecutionStats,
    },
    Mutation {
        message: String,
        rows_affected: usize,
        stats: ExecutionStats,
    },
    SchemaChange {
        message: String,
        stats: ExecutionStats,
    },
    Transaction {
        message: String,
        stats: ExecutionStats,
    },
}

impl From<ApiQueryResult> for QueryResult {
    fn from(value: ApiQueryResult) -> Self {
        match value {
            ApiQueryResult::Select {
                schema,
                rows,
                stats,
            } => Self::Select {
                schema,
                rows,
                stats,
            },
            ApiQueryResult::Mutation {
                message,
                rows_affected,
                stats,
            } => Self::Mutation {
                message,
                rows_affected,
                stats,
            },
            ApiQueryResult::SchemaChange { message, stats } => {
                Self::SchemaChange { message, stats }
            }
            ApiQueryResult::Transaction { message, stats } => Self::Transaction { message, stats },
        }
    }
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

fn parse_cli_args<I>(args: I) -> Result<CliConfig>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter().peekable();
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
                let mut sql_parts = Vec::new();
                while let Some(next) = args.peek() {
                    if next == "--db-path" || next == "--remote" {
                        break;
                    }
                    sql_parts.push(args.next().expect("peeked argument should exist"));
                }
                if sql_parts.is_empty() {
                    bail!("missing sql for execute");
                }
                mode = Some(CommandMode::Execute {
                    sql: sql_parts.join(" "),
                });
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

fn parse_cli_config() -> Result<CliConfig> {
    parse_cli_args(env::args().skip(1))
}

fn execute_embedded(db: &mut Database, sql: &str) -> Result<QueryResult> {
    db.execute(sql).map_err(Into::into)
}

fn execute_remote(client: &Client, remote_url: &str, sql: &str) -> Result<QueryResult> {
    let url = format!("{}/execute", remote_url.trim_end_matches('/'));
    let response = client
        .post(&url)
        .json(&ExecuteRequest {
            sql: sql.to_string(),
        })
        .send()
        .with_context(|| format!("failed to send request to {url}"))?;

    if response.status().is_success() {
        let payload: ExecuteResponse = response
            .json()
            .with_context(|| format!("failed to parse success response from {url}"))?;
        let _request_id = payload.request_id;
        let _ok = payload.ok;
        Ok(payload.result.into())
    } else {
        let status = response.status();
        let payload: ErrorResponse = response
            .json()
            .with_context(|| format!("failed to parse error response from {url}"))?;
        let _request_id = payload.request_id;
        let _ok = payload.ok;
        bail!(
            "remote request failed ({status}, {}): {}",
            payload.error.code,
            payload.error.message
        )
    }
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

fn run_remote_shell(_config: &CliConfig, remote_url: &str) -> Result<()> {
    let client = Client::new();
    println!("skepa_db_cli remote shell ({remote_url}) (type 'help' or 'exit')");

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

        match execute_remote(&client, remote_url, input) {
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

fn run_remote_execute(remote_url: &str, sql: &str) -> Result<()> {
    let client = Client::new();
    let result = execute_remote(&client, remote_url, sql)?;
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

    match (&config.mode, &config.remote_url) {
        (CommandMode::Shell, Some(remote_url)) => run_remote_shell(&config, remote_url),
        (CommandMode::Execute { sql }, Some(remote_url)) => run_remote_execute(remote_url, sql),
        (CommandMode::Shell, None) => run_embedded_shell(&config),
        (CommandMode::Execute { sql }, None) => run_embedded_execute(&config, sql),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn parse_cli_args_supports_remote_execute_mode() {
        let config = parse_cli_args([
            "execute".to_string(),
            "select * from users".to_string(),
            "--remote".to_string(),
            "http://127.0.0.1:8080".to_string(),
        ])
        .expect("args should parse");

        match config.mode {
            CommandMode::Execute { sql } => assert_eq!(sql, "select * from users"),
            CommandMode::Shell => panic!("expected execute mode"),
        }
        assert_eq!(config.remote_url.as_deref(), Some("http://127.0.0.1:8080"));
    }

    #[test]
    fn parse_cli_args_defaults_to_shell_mode() {
        let config = parse_cli_args(Vec::<String>::new()).expect("args should parse");
        assert!(matches!(config.mode, CommandMode::Shell));
        assert_eq!(config.db_path, PathBuf::from("./mydb"));
        assert!(config.remote_url.is_none());
    }

    fn spawn_test_server(
        response_body: String,
        status_line: &str,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");
        let status_line = status_line.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("connection should be accepted");
            let mut buffer = [0_u8; 4096];
            let _ = stream.read(&mut buffer);
            let response = format!(
                "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .expect("response should write");
        });

        (format!("http://{}", addr), handle)
    }

    #[test]
    fn execute_remote_returns_query_result() {
        let (base_url, handle) = spawn_test_server(
            r#"{"ok":true,"request_id":1,"result":{"type":"schema_change","message":"created table users","stats":{"rows_returned":null,"rows_affected":null}}}"#.to_string(),
            "HTTP/1.1 200 OK",
        );
        let client = Client::new();

        let result = execute_remote(&client, &base_url, "create table users (id int)")
            .expect("remote execution should succeed");

        match result {
            QueryResult::SchemaChange { message, .. } => assert_eq!(message, "created table users"),
            _ => panic!("expected schema change result"),
        }

        handle.join().expect("server thread should finish");
    }

    #[test]
    fn execute_remote_maps_http_errors() {
        let (base_url, handle) = spawn_test_server(
            r#"{"ok":false,"request_id":1,"error":{"code":"EXECUTION_ERROR","message":"synthetic remote error"}}"#
                .to_string(),
            "HTTP/1.1 400 Bad Request",
        );
        let client = Client::new();

        let error = execute_remote(&client, &base_url, "bad sql").expect_err("request should fail");
        assert!(error.to_string().contains("EXECUTION_ERROR"));
        assert!(error.to_string().contains("synthetic remote error"));

        handle.join().expect("server thread should finish");
    }
}
