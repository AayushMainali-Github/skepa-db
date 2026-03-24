use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use skepa_db_core::Database;
use skepa_db_core::config::DbConfig;
use skepa_db_core::query_result::QueryResult;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct ServerConfig {
    db_path: PathBuf,
    addr: SocketAddr,
}

#[derive(Debug, Clone)]
struct AppState {
    db: Arc<Mutex<Database>>,
    config: ServerConfig,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    db_path: String,
}

#[derive(Debug, Serialize)]
struct VersionResponse {
    ok: bool,
    version: &'static str,
}

#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct BatchRequest {
    statements: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    ok: bool,
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    ok: bool,
    result: QueryResult,
}

#[derive(Debug, Serialize)]
struct BatchResponse {
    ok: bool,
    results: Vec<QueryResult>,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let _db = state.db.lock().await;
    Json(HealthResponse {
        ok: true,
        db_path: state.config.db_path.display().to_string(),
    })
}

async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        ok: true,
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn execute(
    State(state): State<AppState>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    if request.sql.trim().is_empty() {
        return Err(error_response("sql must not be empty"));
    }

    let mut db = state.db.lock().await;
    let result = db
        .execute(&request.sql)
        .map_err(|error| error_response(error.to_string()))?;

    Ok(Json(ExecuteResponse { ok: true, result }))
}

async fn batch(
    State(_state): State<AppState>,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    if request.statements.is_empty() {
        return Err(error_response("statements must not be empty"));
    }

    Err(error_response("batch endpoint not implemented yet"))
}

fn error_response(message: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            ok: false,
            error: ErrorBody {
                message: message.into(),
            },
        }),
    )
}

fn parse_server_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let mut db_path = env::var("SKEPA_DB_PATH").unwrap_or_else(|_| "./mydb".to_string());
    let mut addr = env::var("SKEPA_DB_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db-path" => {
                db_path = args.next().ok_or("missing value for --db-path")?;
            }
            "--addr" => {
                addr = args.next().ok_or("missing value for --addr")?;
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    Ok(ServerConfig {
        db_path: PathBuf::from(db_path),
        addr: addr.parse()?,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = parse_server_config()?;
    let db = Database::open(DbConfig::new(config.db_path.clone()))?;
    let state = AppState {
        db: Arc::new(Mutex::new(db)),
        config,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/execute", post(execute))
        .route("/batch", post(batch))
        .with_state(state.clone());

    info!(
        "starting skepa_db_server on {} using db {}",
        state.config.addr,
        state.config.db_path.display()
    );

    let listener = tokio::net::TcpListener::bind(state.config.addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
