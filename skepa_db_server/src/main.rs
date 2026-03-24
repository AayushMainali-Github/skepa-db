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
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tracing::{info, warn};
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
    next_request_id: Arc<AtomicU64>,
}

impl AppState {
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
    request_id: u64,
    db_path: String,
}

#[derive(Debug, Serialize)]
struct VersionResponse {
    ok: bool,
    request_id: u64,
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
    request_id: u64,
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    message: String,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    ok: bool,
    request_id: u64,
    result: QueryResult,
}

#[derive(Debug, Serialize)]
struct BatchResponse {
    ok: bool,
    request_id: u64,
    results: Vec<QueryResult>,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let request_id = state.next_request_id();
    let _db = state.db.lock().await;
    info!(request_id, route = "/health", "request completed");
    Json(HealthResponse {
        ok: true,
        request_id,
        db_path: state.config.db_path.display().to_string(),
    })
}

async fn version(State(state): State<AppState>) -> Json<VersionResponse> {
    let request_id = state.next_request_id();
    info!(request_id, route = "/version", "request completed");
    Json(VersionResponse {
        ok: true,
        request_id,
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn execute(
    State(state): State<AppState>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    if request.sql.trim().is_empty() {
        warn!(request_id, route = "/execute", "rejected empty sql");
        return Err(error_response(request_id, "sql must not be empty"));
    }

    let mut db = state.db.lock().await;
    let result = db.execute(&request.sql).map_err(|error| {
        warn!(request_id, route = "/execute", error = %error, "request failed");
        error_response(request_id, error.to_string())
    })?;

    info!(request_id, route = "/execute", "request completed");
    Ok(Json(ExecuteResponse {
        ok: true,
        request_id,
        result,
    }))
}

async fn batch(
    State(state): State<AppState>,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    if request.statements.is_empty() {
        warn!(request_id, route = "/batch", "rejected empty batch");
        return Err(error_response(request_id, "statements must not be empty"));
    }

    let mut results = Vec::with_capacity(request.statements.len());
    let mut db = state.db.lock().await;

    for sql in request.statements {
        if sql.trim().is_empty() {
            warn!(
                request_id,
                route = "/batch",
                "rejected empty statement in batch"
            );
            return Err(error_response(
                request_id,
                "batch statements must not be empty",
            ));
        }

        let result = db.execute(&sql).map_err(|error| {
            warn!(request_id, route = "/batch", error = %error, "batch request failed");
            error_response(request_id, error.to_string())
        })?;
        results.push(result);
    }

    info!(
        request_id,
        route = "/batch",
        statement_count = results.len(),
        "request completed"
    );
    Ok(Json(BatchResponse {
        ok: true,
        request_id,
        results,
    }))
}

fn error_response(
    request_id: u64,
    message: impl Into<String>,
) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            ok: false,
            request_id,
            error: ErrorBody {
                message: message.into(),
            },
        }),
    )
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/execute", post(execute))
        .route("/batch", post(batch))
        .with_state(state)
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
        next_request_id: Arc::new(AtomicU64::new(1)),
    };
    let app = build_app(state.clone());

    info!(
        "starting skepa_db_server on {} using db {}",
        state.config.addr,
        state.config.db_path.display()
    );

    let listener = tokio::net::TcpListener::bind(state.config.addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request};
    use serde_json::{Value, json};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::util::ServiceExt;

    async fn test_app() -> Router {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("skepa-db-server-test-{unique}"));
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
        };
        build_app(state)
    }

    #[tokio::test]
    async fn execute_endpoint_returns_structured_result() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"create table users (id int, name text)"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["ok"], true);
        assert!(json["request_id"].as_u64().is_some());
        assert_eq!(
            json["result"]["SchemaChange"]["message"],
            "created table users"
        );
    }

    #[tokio::test]
    async fn batch_endpoint_executes_multiple_statements() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "statements": [
                                "create table users (id int, name text)",
                                "insert into users values (1, \"ram\")",
                                "select * from users"
                            ]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["ok"], true);
        assert_eq!(json["results"].as_array().expect("results array").len(), 3);
        assert_eq!(
            json["results"][0]["SchemaChange"]["message"],
            "created table users"
        );
        assert_eq!(json["results"][1]["Mutation"]["rows_affected"], 1);
        assert_eq!(json["results"][2]["Select"]["rows"][0][1], "ram");
    }

    #[tokio::test]
    async fn execute_endpoint_rejects_empty_sql() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"   "}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["ok"], false);
        assert_eq!(json["error"]["message"], "sql must not be empty");
    }
}
