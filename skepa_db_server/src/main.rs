use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use skepa_db_core::Database;
use skepa_db_core::config::DbConfig;
use skepa_db_core::parser::command::Command;
use skepa_db_core::parser::parser::parse;
use skepa_db_core::query_result::QueryResult;
use std::collections::HashMap;
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
    auth_token: Option<String>,
    tls_terminated: bool,
}

#[derive(Debug, Default, Deserialize)]
struct ServerFileConfig {
    db_path: Option<PathBuf>,
    addr: Option<String>,
    auth_token: Option<String>,
    tls_terminated: Option<bool>,
}

#[derive(Debug, Clone)]
struct AppState {
    db: Arc<Mutex<Database>>,
    config: ServerConfig,
    next_request_id: Arc<AtomicU64>,
    next_session_id: Arc<AtomicU64>,
    sessions: Arc<Mutex<HashMap<u64, Arc<Mutex<Database>>>>>,
}

#[derive(Debug, Serialize)]
struct ConfigResponse {
    ok: bool,
    request_id: u64,
    config: ConfigView,
}

#[derive(Debug, Serialize)]
struct ConfigView {
    db_path: String,
    addr: String,
    auth_enabled: bool,
    tls_terminated: bool,
}

#[derive(Debug, Serialize)]
struct MetricsResponse {
    ok: bool,
    request_id: u64,
    metrics: MetricsView,
}

#[derive(Debug, Serialize)]
struct MetricsView {
    request_count_issued: u64,
    session_count: usize,
    auth_enabled: bool,
    active_session_transactions: usize,
}

#[derive(Debug, Serialize)]
struct DebugResponse {
    ok: bool,
    request_id: u64,
    data: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct CheckpointResponse {
    ok: bool,
    request_id: u64,
    message: String,
}

impl AppState {
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    fn next_session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::Relaxed)
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

#[derive(Debug, Serialize)]
struct SessionResponse {
    ok: bool,
    request_id: u64,
    session_id: u64,
}

#[derive(Debug, Serialize)]
struct SessionDeletedResponse {
    ok: bool,
    request_id: u64,
    session_id: u64,
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

async fn config_view(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ConfigResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/config")?;

    info!(request_id, route = "/config", "request completed");
    Ok(Json(ConfigResponse {
        ok: true,
        request_id,
        config: ConfigView {
            db_path: state.config.db_path.display().to_string(),
            addr: state.config.addr.to_string(),
            auth_enabled: state.config.auth_token.is_some(),
            tls_terminated: state.config.tls_terminated,
        },
    }))
}

async fn metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<MetricsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/metrics")?;

    let session_handles: Vec<_> = state.sessions.lock().await.values().cloned().collect();
    let mut active_session_transactions = 0usize;
    for session in session_handles {
        if session.lock().await.has_active_transaction() {
            active_session_transactions += 1;
        }
    }

    info!(request_id, route = "/metrics", "request completed");
    Ok(Json(MetricsResponse {
        ok: true,
        request_id,
        metrics: MetricsView {
            request_count_issued: state.next_request_id.load(Ordering::Relaxed) - 1,
            session_count: state.sessions.lock().await.len(),
            auth_enabled: state.config.auth_token.is_some(),
            active_session_transactions,
        },
    }))
}

async fn debug_catalog(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DebugResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/debug/catalog")?;

    let db = state.db.lock().await;
    let data = db.debug_catalog_json().map_err(|error| {
        warn!(request_id, route = "/debug/catalog", error = %error, "request failed");
        error_response(request_id, error.to_string())
    })?;

    info!(request_id, route = "/debug/catalog", "request completed");
    Ok(Json(DebugResponse {
        ok: true,
        request_id,
        data,
    }))
}

async fn debug_storage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DebugResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/debug/storage")?;

    let db = state.db.lock().await;
    let data = db.debug_storage_json().map_err(|error| {
        warn!(request_id, route = "/debug/storage", error = %error, "request failed");
        error_response(request_id, error.to_string())
    })?;

    info!(request_id, route = "/debug/storage", "request completed");
    Ok(Json(DebugResponse {
        ok: true,
        request_id,
        data,
    }))
}

async fn checkpoint(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CheckpointResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/checkpoint")?;

    let db = state.db.lock().await;
    db.checkpoint().map_err(|error| {
        warn!(request_id, route = "/checkpoint", error = %error, "checkpoint failed");
        error_response(request_id, error.to_string())
    })?;

    info!(request_id, route = "/checkpoint", "request completed");
    Ok(Json(CheckpointResponse {
        ok: true,
        request_id,
        message: "checkpoint completed".to_string(),
    }))
}

async fn execute(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/execute")?;
    validate_global_sql(request_id, "/execute", &request.sql)?;

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
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<Json<BatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/batch")?;
    if request.statements.is_empty() {
        warn!(request_id, route = "/batch", "rejected empty batch");
        return Err(error_response(request_id, "statements must not be empty"));
    }

    let mut results = Vec::with_capacity(request.statements.len());
    let mut db = state.db.lock().await;

    for sql in request.statements {
        validate_global_sql(request_id, "/batch", &sql)?;

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

async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SessionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/session")?;
    let session_id = state.next_session_id();
    let session_db =
        Database::open(DbConfig::new(state.config.db_path.clone())).map_err(|error| {
            warn!(request_id, route = "/session", error = %error, "session creation failed");
            error_response(request_id, error.to_string())
        })?;

    state
        .sessions
        .lock()
        .await
        .insert(session_id, Arc::new(Mutex::new(session_db)));

    info!(
        request_id,
        route = "/session",
        session_id,
        "session created"
    );
    Ok(Json(SessionResponse {
        ok: true,
        request_id,
        session_id,
    }))
}

async fn delete_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<u64>,
) -> Result<Json<SessionDeletedResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/session/:id")?;
    let session_db = state.sessions.lock().await.get(&session_id).cloned();

    let Some(session_db) = session_db else {
        warn!(
            request_id,
            route = "/session/:id",
            session_id,
            "session not found"
        );
        return Err(error_response(
            request_id,
            format!("session {session_id} was not found"),
        ));
    };

    if session_db.lock().await.has_active_transaction() {
        warn!(
            request_id,
            route = "/session/:id",
            session_id,
            "refused to delete session with active transaction"
        );
        return Err(error_response(
            request_id,
            format!("session {session_id} has an active transaction"),
        ));
    }

    state.sessions.lock().await.remove(&session_id);

    info!(
        request_id,
        route = "/session/:id",
        session_id,
        "session deleted"
    );
    Ok(Json(SessionDeletedResponse {
        ok: true,
        request_id,
        session_id,
    }))
}

async fn execute_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<u64>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/session/:id/execute")?;
    if request.sql.trim().is_empty() {
        warn!(
            request_id,
            route = "/session/:id/execute",
            session_id,
            "rejected empty sql"
        );
        return Err(error_response(request_id, "sql must not be empty"));
    }

    let session_db = state
        .sessions
        .lock()
        .await
        .get(&session_id)
        .cloned()
        .ok_or_else(|| {
            warn!(
                request_id,
                route = "/session/:id/execute",
                session_id,
                "session not found"
            );
            error_response(request_id, format!("session {session_id} was not found"))
        })?;

    let mut db = session_db.lock().await;
    let result = db.execute(&request.sql).map_err(|error| {
        warn!(
            request_id,
            route = "/session/:id/execute",
            session_id,
            error = %error,
            "request failed"
        );
        error_response(request_id, error.to_string())
    })?;

    info!(
        request_id,
        route = "/session/:id/execute",
        session_id,
        "request completed"
    );
    Ok(Json(ExecuteResponse {
        ok: true,
        request_id,
        result,
    }))
}

fn validate_global_sql(
    request_id: u64,
    route: &'static str,
    sql: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if sql.trim().is_empty() {
        warn!(request_id, route, "rejected empty sql");
        return Err(error_response(request_id, "sql must not be empty"));
    }

    let command = parse(sql).map_err(|error| {
        warn!(request_id, route, error = %error, "failed to parse sql");
        error_response(request_id, error)
    })?;

    if matches!(
        command,
        Command::Begin | Command::Commit | Command::Rollback
    ) {
        warn!(
            request_id,
            route, "rejected transaction command on global endpoint"
        );
        return Err(error_response(
            request_id,
            "transaction commands require a session endpoint",
        ));
    }

    Ok(())
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

fn unauthorized_response(
    request_id: u64,
    message: impl Into<String>,
) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse {
            ok: false,
            request_id,
            error: ErrorBody {
                message: message.into(),
            },
        }),
    )
}

fn validate_auth(
    state: &AppState,
    headers: &HeaderMap,
    request_id: u64,
    route: &'static str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let Some(expected) = state.config.auth_token.as_deref() else {
        return Ok(());
    };

    let Some(header_value) = headers.get(axum::http::header::AUTHORIZATION) else {
        warn!(request_id, route, "missing authorization header");
        return Err(unauthorized_response(request_id, "missing bearer token"));
    };

    let Ok(header_value) = header_value.to_str() else {
        warn!(request_id, route, "invalid authorization header");
        return Err(unauthorized_response(
            request_id,
            "invalid authorization header",
        ));
    };

    let Some(provided) = header_value.strip_prefix("Bearer ") else {
        warn!(request_id, route, "authorization header was not bearer");
        return Err(unauthorized_response(
            request_id,
            "authorization must use Bearer token",
        ));
    };

    if provided != expected {
        warn!(request_id, route, "invalid bearer token");
        return Err(unauthorized_response(request_id, "invalid bearer token"));
    }

    Ok(())
}

fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/config", get(config_view))
        .route("/metrics", get(metrics))
        .route("/debug/catalog", get(debug_catalog))
        .route("/debug/storage", get(debug_storage))
        .route("/checkpoint", post(checkpoint))
        .route("/execute", post(execute))
        .route("/batch", post(batch))
        .route("/session", post(create_session))
        .route("/session/{id}", delete(delete_session))
        .route("/session/{id}/execute", post(execute_session))
        .with_state(state)
}

fn parse_server_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let mut config_path: Option<PathBuf> = env::var("SKEPA_DB_CONFIG").ok().map(PathBuf::from);

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg.as_str() == "--config" {
            config_path = Some(PathBuf::from(
                args.next().ok_or("missing value for --config")?,
            ));
        }
    }

    let file_config = if let Some(path) = config_path {
        let raw = std::fs::read_to_string(&path)?;
        serde_json::from_str::<ServerFileConfig>(&raw)?
    } else {
        ServerFileConfig::default()
    };

    let mut db_path = file_config
        .db_path
        .unwrap_or_else(|| PathBuf::from("./mydb"));
    if let Ok(env_path) = env::var("SKEPA_DB_PATH") {
        db_path = PathBuf::from(env_path);
    }

    let mut addr = file_config
        .addr
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    if let Ok(env_addr) = env::var("SKEPA_DB_ADDR") {
        addr = env_addr;
    }

    let mut auth_token = file_config.auth_token;
    if let Ok(env_token) = env::var("SKEPA_DB_AUTH_TOKEN") {
        auth_token = Some(env_token);
    }

    let mut tls_terminated = file_config.tls_terminated.unwrap_or(false);
    if let Ok(env_tls) = env::var("SKEPA_DB_TLS_TERMINATED") {
        tls_terminated = matches!(env_tls.to_lowercase().as_str(), "1" | "true" | "yes");
    }

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db-path" => {
                db_path = args.next().ok_or("missing value for --db-path")?.into();
            }
            "--addr" => {
                addr = args.next().ok_or("missing value for --addr")?;
            }
            "--auth-token" => {
                auth_token = Some(args.next().ok_or("missing value for --auth-token")?);
            }
            "--tls-terminated" => {
                tls_terminated = true;
            }
            "--config" => {
                let _ = args.next().ok_or("missing value for --config")?;
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    Ok(ServerConfig {
        db_path,
        addr: addr.parse()?,
        auth_token,
        tls_terminated,
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
        next_session_id: Arc::new(AtomicU64::new(1)),
        sessions: Arc::new(Mutex::new(HashMap::new())),
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tower::util::ServiceExt;

    async fn test_app() -> Router {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_path = std::env::temp_dir().join(format!(
            "skepa-db-server-test-{}-{unique}-{id}",
            std::process::id()
        ));
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: None,
            tls_terminated: false,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
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

    #[tokio::test]
    async fn execute_endpoint_rejects_transaction_commands() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"begin"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(
            json["error"]["message"],
            "transaction commands require a session endpoint"
        );
    }

    #[tokio::test]
    async fn batch_endpoint_rejects_transaction_commands() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "statements": ["create table users (id int)", "begin"]
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(
            json["error"]["message"],
            "transaction commands require a session endpoint"
        );
    }

    #[tokio::test]
    async fn create_session_returns_session_id() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session")
                    .body(Body::empty())
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
        assert_eq!(json["session_id"], 1);
    }

    #[tokio::test]
    async fn delete_session_removes_existing_session() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/session/1")
                    .body(Body::empty())
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
        assert_eq!(json["session_id"], 1);
    }

    #[tokio::test]
    async fn delete_session_rejects_active_transaction() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"begin"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(begin_response.status(), StatusCode::OK);

        let delete_response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/session/1")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(delete_response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(delete_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(
            json["error"]["message"],
            "session 1 has an active transaction"
        );
    }

    #[tokio::test]
    async fn session_execute_uses_session_scoped_transaction_state() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"begin"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(begin_response.status(), StatusCode::OK);

        let global_commit_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"commit"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(global_commit_response.status(), StatusCode::BAD_REQUEST);

        let session_commit_response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"commit"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(session_commit_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn session_execute_rejects_unknown_session() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/session/999/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select * from users"}"#))
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
        assert_eq!(json["error"]["message"], "session 999 was not found");
    }

    #[tokio::test]
    async fn execute_requires_bearer_token_when_configured() {
        let db_path = std::env::temp_dir().join("skepa-db-server-auth-test");
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: Some("secret-token".to_string()),
            tls_terminated: false,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select * from users"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["error"]["message"], "missing bearer token");
    }

    #[tokio::test]
    async fn health_stays_public_when_auth_is_configured() {
        let db_path = std::env::temp_dir().join("skepa-db-server-auth-health-test");
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: Some("secret-token".to_string()),
            tls_terminated: true,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn config_endpoint_masks_auth_value_but_reports_auth_enabled() {
        let db_path = std::env::temp_dir().join("skepa-db-server-config-test");
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: Some("secret-token".to_string()),
            tls_terminated: true,
        };
        let db = Database::open(DbConfig::new(db_path.clone())).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/config")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["config"]["auth_enabled"], true);
        assert_eq!(json["config"]["tls_terminated"], true);
        assert_eq!(json["config"]["db_path"], db_path.display().to_string());
        assert!(json["config"].get("auth_token").is_none());
    }

    #[tokio::test]
    async fn metrics_endpoint_reports_basic_server_state() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert!(json["metrics"]["request_count_issued"].as_u64().is_some());
        assert_eq!(json["metrics"]["session_count"], 0);
        assert_eq!(json["metrics"]["auth_enabled"], false);
    }

    #[tokio::test]
    async fn debug_catalog_requires_auth_when_configured() {
        let db_path = std::env::temp_dir().join("skepa-db-server-debug-catalog-test");
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: Some("secret-token".to_string()),
            tls_terminated: false,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/debug/catalog")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn checkpoint_endpoint_runs_when_authorized() {
        let db_path = std::env::temp_dir().join("skepa-db-server-checkpoint-test");
        let config = ServerConfig {
            db_path: db_path.clone(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: Some("secret-token".to_string()),
            tls_terminated: false,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        };
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/checkpoint")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["message"], "checkpoint completed");
    }
}
