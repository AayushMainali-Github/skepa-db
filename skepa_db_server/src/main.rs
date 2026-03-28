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
use skepa_db_core::storage::Schema;
use skepa_db_core::types::Row;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct ServerConfig {
    data_dir: PathBuf,
    default_database: String,
    addr: SocketAddr,
    auth_token: Option<String>,
    tls_terminated: bool,
}

#[derive(Debug, Default, Deserialize)]
struct ServerFileConfig {
    data_dir: Option<PathBuf>,
    default_database: Option<String>,
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
    sessions: Arc<Mutex<HashMap<u64, SessionEntry>>>,
}

#[derive(Debug, Clone)]
struct SessionEntry {
    database: String,
    db: Arc<Mutex<Database>>,
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
    data_dir: String,
    default_database: String,
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

#[derive(Debug, Deserialize)]
struct DatabaseCreateRequest {
    name: String,
}

#[derive(Debug, Serialize)]
struct DatabaseInfo {
    name: String,
    path: String,
    is_default: bool,
}

#[derive(Debug, Serialize)]
struct DatabaseListResponse {
    ok: bool,
    request_id: u64,
    databases: Vec<DatabaseInfo>,
}

#[derive(Debug, Serialize)]
struct DatabaseCreateResponse {
    ok: bool,
    request_id: u64,
    database: DatabaseInfo,
}

#[derive(Debug, Serialize)]
struct DatabaseDeletedResponse {
    ok: bool,
    request_id: u64,
    database: DatabaseInfo,
}

impl AppState {
    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    fn next_session_id(&self) -> u64 {
        self.next_session_id.fetch_add(1, Ordering::Relaxed)
    }

    fn data_dir(&self) -> PathBuf {
        self.config.data_dir.clone()
    }

    fn default_database_name(&self) -> String {
        self.config.default_database.clone()
    }

    fn database_path(&self, name: &str) -> PathBuf {
        self.data_dir().join(name)
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
    timeout_ms: Option<u64>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BatchRequest {
    statements: Vec<String>,
    timeout_ms: Option<u64>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    ok: bool,
    request_id: u64,
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: ApiErrorCode,
    message: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ApiErrorCode {
    InvalidRequest,
    Unauthorized,
    DatabaseAlreadyExists,
    DatabaseDeleteDenied,
    SqlParseError,
    TransactionRequiresSession,
    SessionNotFound,
    SessionHasActiveTransaction,
    UniqueViolation,
    NotNullViolation,
    ForeignKeyViolation,
    Conflict,
    Timeout,
    ExecutionError,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    ok: bool,
    request_id: u64,
    result: ApiQueryResult,
}

#[derive(Debug, Serialize)]
struct BatchResponse {
    ok: bool,
    request_id: u64,
    results: Vec<ApiQueryResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ApiQueryResult {
    Select {
        schema: Schema,
        rows: Vec<Row>,
        stats: skepa_db_core::execution_stats::ExecutionStats,
    },
    Mutation {
        message: String,
        rows_affected: usize,
        stats: skepa_db_core::execution_stats::ExecutionStats,
    },
    SchemaChange {
        message: String,
        stats: skepa_db_core::execution_stats::ExecutionStats,
    },
    Transaction {
        message: String,
        stats: skepa_db_core::execution_stats::ExecutionStats,
    },
}

impl From<QueryResult> for ApiQueryResult {
    fn from(value: QueryResult) -> Self {
        match value {
            QueryResult::Select {
                schema,
                rows,
                stats,
            } => Self::Select {
                schema,
                rows,
                stats,
            },
            QueryResult::Mutation {
                message,
                rows_affected,
                stats,
            } => Self::Mutation {
                message,
                rows_affected,
                stats,
            },
            QueryResult::SchemaChange { message, stats } => Self::SchemaChange { message, stats },
            QueryResult::Transaction { message, stats } => Self::Transaction { message, stats },
        }
    }
}

#[derive(Debug, Serialize)]
struct SessionResponse {
    ok: bool,
    request_id: u64,
    session_id: u64,
    database: String,
}

#[derive(Debug, Serialize)]
struct SessionDeletedResponse {
    ok: bool,
    request_id: u64,
    session_id: u64,
    database: String,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let request_id = state.next_request_id();
    let _db = state.db.lock().await;
    info!(request_id, route = "/health", "request completed");
    Json(HealthResponse {
        ok: true,
        request_id,
        db_path: state
            .database_path(&state.default_database_name())
            .display()
            .to_string(),
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

async fn list_databases(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DatabaseListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/databases")?;

    let data_dir = state.data_dir();
    fs::create_dir_all(&data_dir).map_err(|error| {
        warn!(request_id, route = "/databases", error = %error, "failed to ensure data directory");
        map_db_error_response(request_id, error.to_string())
    })?;

    let default_name = state.default_database_name();
    let mut databases = fs::read_dir(&data_dir)
        .map_err(|error| {
            warn!(request_id, route = "/databases", error = %error, "failed to read data directory");
            map_db_error_response(request_id, error.to_string())
        })?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_type = entry.file_type().ok()?;
            if !file_type.is_dir() {
                return None;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            Some(DatabaseInfo {
                path: entry.path().display().to_string(),
                is_default: name == default_name,
                name,
            })
        })
        .collect::<Vec<_>>();
    databases.sort_by(|left, right| left.name.cmp(&right.name));

    info!(
        request_id,
        route = "/databases",
        count = databases.len(),
        "request completed"
    );
    Ok(Json(DatabaseListResponse {
        ok: true,
        request_id,
        databases,
    }))
}

async fn create_database(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DatabaseCreateRequest>,
) -> Result<Json<DatabaseCreateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/databases")?;
    let name = validate_database_name(request_id, &request.name)?;
    let path = state.database_path(&name);

    if path.exists() {
        warn!(request_id, route = "/databases", database = %name, "database already exists");
        return Err(error_response(
            request_id,
            ApiErrorCode::DatabaseAlreadyExists,
            format!("database '{name}' already exists"),
        ));
    }

    Database::open(DbConfig::new(path.clone())).map_err(|error| {
        warn!(request_id, route = "/databases", database = %name, error = %error, "failed to initialize database");
        map_db_error_response(request_id, error.to_string())
    })?;

    info!(request_id, route = "/databases", database = %name, "database created");
    Ok(Json(DatabaseCreateResponse {
        ok: true,
        request_id,
        database: DatabaseInfo {
            path: path.display().to_string(),
            is_default: name == state.default_database_name(),
            name,
        },
    }))
}

async fn delete_database(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> Result<Json<DatabaseDeletedResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/databases/:name")?;
    let database = validate_database_name(request_id, &database)?;

    if database == state.default_database_name() {
        return Err(error_response(
            request_id,
            ApiErrorCode::DatabaseDeleteDenied,
            format!("database '{database}' is the configured default and cannot be deleted"),
        ));
    }

    let path = state.database_path(&database);
    if !path.exists() {
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            format!("database '{database}' does not exist"),
        ));
    }

    fs::remove_dir_all(&path).map_err(|error| {
        warn!(
            request_id,
            route = "/databases/:name",
            database,
            error = %error,
            "failed to delete database directory"
        );
        map_db_error_response(request_id, error.to_string())
    })?;

    info!(
        request_id,
        route = "/databases/:name",
        database,
        "database deleted"
    );
    Ok(Json(DatabaseDeletedResponse {
        ok: true,
        request_id,
        database: DatabaseInfo {
            path: path.display().to_string(),
            is_default: false,
            name: database,
        },
    }))
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
            db_path: state
                .database_path(&state.default_database_name())
                .display()
                .to_string(),
            data_dir: state.data_dir().display().to_string(),
            default_database: state.default_database_name(),
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
        if session.db.lock().await.has_active_transaction() {
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
        map_db_error_response(request_id, error.to_string())
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
        map_db_error_response(request_id, error.to_string())
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
        map_db_error_response(request_id, error.to_string())
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

    if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        info!(
            request_id,
            route = "/execute",
            idempotency_key,
            "received idempotency key"
        );
    }

    let result = execute_with_timeout(request_id, "/execute", request.timeout_ms, async {
        let mut db = state.db.lock().await;
        db.execute(&request.sql)
            .map_err(|error| map_db_error_response(request_id, error.to_string()))
    })
    .await?;

    info!(request_id, route = "/execute", "request completed");
    Ok(Json(ExecuteResponse {
        ok: true,
        request_id,
        result: result.into(),
    }))
}

async fn execute_database(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    validate_auth(&state, &headers, request_id, "/databases/:name/execute")?;
    let database = validate_database_name(request_id, &database)?;
    validate_global_sql(request_id, "/databases/:name/execute", &request.sql)?;

    if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        info!(
            request_id,
            route = "/databases/:name/execute",
            database,
            idempotency_key,
            "received idempotency key"
        );
    }

    let path = state.database_path(&database);
    if !path.exists() {
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            format!("database '{database}' does not exist"),
        ));
    }

    let result = execute_with_timeout(
        request_id,
        "/databases/:name/execute",
        request.timeout_ms,
        async {
            let mut db = Database::open(DbConfig::new(path)).map_err(|error| {
                warn!(
                    request_id,
                    route = "/databases/:name/execute",
                    database,
                    error = %error,
                    "failed to open database"
                );
                map_db_error_response(request_id, error.to_string())
            })?;
            db.execute(&request.sql)
                .map_err(|error| map_db_error_response(request_id, error.to_string()))
        },
    )
    .await?;

    info!(
        request_id,
        route = "/databases/:name/execute",
        database,
        "request completed"
    );
    Ok(Json(ExecuteResponse {
        ok: true,
        request_id,
        result: result.into(),
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
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            "statements must not be empty",
        ));
    }

    if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        info!(
            request_id,
            route = "/batch",
            idempotency_key,
            "received idempotency key"
        );
    }

    let results = execute_with_timeout(request_id, "/batch", request.timeout_ms, async {
        let mut results = Vec::with_capacity(request.statements.len());
        let mut db = state.db.lock().await;

        for sql in request.statements {
            validate_global_sql(request_id, "/batch", &sql)?;

            let result = db.execute(&sql).map_err(|error| {
                warn!(request_id, route = "/batch", error = %error, "batch request failed");
                map_db_error_response(request_id, error.to_string())
            })?;
            results.push(result.into());
        }

        Ok(results)
    })
    .await?;

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
    create_session_for_database(state, headers, None).await
}

async fn create_database_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(database): Path<String>,
) -> Result<Json<SessionResponse>, (StatusCode, Json<ErrorResponse>)> {
    create_session_for_database(state, headers, Some(database)).await
}

async fn create_session_for_database(
    state: AppState,
    headers: HeaderMap,
    database: Option<String>,
) -> Result<Json<SessionResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    let route = if database.is_some() {
        "/databases/:name/session"
    } else {
        "/session"
    };
    validate_auth(&state, &headers, request_id, route)?;
    let session_id = state.next_session_id();
    let database = match database {
        Some(database) => validate_database_name(request_id, &database)?,
        None => state.default_database_name(),
    };
    let database_path = state.database_path(&database);
    if !database_path.exists() {
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            format!("database '{database}' does not exist"),
        ));
    }

    let session_db = Database::open(DbConfig::new(database_path)).map_err(|error| {
        warn!(request_id, route, database = %database, error = %error, "session creation failed");
        map_db_error_response(request_id, error.to_string())
    })?;

    state.sessions.lock().await.insert(
        session_id,
        SessionEntry {
            database: database.clone(),
            db: Arc::new(Mutex::new(session_db)),
        },
    );

    info!(request_id, route, database, session_id, "session created");
    Ok(Json(SessionResponse {
        ok: true,
        request_id,
        session_id,
        database,
    }))
}

async fn delete_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<u64>,
) -> Result<Json<SessionDeletedResponse>, (StatusCode, Json<ErrorResponse>)> {
    delete_session_for_database(state, headers, None, session_id).await
}

async fn delete_database_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, session_id)): Path<(String, u64)>,
) -> Result<Json<SessionDeletedResponse>, (StatusCode, Json<ErrorResponse>)> {
    delete_session_for_database(state, headers, Some(database), session_id).await
}

async fn delete_session_for_database(
    state: AppState,
    headers: HeaderMap,
    database: Option<String>,
    session_id: u64,
) -> Result<Json<SessionDeletedResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    let route = if database.is_some() {
        "/databases/:name/session/:id"
    } else {
        "/session/:id"
    };
    validate_auth(&state, &headers, request_id, route)?;
    let expected_database = match database {
        Some(database) => Some(validate_database_name(request_id, &database)?),
        None => None,
    };
    let session = state.sessions.lock().await.get(&session_id).cloned();

    let Some(session) = session else {
        warn!(request_id, route, session_id, "session not found");
        return Err(error_response(
            request_id,
            ApiErrorCode::SessionNotFound,
            format!("session {session_id} was not found"),
        ));
    };

    if let Some(expected_database) = expected_database.as_deref()
        && session.database != expected_database
    {
        return Err(error_response(
            request_id,
            ApiErrorCode::SessionNotFound,
            format!("session {session_id} was not found"),
        ));
    }

    if session.db.lock().await.has_active_transaction() {
        warn!(
            request_id,
            route, session_id, "refused to delete session with active transaction"
        );
        return Err(error_response(
            request_id,
            ApiErrorCode::SessionHasActiveTransaction,
            format!("session {session_id} has an active transaction"),
        ));
    }

    state.sessions.lock().await.remove(&session_id);

    info!(
        request_id,
        route,
        database = %session.database,
        session_id,
        "session deleted"
    );
    Ok(Json(SessionDeletedResponse {
        ok: true,
        request_id,
        session_id,
        database: session.database,
    }))
}

async fn execute_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<u64>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    execute_session_for_database(state, headers, None, session_id, request).await
}

async fn execute_database_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((database, session_id)): Path<(String, u64)>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    execute_session_for_database(state, headers, Some(database), session_id, request).await
}

async fn execute_session_for_database(
    state: AppState,
    headers: HeaderMap,
    database: Option<String>,
    session_id: u64,
    request: ExecuteRequest,
) -> Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request_id = state.next_request_id();
    let route = if database.is_some() {
        "/databases/:name/session/:id/execute"
    } else {
        "/session/:id/execute"
    };
    validate_auth(&state, &headers, request_id, route)?;
    if request.sql.trim().is_empty() {
        warn!(request_id, route, session_id, "rejected empty sql");
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            "sql must not be empty",
        ));
    }

    let expected_database = match database {
        Some(database) => Some(validate_database_name(request_id, &database)?),
        None => None,
    };

    let session = state
        .sessions
        .lock()
        .await
        .get(&session_id)
        .cloned()
        .ok_or_else(|| {
            warn!(request_id, route, session_id, "session not found");
            error_response(
                request_id,
                ApiErrorCode::SessionNotFound,
                format!("session {session_id} was not found"),
            )
        })?;

    if let Some(expected_database) = expected_database.as_deref()
        && session.database != expected_database
    {
        return Err(error_response(
            request_id,
            ApiErrorCode::SessionNotFound,
            format!("session {session_id} was not found"),
        ));
    }

    if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        info!(
            request_id,
            route,
            database = %session.database,
            session_id,
            idempotency_key,
            "received idempotency key"
        );
    }

    let result = execute_with_timeout(request_id, route, request.timeout_ms, async {
        let mut db = session.db.lock().await;
        db.execute(&request.sql)
            .map_err(|error| map_db_error_response(request_id, error.to_string()))
    })
    .await?;

    info!(
        request_id,
        route,
        database = %session.database,
        session_id,
        "request completed"
    );
    Ok(Json(ExecuteResponse {
        ok: true,
        request_id,
        result: result.into(),
    }))
}

fn validate_global_sql(
    request_id: u64,
    route: &'static str,
    sql: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if sql.trim().is_empty() {
        warn!(request_id, route, "rejected empty sql");
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            "sql must not be empty",
        ));
    }

    let command = parse(sql).map_err(|error| {
        warn!(request_id, route, error = %error, "failed to parse sql");
        error_response(request_id, ApiErrorCode::SqlParseError, error)
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
            ApiErrorCode::TransactionRequiresSession,
            "transaction commands require a session endpoint",
        ));
    }

    Ok(())
}

fn error_response(
    request_id: u64,
    code: ApiErrorCode,
    message: impl Into<String>,
) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            ok: false,
            request_id,
            error: ErrorBody {
                code,
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
                code: ApiErrorCode::Unauthorized,
                message: message.into(),
            },
        }),
    )
}

fn map_db_error_response(
    request_id: u64,
    message: impl Into<String>,
) -> (StatusCode, Json<ErrorResponse>) {
    let message = message.into();
    let lowercase = message.to_lowercase();
    let code = if lowercase.contains("unique")
        || lowercase.contains("primary key")
        || lowercase.contains("duplicate")
    {
        ApiErrorCode::UniqueViolation
    } else if lowercase.contains("not null") {
        ApiErrorCode::NotNullViolation
    } else if lowercase.contains("foreign key") || lowercase.contains("references") {
        ApiErrorCode::ForeignKeyViolation
    } else if lowercase.contains("conflict") {
        ApiErrorCode::Conflict
    } else if lowercase.contains("parse") || lowercase.contains("syntax") {
        ApiErrorCode::SqlParseError
    } else {
        ApiErrorCode::ExecutionError
    };

    error_response(request_id, code, message)
}

fn validate_database_name(
    request_id: u64,
    name: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            "database name must not be empty",
        ));
    }

    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(error_response(
            request_id,
            ApiErrorCode::InvalidRequest,
            "database name may only contain letters, digits, '-' and '_'",
        ));
    }

    Ok(trimmed.to_string())
}

async fn execute_with_timeout<T, F>(
    request_id: u64,
    route: &'static str,
    timeout_ms: Option<u64>,
    future: F,
) -> Result<T, (StatusCode, Json<ErrorResponse>)>
where
    F: std::future::Future<Output = Result<T, (StatusCode, Json<ErrorResponse>)>>,
{
    if let Some(timeout_ms) = timeout_ms {
        match timeout(Duration::from_millis(timeout_ms), future).await {
            Ok(result) => result,
            Err(_) => {
                warn!(request_id, route, timeout_ms, "request timed out");
                Err(error_response(
                    request_id,
                    ApiErrorCode::Timeout,
                    format!("request exceeded timeout of {timeout_ms}ms"),
                ))
            }
        }
    } else {
        future.await
    }
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
        .route("/databases", get(list_databases).post(create_database))
        .route("/databases/{name}", delete(delete_database))
        .route("/databases/{name}/execute", post(execute_database))
        .route("/databases/{name}/session", post(create_database_session))
        .route(
            "/databases/{name}/session/{id}",
            delete(delete_database_session),
        )
        .route(
            "/databases/{name}/session/{id}/execute",
            post(execute_database_session),
        )
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

    let mut db_path = file_config.db_path;
    let mut data_dir = file_config.data_dir.unwrap_or_else(|| PathBuf::from("."));
    let mut default_database = file_config
        .default_database
        .unwrap_or_else(|| "mydb".to_string());
    if let Ok(env_path) = env::var("SKEPA_DB_PATH") {
        db_path = Some(PathBuf::from(env_path));
    }
    if let Ok(env_data_dir) = env::var("SKEPA_DB_DATA_DIR") {
        data_dir = PathBuf::from(env_data_dir);
    }
    if let Ok(env_default_database) = env::var("SKEPA_DB_DEFAULT_DATABASE") {
        default_database = env_default_database;
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
                db_path = Some(args.next().ok_or("missing value for --db-path")?.into());
            }
            "--data-dir" => {
                data_dir = args.next().ok_or("missing value for --data-dir")?.into();
            }
            "--default-database" => {
                default_database = args.next().ok_or("missing value for --default-database")?;
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

    if let Some(db_path) = db_path {
        data_dir = db_path
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        default_database = db_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("db path must end with a database directory name")?
            .to_string();
    }

    if default_database.trim().is_empty() {
        return Err("default database must not be empty".into());
    }

    Ok(ServerConfig {
        data_dir,
        default_database,
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
    let default_db_path = config.data_dir.join(&config.default_database);
    let db = Database::open(DbConfig::new(default_db_path.clone()))?;
    let state = AppState {
        db: Arc::new(Mutex::new(db)),
        config,
        next_request_id: Arc::new(AtomicU64::new(1)),
        next_session_id: Arc::new(AtomicU64::new(1)),
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };
    let app = build_app(state.clone());

    info!(
        "starting skepa_db_server on {} using data dir {} with default database {}",
        state.config.addr,
        state.config.data_dir.display(),
        state.config.default_database
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

    async fn test_state() -> AppState {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let root_dir = std::env::temp_dir().join(format!(
            "skepa-db-server-test-{}-{unique}-{id}",
            std::process::id()
        ));
        let db_path = root_dir.join("default");
        let config = ServerConfig {
            data_dir: root_dir,
            default_database: "default".to_string(),
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            auth_token: None,
            tls_terminated: false,
        };
        let db = Database::open(DbConfig::new(db_path)).expect("test db should open");
        AppState {
            db: Arc::new(Mutex::new(db)),
            config,
            next_request_id: Arc::new(AtomicU64::new(1)),
            next_session_id: Arc::new(AtomicU64::new(1)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn test_app() -> Router {
        build_app(test_state().await)
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
        assert_eq!(json["result"]["type"], "schema_change");
        assert_eq!(json["result"]["message"], "created table users");
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
        assert_eq!(json["results"][0]["type"], "schema_change");
        assert_eq!(json["results"][0]["message"], "created table users");
        assert_eq!(json["results"][1]["type"], "mutation");
        assert_eq!(json["results"][1]["rows_affected"], 1);
        assert_eq!(json["results"][2]["type"], "select");
        assert_eq!(json["results"][2]["rows"][0][1], "ram");
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
        assert_eq!(json["error"]["code"], "INVALID_REQUEST");
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
        assert_eq!(json["error"]["code"], "TRANSACTION_REQUIRES_SESSION");
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
        assert_eq!(json["error"]["code"], "TRANSACTION_REQUIRES_SESSION");
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
        assert_eq!(json["error"]["code"], "SESSION_HAS_ACTIVE_TRANSACTION");
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
        assert_eq!(json["error"]["code"], "SESSION_NOT_FOUND");
        assert_eq!(json["error"]["message"], "session 999 was not found");
    }

    #[tokio::test]
    async fn create_database_session_binds_session_to_named_database() {
        let app = test_app().await;

        let create_database_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_database_response.status(), StatusCode::OK);

        let create_session_response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(create_session_response.status(), StatusCode::OK);
        let body = to_bytes(create_session_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["session_id"], 1);
        assert_eq!(json["database"], "analytics");
    }

    #[tokio::test]
    async fn database_session_execute_uses_named_database_transaction_state() {
        let app = test_app().await;

        let create_database_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_database_response.status(), StatusCode::OK);

        let create_table_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"create table users (id int primary key, name text)"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_table_response.status(), StatusCode::OK);

        let create_session_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_session_response.status(), StatusCode::OK);

        let begin_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"begin"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(begin_response.status(), StatusCode::OK);

        let insert_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"insert into users values (1, \"ram\")"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(insert_response.status(), StatusCode::OK);

        let stateless_select_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select * from users"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(stateless_select_response.status(), StatusCode::OK);
        let body = to_bytes(stateless_select_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["result"]["rows"].as_array().unwrap().len(), 0);

        let commit_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"commit"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(commit_response.status(), StatusCode::OK);

        let committed_select_response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select * from users"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(committed_select_response.status(), StatusCode::OK);
        let body = to_bytes(committed_select_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["result"]["rows"][0][1], "ram");
    }

    #[tokio::test]
    async fn database_session_routes_reject_session_for_other_database() {
        let app = test_app().await;

        for database in ["analytics", "archive"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/databases")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(r#"{{"name":"{database}"}}"#)))
                        .expect("request should build"),
                )
                .await
                .expect("request should succeed");
            assert_eq!(response.status(), StatusCode::OK);
        }

        let create_session_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/session")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_session_response.status(), StatusCode::OK);

        let wrong_database_response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/archive/session/1/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select 1"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(wrong_database_response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(wrong_database_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["error"]["code"], "SESSION_NOT_FOUND");
        assert_eq!(json["error"]["message"], "session 1 was not found");
    }

    #[tokio::test]
    async fn execute_endpoint_returns_timeout_code() {
        let state = test_state().await;
        let db_guard = state.db.lock().await;
        let app = build_app(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "sql": "create table users (id int)",
                            "timeout_ms": 0
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        drop(db_guard);

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["error"]["code"], "TIMEOUT");
    }

    #[tokio::test]
    async fn execute_requires_bearer_token_when_configured() {
        let db_path = std::env::temp_dir().join("skepa-db-server-auth-test");
        let config = ServerConfig {
            data_dir: db_path
                .parent()
                .expect("db path should have parent")
                .to_path_buf(),
            default_database: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("db path should have file name")
                .to_string(),
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
        assert_eq!(json["error"]["code"], "UNAUTHORIZED");
        assert_eq!(json["error"]["message"], "missing bearer token");
    }

    #[tokio::test]
    async fn execute_endpoint_maps_constraint_errors_to_stable_codes() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"create table users (id int primary key)"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let first_insert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"insert into users values (1)"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(first_insert.status(), StatusCode::OK);

        let duplicate_insert = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"insert into users values (1)"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(duplicate_insert.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(duplicate_insert.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["error"]["code"], "UNIQUE_VIOLATION");
    }

    #[tokio::test]
    async fn health_stays_public_when_auth_is_configured() {
        let db_path = std::env::temp_dir().join("skepa-db-server-auth-health-test");
        let config = ServerConfig {
            data_dir: db_path
                .parent()
                .expect("db path should have parent")
                .to_path_buf(),
            default_database: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("db path should have file name")
                .to_string(),
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
            data_dir: db_path
                .parent()
                .expect("db path should have parent")
                .to_path_buf(),
            default_database: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("db path should have file name")
                .to_string(),
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
        assert_eq!(
            json["config"]["data_dir"],
            db_path
                .parent()
                .expect("db path should have parent")
                .display()
                .to_string()
        );
        assert_eq!(
            json["config"]["default_database"],
            "skepa-db-server-config-test"
        );
        assert!(json["config"].get("auth_token").is_none());
    }

    #[tokio::test]
    async fn list_databases_reports_default_database_directory() {
        let state = test_state().await;
        let expected_name = state.default_database_name();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/databases")
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
        let databases = json["databases"].as_array().expect("databases array");
        assert!(databases
            .iter()
            .any(|database| database["name"] == expected_name && database["is_default"] == true));
    }

    #[tokio::test]
    async fn create_database_creates_named_database_directory() {
        let app = test_app().await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["database"]["name"], "analytics");
        assert_eq!(json["database"]["is_default"], false);

        let list_response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/databases")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(list_response.status(), StatusCode::OK);
        let body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        let databases = json["databases"].as_array().expect("databases array");
        assert!(
            databases
                .iter()
                .any(|database| database["name"] == "analytics")
        );
    }

    #[tokio::test]
    async fn named_database_execute_uses_requested_database() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let create_table_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"create table users (id int primary key, name text)"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_table_response.status(), StatusCode::OK);

        let insert_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"sql":"insert into users values (1, \"ram\")"}"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(insert_response.status(), StatusCode::OK);

        let select_response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/analytics/execute")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"select * from users"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(select_response.status(), StatusCode::OK);
        let body = to_bytes(select_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["result"]["type"], "select");
        assert_eq!(json["result"]["rows"][0][1], "ram");
    }

    #[tokio::test]
    async fn named_database_execute_rejects_missing_database() {
        let app = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases/missing/execute")
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
        assert_eq!(
            json["error"]["message"],
            "database 'missing' does not exist"
        );
    }

    #[tokio::test]
    async fn delete_database_removes_named_database_directory() {
        let app = test_app().await;

        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"analytics"}"#))
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(create_response.status(), StatusCode::OK);

        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/databases/analytics")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");
        assert_eq!(delete_response.status(), StatusCode::OK);

        let list_response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/databases")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(list_response.status(), StatusCode::OK);
        let body = to_bytes(list_response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        let databases = json["databases"].as_array().expect("databases array");
        assert!(
            !databases
                .iter()
                .any(|database| database["name"] == "analytics")
        );
    }

    #[tokio::test]
    async fn delete_database_rejects_default_database() {
        let state = test_state().await;
        let default_name = state.default_database_name();
        let app = build_app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/databases/{default_name}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        let json: Value = serde_json::from_slice(&body).expect("json body should parse");
        assert_eq!(json["error"]["code"], "DATABASE_DELETE_DENIED");
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
            data_dir: db_path
                .parent()
                .expect("db path should have parent")
                .to_path_buf(),
            default_database: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("db path should have file name")
                .to_string(),
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
            data_dir: db_path
                .parent()
                .expect("db path should have parent")
                .to_path_buf(),
            default_database: db_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("db path should have file name")
                .to_string(),
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
