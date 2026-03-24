use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Serialize;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let app = Router::new().route("/health", get(health));
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;

    info!("starting skepa_db_server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
