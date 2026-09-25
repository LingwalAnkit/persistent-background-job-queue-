mod config;
mod db;
mod executor;
mod handlers;
mod modles;
mod routes;
mod worker;

use crate::config::Config;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let token = CancellationToken::new();
    let config = Config::from_env();
    let pool = db::create_pool(&config.database_url).await;
    {
        let token = token.clone();
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
        tracing::info!("shutdown signal received, finishing in-flight work...");
        token.cancel();
    }
    let worker_handles = worker::start_workers(pool.clone(), 4, token.clone());
    let app = routes::build_routes(pool);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    tracing::info!(addr = %&listener.local_addr().unwrap(), "listening");
    let shutdown_token = token.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_token.cancelled().await;
        })
        .await
        .unwrap();
    for handle in worker_handles {
        let _ = handle.await;
    }

    tracing::info!("shutdown complete");
}
