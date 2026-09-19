mod config;
mod db;
mod executor;
mod handlers;
mod modles;
mod routes;
mod worker;

use tokio_util::sync::CancellationToken;

use crate::config::Config;

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();
    let config = Config::from_env();
    let pool = db::create_pool(&config.database_url).await;
    {
        let token = token.clone();
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
        println!("Shutdown signal received, finihing in flight work...");
        token.cancel();
    }
    let worker_handles = worker::start_workers(pool.clone(), 4, token.clone());
    let app = routes::build_routes(pool);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

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

    println!("shutdown complete");
}
