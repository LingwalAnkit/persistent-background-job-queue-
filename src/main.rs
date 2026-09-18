mod config;
mod db;
mod executor;
mod handlers;
mod modles;
mod routes;
mod worker;

use crate::config::Config;

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    let pool = db::create_pool(&config.database_url).await;
    worker::start_worker(pool.clone(), 4);
    let app = routes::build_routes(pool);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
