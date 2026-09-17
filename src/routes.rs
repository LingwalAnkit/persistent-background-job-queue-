use axum::{
    Router,
    routing::{get, post},
};
use sqlx::PgPool;

use crate::handlers::{
    health::health,
    job::{create_job, get_job},
};

pub fn build_routes(pool: PgPool) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/jobs", post(create_job))
        .route("/job/:id", get(get_job))
        .with_state(pool)
}
