use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn create_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(5) // max connections
        .connect(database_url) // connect to the database
        .await
        .expect("failed to connect to Postgres")
}
