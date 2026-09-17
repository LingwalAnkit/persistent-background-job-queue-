use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use sqlx::PgPool;

use uuid::Uuid;

use crate::{modles::CreateJobRequest, modles::Job};

pub async fn create_job(
    State(pool): State<PgPool>,
    Json(req): Json<CreateJobRequest>,
) -> Result<Json<Job>, StatusCode> {
    let job = sqlx::query_as::<_, Job>(
        r#"
            INSERT INTO jobs (job_type, payload)
            VALUES ($1, $2)
            RETURNING id, job_type, payload, created_at, updated_at, attempts, max_attempts, error
        "#,
    ) // VALUES ($1, $2) placeholder
    .bind(&req.job_type)
    .bind(&req.payload)
    .fetch_one(&pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(job))
}

//"Run this SQL query, and take the row(s) returned by PostgreSQL and convert them into my Rust Job struct."
// jobs is the table name
// fetch_one = "I expect a row. Give me one row."

pub async fn get_job(
    State(pool): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, StatusCode> {
    let job = sqlx::query_as::<_, Job>(
        r#"
        SELECT id, job_type, payload, status, attempts, max_attempts, error, created_at, updated_at
        FROM jobs
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(&pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    job.map(Json).ok_or(StatusCode::NOT_FOUND)
}

// fetch_optional = "I expect zero or one row. Give me one row if it exists."
