use std::path;

use axum::{
    Json,
    extract::{Path, State},
    http::{self, StatusCode},
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
            INSERT INTO jobs (job_type, payload, created_at, updated_at, attempts, max_attempts, error)
            VALUES ($1, $2)
            RETURNING id, job_type, payload, created_at, updated_at, attempts, max_attempts, error
        "#
    )
    .bind(&req.job_type)
    .bind(&req.payload)
    .fetch_one(&pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(job))
}

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
