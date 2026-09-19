use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct CreateJobRequest {
    pub job_type: String,
    pub payload: serde_json::Value,
    pub max_attempts: Option<i32>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub attempts: i32,
    pub max_attempts: i32,
    pub error: Option<String>,
    pub status: String,
    pub run_at: DateTime<Utc>,
}
