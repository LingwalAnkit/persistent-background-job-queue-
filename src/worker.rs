use sqlx::PgPool;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::executor::execute_job;
use crate::modles::Job;
use crate::worker;

pub fn start_workers(
    pool: PgPool,
    num_workers: usize,
    token: CancellationToken,
) -> Vec<JoinHandle<()>> {
    (0..num_workers)
        .map(|worker_id| {
            let pool = pool.clone();
            let token = token.clone();
            tokio::spawn(async move { worker_loop(worker_id, pool, token).await })
        })
        .collect()
}

async fn worker_loop(worker_id: usize, pool: PgPool, token: CancellationToken) {
    loop {
        if token.is_cancelled() {
            info!(worker_id, "shutting down");
            break;
        }

        match claim_next_job(&pool).await {
            Ok(Some(job)) => {
                info!(
                    worker_id,
                    job_id = %job.id,
                    job_type = %job.job_type,
                    attempt = job.attempts,
                    max_attempts = job.max_attempts,
                    "claimed job"
                );

                match execute_job(&job).await {
                    Ok(()) => match mark_succeeded(&pool, job.id).await {
                        Ok(_) => info!(worker_id, job_id = %job.id, "job succeeded"),
                        Err(e) => {
                            error!(worker_id , job_id = %job.id , error = %e , "failed to save success")
                        }
                    },
                    Err(err_msg) => match handle_failure(&pool, &job, &err_msg).await {
                        Ok(true) => {
                            warn!(worker_id, job_id = %job.id, error = %err_msg, "job failed, will retry")
                        }
                        Ok(false) => {
                            error!(worker_id, job_id = %job.id, error = %err_msg, "job permanently failed")
                        }
                        Err(e) => {
                            error!(worker_id, job_id = %job.id, error = %e, "failed to save failure")
                        }
                    },
                }
                // loop back around immediately — a job just finished, check for cancellation next
            }
            Ok(None) => {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = token.cancelled() => {
                        info!(worker_id, "shutting down");
                        break;
                    }
                }
            }
            Err(e) => {
                error!(worker_id, error = %e, "error claiming job");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = token.cancelled() => break,
                }
            }
        }
    }
}

async fn claim_next_job(pool: &PgPool) -> Result<Option<Job>, sqlx::Error> {
    sqlx::query_as::<_, Job>(
        r#"
        UPDATE jobs
        SET status = 'running', attempts = attempts + 1, updated_at = now()
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = 'pending' AND run_at <= now()
            ORDER BY run_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, job_type, payload, status, attempts, max_attempts, error, run_at, created_at, updated_at
        "#,
    )
    .fetch_optional(pool)
    .await
}

async fn mark_succeeded(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET status = 'succeeded', updated_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn handle_failure(pool: &PgPool, job: &Job, error: &str) -> Result<bool, sqlx::Error> {
    if job.attempts >= job.max_attempts {
        mark_failed(pool, job.id, error).await?;
        Ok(false)
    } else {
        let backoff_secs = 2i64.pow(job.attempts as u32).min(300);
        retry_later(pool, job.id, error, backoff_secs).await?;
        Ok(true)
    }
}

async fn mark_failed(pool: &PgPool, id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET status = 'failed', error = $1, updated_at = now() WHERE id = $2")
        .bind(error)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn retry_later(
    pool: &PgPool,
    id: Uuid,
    error: &str,
    backoff_secs: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE jobs
        SET status = 'pending', error = $1, run_at = now() + ($2 || ' seconds')::interval, updated_at = now()
        WHERE id = $3
        "#,
    )
    .bind(error)
    .bind(backoff_secs.to_string())
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}
