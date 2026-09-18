use crate::executor::execute_job;
use sqlx::{PgPool, pool};
use std::time::Duration;
use uuid::Uuid;

use crate::modles::Job;

pub async fn start_worker(pool: PgPool, num_workers: usize) {
    for worker_id in 0..num_workers {
        let pool = pool.clone(); // You clone the PgPool handle so each async task can own a copy.
        tokio::spawn(async move {
            worker_loop(worker_id, pool).await;
        }); // This creates an independent asynchronous task.
    }
}

async fn worker_loop(worker_id: usize, pool: PgPool) {
    loop {
        match claim_next_job(&pool).await {
            Ok(Some(job)) => {
                println!(
                    "[worker {}] claimed job {} ({})",
                    worker_id,
                    job.id,
                    job.job_type // job → variable containing an instance of Job
                );
                match execute_job(&job).await {
                    Ok(()) => match mark_succeeded(&pool, job.id).await {
                        Ok(_) => println!("[worker {}] job {} succeeded", worker_id, job.job_type),
                        Err(e) => eprintln!(
                            "[worker {}] error marking job {} succeeded: {}",
                            worker_id, job.id, e
                        ),
                    },
                    Err(err_msg) => match mark_failed(&pool, job.id, &err_msg).await {
                        Ok(_) => {
                            println!("[worker {}] job {} failed: {}", worker_id, job.id, err_msg)
                        }
                        Err(e) => eprintln!(
                            "[worker {}] failed to save failure for {}: {}",
                            worker_id, job.id, e
                        ),
                    },
                }
            }

            Ok(None) => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(e) => {
                eprintln!("[worker {}] error claiming job {}", worker_id, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
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
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, job_type, payload, status, attempts, max_attempts, error, created_at, updated_at
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

async fn mark_failed(pool: &PgPool, id: Uuid, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE jobs SET status = 'failed', error = $1, updated_at = now() WHERE id = $2")
        .bind(error)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}
