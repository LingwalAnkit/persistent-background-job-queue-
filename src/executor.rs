use std::time::Duration;

use crate::modles::Job;

pub async fn execute_job(job: &Job) -> Result<(), String> {
    match job.job_type.as_str() {
        "send_email" => send_email(job).await,
        other => Err(format!("Other job type: {}", other)),
    }
}

pub async fn send_email(job: &Job) -> Result<(), String> {
    let recipient = job
        .payload
        .get("recipient")
        .and_then(|v| v.as_str())
        // find recipient from payload and then convert to str
        .ok_or("recipient not found")?;

    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Pretending to send mail to {}", recipient);
    Ok(())
}
