# Persistent Background Job Queue

A compact background job processing service built with Rust, Axum, Tokio, SQLx, and PostgreSQL. It accepts jobs over HTTP, stores them durably, and lets concurrent workers claim and execute eligible jobs with retry limits and exponential backoff.

The project explores the core mechanics behind production job systems: durable state, safe concurrent claiming, asynchronous execution, failure tracking, delayed retries, and lifecycle management.

> [!IMPORTANT]
> The queue engine and API components are implemented, but the current `main` function has a startup-order issue: it waits for `Ctrl+C` before starting the workers and HTTP server. See [Current limitations](#current-limitations-and-known-issues) before running the latest revision.

## Why this project exists

Tasks such as sending email, processing media, or calling third-party services should not make an HTTP request wait for slow or unreliable work. An in-memory task queue is simple, but loses pending work when the process exits.

This project moves job state into PostgreSQL. Producers can persist work quickly, while independent asynchronous workers process it in the background. Database row locking prevents workers from claiming the same pending job concurrently.

## How it works

1. A client sends a job type, JSON payload, and optional retry limit to `POST /jobs`.
2. The API inserts the job into PostgreSQL with a `pending` status.
3. One of four Tokio workers looks for the next eligible job (`run_at <= now()`).
4. PostgreSQL's `FOR UPDATE SKIP LOCKED` lets a worker claim a row without colliding with other workers.
5. Claiming changes the status to `running` and increments `attempts` atomically.
6. The executor dispatches the job by its `job_type`.
7. Successful jobs become `succeeded`. Failed jobs return to `pending` with a future `run_at`, or become permanently `failed` after exhausting `max_attempts`.

Retry delays use exponential backoff: `2^attempts` seconds, capped at 300 seconds. Because `attempts` is incremented when a job is claimed, the first retry is scheduled after 2 seconds.

```mermaid
flowchart LR
    Client[HTTP client] -->|POST /jobs| API[Axum API]
    API -->|INSERT| DB[(PostgreSQL)]
    Workers[Tokio worker pool] -->|Claim with SKIP LOCKED| DB
    DB -->|Eligible job| Workers
    Workers --> Executor[Job executor]
    Executor -->|Success| Success[Mark succeeded]
    Executor -->|Retryable failure| Retry[Schedule with backoff]
    Executor -->|Attempts exhausted| Failed[Mark failed]
    Success --> DB
    Retry --> DB
    Failed --> DB
    Client -->|GET /job/:id| API
    API -->|Read status| DB
```

## Architecture and major components

| Component | Responsibility |
| --- | --- |
| HTTP API | Accepts new jobs, exposes health information, and returns a job by UUID. |
| PostgreSQL | Persists payloads, execution state, attempt counts, errors, and retry schedules. |
| Worker pool | Runs four asynchronous workers and polls once per second when no job is available. |
| Claiming query | Atomically selects and updates one eligible job using `FOR UPDATE SKIP LOCKED`. |
| Executor | Routes supported job types to their handlers. Currently supports `send_email`. |
| Retry handler | Reschedules failed work with capped exponential backoff until the retry limit is reached. |
| Cancellation token | Coordinates the intended HTTP-server and worker shutdown lifecycle. |

## Technology stack

- **Rust 2024 edition** for memory-safe systems programming
- **Axum 0.7** for HTTP routing and JSON extraction
- **Tokio** for the async runtime, worker tasks, timers, and signal handling
- **Tokio Util** for cooperative cancellation
- **PostgreSQL** as the durable queue and state store
- **SQLx 0.7** for asynchronous PostgreSQL access and row mapping
- **Serde / serde_json** for request and payload serialization
- **UUID** for job identifiers
- **Chrono** for UTC timestamps
- **dotenvy** for local environment configuration

## Implemented features

- Durable PostgreSQL-backed job storage
- JSON job payloads
- Optional per-job `max_attempts` with a default of 3
- Four concurrent asynchronous workers
- Atomic job claiming with `FOR UPDATE SKIP LOCKED`
- Scheduled execution through the `run_at` timestamp
- Exponential retry backoff capped at five minutes
- Persistent status, attempt count, last error, and timestamps
- Job lookup by UUID
- Health endpoint
- Cooperative cancellation primitives and worker task joining
- Simulated `send_email` job handler with payload validation

## Local setup

### Prerequisites

- Rust toolchain with Rust 2024 edition support (Rust 1.85 or newer)
- PostgreSQL
- `sqlx-cli` for applying the included migrations

Install the PostgreSQL-only SQLx CLI if it is not already available:

```bash
cargo install sqlx-cli --no-default-features --features postgres
```

### 1. Create a database

Create an empty PostgreSQL database, for example:

```sql
CREATE DATABASE persistent_job_queue;
```

### 2. Configure the connection

Create a local `.env` file in the repository root:

```dotenv
DATABASE_URL=postgres://postgres:your_password@localhost:5432/persistent_job_queue
```

`.env` is ignored by Git. Do not commit real credentials.

### 3. Apply migrations

```bash
sqlx migrate run
```

The migrations create the `jobs` table and add the `run_at` column used for delayed retries.

### 4. Build and check the project

```bash
cargo check
cargo build
```

### 5. Run locally

The intended command is:

```bash
cargo run
```

The service is configured to bind to `127.0.0.1:3000`. In the current revision, the startup-order issue described below must first be corrected in `src/main.rs`; otherwise the process waits for `Ctrl+C` before constructing the workers and server.

## API

### Health check

```http
GET /health
```

Successful response:

```text
OK
```

### Create a job

```http
POST /jobs
Content-Type: application/json
```

The only currently supported executor type is the exact, case-sensitive string `send_email`:

```json
{
  "job_type": "send_email",
  "payload": {
    "recipient": "developer@example.com"
  },
  "max_attempts": 3
}
```

`max_attempts` is optional and defaults to `3`. The handler currently simulates delivery by waiting for 500 ms and writing the recipient to standard output; it does not send real email.

Example with cURL:

```bash
curl -X POST http://127.0.0.1:3000/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_type":"send_email","payload":{"recipient":"developer@example.com"},"max_attempts":3}'
```

Example response immediately after creation:

```json
{
  "id": "e99f4101-b500-4006-afe4-2c444e0ed568",
  "job_type": "send_email",
  "payload": {
    "recipient": "developer@example.com"
  },
  "created_at": "2026-09-19T10:30:00Z",
  "updated_at": "2026-09-19T10:30:00Z",
  "attempts": 0,
  "max_attempts": 3,
  "error": null,
  "status": "pending",
  "run_at": "2026-09-19T10:30:00Z"
}
```

Possible status values are `pending`, `running`, `succeeded`, and `failed` by application convention. The database does not currently enforce this set with a constraint.

### Get a job

```http
GET /job/:id
```

Example:

```bash
curl http://127.0.0.1:3000/job/e99f4101-b500-4006-afe4-2c444e0ed568
```

The response contains the complete persisted job record. A missing UUID returns `404 Not Found`; malformed UUIDs are rejected by Axum.

## Project structure

```text
.
├── migrations/
│   ├── 20260917134323_create_jobs_table.sql
│   └── 20260919105855_add_run_at_to_jobs.sql
├── src/
│   ├── handlers/
│   │   ├── health.rs       # Health endpoint
│   │   ├── job.rs          # Create and retrieve job handlers
│   │   └── mod.rs
│   ├── modles/
│   │   ├── job.rs          # API request and persisted job types
│   │   └── mod.rs
│   ├── config.rs           # DATABASE_URL loading
│   ├── db.rs               # PostgreSQL connection pool
│   ├── executor.rs         # Job dispatch and handlers
│   ├── main.rs             # Process startup and shutdown orchestration
│   ├── routes.rs           # Axum route registration
│   └── worker.rs           # Claiming, execution, retry, and state transitions
├── Cargo.toml
└── Cargo.lock
```

`modles` is the directory name currently used by the Rust module tree; renaming it to `models` is a planned cleanup.

## Current limitations and known issues

- **Startup sequencing:** `main` awaits `tokio::signal::ctrl_c()` before starting workers or binding the HTTP listener. The signal listener must run concurrently with the application rather than block startup.
- **Email execution is simulated:** `send_email` validates `payload.recipient`, sleeps briefly, and logs a message; no SMTP or email provider is integrated.
- **No recovery for interrupted jobs:** a process crash can leave jobs in `running`, and there is no lease timeout or reaper to return them to `pending`.
- **Limited job types:** unsupported strings are accepted by the API, then retried and eventually failed by the executor.
- **Minimal validation:** `max_attempts` is not constrained to a positive range, and payload schemas are validated only during execution.
- **Basic API errors:** database failures become an opaque `500 Internal Server Error`; there is no structured error body or application-level logging context.
- **Polling workers:** idle workers query PostgreSQL every second instead of using notifications or adaptive polling.
- **Hard-coded runtime settings:** bind address, worker count, polling interval, and connection-pool size are not configurable.
- **No authentication, authorization, rate limiting, or per-tenant isolation.**
- **No automated test suite or CI pipeline is currently included.**

## Project status / roadmap

### Done

- [x] PostgreSQL job schema and migrations
- [x] HTTP endpoints to create and retrieve jobs
- [x] Persistent JSON payloads and job metadata
- [x] Concurrent Tokio worker pool
- [x] Contention-safe job claiming with `SKIP LOCKED`
- [x] Status transitions for successful and failed execution
- [x] Configurable maximum attempts per job
- [x] Delayed retries with capped exponential backoff
- [x] Scheduled eligibility through `run_at`
- [x] Last-error persistence
- [x] Cancellation-aware worker loops and join handles
- [x] One simulated `send_email` executor

### In progress

- [ ] Correct application startup and graceful-shutdown sequencing
- [ ] Add unit and integration coverage for API, claiming, and retry behavior
- [ ] Improve request validation and structured API error responses
- [ ] Recover jobs left in `running` after worker or process failure

### Planned

- [ ] Replace the simulated email handler with a real provider integration
- [ ] Introduce typed job definitions and payload schemas
- [ ] Add more job handlers and a cleaner executor registration model
- [ ] Make worker count, polling, bind address, and backoff configurable
- [ ] Add job listing, filtering, cancellation, and manual retry endpoints
- [ ] Add observability with structured logs, metrics, and tracing
- [ ] Add database constraints and indexes for queue invariants and claim performance
- [ ] Add containerized local development and automated CI checks
- [ ] Rename `modles` to `models`

## Contributing

Contributions are welcome, especially around the in-progress roadmap items. Before opening a change, run:

```bash
cargo fmt --check
cargo check
```

Keep migrations forward-only and ensure concurrent job-claim behavior remains safe.

## License

No license has been added yet. Until one is selected, the repository remains all rights reserved by default.
