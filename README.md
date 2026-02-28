# Quota-Aware GPU Job Scheduler (Core + FastAPI Service)

A small, in-memory **job submission + scheduling** service for a **multi-tenant GPU platform**.

- **Core scheduler**: quota-aware scheduling with **aging-based fairness**
- **Service layer**: a minimal **FastAPI** REST API for submitting, scheduling, completing, ingesting, and inspecting jobs

---

## Project layout

- `scheduler.py` — core scheduling logic
- `server.py` — FastAPI app exposing scheduler operations
- `tests/` — unit + API tests (unittest)
- `Makefile` — repeatable commands (run/test/smoke/ci)
- `.github/workflows/ci.yml` — GitHub Actions CI

---

## Core concepts

- Global **GPU capacity per type**
- Per-tenant **quota per GPU type**
- Jobs request:
  - `gpu_type` (string)
  - `gpus` (int > 0)
- States:
  - `WAITING -> RUNNING -> COMPLETED`

### Scheduling rule (priority + aging)
To avoid starvation, jobs gain priority the longer they wait:

```
effective_priority = priority + (now - submitted_at) // AGING_SECONDS
```

`AGING_SECONDS = 60`.

Selection order within a GPU type:
1. Higher **effective priority** first
2. Earlier `submitted_at` first
3. Lexicographically smaller `job_id` first

---

## API overview (FastAPI)

The service exposes these endpoints:

- `GET /healthz` → health check
- `POST /jobs` → submit a single job (201 on success)
- `POST /jobs/batch` → submit a list of jobs (partial success)
- `POST /ingest?fmt=ndjson|csv` → ingest jobs from uploaded file (partial success)
- `POST /schedule?now=<int>` → run one scheduling “tick”
- `POST /jobs/{job_id}/complete` → complete a RUNNING job
- `GET /jobs/{job_id}` → get job status
- `GET /stats` → scheduler state snapshot

### Default capacity + quotas
On startup, `server.py` initializes an in-memory scheduler with:

- Capacity: `A100=4`, `H100=2`
- Tenant quotas:
  - `t1`: `A100=2`, `H100=1`
  - `t2`: `A100=2`, `H100=1`

---

## Error responses (consistent shape)

All error responses use:

```json
{
  "error": {
    "code": "SOME_CODE",
    "message": "Human-readable message",
    "details": []
  }
}
```

Common codes:
- `VALIDATION_ERROR` (422) — request payload/query validation failed
- `DUPLICATE_JOB` (409) — job_id already exists
- `JOB_NOT_FOUND` (404) — unknown job_id
- `INVALID_STATE` (400) — completing a job that isn’t RUNNING
- `BAD_FORMAT` (400) — ingest `fmt` not in `ndjson|csv`
- `BAD_ENCODING` (400) — ingest file not UTF-8
- `SERVICE_NOT_READY` (503) — scheduler/lock not initialized

---

## How to run locally

### 1) Create a virtual environment + install deps

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Or, if you’re using the Makefile:

```bash
make install
```

### 2) Run tests

```bash
make test
```

(Equivalent: `python -m unittest discover -v -s . -p "test_*.py"`)

### 3) Run the API server

```bash
make run
```

(Equivalent: `python -m uvicorn server:app --host 0.0.0.0 --port 8000`)

### 4) Smoke test (start server + hit HTTP endpoints)

```bash
make smoke
```

---

## API examples (curl)

### Health check
```bash
curl -s http://127.0.0.1:8000/healthz
```

### Submit a job
```bash
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_id":"j1",
    "tenant_id":"t1",
    "gpu_type":"A100",
    "gpus":1,
    "priority":5,
    "submitted_at":0
  }'
```

### Schedule (tick)
```bash
curl -s -X POST "http://127.0.0.1:8000/schedule?now=120"
```

### Complete a job
```bash
curl -s -X POST http://127.0.0.1:8000/jobs/j1/complete
```

### Stats
```bash
curl -s http://127.0.0.1:8000/stats
```

### Batch submit
```bash
curl -s -X POST http://127.0.0.1:8000/jobs/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"job_id":"b1","tenant_id":"t1","gpu_type":"A100","gpus":1,"priority":1,"submitted_at":0},
    {"job_id":"b2","tenant_id":"t2","gpu_type":"H100","gpus":1,"priority":9,"submitted_at":0}
  ]'
```

### Ingest jobs (NDJSON)
`fmt=ndjson` expects **one JSON object per line**.

```bash
curl -s -X POST "http://127.0.0.1:8000/ingest?fmt=ndjson" \
  -F "file=@jobs.ndjson"
```

### Ingest jobs (CSV)
`fmt=csv` expects a header row with fields like:
`job_id,tenant_id,gpu_type,gpus,priority,submitted_at[,max_runtime]`

```bash
curl -s -X POST "http://127.0.0.1:8000/ingest?fmt=csv" \
  -F "file=@jobs.csv"
```

---

## CI (GitHub Actions)

A minimal CI workflow lives at `.github/workflows/ci.yml` and runs the same commands you run locally:
- install deps from `requirements.txt`
- run `make test`
- optionally run `make smoke` (real server startup + HTTP checks)

---

## Notes

- The scheduler is in-memory (no persistence).
- The API layer uses a single lock to serialize access to scheduler state for thread safety.
