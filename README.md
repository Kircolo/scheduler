# DigitalOcean-Style 3-Hour Mock: Quota-Aware GPU Job Scheduler (Core + Optional Service Layer)

You are building the core of a small “job submission + scheduling” service used by a multi-tenant GPU platform.

## Constraints
- **Python 3.x**
- **Standard library only** (no FastAPI/Flask/pytest)
- **Do not mutate input Job objects**
- Your implementation should be clear enough to walk through in a code review.

## Core Concepts
- The system has **global GPU capacity per GPU type** (e.g., A100, H100).
- Each tenant has a **quota per GPU type**.
- Jobs request a GPU type and a number of GPUs (`gpus`).
- Jobs move through states: `WAITING -> RUNNING -> COMPLETED`.

## Required Behavior
### 1) Submitting jobs
- `submit(job)` adds a job to the WAITING queue.
- Duplicate `job_id` must raise `ValueError`.

### 2) Scheduling jobs
- `schedule(now)` starts as many jobs as possible (and returns the list of `job_id`s started in **this** call).
- A job can start only if:
  - There is enough **global remaining capacity** for its `gpu_type`, and
  - The tenant has enough **remaining quota** for its `gpu_type`.
- **Selection rule (highest first):**
  1. Higher **effective priority** wins.
  2. If tied, earlier `submitted_at` wins.
  3. If tied, lexicographically smaller `job_id` wins.

#### Effective priority (aging / fairness)
To avoid starvation, jobs gain priority the longer they wait:

```
effective_priority = priority + (now - submitted_at) // AGING_SECONDS
```

Use `AGING_SECONDS = 60`.

### 3) Completing jobs
- `complete(job_id)` marks a RUNNING job as COMPLETED and frees its resources.
- Completing a job that is not RUNNING must raise `ValueError`.
- Completing an unknown job must raise `KeyError`.

### 4) Status / visibility helpers
- `status(job_id)` returns one of: `"WAITING"`, `"RUNNING"`, `"COMPLETED"`.
  - Unknown job_id should raise `KeyError`.
- `running()` returns the list of running job_ids (sorted by `started_at`, then `job_id`).
- `waiting()` returns the number of waiting jobs.

## Optional “Service Layer” (Stretch)
If you finish early, implement a minimal JSON HTTP interface using `http.server`:

- `POST /jobs` -> submit job
- `POST /jobs/<id>/complete` -> complete
- `POST /schedule?now=123` -> schedule
- `GET /jobs/<id>` -> status

Keep the scheduling logic in `scheduler.py` and make the HTTP layer thin.

## How to run tests
From this folder:

```
python -m unittest -v
```

## Deliverable
Implement `scheduler.py` so all tests pass in `tests/test_scheduler.py`.
