from __future__ import annotations

import threading
import csv
import json
import io
from _thread import LockType
from typing import Dict, Optional, List, Any
from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File, Body
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field, ValidationError
from contextlib import asynccontextmanager
from scheduler import Scheduler, Job

# -------------------------
# Startup
# -------------------------

# build a basic "default" Scheduler [hardcoded]
def _build_default_scheduler() -> Scheduler:
    capacity = {"A100": 4, "H100": 2}
    tenant_quotas = {
        "t1": {"A100": 2, "H100": 1},
        "t2": {"A100": 2, "H100": 1},
    }
    return Scheduler(capacity=capacity, tenant_quotas=tenant_quotas)

# FastAPI calls this once on startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI calls this once at startup, then again at shutdown.

    We use it to:
    - create a single Scheduler instance (in-memory)
    - create a Lock to protect it from concurrent requests
    """
    app.state.lock = threading.Lock()
    app.state.scheduler = _build_default_scheduler()
    yield

app = FastAPI(
    title="GPU Job Scheduler",
    version="1.0",
    lifespan=lifespan,
)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """
    Standardize all HTTPException responses to:
      {"error": {"code": "...", "message": "...", ...}}
    """
    detail = exc.detail
    if isinstance(detail, dict) and "code" in detail and "message" in detail:
        payload = {"error": detail}
    else:
        payload = {"error": {"code": "HTTP_ERROR", "message": str(detail)}}
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Standardize FastAPI/Pydantic validation errors (422).
    """
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid request",
                "details": exc.errors(),  # list of field-level issues
            }
        },
    )

# helper for raising consistent errors
def api_error(status_code: int, code: str, message: str, **extra) -> None:
    detail = {"code": code, "message": message, **extra}
    raise HTTPException(status_code=status_code, detail=detail)

# ensure endpoint has a scheduler
def _get_scheduler() -> Scheduler:
    scheduler = getattr(app.state, "scheduler", None)
    if scheduler is None:
        api_error(503, "SERVICE_NOT_READY", "Scheduler was not initialized")
    return scheduler

def _get_lock() -> LockType:
    """
    Same idea as _get_scheduler(), but for the lock.

    Why lock?
    The scheduler holds mutable state (dicts). Concurrent requests could interleave
    and corrupt invariants unless we serialize access.
    """
    lock = getattr(app.state, "lock", None)
    if lock is None:
        api_error(503, "SERVICE_NOT_READY", "Lock was not initialized")
    return lock

# -------------------------
# # Pydantic Schemas
# -------------------------

class JobIn(BaseModel):
    """
    This defines the JSON body expected by POST /jobs.

    FastAPI will:
    - parse incoming JSON into this model
    - validate types + constraints
    - if invalid, automatically return a 422 error
    """
    job_id: str = Field(min_length=1)       # ensures names/ids are at min len = 1
    tenant_id: str = Field(min_length=1)
    gpu_type: str = Field(min_length=1)
    gpus: int = Field(gt=0)                 # gpus > 0
    priority: int
    submitted_at: int
    max_runtime: Optional[int] = None


class SubmitResp(BaseModel):
    """
    Response body returned by POST /jobs.
    """
    job_id: str
    status: str


class ScheduleResp(BaseModel):
    """
    Response body returned by POST /schedule.
    """
    started: List[str]
    running: List[str]
    waiting: int

# -------------------------
# Routes (REST endpoints)
# -------------------------

@app.get("/healthz")
def healthz() -> Dict[str, bool]:
    """
    Basic health check endpoint. Useful for deployments / load balancers.
    """
    return {"ok": True}

# API version of Scheduler.submit(job). Code 201 Created
@app.post("/jobs", response_model=SubmitResp, status_code=201)
def submit_job(job: JobIn) -> SubmitResp:
    """
    POST /jobs
    Body: JobIn JSON
    Effect: creates a WAITING job in the scheduler

    Error mapping:
    - duplicate job_id -> 409 Conflict (scheduler.submit raises ValueError)
    """
    s = _get_scheduler()
    lock = _get_lock()
    
    # convert verified Pydantic model with core dataclass
    job_obj = Job(**job.model_dump())

    with lock:
        try:
            s.submit(job_obj)
        except ValueError as e:
            # duplicate job
            api_error(409, "DUPLICATE_JOB", str(e))
    
        # worked
        return SubmitResp(job_id=job_obj.job_id, status=s.status(job_obj.job_id))
    
@app.post("/jobs/batch")
def submit_jobs_batch(jobs: List[dict] = Body(...)) -> Dict[str, Any]:
    """
    Batch submit jobs. Partial success:
    - invalid rows are rejected with VALIDATION_ERROR
    - duplicate job_id are rejected with DUPLICATE_JOB
    """
    s = _get_scheduler()
    lock = _get_lock()

    accepted: List[str] = []
    rejected: List[Dict[str, Any]] = []

    with lock:
        for idx, raw in enumerate(jobs, start=0):
            # 1) Validate structure per-item (so one bad row doesn't kill the batch)
            try:
                job_in = JobIn.model_validate(raw)
            except ValidationError as e:
                rejected.append({
                    "index": idx,
                    "job_id": raw.get("job_id"),
                    "code": "VALIDATION_ERROR",
                    "message": "Invalid job payload",
                    "details": e.errors(),
                })
                continue

            # 2) Convert -> core dataclass and submit
            job_obj = Job(**job_in.model_dump())
            try:
                s.submit(job_obj)
            except ValueError as e:
                rejected.append({
                    "index": idx,
                    "job_id": job_obj.job_id,
                    "code": "DUPLICATE_JOB",
                    "message": str(e),
                })
            else:
                accepted.append(job_obj.job_id)

    return {
        "accepted": accepted,
        "rejected": rejected,
        "counts": {"accepted": len(accepted), "rejected": len(rejected)},
}

@app.post("/ingest")
def ingest(
    file: UploadFile = File(...),
    fmt: str = Query("ndjson", description="ndjson or csv"),
) -> Dict[str, Any]:
    """
    File ingestion endpoint.
    - ndjson: one JSON object per line
    - csv: header row with job fields
    Partial success, duplicates rejected.
    """
    s = _get_scheduler()
    lock = _get_lock()

    try:
        raw_bytes = file.file.read()
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        api_error(400, "BAD_ENCODING", "File must be UTF-8")

    accepted: List[str] = []
    rejected: List[Dict[str, Any]] = []

    def handle_item(raw_item: dict, source: Dict[str, Any]) -> None:
        nonlocal accepted, rejected

        try:
            job_in = JobIn.model_validate(raw_item)
        except ValidationError as e:
            rejected.append({
                **source,
                "job_id": raw_item.get("job_id"),
                "code": "VALIDATION_ERROR",
                "message": "Invalid job payload",
                "details": e.errors(),
            })
            return

        job_obj = Job(**job_in.model_dump())
        try:
            s.submit(job_obj)
        except ValueError as e:
            rejected.append({
                **source,
                "job_id": job_obj.job_id,
                "code": "DUPLICATE_JOB",
                "message": str(e),
            })
        else:
            accepted.append(job_obj.job_id)

    with lock:
        if fmt == "ndjson":
            for line_no, line in enumerate(text.splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    rejected.append({
                        "line": line_no,
                        "job_id": None,
                        "code": "PARSE_ERROR",
                        "message": f"Invalid JSON on line {line_no}",
                        "details": str(e),
                    })
                    continue
                if not isinstance(obj, dict):
                    rejected.append({
                        "line": line_no,
                        "job_id": None,
                        "code": "PARSE_ERROR",
                        "message": f"Line {line_no} is not a JSON object",
                    })
                    continue
                handle_item(obj, {"line": line_no})

        elif fmt == "csv":
            reader = csv.DictReader(io.StringIO(text))
            # row index here counts data rows; header is handled by DictReader
            for row_no, row in enumerate(reader, start=1):
                # DictReader gives strings; Pydantic will coerce ints where possible.
                handle_item(row, {"row": row_no})

        else:
            api_error(400, "BAD_FORMAT", "fmt must be 'ndjson' or 'csv'")

    return {
        "filename": file.filename,
        "format": fmt,
        "accepted": accepted,
        "rejected": rejected,
        "counts": {"accepted": len(accepted), "rejected": len(rejected)},
}
    
# run schedule() & return ids of jobs started
@app.post("/schedule", response_model=ScheduleResp)
def schedule(now: int = Query(..., description="Integer timestamp used for aging-based fairness (effective priority).")) -> ScheduleResp:
    """
    POST /schedule?now=123
    Effect: starts as many jobs as possible and returns which started
    """
    s = _get_scheduler()
    lock = _get_lock()

    with lock:
        started = s.schedule(now)
        return ScheduleResp(started=started, running=s.running(), waiting=s.waiting())
    
# mark job_id complete
@app.post("/jobs/{job_id}/complete")
def complete_job(job_id: str) -> Dict[str, str]:
    """
    POST /jobs/<job_id>/complete
    Effect: marks a RUNNING job as COMPLETED and frees resources

    Error mapping:
    - unknown job_id -> 404 Not Found (scheduler.complete raises KeyError)
    - job not RUNNING -> 400 Bad Request (scheduler.complete raises ValueError)
    """

    s = _get_scheduler()
    lock = _get_lock()

    with lock:
        try:
            s.complete(job_id)
        except KeyError:
            api_error(404, "JOB_NOT_FOUND", f"Unknown job_id: {job_id}")
        except ValueError as e:
            api_error(400, "INVALID_STATE", str(e))
        
        # worked & return job_id shown as COMPLETED
        return {"job_id": job_id, "status": s.status(job_id)}
    
# return status of job_id
@app.get("/jobs/{job_id}")
def get_job_status(job_id: str) -> Dict[str, str]:
    """
    GET /jobs/<job_id>
    Returns the current status: 
    
    WAITING / RUNNING / COMPLETED

    Error mapping:
    - unknown job_id -> 404 Not Found (scheduler.complete raises KeyError)
    """
    s = _get_scheduler()
    lock = _get_lock()

    with lock:
        try:
            status = s.status(job_id)
        except KeyError:
            api_error(404, "JOB_NOT_FOUND", f"Unknown job_id: {job_id}")
        
        # worked
        return {"job_id": job_id, "status": status}
    
@app.get("/stats")
def stats() -> Dict[str, Any]:
    s = _get_scheduler()
    lock = _get_lock()
    with lock:
        return s.stats()