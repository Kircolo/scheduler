from __future__ import annotations

import threading
import csv
import json
import io
import os
import time
import uuid
import logging
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

logger = logging.getLogger(__name__)

# default values

class ConfigError(ValueError):
    """Raised when startup configuration is invalid."""


def _default_capacity_and_quotas() -> tuple[Dict[str, int], Dict[str, Dict[str, int]]]:
    # Keep defaults small and obvious for interview/demo purposes.
    capacity = {"A100": 4, "H100": 2}
    tenant_quotas = {
        "t1": {"A100": 2, "H100": 1},
        "t2": {"A100": 2, "H100": 1},
    }
    return capacity, tenant_quotas


def _load_json_env(var_name: str) -> Optional[dict]:
    raw = os.getenv(var_name)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        raise ConfigError(f"{var_name} is set but empty")
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ConfigError(f"{var_name} contains invalid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise ConfigError(f"{var_name} must be a JSON object (dict)")
    return obj


def _validate_capacity(capacity: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for gpu_type, count in capacity.items():
        if not isinstance(gpu_type, str) or not gpu_type.strip():
            raise ConfigError("capacity keys must be non-empty strings")

        # bool is a subclass of int, so guard it explicitly
        if not isinstance(count, int) or isinstance(count, bool):
            raise ConfigError(f"capacity[{gpu_type!r}] must be an int")
        if count <= 0:
            raise ConfigError(f"capacity[{gpu_type!r}] must be > 0")

        out[gpu_type] = count

    if not out:
        raise ConfigError("capacity must not be empty")

    return out


def _validate_tenant_quotas(tenant_quotas: dict, capacity_keys: set[str]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}

    for tenant_id, quotas in tenant_quotas.items():
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ConfigError("tenant_quotas keys must be non-empty strings (tenant_id)")
        if not isinstance(quotas, dict):
            raise ConfigError(f"tenant_quotas[{tenant_id!r}] must be a JSON object (dict)")

        clean: Dict[str, int] = {}
        for gpu_type, limit in quotas.items():
            if not isinstance(gpu_type, str) or not gpu_type.strip():
                raise ConfigError(f"tenant_quotas[{tenant_id!r}] has a non-string gpu_type key")
            if gpu_type not in capacity_keys:
                raise ConfigError(
                    f"tenant_quotas[{tenant_id!r}] references unknown gpu_type {gpu_type!r} (not in capacity)"
                )
            if not isinstance(limit, int) or isinstance(limit, bool):
                raise ConfigError(f"tenant_quotas[{tenant_id!r}][{gpu_type!r}] must be an int")
            if limit < 0:
                raise ConfigError(f"tenant_quotas[{tenant_id!r}][{gpu_type!r}] must be >= 0")

            clean[gpu_type] = limit

        out[tenant_id] = clean

    if not out:
        raise ConfigError("tenant_quotas must not be empty")

    return out


def _build_scheduler_from_env_or_default() -> Scheduler:
    """
    Build Scheduler config from environment (fail-fast on invalid config).

    Env vars (optional):
      - SCHEDULER_CAPACITY_JSON: JSON object like {"A100": 4, "H100": 2}
      - SCHEDULER_TENANT_QUOTAS_JSON: JSON object like {"t1": {"A100": 2}}
    """
    capacity, tenant_quotas = _default_capacity_and_quotas()

    cap_obj = _load_json_env("SCHEDULER_CAPACITY_JSON")
    if cap_obj is not None:
        capacity = _validate_capacity(cap_obj)
    else:
        capacity = _validate_capacity(capacity)

    quotas_obj = _load_json_env("SCHEDULER_TENANT_QUOTAS_JSON")
    if quotas_obj is not None:
        tenant_quotas = _validate_tenant_quotas(quotas_obj, set(capacity))
    else:
        # Validate defaults against the final capacity (in case capacity was overridden).
        tenant_quotas = _validate_tenant_quotas(tenant_quotas, set(capacity))

    logger.info(
        "Scheduler config loaded (gpu_types=%s tenants=%d)",
        sorted(capacity.keys()),
        len(tenant_quotas),
    )
    return Scheduler(capacity=capacity, tenant_quotas=tenant_quotas)

# LOGGING

def _new_request_id() -> str:
    return uuid.uuid4().hex

def _set_log_fields(request: Request, **fields: Any) -> None:
    """
    Stash handler-specific fields (job_id, tenant_id, result, etc.)
    so the middleware can include them in the final request log.
    """
    lf = getattr(request.state, "log_fields", None)
    if lf is None:
        lf = {}
        request.state.log_fields = lf
    for k, v in fields.items():
        if v is not None:
            lf[k] = v


def _log_json(level: int, event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.log(level, json.dumps(payload, separators=(",", ":"), ensure_ascii=False))

# build a basic "default" Scheduler [hardcoded]
def _build_default_scheduler() -> Scheduler:
    capacity = {"A100": 4, "H100": 2}
    tenant_quotas = {
        "t1": {"A100": 2, "H100": 1},
        "t2": {"A100": 2, "H100": 1},
    }
    return Scheduler(capacity=capacity, tenant_quotas=tenant_quotas)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup responsibilities:
    - create a single Scheduler instance (in-memory)
    - create a Lock to protect it from concurrent requests

    Fail-fast philosophy:
    - If config is invalid or scheduler initialization fails, raise and crash on startup.
    """
    try:
        app.state.lock = threading.Lock()
        app.state.scheduler = _build_scheduler_from_env_or_default()
    except Exception:
        logger.exception("Startup failed")
        raise

    yield

app = FastAPI(
    title="GPU Job Scheduler",
    version="1.0",
    lifespan=lifespan,
)

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or _new_request_id()
    request.state.request_id = request_id
    request.state.log_fields = {}  # handler will populate this

    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception as e:
        latency_ms = (time.perf_counter() - start) * 1000.0
        _log_json(
            logging.ERROR,
            "request_failed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            latency_ms=round(latency_ms, 3),
            error_type=type(e).__name__,
            error=str(e),
            **getattr(request.state, "log_fields", {}),
        )
        raise

    latency_ms = (time.perf_counter() - start) * 1000.0

    # Always echo request id back to clients for correlation
    response.headers["X-Request-ID"] = request_id

    _log_json(
        logging.INFO,
        "request_done",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        latency_ms=round(latency_ms, 3),
        **getattr(request.state, "log_fields", {}),
    )
    return response

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
def submit_job(job: JobIn, request: Request) -> SubmitResp:
    """
    POST /jobs
    Body: JobIn JSON
    Effect: creates a WAITING job in the scheduler

    Error mapping:
    - duplicate job_id -> 409 Conflict (scheduler.submit raises ValueError)
    """
    _set_log_fields(
        request,
        job_id=job.job_id,
        tenant_id=job.tenant_id,
        gpu_type=job.gpu_type,
        result="attempt_submit",
    )

    s = _get_scheduler()
    lock = _get_lock()
    
    # convert verified Pydantic model with core dataclass
    job_obj = Job(**job.model_dump())

    with lock:
        try:
            s.submit(job_obj)
        except ValueError as e:
            # duplicate job
            _set_log_fields(request, result="DUPLICATE_JOB")
            api_error(409, "DUPLICATE_JOB", str(e))
    
        # worked
        _set_log_fields(request, result="OK")
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
def schedule(request: Request, now: int = Query(..., description="Integer timestamp used for aging-based fairness (effective priority).")) -> ScheduleResp:
    """
    POST /schedule?now=123
    Effect: starts as many jobs as possible and returns which started
    """
    s = _get_scheduler()
    lock = _get_lock()

    with lock:
        started = s.schedule(now)
        running = s.running()
        waiting = s.waiting()

    _set_log_fields(
        request,
        result="OK",
        started_count=len(started),
        running_count=len(running),
        waiting=waiting,
    )
    return ScheduleResp(started=started, running=running, waiting=waiting)
    
# mark job_id complete
@app.post("/jobs/{job_id}/complete")
def complete_job(job_id: str, request: Request) -> Dict[str, str]:
    """
    POST /jobs/<job_id>/complete
    Effect: marks a RUNNING job as COMPLETED and frees resources

    Error mapping:
    - unknown job_id -> 404 Not Found (scheduler.complete raises KeyError)
    - job not RUNNING -> 400 Bad Request (scheduler.complete raises ValueError)
    """
    _set_log_fields(request, job_id=job_id, result="attempt_complete")

    s = _get_scheduler()
    lock = _get_lock()

    with lock:
        try:
            s.complete(job_id)
        except KeyError:
            _set_log_fields(request, result="JOB_NOT_FOUND")
            api_error(404, "JOB_NOT_FOUND", f"Unknown job_id: {job_id}")
        except ValueError as e:
            _set_log_fields(request, result="INVALID_STATE")
            api_error(400, "INVALID_STATE", str(e))
        
        # worked & return job_id shown as COMPLETED
        _set_log_fields(request, result="OK")
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