from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any


AGING_SECONDS = 60


@dataclass(frozen=True)
class Job:
    job_id: str
    tenant_id: str
    gpu_type: str
    gpus: int
    priority: int
    submitted_at: int
    max_runtime: Optional[int] = None  # unused in core tests (optional stretch)


class Scheduler:
    """Quota-aware scheduler with aging-based fairness.

    Notes:
      - Treat `capacity` and `tenant_quotas` as the configured totals.
      - Track current usage internally.
      - Do NOT mutate Job objects (they are frozen).
    """

    def __init__(self, capacity: Dict[str, int], tenant_quotas: Dict[str, Dict[str, int]]):
        # TODO: store copies if you want immutability guarantees
        self._capacity = dict(capacity)
        self._tenant_quotas = {tenant: dict(quota) for tenant, quota in tenant_quotas.items()}

        # internal state
        # job_id -> Job
        self._jobs: Dict[str, Job] = {}

        # job_id -> status string
        self._status: Dict[str, str] = {}

        # job_id -> started_at (only for RUNNING)
        self._started_at: Dict[str, int] = {}

        # current usage
        self._used_capacity: Dict[str, int] = {gpu_type: 0 for gpu_type in self._capacity}
        # tenant -> gpu_type -> used
        self._used_quota: Dict[str, Dict[str, int]] = {
            t: {gt: 0 for gt in qs} for t, qs in self._tenant_quotas.items()
        }

    def submit(self, job: Job) -> None:
        """Submit a job into the WAITING queue."""
        # ensure that job is not a duplicate
        if job.job_id in self._jobs:
            raise ValueError(f"Duplicate job was submitted {job.job_id}")
        
        # add job to waiting queue + update states
        self._jobs[job.job_id] = job
        self._status[job.job_id] = "WAITING"

    def schedule(self, now: int) -> List[str]:
        """Start as many jobs as possible and return IDs started in this call."""
        started_jobs = []

        # go through every gpu and find how many free gpus there are
        for gpu_type in self._capacity:

            # list of job_ids that could possibly start
            candidate_jobs = []

            # look at jobs
            for job_id in self._jobs:
                # look at only waiting jobs
                if self.status(job_id) == "WAITING":
                    job = self._jobs[job_id]
                    # add job if it needs gpu_type
                    if job.gpu_type == gpu_type:
                        candidate_jobs.append(job_id)

            # sort candidate_jobs by: 1. largest effective priority TIE BREAKS: 2. earlier submit 3. smaller job_id
            def sort_key(job_id: str) -> tuple[int, int, str]:
                job = self._jobs[job_id]
                # find how long job has waited
                priority = self._effective_priority(job, now)
                
                return (-priority, job.submitted_at, job_id)
            candidate_jobs.sort(key=sort_key)

            # see if job fits within quota and allowance
            for job_id in candidate_jobs:
                job = self._jobs[job_id]
                if self._fits(job):
                    # start job + state updates
                    self._start(job, now)
                    started_jobs.append(job.job_id)

        return started_jobs

    def complete(self, job_id: str) -> None:
        """Mark a RUNNING job as COMPLETED and free its resources."""
        # KeyError on unknown job_id
        status = self._status[job_id]
        # ensure only called on a running job
        if status != "RUNNING":
            raise ValueError(f"complete() called on non-RUNNING job: {job_id}")
        
        self._finish(self._jobs[job_id])



    def status(self, job_id: str) -> str:
        """Return WAITING/RUNNING/COMPLETED for known jobs; else raise KeyError."""
        # job_id not recognized
        if job_id not in self._jobs:
            raise KeyError(job_id)

        # return status of job_id
        return self._status[job_id]

    def running(self) -> List[str]:
        """Return running job IDs sorted by (started_at, job_id)."""
        running_pair = [(start_time, job_id) for job_id, start_time in self._started_at.items()]
        # sort by start_time & then job_id
        running_pair.sort()
        return [job_id for start_time, job_id in running_pair]

    def waiting(self) -> int:
        """Return number of WAITING jobs."""
        waiting = 0
        for job_id in self._jobs:
            if self._status.get(job_id) == "WAITING":
                waiting += 1
        return waiting
    
    def stats(self) -> Dict[str, Any]:
        """
        Return a snapshot of scheduler state suitable for an API response.
        """
        # counts by status
        job_counts: Dict[str, int] = {"WAITING": 0, "RUNNING": 0, "COMPLETED": 0}
        for st in self._status.values():
            job_counts[st] = job_counts.get(st, 0) + 1
        
        # capacity
        cap_total = dict(self._capacity)
        cap_used = {gt: int(self._used_capacity.get(gt, 0)) for gt in cap_total}
        cap_available = {gt: cap_total[gt] - cap_used[gt] for gt in cap_total}

        # quotas per tenant
        quotas_total = {t: dict(q) for t, q in self._tenant_quotas.items()}
        quotas_used = {t: {gt: int(self._used_quota[t].get(gt, 0)) for gt in quotas_total[t]} for t in quotas_total}
        quotas_available = {
            t: {gt: quotas_total[t][gt] - quotas_used[t].get(gt, 0) for gt in quotas_total[t]}
            for t in quotas_total
        }

        # return as json
        return {
        "jobs": job_counts,
        "capacity": {"total": cap_total, "used": cap_used, "available": cap_available},
        "tenant_quotas": {
            t: {"total": quotas_total[t], "used": quotas_used[t], "available": quotas_available[t]}
            for t in quotas_total
        },
        "running": self.running(),
        "waiting": self.waiting(),
        "total_jobs": len(self._jobs),
        }

    # ---------- helpers you may find useful ----------

    def _effective_priority(self, job: Job, now: int) -> int:
        waited = max(0, now - job.submitted_at)
        return job.priority + (waited // AGING_SECONDS)

    def _fits(self, job: Job) -> bool:
        # Global capacity check
        if job.gpu_type not in self._capacity:
            return False
        if job.gpus <= 0:
            return False
        remaining = self._capacity[job.gpu_type] - self._used_capacity.get(job.gpu_type, 0)
        if remaining < job.gpus:
            return False

        # Tenant quota check
        tenant = job.tenant_id
        quotas = self._tenant_quotas.get(tenant)
        if quotas is None:
            return False
        if job.gpu_type not in quotas:
            return False
        remaining_q = quotas[job.gpu_type] - self._used_quota[tenant].get(job.gpu_type, 0)
        return remaining_q >= job.gpus

    def _start(self, job: Job, now: int) -> None:
        # Update usage + status
        self._used_capacity[job.gpu_type] += job.gpus
        self._used_quota[job.tenant_id][job.gpu_type] += job.gpus
        self._status[job.job_id] = "RUNNING"
        self._started_at[job.job_id] = now

    def _finish(self, job: Job) -> None:
        # Free usage + status
        self._used_capacity[job.gpu_type] -= job.gpus
        self._used_quota[job.tenant_id][job.gpu_type] -= job.gpus
        self._status[job.job_id] = "COMPLETED"
        self._started_at.pop(job.job_id, None)
