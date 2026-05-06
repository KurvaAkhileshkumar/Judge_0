"""
job_queue.py
────────────
Redis-backed priority job queue for autograder submissions.

Two queues:
  judge0:jobs:retry  — failed submissions waiting for a retry
  judge0:jobs:normal — new submissions

Priority mechanism
──────────────────
Workers call dequeue() which uses BLPOP with the retry queue listed
first.  BLPOP checks keys left-to-right and pops from the first
non-empty one — so any retried job is always served before any new job,
regardless of how many new submissions are waiting.

Within the retry queue, RPUSH + BLPOP gives FIFO order: the job that
failed earliest is retried first (fair to students who have waited longest).

Queue layout (HEAD = next to be popped):

  judge0:jobs:retry   HEAD ← [job-A-retry1] [job-B-retry1] [job-C-retry1] ← TAIL
  judge0:jobs:normal  HEAD ← [job-D-new]    [job-E-new]    [job-F-new]    ← TAIL

  Worker calls: BLPOP judge0:jobs:retry judge0:jobs:normal 30
  → pops job-A-retry1 (retry queue non-empty → served first)
  → when retry queue empties, pops job-D-new from normal queue
"""

import json
import time
import uuid
from dataclasses import dataclass, asdict, field
from typing import Optional, Any


RETRY_QUEUE   = "judge0:jobs:retry"
NORMAL_QUEUE  = "judge0:jobs:normal"
RESULT_PREFIX = "judge0:result:"
RESULT_TTL_S  = 3600   # results expire after 1 hour


@dataclass
class QueuedJob:
    ticket_id:    str
    student_id:   str
    submitted_at: float
    payload:      dict          # serialised Submission fields
    retry_count:  int = 0


class PriorityJobQueue:
    """
    Wraps a Redis client with enqueue / requeue / dequeue / result storage.

    Usage:
        r     = redis.Redis(host=..., password=..., decode_responses=True)
        queue = PriorityJobQueue(r)

        # Accepting a new submission:
        job = QueuedJob(
            ticket_id    = uuid.uuid4().hex,
            student_id   = "student_001",
            submitted_at = time.time(),
            payload      = { ...Submission fields as dict... },
        )
        queue.enqueue(job)

        # In your worker loop:
        job = queue.dequeue(timeout=30)
        ...
        if result.needs_requeue:
            queue.requeue(job)   # goes to front of retry queue
        else:
            queue.store_result(job.ticket_id, result_dict)

        # In your API (student polling):
        data = queue.get_result(ticket_id)
    """

    def __init__(self, redis_client):
        self.r = redis_client

    # ── Submission side ───────────────────────────────────────────────────

    def enqueue(self, job: QueuedJob) -> None:
        """New submission — added to the BACK of the normal queue (FIFO)."""
        self.r.rpush(NORMAL_QUEUE, json.dumps(asdict(job)))

    def requeue(self, job: QueuedJob) -> None:
        """
        Failed submission — added to the BACK of the retry queue.

        Retry queue is always drained before normal queue (BLPOP priority),
        so the job will be picked up before any new submission regardless
        of its position within the retry queue.  FIFO within retry is
        fairer: the student who has been waiting longest is served first.
        """
        job.retry_count += 1
        self.r.rpush(RETRY_QUEUE, json.dumps(asdict(job)))

    # ── Worker side ───────────────────────────────────────────────────────

    def dequeue(self, timeout: int = 30) -> Optional[QueuedJob]:
        """
        Block until a job is available, checking retry queue first.

        BLPOP pops from the first key that has data:
          1. judge0:jobs:retry   (retried submissions — served first)
          2. judge0:jobs:normal  (new submissions — served only when retry empty)

        Returns None on timeout (worker should loop and call again).
        """
        result = self.r.blpop([RETRY_QUEUE, NORMAL_QUEUE], timeout=timeout)
        if result is None:
            return None
        _, raw = result
        data = json.loads(raw)
        return QueuedJob(**data)

    # ── Result side ───────────────────────────────────────────────────────

    def store_result(self, ticket_id: str, result: dict) -> None:
        """Store grading result. Expires after RESULT_TTL_S seconds."""
        self.r.setex(
            f"{RESULT_PREFIX}{ticket_id}",
            RESULT_TTL_S,
            json.dumps(result),
        )

    def get_result(self, ticket_id: str) -> Optional[dict]:
        """
        Returns the stored result dict, or None if not yet available.
        API should return HTTP 202 (pending) when this returns None.
        """
        raw = self.r.get(f"{RESULT_PREFIX}{ticket_id}")
        return json.loads(raw) if raw else None

    # ── Observability ─────────────────────────────────────────────────────

    def depths(self) -> dict:
        """Current queue depths — useful for health checks and pre-flight gates."""
        return {
            "retry":  self.r.llen(RETRY_QUEUE),
            "normal": self.r.llen(NORMAL_QUEUE),
        }
