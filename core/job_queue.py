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
RESULT_TTL_S  = 86400   # Fix 1.4: was 3600 (1 h); raised to 86400 (24 h)
                          # Covers 21-min drain + 23-h student review window.

# Fix 2.1: In-flight tracking.  After BLPOP, the job is atomically pushed
# into PROCESSING_QUEUE and a visibility-timeout key is set.
# If the worker dies, the visibility key expires; the reconciler requeues.
PROCESSING_QUEUE = "judge0:jobs:processing"
INFLIGHT_PREFIX  = "judge0:inflight:"
INFLIGHT_TTL_S   = 300   # 5 min: max realistic job wall time

# Fix 2.6: Pending-deadline key.  Set at enqueue time.  If this key expires
# (via Redis TTL) and no result has been stored, the reconciler writes a
# terminal system_error so the student sees failure instead of a spinner.
PENDING_DEADLINE_PREFIX = "judge0:pending_deadline:"
# Phase 0 fix: raised from 1800 (30 min) → 7200 (2 hours).
# At the default scale (workers × MAX_RUNNERS=1 sandboxes, 15s/job):
#   6 sandboxes → drain 1000 jobs in ~2500s (42 min) > old 1800s limit.
# The old value caused the reconciler to write system_error for the tail
# ~43% of a 1000-submission burst before those jobs were even evaluated.
# 7200s covers drain time with 5× headroom at any realistic worker count.
MAX_JOB_WAIT_S          = 7200  # 2 hours

# SSE notification channel prefix (Fix 3.2)
NOTIFY_PREFIX = "judge0:notify:"


@dataclass
class QueuedJob:
    ticket_id:    str
    student_id:   str
    submitted_at: float
    payload:      dict          # serialised Submission fields
    retry_count:  int = 0
    idem_key:     str = ""      # full Redis key (judge0:idem:…); deleted on system_error
                                # so the student can resubmit without changing their code


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
        """New submission — added to the BACK of the normal queue (FIFO).

        Fix 2.6: also sets a pending-deadline key with TTL=MAX_JOB_WAIT_S.
        If no result is stored before the key expires, the reconciler writes
        a terminal system_error so students never see an infinite spinner.
        """
        raw = json.dumps(asdict(job))
        pipe = self.r.pipeline()
        pipe.rpush(NORMAL_QUEUE, raw)
        pipe.setex(
            f"{PENDING_DEADLINE_PREFIX}{job.ticket_id}",
            MAX_JOB_WAIT_S,
            "1",
        )
        pipe.execute()

    def requeue(self, job: QueuedJob) -> None:
        """
        Failed submission — added to the BACK of the retry queue.

        Retry queue is always drained before normal queue (BLPOP priority),
        so the job will be picked up before any new submission regardless
        of its position within the retry queue.  FIFO within retry is
        fairer: the student who has been waiting longest is served first.

        Fix 2.6: reset the pending-deadline TTL for the retried job so it
        gets another full MAX_JOB_WAIT_S window before being timed out.
        """
        job.retry_count += 1
        raw = json.dumps(asdict(job))
        pipe = self.r.pipeline()
        pipe.rpush(RETRY_QUEUE, raw)
        pipe.setex(
            f"{PENDING_DEADLINE_PREFIX}{job.ticket_id}",
            MAX_JOB_WAIT_S,
            "1",
        )
        pipe.execute()

    # ── Worker side ───────────────────────────────────────────────────────

    def dequeue(self, timeout: int = 30) -> Optional[QueuedJob]:
        """
        Block until a job is available, checking retry queue first.

        Fix 2.1: after popping (BLPOP is still used to preserve FIFO + retry
        priority semantics), the raw job is immediately pushed into
        PROCESSING_QUEUE and a visibility-timeout key is set in a single
        pipeline round-trip.  If the worker crashes before ack(), the
        reconciler detects the expired visibility key and requeues the job.

        Returns None on timeout (worker should loop and call again).
        """
        result = self.r.blpop([RETRY_QUEUE, NORMAL_QUEUE], timeout=timeout)
        if result is None:
            return None
        _, raw = result
        data = json.loads(raw)
        job  = QueuedJob(**data)

        # Register the in-flight job atomically
        pipe = self.r.pipeline()
        pipe.rpush(PROCESSING_QUEUE, raw)
        pipe.setex(f"{INFLIGHT_PREFIX}{job.ticket_id}", INFLIGHT_TTL_S, raw)
        pipe.execute()

        return job

    def ack(self, job: QueuedJob) -> None:
        """
        Fix 2.1: Acknowledge a successfully processed job.
        Removes the job from PROCESSING_QUEUE and deletes the visibility key.
        Must be called after store_result() (or after requeue() for retried jobs).

        Uses the original raw payload stored in the INFLIGHT key for the lrem
        lookup — this handles the case where requeue() incremented retry_count
        after the job was registered in PROCESSING_QUEUE.
        """
        inflight_key = f"{INFLIGHT_PREFIX}{job.ticket_id}"
        # Retrieve original raw (pre-increment) for the PROCESSING_QUEUE match
        original_raw = self.r.get(inflight_key)
        pipe = self.r.pipeline()
        if original_raw:
            pipe.lrem(PROCESSING_QUEUE, 1, original_raw)
        pipe.delete(inflight_key)
        pipe.execute()

    # ── Result side ───────────────────────────────────────────────────────

    def store_result(self, ticket_id: str, result: dict, idem_key: str = "") -> None:
        """Store grading result and publish for SSE delivery (Fix 3.2).

        The publish fires on the same Redis connection so SSE clients
        subscribed to judge0:notify:{ticket_id} receive the result
        immediately without polling.

        If idem_key is provided and the result is a system_error, the
        idempotency key is deleted so the student can resubmit the same
        code without being handed the old failure for 24 hours.
        """
        result_json = json.dumps(result)
        pipe = self.r.pipeline()
        pipe.setex(
            f"{RESULT_PREFIX}{ticket_id}",
            RESULT_TTL_S,
            result_json,
        )
        # Fix 2.6: remove the pending-deadline key — job is done.
        pipe.delete(f"{PENDING_DEADLINE_PREFIX}{ticket_id}")
        # Phase 1: on system_error, release the idempotency lock so the student
        # can resubmit without being trapped behind a stale failure for 24 h.
        if idem_key and "system_error" in result:
            pipe.delete(idem_key)
        # Fix 3.2: notify any SSE subscriber waiting on this ticket.
        pipe.publish(f"{NOTIFY_PREFIX}{ticket_id}", result_json)
        pipe.execute()

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

    def is_at_capacity(self, max_depth: int = 1000) -> bool:
        """
        Returns True when the combined queue depth (retry + normal) is at or
        above max_depth.

        Platform API endpoints should call this before enqueuing and return
        HTTP 429 when it is True — this is the library's backpressure hook.
        The threshold should equal the maximum in-flight jobs that workers can
        drain within an acceptable latency window.

        Example platform usage:
            if queue.is_at_capacity():
                return Response({"error": "Server busy, retry later"}, status=429)
            queue.enqueue(job)
        """
        d = self.depths()
        return (d["retry"] + d["normal"]) >= max_depth
