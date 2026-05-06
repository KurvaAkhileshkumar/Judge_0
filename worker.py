"""
worker.py
─────────
Application-level worker.  Pulls grading jobs from the Redis priority
queue, calls Autograder.grade(), and either stores the result or requeues.

Priority behaviour
──────────────────
  judge0:jobs:retry  — checked first (BLPOP key order)
  judge0:jobs:normal — checked only when retry queue is empty

A retried job is always served before any new submission.

Retry policy
────────────
  MAX_RETRY_COUNT = 3  (configurable via env MAX_RETRY_COUNT)

  attempt 1 (retry_count=1): requeued immediately after first infra failure
  attempt 2 (retry_count=2): requeued again if still infra failure
  attempt 3 (retry_count=3): requeued again
  attempt 4 (retry_count=4): retry_count > MAX_RETRY_COUNT → system_error stored

  No sleep between retries here — the natural queue drain time (how long
  the job sits in the retry queue before being picked up again) acts as
  the back-off.  If workers are busy, the job waits in the queue; if a
  worker is free, it picks it up immediately.  This is better than
  sleeping inside the worker because:
    1. The worker thread is free to process other jobs while this one waits
    2. Back-off is automatic based on actual system load, not a fixed timer

Run:
    python worker.py

Environment variables:
    REDIS_HOST         (default: localhost)
    REDIS_PORT         (default: 6379)
    REDIS_PASSWORD     (default: none)
    JUDGE0_URL         (default: http://localhost:2358)
    JUDGE0_API_KEY     (default: none)
    MAX_RETRY_COUNT    (default: 3)
    CALLBACK_PORT      (default: 0 → OS picks a free port)
    CALLBACK_HOST      (default: auto-detected container IP)
                       Override on Linux if auto-detect gives wrong interface.
"""

import json
import os
import signal
import socket
import sys
import time

import redis

from core.job_queue     import PriorityJobQueue, QueuedJob
from core.judge0_client import Judge0Config, CallbackServer
from core.harness_builder import TestCase
from core.log           import get_logger
from autograder         import Autograder, Submission


log = get_logger(__name__)
MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", 3))


def _own_ip() -> str:
    """Return this container's IP on the Docker bridge network.
    Used as the callback_host so Judge0 can POST results back to us.
    Reads CALLBACK_HOST env var first; falls back to auto-detection.
    """
    override = os.getenv("CALLBACK_HOST")
    if override:
        return override
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


def _read_secret(env_name: str) -> str | None:
    """Read a secret from env var, falling back to a Docker secrets file."""
    val = os.getenv(env_name)
    if val:
        return val
    file_path = os.getenv(f"{env_name}_FILE")
    if file_path:
        try:
            with open(file_path) as fh:
                return fh.read().strip() or None
        except OSError:
            pass
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def job_to_submission(job: QueuedJob) -> Submission:
    """Reconstruct a Submission dataclass from the queued payload dict."""
    p = job.payload
    return Submission(
        student_id      = job.student_id,
        language        = p["language"],
        student_code    = p["student_code"],
        test_cases      = [
            TestCase(
                expected   = tc["expected"],
                inputs     = tc.get("inputs"),
                stdin_text = tc.get("stdin_text"),
            )
            for tc in p["test_cases"]
        ],
        mode            = p.get("mode",            "function"),
        function_name   = p.get("function_name",   "solve"),
        per_tc_limit_s  = p.get("per_tc_limit_s",  2),
        memory_limit_mb = p.get("memory_limit_mb", 256),
        param_types     = p.get("param_types"),
        return_type     = p.get("return_type",     "auto"),
    )


def result_to_dict(result) -> dict:
    """Serialise GradingResult to a JSON-safe dict for Redis storage."""
    if result.system_error:
        return {"system_error": result.system_error}

    if result.security_error:
        return {"security_error": result.security_error}

    return {
        "score":      result.submission.score,
        "total":      result.submission.total,
        "global_tle": result.submission.global_tle,
        "tc_results": [
            {
                "tc_num":   r.tc_num,
                "status":   r.status,
                "got":      r.got,
                "expected": r.expected,
                "detail":   r.detail,
                "warning":  r.warning,
            }
            for r in result.submission.tc_results
        ],
        "time_taken_s": result.judge0_raw.time_taken_s if result.judge0_raw else None,
        "memory_kb":    result.judge0_raw.memory_kb    if result.judge0_raw else None,
    }


# ── Worker loop ───────────────────────────────────────────────────────────────

def run_worker(queue: PriorityJobQueue, grader: Autograder) -> None:
    log.info("worker_started", queues=["retry", "normal"])

    def _handle_stop(sig, frame):
        nonlocal running
        running = False
        log.info("worker_stopping", signal=sig)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT,  _handle_stop)

    while running:
        # ── Pull next job (retry queue has priority) ──────────────────────
        job = queue.dequeue(timeout=5)
        if job is None:
            continue   # timeout — loop and check running flag

        ticket_id = job.ticket_id
        depths    = queue.depths()
        log.info(
            "job_dequeued",
            ticket_id=ticket_id[:8],
            retry_count=job.retry_count,
            queue_retry=depths["retry"],
            queue_normal=depths["normal"],
        )

        try:
            submission = job_to_submission(job)
            result     = grader.grade(submission, retry_count=job.retry_count)

            # ── Infrastructure failure — requeue or give up ───────────────
            if result.needs_requeue:
                if job.retry_count >= MAX_RETRY_COUNT:
                    log.warning(
                        "job_retries_exhausted",
                        ticket_id=ticket_id[:8],
                        max_retries=MAX_RETRY_COUNT,
                    )
                    queue.store_result(ticket_id, {
                        "system_error": (
                            "Grading server temporarily overloaded. "
                            "Your submission was not evaluated. Please resubmit."
                        )
                    }, idem_key=job.idem_key)
                    queue.ack(job)
                else:
                    log.info(
                        "job_requeued",
                        ticket_id=ticket_id[:8],
                        attempt=job.retry_count + 1,
                        max_retries=MAX_RETRY_COUNT,
                    )
                    queue.requeue(job)
                    queue.ack(job)

            # ── Normal result — store for the student to poll ─────────────
            else:
                queue.store_result(ticket_id, result_to_dict(result), idem_key=job.idem_key)
                queue.ack(job)
                log.info(
                    "job_done",
                    ticket_id=ticket_id[:8],
                    score=result.submission.score,
                    total=result.submission.total,
                )

        except Exception as exc:
            log.error("job_unexpected_error", ticket_id=ticket_id[:8], error=str(exc), exc_info=True)
            try:
                queue.store_result(ticket_id, {
                    "system_error": f"Internal grading error. Please resubmit. ({exc})"
                }, idem_key=job.idem_key)
                queue.ack(job)
            except Exception as store_exc:
                log.error(
                    "job_result_write_failed",
                    ticket_id=ticket_id[:8],
                    error=str(store_exc),
                )

    log.info("worker_stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    r = redis.Redis(
        host             = os.getenv("REDIS_HOST", "localhost"),
        port             = int(os.getenv("REDIS_PORT", 6379)),
        password         = _read_secret("REDIS_PASSWORD") or None,
        decode_responses = True,
    )

    judge0_cfg = Judge0Config(
        base_url = os.getenv("JUDGE0_URL", "http://localhost:2358"),
        api_key  = os.getenv("JUDGE0_API_KEY") or None,
    )

    # Callback mode is always active — polling has been removed.
    # CALLBACK_PORT=0 lets the OS pick a free port automatically;
    # each replica gets a unique port with no configuration.
    # Judge0 POSTs results back to this worker via the Docker internal network.
    cb_port         = int(os.getenv("CALLBACK_PORT", "0"))
    callback_host   = _own_ip()
    callback_server = CallbackServer()
    try:
        callback_server.start(port=cb_port)
        log.info(
            "callback_server_started",
            url=callback_server.url(callback_host),
        )
    except OSError as exc:
        log.error("callback_server_failed", error=str(exc))
        sys.exit(1)

    queue  = PriorityJobQueue(r)
    grader = Autograder(
        judge0_cfg,
        callback_server = callback_server,
        callback_host   = callback_host,
    )

    try:
        run_worker(queue, grader)
    finally:
        callback_server.stop()
