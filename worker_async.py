"""
worker_async.py  —  Fix 3.1: Concurrent async worker
──────────────────────────────────────────────────────
Replaces the single-threaded worker.py with an asyncio event loop that
dispatches up to MAX_CONCURRENCY grading jobs in parallel.

Architecture:
  - Single event loop on the main thread.
  - `dequeue()` and `grader.grade()` are blocking I/O bound; both are
    offloaded via `asyncio.to_thread()` to a shared ThreadPoolExecutor.
  - An asyncio.Semaphore caps live tasks at MAX_CONCURRENCY.
  - Each task calls `queue.ack(job)` in all exit paths (Fix 2.1).
  - Graceful shutdown: on SIGTERM/SIGINT the dequeue loop stops and
    all in-flight tasks are awaited before the process exits.

Run:
    python worker_async.py

Environment variables:
    REDIS_HOST            (default: localhost)
    REDIS_PORT            (default: 6379)
    REDIS_PASSWORD        (default: none)
    JUDGE0_URL            (default: http://localhost:2358)
    JUDGE0_API_KEY        (default: none)
    MAX_RETRY_COUNT       (default: 3)
    WORKER_CONCURRENCY    (default: 48)
    CALLBACK_PORT         (default: 0 → OS picks a free port)
    CALLBACK_HOST         (default: auto-detected container IP)
"""

import asyncio
import json
import os
import signal
import socket
import sys
from concurrent.futures import ThreadPoolExecutor

import redis

from core.job_queue       import PriorityJobQueue, QueuedJob
from core.judge0_client   import Judge0Config, CallbackServer
from core.harness_builder import TestCase
from core.log             import get_logger
from autograder           import Autograder, Submission
from worker               import job_to_submission, result_to_dict, MAX_RETRY_COUNT


log = get_logger(__name__)
MAX_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 48))


def _own_ip() -> str:
    """Return this container's IP on the Docker bridge network."""
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


# ── Per-job task ──────────────────────────────────────────────────────────────

async def process_job(
    job:     QueuedJob,
    queue:   PriorityJobQueue,
    grader:  Autograder,
    sem:     asyncio.Semaphore,
) -> None:
    """
    Process a single grading job under the concurrency semaphore.
    All paths call queue.ack(job) to remove from PROCESSING_QUEUE.
    """
    async with sem:
        ticket_id = job.ticket_id
        try:
            submission = job_to_submission(job)
            result     = await asyncio.to_thread(
                grader.grade, submission, job.retry_count
            )

            if result.needs_requeue:
                if job.retry_count >= MAX_RETRY_COUNT:
                    log.warning(
                        "job_retries_exhausted",
                        ticket_id=ticket_id[:8],
                        max_retries=MAX_RETRY_COUNT,
                    )
                    await asyncio.to_thread(
                        queue.store_result,
                        ticket_id,
                        {
                            "system_error": (
                                "Grading server temporarily overloaded. "
                                "Your submission was not evaluated. Please resubmit."
                            )
                        },
                        job.idem_key,
                    )
                    # Retries exhausted — delete Judge0 submission now
                    if result.judge0_raw and result.judge0_raw.token:
                        await asyncio.to_thread(
                            grader.judge0.delete_submission,
                            result.judge0_raw.token,
                        )
                    await asyncio.to_thread(queue.ack, job)
                else:
                    log.info(
                        "job_requeued",
                        ticket_id=ticket_id[:8],
                        attempt=job.retry_count + 1,
                        max_retries=MAX_RETRY_COUNT,
                    )
                    await asyncio.to_thread(queue.requeue, job)
                    await asyncio.to_thread(queue.ack, job)

            else:
                await asyncio.to_thread(
                    queue.store_result, ticket_id, result_to_dict(result), job.idem_key
                )
                await asyncio.to_thread(queue.ack, job)
                log.info(
                    "job_done",
                    ticket_id=ticket_id[:8],
                    score=result.submission.score,
                    total=result.submission.total,
                )

        except Exception as exc:
            log.error("job_unexpected_error", ticket_id=ticket_id[:8], error=str(exc), exc_info=True)
            try:
                await asyncio.to_thread(
                    queue.store_result,
                    ticket_id,
                    {"system_error": f"Internal grading error. Please resubmit. ({exc})"},
                    job.idem_key,
                )
                await asyncio.to_thread(queue.ack, job)
            except Exception as store_exc:
                log.error(
                    "job_result_write_failed",
                    ticket_id=ticket_id[:8],
                    error=str(store_exc),
                )


# ── Main dequeue loop ─────────────────────────────────────────────────────────

async def run_worker_async(queue: PriorityJobQueue, grader: Autograder) -> None:
    sem      = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks:   set[asyncio.Task] = set()
    running  = True

    loop = asyncio.get_running_loop()

    def _stop(sig):
        nonlocal running
        running = False
        log.info("worker_stopping", signal=sig)

    loop.add_signal_handler(signal.SIGTERM, _stop, signal.SIGTERM)
    loop.add_signal_handler(signal.SIGINT,  _stop, signal.SIGINT)

    log.info("worker_started", concurrency=MAX_CONCURRENCY)

    while running:
        # Non-blocking dequeue: wait up to 5 s in a thread
        job = await asyncio.to_thread(queue.dequeue, 5)
        if job is None:
            continue   # timeout — check running flag

        # Reap completed tasks to avoid unbounded set growth
        done = {t for t in tasks if t.done()}
        tasks -= done

        task = asyncio.create_task(process_job(job, queue, grader, sem))
        tasks.add(task)

    # Graceful shutdown: wait for all in-flight tasks
    if tasks:
        log.info("worker_draining", in_flight=len(tasks))
        await asyncio.gather(*tasks, return_exceptions=True)

    log.info("worker_stopped")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    r = redis.Redis(
        host     = os.getenv("REDIS_HOST", "localhost"),
        port     = int(os.getenv("REDIS_PORT", 6379)),
        password = _read_secret("REDIS_PASSWORD") or None,
        decode_responses = False,
    )
    try:
        r.ping()
    except redis.ConnectionError as e:
        log.error("redis_connection_failed", error=str(e))
        sys.exit(1)

    queue = PriorityJobQueue(r)

    # Callback mode is always active — polling has been removed.
    cb_port       = int(os.getenv("CALLBACK_PORT", "0"))
    callback_host = _own_ip()
    cfg = Judge0Config(
        base_url = os.getenv("JUDGE0_URL", "http://localhost:2358"),
        api_key  = os.getenv("JUDGE0_API_KEY") or None,
    )

    cb_server = CallbackServer()
    try:
        cb_server.start(port=cb_port)
        log.info("callback_server_started", url=cb_server.url(callback_host))
    except OSError as exc:
        log.error("callback_server_failed", error=str(exc))
        sys.exit(1)

    grader = Autograder(
        cfg,
        callback_server = cb_server,
        callback_host   = callback_host,
    )

    # Use a thread pool large enough for MAX_CONCURRENCY + housekeeping threads
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY + 4)

    try:
        loop = asyncio.new_event_loop()
        loop.set_default_executor(executor)
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_worker_async(queue, grader))
    finally:
        executor.shutdown(wait=False)
        cb_server.stop()
        loop.close()
