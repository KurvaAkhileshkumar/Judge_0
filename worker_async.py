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
    CALLBACK_PORT         (unset → polling mode)
    CALLBACK_HOST         (default: host.docker.internal)
"""

import asyncio
import json
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

import redis

from core.job_queue       import PriorityJobQueue, QueuedJob
from core.judge0_client   import Judge0Config, CallbackServer
from core.harness_builder import TestCase
from autograder           import Autograder, Submission
from worker               import job_to_submission, result_to_dict, MAX_RETRY_COUNT


MAX_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", 48))


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
                    print(
                        f"[{ticket_id[:8]}] All {MAX_RETRY_COUNT} retries exhausted "
                        f"— storing system_error",
                        flush=True,
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
                    )
                    await asyncio.to_thread(queue.ack, job)
                else:
                    print(
                        f"[{ticket_id[:8]}] Infra failure "
                        f"(retry {job.retry_count + 1}/{MAX_RETRY_COUNT}) "
                        f"— requeueing",
                        flush=True,
                    )
                    await asyncio.to_thread(queue.requeue, job)
                    await asyncio.to_thread(queue.ack, job)

            else:
                await asyncio.to_thread(
                    queue.store_result, ticket_id, result_to_dict(result)
                )
                await asyncio.to_thread(queue.ack, job)
                print(
                    f"[{ticket_id[:8]}] Done — "
                    f"score={result.submission.score}/{result.submission.total}",
                    flush=True,
                )

        except Exception as exc:
            print(f"[{ticket_id[:8]}] Unexpected error: {exc}", flush=True)
            try:
                await asyncio.to_thread(
                    queue.store_result,
                    ticket_id,
                    {"system_error": f"Internal grading error. Please resubmit. ({exc})"},
                )
                await asyncio.to_thread(queue.ack, job)
            except Exception as inner:
                print(f"[{ticket_id[:8]}] Failed to store error result: {inner}", flush=True)


# ── Main dequeue loop ─────────────────────────────────────────────────────────

async def run_worker_async(queue: PriorityJobQueue, grader: Autograder) -> None:
    sem      = asyncio.Semaphore(MAX_CONCURRENCY)
    tasks:   set[asyncio.Task] = set()
    running  = True

    loop = asyncio.get_running_loop()

    def _stop(sig):
        nonlocal running
        running = False
        print(f"[async-worker] Stopping (signal {sig})...", flush=True)

    loop.add_signal_handler(signal.SIGTERM, _stop, signal.SIGTERM)
    loop.add_signal_handler(signal.SIGINT,  _stop, signal.SIGINT)

    print(
        f"[async-worker] Started. Concurrency={MAX_CONCURRENCY}.",
        flush=True,
    )

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
        print(
            f"[async-worker] Waiting for {len(tasks)} in-flight tasks...",
            flush=True,
        )
        await asyncio.gather(*tasks, return_exceptions=True)

    print("[async-worker] Stopped.", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    r = redis.Redis(
        host     = os.getenv("REDIS_HOST", "localhost"),
        port     = int(os.getenv("REDIS_PORT", 6379)),
        password = os.getenv("REDIS_PASSWORD") or None,
        decode_responses = False,
    )
    try:
        r.ping()
    except redis.ConnectionError as e:
        print(f"[async-worker] Cannot connect to Redis: {e}", file=sys.stderr)
        sys.exit(1)

    queue = PriorityJobQueue(r)

    callback_port = os.getenv("CALLBACK_PORT")
    callback_host = os.getenv("CALLBACK_HOST", "host.docker.internal")
    cfg = Judge0Config(
        base_url    = os.getenv("JUDGE0_URL", "http://localhost:2358"),
        api_key     = os.getenv("JUDGE0_API_KEY") or None,
        callback_host = callback_host if callback_port is not None else None,
        callback_port = int(callback_port) if callback_port is not None else None,
    )

    cb_server: CallbackServer | None = None
    if cfg.callback_host:
        cb_server = CallbackServer(port=cfg.callback_port or 0)
        cb_server.start()
        cfg.callback_port = cb_server.port
        print(f"[async-worker] Callback server on port {cfg.callback_port}", flush=True)

    grader = Autograder(cfg)

    # Use a thread pool large enough for MAX_CONCURRENCY + housekeeping threads
    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY + 4)

    try:
        loop = asyncio.new_event_loop()
        loop.set_default_executor(executor)
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_worker_async(queue, grader))
    finally:
        executor.shutdown(wait=False)
        if cb_server:
            cb_server.stop()
        loop.close()
