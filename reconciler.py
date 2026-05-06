"""
reconciler.py  —  Fix 2.2: At-least-once delivery guarantee
─────────────────────────────────────────────────────────────
Background daemon that detects and recovers from two failure modes:

  1. Worker-crash recovery (visibility-timeout pattern)
     Every `SCAN_INTERVAL_S` seconds we walk PROCESSING_QUEUE.
     If a job's INFLIGHT key has expired (worker died mid-job) AND no result
     has been stored yet, the job is requeued to RETRY_QUEUE.

  2. Pending-deadline enforcement (Fix 2.6 companion)
     Via Redis keyspace notifications (expired events on PENDING_DEADLINE_*
     keys), any submission that was never picked up within MAX_JOB_WAIT_S
     gets a "system_error" result written immediately.

Run:
    python reconciler.py

Environment variables (inherit from worker.py):
    REDIS_HOST         (default: localhost)
    REDIS_PORT         (default: 6379)
    REDIS_PASSWORD     (default: none)
"""

import json
import os
import signal
import sys
import time

import redis

from core.job_queue import (
    PriorityJobQueue,
    QueuedJob,
    PROCESSING_QUEUE,
    INFLIGHT_PREFIX,
    INFLIGHT_TTL_S,
    PENDING_DEADLINE_PREFIX,
    RESULT_PREFIX,
    RESULT_TTL_S,
    RETRY_QUEUE,
    NOTIFY_PREFIX,
)


SCAN_INTERVAL_S = int(os.getenv("RECONCILER_SCAN_INTERVAL_S", 60))

# ── Keyspace-notification channel prefix for expired keys ─────────────────────
# Pattern: __keyevent@0__:expired
_KS_CHANNEL = "__keyevent@{db}__:expired"


def _requeue_raw(r: redis.Redis, raw: str) -> None:
    """Push raw JSON job back to the retry queue with retry_count incremented."""
    try:
        job_dict = json.loads(raw)
        job_dict["retry_count"] = job_dict.get("retry_count", 0) + 1
        r.lpush(RETRY_QUEUE, json.dumps(job_dict))
    except (json.JSONDecodeError, Exception) as e:
        print(f"[reconciler] Failed to requeue raw job: {e}", flush=True)


def _write_expired_result(r: redis.Redis, ticket_id: str) -> None:
    """Write a system_error result for a submission that never got graded."""
    result = json.dumps({
        "system_error": (
            "Submission was not processed within the time limit. "
            "The grading server may have been overloaded. Please resubmit."
        )
    })
    pipe = r.pipeline()
    result_key = f"{RESULT_PREFIX}{ticket_id}"
    # Only write if no result already stored (could have finished in parallel)
    pipe.set(result_key, result, nx=True, ex=RESULT_TTL_S)
    pipe.publish(f"{NOTIFY_PREFIX}{ticket_id}", result)
    pipe.execute()


# ── Worker-crash scan ─────────────────────────────────────────────────────────

def scan_processing_queue(r: redis.Redis) -> None:
    """
    Walk the entire PROCESSING_QUEUE list.
    For each entry: if INFLIGHT key is gone (TTL expired = worker died)
    and no result exists → requeue.
    If result exists but job still in list → just remove (stale cleanup).
    """
    # Snapshot the queue (LRANGE returns all elements without blocking)
    raws = r.lrange(PROCESSING_QUEUE, 0, -1)
    if not raws:
        return

    for raw in raws:
        raw_str = raw.decode() if isinstance(raw, bytes) else raw
        try:
            job_dict  = json.loads(raw_str)
            ticket_id = job_dict.get("ticket_id", "")
        except (json.JSONDecodeError, KeyError):
            # Corrupted entry — remove it
            r.lrem(PROCESSING_QUEUE, 1, raw)
            continue

        inflight_key = f"{INFLIGHT_PREFIX}{ticket_id}"
        result_key   = f"{RESULT_PREFIX}{ticket_id}"

        inflight_exists = r.exists(inflight_key)
        result_exists   = r.exists(result_key)

        if inflight_exists:
            # Worker is still alive (or recently died but TTL not yet expired)
            continue

        # Inflight key gone — worker died
        if result_exists:
            # Worker stored result before dying — just clean up the stale entry
            r.lrem(PROCESSING_QUEUE, 1, raw)
            print(
                f"[reconciler] Cleaned stale PROCESSING entry for "
                f"{ticket_id[:8]} (result already exists)",
                flush=True,
            )
        else:
            # Worker died without storing a result — requeue with retry++
            r.lrem(PROCESSING_QUEUE, 1, raw)
            _requeue_raw(r, raw_str)
            print(
                f"[reconciler] Requeued crashed job {ticket_id[:8]}",
                flush=True,
            )


# ── Keyspace-notification listener (pending deadline enforcement) ─────────────

def listen_for_expired_deadlines(r: redis.Redis, db: int = 0) -> None:
    """
    Subscribe to Redis keyspace notifications for expired keys.
    When a PENDING_DEADLINE_* key expires, write a system_error result
    for that ticket_id if no result is present yet.

    This runs in the same process as the scan loop but on a separate pubsub
    connection; we use a non-blocking poll in the main loop.
    """
    channel = _KS_CHANNEL.format(db=db)
    pubsub  = r.pubsub()
    pubsub.psubscribe(channel)
    return pubsub


def handle_expired_message(r: redis.Redis, message: dict) -> None:
    """Called for each expired-key notification."""
    if message["type"] not in ("message", "pmessage"):
        return
    key_name = message.get("data", b"")
    if isinstance(key_name, bytes):
        key_name = key_name.decode()

    if key_name.startswith(PENDING_DEADLINE_PREFIX):
        ticket_id = key_name[len(PENDING_DEADLINE_PREFIX):]
        result_key = f"{RESULT_PREFIX}{ticket_id}"
        if not r.exists(result_key):
            _write_expired_result(r, ticket_id)
            print(
                f"[reconciler] Deadline expired for {ticket_id[:8]} — "
                f"wrote system_error",
                flush=True,
            )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_reconciler(r: redis.Redis) -> None:
    # Enable keyspace notifications for expired events if not already set
    # "Ex" = Keyevent (E) + expired (x)
    try:
        r.config_set("notify-keyspace-events", "Ex")
    except redis.ResponseError:
        print(
            "[reconciler] WARNING: Could not set notify-keyspace-events "
            "(may lack CONFIG SET permissions). "
            "Pending deadline enforcement will be disabled.",
            flush=True,
        )

    db = int(r.client_info().get("db", 0)) if hasattr(r, "client_info") else 0
    pubsub = listen_for_expired_deadlines(r, db=db)

    running = True

    def _stop(sig, frame):
        nonlocal running
        running = False
        print(f"[reconciler] Stopping (signal {sig})...", flush=True)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    print(
        f"[reconciler] Started. Scan interval={SCAN_INTERVAL_S}s.",
        flush=True,
    )

    last_scan = 0.0

    while running:
        # Non-blocking pubsub poll (expired-deadline events)
        try:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.01)
            if msg:
                handle_expired_message(r, msg)
        except Exception as e:
            print(f"[reconciler] pubsub error: {e}", flush=True)

        # Periodic full scan
        now = time.monotonic()
        if now - last_scan >= SCAN_INTERVAL_S:
            try:
                scan_processing_queue(r)
            except Exception as e:
                print(f"[reconciler] scan error: {e}", flush=True)
            last_scan = now

        time.sleep(0.1)

    pubsub.close()
    print("[reconciler] Stopped.", flush=True)


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
        print(f"[reconciler] Cannot connect to Redis: {e}", file=sys.stderr)
        sys.exit(1)

    run_reconciler(r)
