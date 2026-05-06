"""
api.py  —  Fix 3.2 (SSE delivery) + Fix 2.3 (idempotency + admission control)
───────────────────────────────────────────────────────────────────────────────
Flask REST API for the grading platform.

Endpoints:
  POST  /submit                      — enqueue a new submission
  GET   /status/<ticket_id>          — poll result (202 pending / 200 done)
  GET   /results/stream/<ticket_id>  — SSE stream; pushes result when ready
  GET   /health                      — liveness + queue depths

Fix 2.3 — Idempotency:
  SHA-256 of (student_id + ":" + assessment_id + ":" + sha256(source_code))
  stored in Redis as "judge0:idem:{key}" → ticket_id  (TTL = 24 h)
  Duplicate POST within 24 h returns the same ticket_id (200 instead of 202).

Fix 3.2 — SSE delivery:
  /results/stream/<ticket_id> subscribes to "judge0:notify:{ticket_id}" via
  Redis pub/sub.  If a result already exists in Redis it is emitted immediately
  (avoids a race between enqueue completion and the client opening the SSE
  connection).  The stream closes after the first result event.

Run:
    python api.py

Environment variables:
    REDIS_HOST          (default: localhost)
    REDIS_PORT          (default: 6379)
    REDIS_PASSWORD      (default: none)
    MAX_QUEUE_DEPTH     (default: 5000)
    SSE_TIMEOUT_S       (default: 1800)
    PORT                (default: 5000)
"""

import hashlib
import json
import os
import time
import uuid

import redis
from flask import Flask, Response, jsonify, request, stream_with_context

from core.job_queue import (
    PriorityJobQueue,
    QueuedJob,
    NOTIFY_PREFIX,
    RESULT_PREFIX,
    RESULT_TTL_S,
)
from core.harness_builder import TestCase


# ── Constants ─────────────────────────────────────────────────────────────────

MAX_QUEUE_DEPTH  = int(os.getenv("MAX_QUEUE_DEPTH",  5000))
SSE_TIMEOUT_S    = int(os.getenv("SSE_TIMEOUT_S",    1800))
IDEM_PREFIX      = "judge0:idem:"
IDEM_TTL_S       = 86400   # 24 hours


# ── App & Redis setup ─────────────────────────────────────────────────────────

app = Flask(__name__)

_redis_client: redis.Redis | None = None
_queue:        PriorityJobQueue  | None = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host     = os.getenv("REDIS_HOST", "localhost"),
            port     = int(os.getenv("REDIS_PORT", 6379)),
            password = os.getenv("REDIS_PASSWORD") or None,
            decode_responses = False,
        )
    return _redis_client


def _get_queue() -> PriorityJobQueue:
    global _queue
    if _queue is None:
        _queue = PriorityJobQueue(_get_redis())
    return _queue


# ── Idempotency helpers ───────────────────────────────────────────────────────

def _idem_key(student_id: str, assessment_id: str, source_code: str) -> str:
    """Deterministic key for deduplication."""
    code_hash = hashlib.sha256(source_code.encode()).hexdigest()
    raw = f"{student_id}:{assessment_id}:{code_hash}"
    return IDEM_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/submit")
def submit():
    """
    Enqueue a grading job.

    Request body (JSON):
    {
      "student_id":      "s123",
      "assessment_id":   "a456",       # used for idempotency only
      "language":        "python",     # python | c | cpp | java
      "student_code":    "def solve...",
      "test_cases": [
        { "expected": "42", "inputs": [...] },
        { "expected": "7",  "stdin_text": "..." },
        ...
      ],
      "mode":            "function",   # or "stdio"
      "function_name":   "solve",
      "per_tc_limit_s":  2,
      "memory_limit_mb": 256,
      "param_types":     ["int", "int"],
      "return_type":     "int"
    }

    Returns:
      202  { "ticket_id": "...", "status": "queued" }
      200  { "ticket_id": "...", "status": "duplicate" }   (idempotent re-submit)
      400  { "error": "..." }
      429  { "error": "Queue at capacity. Try again later." }
    """
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "Request body must be JSON"}), 400

    # Required fields
    for field_name in ("student_id", "assessment_id", "language", "student_code", "test_cases"):
        if field_name not in body:
            return jsonify({"error": f"Missing required field: {field_name}"}), 400

    student_id    = str(body["student_id"])
    assessment_id = str(body["assessment_id"])
    language      = str(body["language"])
    student_code  = str(body["student_code"])
    test_cases    = body["test_cases"]

    if not isinstance(test_cases, list) or not test_cases:
        return jsonify({"error": "test_cases must be a non-empty list"}), 400

    # Fix 2.3 — Idempotency check
    idem_key = _idem_key(student_id, assessment_id, student_code)
    r = _get_redis()
    existing = r.get(idem_key)
    if existing:
        ticket_id = existing.decode() if isinstance(existing, bytes) else existing
        return jsonify({"ticket_id": ticket_id, "status": "duplicate"}), 200

    # Fix 2.3 — Admission control
    queue = _get_queue()
    if queue.is_at_capacity(MAX_QUEUE_DEPTH):
        return jsonify({"error": "Queue at capacity. Try again later."}), 429

    # Build ticket + payload
    ticket_id = str(uuid.uuid4())
    payload = {
        "language":        language,
        "student_code":    student_code,
        "test_cases":      test_cases,
        "mode":            body.get("mode",            "function"),
        "function_name":   body.get("function_name",   "solve"),
        "per_tc_limit_s":  body.get("per_tc_limit_s",  2),
        "memory_limit_mb": body.get("memory_limit_mb", 256),
        "param_types":     body.get("param_types"),
        "return_type":     body.get("return_type",     "auto"),
    }

    job = QueuedJob(
        ticket_id  = ticket_id,
        student_id = student_id,
        payload    = payload,
    )
    queue.enqueue(job)

    # Store idempotency key after successful enqueue
    r.setex(idem_key, IDEM_TTL_S, ticket_id)

    return jsonify({"ticket_id": ticket_id, "status": "queued"}), 202


@app.get("/status/<ticket_id>")
def status(ticket_id: str):
    """
    Poll for a grading result.

    Returns:
      202  { "status": "pending" }
      200  { "status": "done", "result": { ... } }
      404  { "error": "Unknown ticket_id" }
    """
    r = _get_redis()
    raw = r.get(f"{RESULT_PREFIX}{ticket_id}")
    if raw is None:
        # Check whether we've seen this ticket at all (pending deadline key)
        # For simplicity, return 202 (the client should keep polling or use SSE)
        return jsonify({"status": "pending"}), 202

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        return jsonify({"status": "done", "result": {}}), 200

    return jsonify({"status": "done", "result": result}), 200


@app.get("/results/stream/<ticket_id>")
def results_stream(ticket_id: str):
    """
    SSE endpoint — Fix 3.2.
    Emits a single "result" event when grading completes, then closes.

    Headers set:
      Content-Type:     text/event-stream
      Cache-Control:    no-cache
      X-Accel-Buffering: no   (disables nginx proxy buffering)
      Connection:       keep-alive
    """
    r      = _get_redis()
    notify = f"{NOTIFY_PREFIX}{ticket_id}"

    @stream_with_context
    def _generate():
        # Check whether a result is already stored (avoids race condition
        # where result arrived before the client opened the stream)
        existing = r.get(f"{RESULT_PREFIX}{ticket_id}")
        if existing:
            payload = existing.decode() if isinstance(existing, bytes) else existing
            yield f"event: result\ndata: {payload}\n\n"
            return

        # Subscribe to the pub/sub channel for this ticket
        pubsub = r.pubsub()
        pubsub.subscribe(notify)

        deadline = time.monotonic() + SSE_TIMEOUT_S
        try:
            while time.monotonic() < deadline:
                # Non-blocking get_message — keeps the generator cooperative
                msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg["type"] == "message":
                    data = msg["data"]
                    payload = data.decode() if isinstance(data, bytes) else data
                    yield f"event: result\ndata: {payload}\n\n"
                    return

                # Heartbeat to keep the connection alive through load balancers
                yield ": heartbeat\n\n"

            # Timeout — send a system_error
            timeout_payload = json.dumps({
                "system_error": "Result stream timed out. Use /status to poll."
            })
            yield f"event: result\ndata: {timeout_payload}\n\n"
        finally:
            pubsub.unsubscribe(notify)
            pubsub.close()

    return Response(
        _generate(),
        mimetype = "text/event-stream",
        headers  = {
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@app.get("/health")
def health():
    """
    Liveness + readiness probe.

    Returns:
      200  { "status": "ok", "queue": { "retry": N, "normal": N } }
      503  { "status": "error", "error": "..." }
    """
    try:
        r = _get_redis()
        r.ping()
        depths = _get_queue().depths()
        return jsonify({"status": "ok", "queue": depths}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 503


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    # Use threaded=True so SSE streams don't block other requests
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)
