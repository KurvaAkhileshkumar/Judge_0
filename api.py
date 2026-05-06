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
    REDIS_PASSWORD      (default: none)  OR  REDIS_PASSWORD_FILE (Docker secrets path)
    MAX_QUEUE_DEPTH     (default: 5000)
    SSE_TIMEOUT_S       (default: 1800)
    PORT                (default: 5000)
    HEALTH_TOKEN        (optional) — if set, GET /health requires
                        "Authorization: Bearer <token>" or "X-Health-Token: <token>"
    LOG_LEVEL           (default: INFO)
"""

import hashlib
import hmac
import json
import os
import time
import uuid

import redis
import requests as http_requests
from flask import Flask, Response, jsonify, request, stream_with_context
from pydantic import BaseModel, ValidationError, field_validator, model_validator
from typing import Any

from core.job_queue import (
    PriorityJobQueue,
    QueuedJob,
    NOTIFY_PREFIX,
    RESULT_PREFIX,
    RESULT_TTL_S,
)
from core.harness_builder import TestCase
from core.judge0_client import _judge0_breaker
from core.log import get_logger

log = get_logger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

MAX_QUEUE_DEPTH  = int(os.getenv("MAX_QUEUE_DEPTH",  5000))
SSE_TIMEOUT_S    = int(os.getenv("SSE_TIMEOUT_S",    1800))
IDEM_PREFIX      = "judge0:idem:"
IDEM_TTL_S       = 86400   # 24 hours


# ── App & Redis setup ─────────────────────────────────────────────────────────

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024  # 1 MB — rejects oversized payloads before any parsing

_redis_client: redis.Redis | None = None
_queue:        PriorityJobQueue  | None = None


def _get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host     = os.getenv("REDIS_HOST", "localhost"),
            port     = int(os.getenv("REDIS_PORT", 6379)),
            password = _read_secret("REDIS_PASSWORD") or None,
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

    # ── Pydantic validation ──────────────────────────────────────────────────
    try:
        req = _SubmitRequest.model_validate(body)
    except ValidationError as exc:
        errors = [
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        ]
        log.warning("submit_validation_failed", errors=errors)
        return jsonify({"error": "Validation failed", "details": errors}), 400

    student_id    = req.student_id
    assessment_id = req.assessment_id
    language      = req.language
    student_code  = req.student_code

    # Fix 2.3 — Idempotency check
    idem_key = _idem_key(student_id, assessment_id, student_code)
    r = _get_redis()
    existing = r.get(idem_key)
    if existing:
        ticket_id = existing.decode() if isinstance(existing, bytes) else existing
        log.info("submit_duplicate", ticket_id=ticket_id[:8], student_id=student_id)
        return jsonify({"ticket_id": ticket_id, "status": "duplicate"}), 200

    # Fix 2.3 — Admission control
    queue = _get_queue()
    if queue.is_at_capacity(MAX_QUEUE_DEPTH):
        log.warning("submit_queue_full", max_depth=MAX_QUEUE_DEPTH)
        return jsonify({"error": "Queue at capacity. Try again later."}), 429

    # Build ticket + payload
    ticket_id = str(uuid.uuid4())
    payload = {
        "language":        language,
        "student_code":    student_code,
        "test_cases":      [
            {"expected": tc.expected, "inputs": tc.inputs, "stdin_text": tc.stdin_text}
            for tc in req.test_cases
        ],
        "mode":            req.mode,
        "function_name":   req.function_name,
        "per_tc_limit_s":  req.per_tc_limit_s,
        "memory_limit_mb": req.memory_limit_mb,
        "param_types":     req.param_types,
        "return_type":     req.return_type,
    }

    job = QueuedJob(
        ticket_id    = ticket_id,
        student_id   = student_id,
        submitted_at = time.time(),
        payload      = payload,
        idem_key     = idem_key,
    )
    queue.enqueue(job)

    # Store idempotency key after successful enqueue
    r.setex(idem_key, IDEM_TTL_S, ticket_id)

    log.info("submit_queued", ticket_id=ticket_id[:8], student_id=student_id,
             language=language, tc_count=len(req.test_cases))
    return jsonify({"ticket_id": ticket_id, "status": "queued"}), 202


@app.get("/status/<ticket_id>")
def status(ticket_id: str):
    """
    Poll for a grading result.

    Returns:
      202  { "status": "pending", "queue_depth": N, "estimated_wait_s": N }
      200  { "status": "done", "result": { ... } }
    """
    r = _get_redis()
    raw = r.get(f"{RESULT_PREFIX}{ticket_id}")
    if raw is None:
        depths = _get_queue().depths()
        queue_depth = depths["retry"] + depths["normal"]
        # 15 s is the typical wall time for a 1000-TC harness job at standard scale.
        # This gives a rough ETA; the frontend should display it as "up to N seconds".
        return jsonify({
            "status":           "pending",
            "queue_depth":      queue_depth,
            "estimated_wait_s": queue_depth * 15,
        }), 202

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

    If HEALTH_TOKEN is set, the request must supply it as:
      Authorization: Bearer <token>
      or X-Health-Token: <token>

    Returns:
      200  { "status": "ok" | "degraded", "redis": {...}, "judge0": {...}, "circuit_breaker": {...} }
      401  { "error": "Unauthorized" }   — when HEALTH_TOKEN is set and not matched
      503  { "status": "error", ... }   — only when Redis is unreachable (true outage)

    "degraded" means Judge0 is unreachable or the circuit breaker is open but
    Redis is fine — workers will keep retrying, nothing is permanently lost.
    "error" means Redis is down — the queue itself is unavailable.
    """
    # ── Token auth (timing-safe comparison via hmac.compare_digest) ──────────
    if HEALTH_TOKEN:
        auth_header = request.headers.get("Authorization", "")
        provided = (
            auth_header.removeprefix("Bearer ").strip()
            or request.headers.get("X-Health-Token", "")
        )
        if not hmac.compare_digest(provided, HEALTH_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401

    checks = {}
    redis_ok = True

    # ── Redis ─────────────────────────────────────────────────────────────
    try:
        r = _get_redis()
        r.ping()
        depths = _get_queue().depths()
        checks["redis"] = {"ok": True, "queue": depths}
    except Exception as e:
        checks["redis"] = {"ok": False, "error": str(e)}
        redis_ok = False

    # ── Judge0 ────────────────────────────────────────────────────────────
    judge0_url = os.getenv("JUDGE0_URL", "http://localhost:2358")
    try:
        resp = http_requests.get(f"{judge0_url}/system_info", timeout=3)
        judge0_ok = resp.status_code == 200
        checks["judge0"] = {"ok": judge0_ok}
    except Exception as e:
        checks["judge0"] = {"ok": False, "error": str(e)}
        judge0_ok = False

    # ── Circuit breaker ───────────────────────────────────────────────────
    breaker_open = _judge0_breaker.is_open()
    checks["circuit_breaker"] = {"open": breaker_open}

    # Determine overall status
    if not redis_ok:
        status_str = "error"
    elif not judge0_ok or breaker_open:
        status_str = "degraded"
    else:
        status_str = "ok"

    http_status = 503 if not redis_ok else 200
    return jsonify({"status": status_str, **checks}), http_status


# ── Entry point ───────────────────────────────────────────────────────────────

@app.before_request
def _log_request():
    log.info(
        "http_request",
        method=request.method,
        path=request.path,
        remote=request.remote_addr,
    )


@app.after_request
def _log_response(response):
    log.info(
        "http_response",
        method=request.method,
        path=request.path,
        status=response.status_code,
    )
    return response

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    # Use threaded=True so SSE streams don't block other requests
    app.run(host="0.0.0.0", port=port, threaded=True, debug=False)
