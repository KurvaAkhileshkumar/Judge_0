"""
edwisely_api.py  —  Edwisely Hackathon Grading Bridge
======================================================
Sits between the Edwisely student frontend and the Judge0 EC2 grader.

Flow:
  1. Student POSTs to /api/grade with a Bearer token + code + test cases
  2. This service verifies the token against Edwisely's auth endpoint
  3. On success, forwards the job to Judge0 EC2 /submit
  4. Returns { ticket_id } immediately to the student
  5. Judge0 EC2 calls POST /api/webhook/result when grading finishes
  6. This service stores the result and optionally notifies the student

Endpoints:
  POST /api/grade                — submit code for grading
  GET  /api/result/<ticket_id>  — poll for result
  POST /api/webhook/result       — Judge0 EC2 calls this when done (internal)
  GET  /api/health               — health check

Environment variables:
  JUDGE0_EC2_URL      Judge0 EC2 base URL  (default: http://ec2-52-66-244-88.ap-south-1.compute.amazonaws.com:5001)
  EDWISELY_AUTH_URL   Your auth verify URL  (default: https://studenthackathon.edwisely.com/api/verify-token)
  WEBHOOK_BASE_URL    Public URL of this service (default: https://studenthackathon.edwisely.com)
  WEBHOOK_SECRET      Shared secret to validate incoming webhooks from Judge0 EC2
  PORT                Server port (default: 8000)
"""

import hashlib
import hmac
import json
import os
from datetime import datetime

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

JUDGE0_EC2_URL   = os.getenv("JUDGE0_EC2_URL",   "http://ec2-52-66-244-88.ap-south-1.compute.amazonaws.com:5001")
EDWISELY_AUTH_URL = os.getenv("EDWISELY_AUTH_URL", "https://studenthackathon.edwisely.com/api/verify-token")
WEBHOOK_BASE_URL  = os.getenv("WEBHOOK_BASE_URL",  "https://studenthackathon.edwisely.com")
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET",    "change-me-in-production")

# In-memory result store — replace with your DB (Postgres, MongoDB, etc.)
# Key: ticket_id  →  Value: { result, student_id, assessment_id, received_at }
_results: dict = {}


# ── Auth helper ───────────────────────────────────────────────────────────────

def _verify_student_token(token: str) -> dict | None:
    """
    Verify the student's Bearer token against Edwisely's auth endpoint.

    Returns the student payload dict on success, None on failure.

    Expected response from your auth endpoint (HTTP 200):
      { "student_id": "...", "name": "...", "assessment_id": "..." }

    Replace this with your actual auth logic if different.
    """
    try:
        resp = requests.get(
            EDWISELY_AUTH_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/api/grade")
def grade():
    """
    Student submits code for grading.

    Headers:
      Authorization: Bearer <student_token>

    Request body:
    {
      "language":        "python | c | cpp | java",
      "student_code":    "...",
      "test_cases":      [ { "stdin_text": "...", "expected": "..." } ],
      "mode":            "stdio | function",          (default: stdio)
      "function_name":   "solve",                     (function mode only)
      "param_types":     ["int", "int"],              (function mode only)
      "return_type":     "int",                       (function mode only)
      "per_tc_limit_s":  2,                           (optional, default 2)
      "memory_limit_mb": 256                          (optional, default 256)
    }

    Response 202:
      { "ticket_id": "uuid", "status": "queued" }

    Response 401:
      { "error": "Unauthorized" }

    Response 400:
      { "error": "..." }
    """

    # ── Step 1: Verify student token ─────────────────────────────────────────
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized — missing Bearer token"}), 401

    token   = auth_header.removeprefix("Bearer ").strip()
    student = _verify_student_token(token)
    if not student:
        return jsonify({"error": "Unauthorized — invalid or expired token"}), 401

    student_id    = student.get("student_id") or student.get("id")
    assessment_id = student.get("assessment_id") or student.get("hackathon_id", "default")

    # ── Step 2: Parse request body ───────────────────────────────────────────
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "Request body must be JSON"}), 400

    language     = body.get("language")
    student_code = body.get("student_code")
    test_cases   = body.get("test_cases")

    if not language:
        return jsonify({"error": "language is required"}), 400
    if not student_code:
        return jsonify({"error": "student_code is required"}), 400
    if not test_cases or not isinstance(test_cases, list):
        return jsonify({"error": "test_cases must be a non-empty array"}), 400

    # ── Step 3: Forward to Judge0 EC2 ────────────────────────────────────────
    webhook_url = f"{WEBHOOK_BASE_URL}/api/webhook/result"

    judge0_payload = {
        "student_id":    student_id,
        "assessment_id": assessment_id,
        "language":      language,
        "student_code":  student_code,
        "test_cases":    test_cases,
        "mode":          body.get("mode", "stdio"),
        "function_name": body.get("function_name", "solve"),
        "param_types":   body.get("param_types", []),
        "return_type":   body.get("return_type", "auto"),
        "per_tc_limit_s":  body.get("per_tc_limit_s", 2),
        "memory_limit_mb": body.get("memory_limit_mb", 256),
        "callback_url":  webhook_url,
    }

    try:
        resp = requests.post(
            f"{JUDGE0_EC2_URL}/submit",
            json=judge0_payload,
            timeout=10,
        )
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Grading service unreachable. Try again."}), 503
    except requests.exceptions.Timeout:
        return jsonify({"error": "Grading service timed out. Try again."}), 503

    if resp.status_code == 429:
        return jsonify({"error": "Grading queue full. Please retry in a moment."}), 429
    if resp.status_code not in (200, 202):
        return jsonify({"error": "Grading service error", "detail": resp.text}), 502

    data      = resp.json()
    ticket_id = data["ticket_id"]
    status    = data["status"]   # "queued" or "duplicate"

    return jsonify({
        "ticket_id":    ticket_id,
        "status":       status,
        "student_id":   student_id,
        "assessment_id": assessment_id,
    }), 202 if status == "queued" else 200


@app.post("/api/webhook/result")
def webhook_result():
    """
    Judge0 EC2 calls this endpoint when grading is complete.

    This is an INTERNAL endpoint — not called by students.
    Judge0 EC2 POSTs here automatically when a job finishes.

    Payload from Judge0 EC2:
    {
      "ticket_id": "uuid",
      "result": {
        "tc_results": [ { "tc_num": 1, "status": "PASS", "got": "5", "expected": "5" } ],
        "score": 2,
        "total": 2
      }
    }
    """
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "Invalid payload"}), 400

    ticket_id = body.get("ticket_id")
    result    = body.get("result")

    if not ticket_id or result is None:
        return jsonify({"error": "Missing ticket_id or result"}), 400

    # Store the result (replace with your DB write here)
    _results[ticket_id] = {
        "result":      result,
        "received_at": datetime.utcnow().isoformat(),
    }

    # ── Hook: notify student, update DB, trigger frontend push, etc. ─────────
    # Examples:
    #   db.submissions.update(ticket_id=ticket_id, result=result, status="done")
    #   send_websocket_push(ticket_id, result)
    #   send_email_result(ticket_id, result)
    # ─────────────────────────────────────────────────────────────────────────

    score = result.get("score", 0)
    total = result.get("total", 0)
    print(f"[webhook] ticket={ticket_id} score={score}/{total}")

    return jsonify({"ok": True}), 200


@app.get("/api/result/<ticket_id>")
def get_result(ticket_id: str):
    """
    Student polls for grading result.

    Response 200 (done):
    {
      "ticket_id": "uuid",
      "status": "done",
      "score": 2,
      "total": 2,
      "tc_results": [ ... ],
      "received_at": "2026-06-11T..."
    }

    Response 202 (still processing):
      { "ticket_id": "uuid", "status": "pending" }
    """
    entry = _results.get(ticket_id)
    if not entry:
        return jsonify({"ticket_id": ticket_id, "status": "pending"}), 202

    result = entry["result"]
    return jsonify({
        "ticket_id":   ticket_id,
        "status":      "done",
        "score":       result.get("score"),
        "total":       result.get("total"),
        "tc_results":  result.get("tc_results", []),
        "system_error": result.get("system_error"),
        "received_at": entry["received_at"],
    }), 200


@app.get("/api/health")
def health():
    """Health check — also verifies connectivity to Judge0 EC2."""
    try:
        resp = requests.get(f"{JUDGE0_EC2_URL}/health", timeout=3)
        judge0_ok = resp.status_code == 200
        judge0_detail = resp.json() if judge0_ok else resp.text
    except Exception as e:
        judge0_ok     = False
        judge0_detail = str(e)

    return jsonify({
        "status":  "ok" if judge0_ok else "degraded",
        "judge0":  {"ok": judge0_ok, "detail": judge0_detail},
        "webhook": f"{WEBHOOK_BASE_URL}/api/webhook/result",
    }), 200 if judge0_ok else 503


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print(f"Starting Edwisely Grading Bridge on port {port}")
    print(f"  Judge0 EC2:    {JUDGE0_EC2_URL}")
    print(f"  Auth URL:      {EDWISELY_AUTH_URL}")
    print(f"  Webhook URL:   {WEBHOOK_BASE_URL}/api/webhook/result")
    app.run(host="0.0.0.0", port=port, debug=False)
