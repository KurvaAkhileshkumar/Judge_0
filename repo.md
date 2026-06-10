I now have a complete picture of the entire codebase. Let me write the comprehensive audit.

---

# Judge0 Autograder — Complete Repository Audit

## 1. High-Level Architecture Overview

This is **not** the Judge0 open-source project itself. It is a **custom autograding platform layer** built *on top of* the Judge0 sandbox execution engine. The system accepts student code submissions, wraps them in safe harness code, executes them inside Judge0's isolate sandboxes, and returns structured per-test-case results.

**The system has three tiers:**

```
┌─────────────────────────────────────────────────────────────────┐
│  Tier 1: Client Layer                                           │
│  HTTP clients (students, LMS, load tester) call the Flask API   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ POST /submit  |  GET /results/stream
┌──────────────────────────▼──────────────────────────────────────┐
│  Tier 2: Orchestration Layer (our code)                         │
│  Flask API → Redis Queue → Python Workers → Reconciler          │
└──────────────────────────┬──────────────────────────────────────┘
                           │ POST /submissions (Judge0 API)
┌──────────────────────────▼──────────────────────────────────────┐
│  Tier 3: Sandbox Execution Layer (Judge0)                       │
│  Rails/Puma API → Resque workers → isolate (Linux sandbox)      │
│  PostgreSQL (Judge0 internal DB)                                 │
└─────────────────────────────────────────────────────────────────┘

Shared infrastructure:
  Redis — queue, results, pub/sub, idempotency
```

**Supported languages:** Python, C, C++, Java  
**Execution modes:** `function` (student writes a function, harness calls it) or `stdio` (student writes a full program, harness redirects stdin/stdout)

---

## 2. Repository Structure Breakdown

```
judge0_3/
├── ── PRODUCTION CODE (CORE SYSTEM) ──────────────────────────────
│
├── api.py                        Flask REST API (submit, stream results, health)
├── worker.py                     Sync worker (also provides shared helpers for async worker)
├── worker_async.py               Async worker (production — used by Docker)
├── autograder.py                 Main grading orchestrator
├── reconciler.py                 Crash recovery + deadline enforcement daemon
│
├── core/                         Core library package
│   ├── __init__.py               (empty)
│   ├── harness_builder.py        Harness code generator for all 4 languages
│   ├── judge0_client.py          Judge0 HTTP client + callback server + circuit breaker
│   ├── job_queue.py              Redis-backed priority queue
│   ├── output_parser.py          Parses delimited harness stdout
│   └── log.py                    Structured JSON logging (structlog)
│
├── security/                     Security analysis package
│   ├── __init__.py               (empty)
│   └── security.py               AST (Python) + regex (C/C++/Java) code checker
│
├── harnesses/                    Language harness templates
│   ├── python_harness.py         Python harness template (fork/poll parallel)
│   ├── c_harness.c               C harness template (fork/poll parallel)
│   ├── cpp_harness.cpp           C++ harness template (fork/poll parallel)
│   └── java_harness.java         Java harness template (thread parallel)
│
├── judge0_autograder/            Public package interface
│   ├── __init__.py               Exports Autograder, Submission, etc.
│   └── __pycache__/
│
├── ── INFRASTRUCTURE / DEPLOYMENT ────────────────────────────────
│
├── Dockerfile                    Python service image (api, grading_worker, reconciler)
├── docker-compose.yml            Local dev deployment (all services)
├── docker-compose.ec2.yml        EC2 deployment (base)
├── docker-compose.ec2.w3mr3.yml  EC2 variant: 3 workers × 3 runners
├── docker-compose.ec2.w4mr2.yml  EC2 variant: 4 workers × 2 runners
├── docker-compose.ec2.w4mr3.yml  EC2 variant: 4 workers × 3 runners
├── docker-compose.ec2.w5mr2.yml  EC2 variant: 5 workers × 2 runners
├── judge0.conf                   Judge0 config (local dev)
├── judge0.ec2.conf               Judge0 config (EC2, base)
├── judge0.ec2.mr3.conf           Judge0 config (EC2, 3 runners per worker)
├── .env                          Secrets (REDIS_PASSWORD, POSTGRES_PASSWORD, etc.)
├── .env.example                  Template for .env
├── .dockerignore                 Docker build exclusions
│
├── secrets/                      Docker secrets support files
│   ├── .gitignore                (prevents committing actual secrets)
│   ├── postgres_password.example
│   ├── redis_password.example
│   └── rails_secret_key.example
│
├── ── LOAD TESTING ────────────────────────────────────────────────
│
├── main.go                       Go load tester (direct Judge0 or full Flask stack)
├── go.mod                        Go module file (module: loadtest, go 1.22.2)
├── question_bank.json            4-TC stdio problem bank (default)
├── question_bank_30tc.json       30-TC version
├── question_bank_mixed.json      Mixed-problem bank
├── question_bank_mixed_30tc.json Mixed + 30 TCs
├── run_load_tests.sh             Shell script to run load test suites
├── collect_ec2_metrics.py        Live system metrics collector during load tests
├── generate_ec2_report.py        Excel report generator from JSON results
├── generate_cumulative_report.py Cumulative/comparison Excel report generator
│
├── ── LOAD TEST ARTIFACTS (data files, not code) ──────────────────
│
├── load_test_*.json              Load test configuration inputs
├── load_test_report_*.json       Historical load test output reports
├── results_ec2_*.json            EC2-specific test results
├── metrics.jsonl                 Live metrics from last collect run
├── *.log                         Load test run logs
├── load_test_1000u_mixed_30tc.xlsx  One Excel output artifact
├── Reports/                      Excel report outputs + JSON sources
│   ├── *.xlsx                    Excel reports (A–E variant naming)
│   └── Jsons/                    Source JSONs + metrics.jsonl per run
├── Reports_backup_20260528_033003/  Backup copy of Reports/ (2026-05-28)
│
├── ── REQUIREMENTS / PACKAGING ────────────────────────────────────
│
├── requirements-api.txt          Python deps: flask, redis, gunicorn, gevent, structlog, pydantic, requests
├── requirements-report.txt       Python deps (host-only): openpyxl, redis
│
├── ── MAINTENANCE SCRIPTS ─────────────────────────────────────────
│
├── cleanup.sh                    Post-load-test cleanup (truncate PG, Redis AOF, Docker logs)
├── deploy-ec2.sh                 EC2 deployment script
│
├── ── DOCUMENTATION / SPECS ───────────────────────────────────────
│
├── README.md                     Project documentation
├── IO_Type_Detection_Spec.docx   IO type detection specification
├── Judge0_Harness_Architecture.pptx  Architecture presentation
├── full_conversation.txt         Developer LLM conversation history
│
├── ── MISC ROOT FILES ─────────────────────────────────────────────
├── __init__.py                   Empty file at project root
├── .gitignore
└── .claude/                      Claude Code settings
```

---

## 3. End-to-End Execution Flow

### Submission Lifecycle (Happy Path)

```
1. CLIENT → POST /submit {student_id, language, student_code, test_cases, ...}
   │
   ▼
2. api.py: Pydantic validation → idempotency check (Redis judge0:idem:{hash})
           → capacity check (queue depth < MAX_QUEUE_DEPTH=5000)
           → generate UUID ticket_id
           → queue.enqueue(QueuedJob) — RPUSH to judge0:jobs:normal
           → SETEX judge0:idem:{hash} TTL=2h
           → SETEX judge0:pending_deadline:{ticket_id} TTL=2h
           → return 202 {ticket_id, status:"queued"}
   │
   ▼
3. CLIENT → GET /results/stream/{ticket_id}  (SSE stream opens)
   │         api.py subscribes to judge0:notify:{ticket_id} via Redis pub/sub
   │         heartbeats sent every ~1s to keep connection alive
   │
   ▼ (meanwhile, asynchronously)
4. worker_async.py: queue.dequeue() → BLPOP judge0:jobs:retry THEN judge0:jobs:normal
   │                → RPUSH to judge0:jobs:processing + SETEX judge0:inflight:{id} TTL=5min
   │
   ▼
5. autograder.py: HarnessBuilder → create session_id, delim
   │
   ▼
6. security/security.py: check(student_code, language, delim)
   │   Python: AST walk (blocked imports, builtins, dunders, infinite loops)
   │   C/C++/Java: regex (system(), fork(), sockets, inline asm, etc.)
   │   → if blocked: return GradingResult(security_error=...) → store to Redis
   │
   ▼
7. autograder.py: _detect_function_name() — function mode only
   │   → if no function found: return all ERROR results
   │
   ▼
8. sanitize_for_injection() — strip trailing whitespace / escape comment closers
   │
   ▼
9. HarnessBuilder.build() → generate complete source code:
   │   Python: template.format(...) with student code + test cases embedded
   │   C/C++:  template.format(...) with parallel fork/poll runner generated inline
   │   Java:   template.replace(...) with parallel thread runner generated inline
   │
   ▼
10. Judge0Client.submit_and_wait():
    │   POST /submissions?base64_encoded=true&wait=false
    │       {source_code: b64(harness), language_id, cpu_time_limit,
    │        memory_limit=4194304, number_of_processes=220, callback_url=...}
    │   → Returns token immediately (async)
    │   → Blocks on threading.Event waiting for Judge0 webhook
    │
    ▼ (Judge0 sandbox execution)
11. Judge0 server → Resque queue → isolate worker:
    │   compile/run harness code in isolate sandbox with cgroup limits
    │   harness forks N children (one per TC) simultaneously
    │   each child: runs TC, writes result JSON to pipe, exits
    │   parent: poll() collects results, prints delimited output
    │   harness stdout → Judge0 stores in PostgreSQL
    │   Judge0 → PUT {token, stdout, status, ...} to callback_url
    │
    ▼
12. CallbackServer._deliver(token, payload) → signals threading.Event
    │
    ▼
13. Judge0Client._parse_webhook_payload() → Judge0Result(stdout, status_id, ...)
    │
    ▼
14. autograder.py: parse_judge0_response():
    │   OutputParser extracts @@TC_RESULT__{session_id}__START_N...END_N blocks
    │   For OUTPUT status: compares got vs expected (string + float-tolerant)
    │   Assigns PASS/FAIL/TLE/MLE/SEGV/FPE/ERROR per TC
    │   Returns ParsedSubmission(tc_results, score, total, global_tle)
    │
    ▼
15. worker_async.py:
    │   queue.store_result(ticket_id, result_dict, idem_key)
    │     → SETEX judge0:result:{ticket_id} TTL=2h
    │     → DEL judge0:pending_deadline:{ticket_id}
    │     → PUBLISH judge0:notify:{ticket_id} {result_json}
    │   queue.ack(job) → Lua script: GET + LREM + DEL (atomic)
    │   Judge0Client.delete_submission(token)  ← keeps Judge0's PG table small
    │
    ▼
16. api.py SSE stream:
    │   Redis pub/sub fires → yield "event: result\ndata: {result_json}\n\n"
    │   → stream closes
    │
    ▼
17. CLIENT receives SSE event with {score, total, tc_results[...]}
```

### Failure / Retry Flow

```
Worker crashes mid-job:
  reconciler.py scans PROCESSING_QUEUE every 60s
  if INFLIGHT key expired (5 min TTL) AND no result → requeue to RETRY_QUEUE

Infrastructure failure (Judge0 5xx/timeout/OOM):
  autograder returns GradingResult(needs_requeue=True)
  worker_async: if retry_count < MAX_RETRY_COUNT(3):
    backoff = 5 * 3^attempt  (5s, 15s, 45s)
    asyncio.sleep(backoff) OUTSIDE semaphore (doesn't block other jobs)
    queue.requeue(job) → RPUSH judge0:jobs:retry
  else: store system_error result, flush Resque queue to break OOM cascade

Job never processed (e.g. workers all down for > 2h):
  Redis keyspace notification fires for judge0:pending_deadline:{ticket_id}
  reconciler._write_expired_result() → SETNX result + PUBLISH (nx=True, won't overwrite real result)
```

---

## 4. Folder-by-Folder Analysis

### `/` (root)
The project root contains all runnable Python entry points (`api.py`, `worker.py`, `worker_async.py`, `autograder.py`, `reconciler.py`), infrastructure config files, the Go load tester (`main.go`), and many load-test data artifacts. There is no formal Python package structure — everything is at root level or in one-deep subdirectories.

### `core/`
The core library. All five files are actively imported in production. This is the "engine" of the system: queue management, language-specific harness code generation, Judge0 HTTP communication, output parsing, and structured logging.

### `security/`
A single-module security package. `security.py` is always called before any submission reaches Judge0, making it part of the critical path. The `__init__.py` is empty (not re-exporting anything).

### `harnesses/`
Language harness templates. These are not Python modules — they are source-code template files read at runtime by `HarnessBuilder`. The Python one uses Python `.format()` placeholder syntax; the Java one uses `.replace()` due to ubiquitous `{}` in Java syntax; C/C++ use `.format()`. All are critical.

### `judge0_autograder/`
A package wrapper that re-exports all public types from the system. Its `__init__.py` serves as the "public API" for any downstream service that does `from judge0_autograder import Autograder, Submission`. The `__pycache__/` is auto-generated.

### `secrets/`
Contains `.gitignore` to prevent committing real secrets, plus `.example` files showing the expected format. The actual secret files are never committed. These support Docker secrets injection.

### `Reports/` and `Reports_backup_20260528_033003/`
Output artifacts from load testing. Excel `.xlsx` files and their source JSON + metrics JSONL files. Named by scenario variant (A–E) and load pattern (ramp/spike/pipe). The backup is a dated snapshot. Not part of the production system.

---

## 5. File-by-File Analysis

### `api.py` — Flask REST API
**What it does:** Exposes the student-facing HTTP interface. Three endpoints: `POST /submit`, `GET /results/stream/<ticket_id>`, `GET /health`.  
**Why it exists:** Entry point for all submissions. Handles idempotency, admission control, SSE delivery.  
**Critical path:** Yes. The first and last thing any client touches.  
**Depends on:** `core/job_queue.py`, `core/harness_builder.py` (for `TestCase`), `core/judge0_client.py` (for circuit breaker), `core/log.py`. External: `redis`, `flask`, `pydantic`, `gunicorn+gevent`.  
**Depended on by:** Nothing in the codebase (it's an entry point). Called by clients.

**Key behaviors:**
- Idempotency: SHA-256(student_id + ":" + assessment_id + ":" + SHA-256(source_code)) → 2h Redis TTL
- Admission control: if `queue.is_at_capacity(5000)` → 429
- SSE: uses `stream_with_context` + Redis pub/sub; checks for already-stored result to avoid race condition; sends heartbeat comments every ~1s; times out at 1800s with a system_error event
- `/health`: checks Redis ping, Judge0 `/system_info`, circuit breaker state; returns `"ok"/"degraded"/"error"`
- `HEALTH_TOKEN` guard uses `hmac.compare_digest` (timing-safe)
- Max payload: 1 MB (flask `MAX_CONTENT_LENGTH`)

---

### `worker.py` — Synchronous Worker + Shared Helpers
**What it does:** Two roles. (1) Can run as a standalone synchronous worker process (single-threaded, one job at a time). (2) Provides shared helper functions that `worker_async.py` imports.  
**Why it exists:** Was the original worker before the async version was written. The shared functions (`job_to_submission`, `result_to_dict`, `MAX_RETRY_COUNT`) were left here rather than moved to avoid circular dependencies.  
**Critical path:** The helpers are critical. The standalone worker loop is used for debugging/fallback.  
**Depends on:** `core/job_queue.py`, `core/judge0_client.py`, `core/harness_builder.py`, `core/log.py`, `autograder.py`.  
**Depended on by:** `worker_async.py` (`from worker import job_to_submission, result_to_dict, MAX_RETRY_COUNT`).

**Key behaviors:**
- `job_to_submission()`: reconstructs `Submission` dataclass from the Redis-stored payload dict
- `result_to_dict()`: serializes `GradingResult` to a JSON-safe dict for Redis storage
- `run_worker()`: blocking loop with SIGTERM/SIGINT handler; no concurrency; simple retry logic (requeue immediately, no backoff)

---

### `worker_async.py` — Async Concurrent Worker (Production)
**What it does:** The production worker. An asyncio event loop with up to `MAX_CONCURRENCY=48` concurrent grading coroutines. Each coroutine uses `asyncio.to_thread()` to offload blocking calls (queue/grader) to a `ThreadPoolExecutor`.  
**Why it exists:** The sync worker processes jobs serially. At 48 concurrent coroutines, this worker can keep 48 Judge0 sandboxes busy simultaneously without needing 48 OS threads doing blocking I/O.  
**Critical path:** Yes. This is the service Docker runs in the `grading_worker` container.  
**Depends on:** `worker.py` (for shared helpers), `core/job_queue.py`, `core/judge0_client.py`, `autograder.py`, `core/log.py`.  
**Depended on by:** Docker Compose `grading_worker` service CMD.

**Key behaviors:**
- `asyncio.Semaphore(MAX_CONCURRENCY)` caps live tasks
- Backoff sleep (5s/15s/45s) runs *outside* the semaphore — sleeping jobs don't hold concurrency slots
- `_flush_resque_queue()`: one-shot flush of Judge0's internal Resque queues when retries are exhausted; breaks OOM-kill cascade
- Graceful shutdown: SIGTERM/SIGINT stops the dequeue loop; `asyncio.gather()` awaits all in-flight tasks
- ThreadPoolExecutor: `MAX_CONCURRENCY + 4 = 52` threads

---

### `autograder.py` — Main Grading Orchestrator
**What it does:** The central coordinating class `Autograder` that ties together all the grading sub-systems. `grade(submission)` runs the full pipeline.  
**Why it exists:** Separates the orchestration concern from the queue concern (worker) and the execution concern (Judge0Client/HarnessBuilder).  
**Critical path:** Yes. Every job passes through here.  
**Depends on:** `core/harness_builder.py`, `core/output_parser.py`, `core/judge0_client.py`, `security/security.py`.  
**Depended on by:** `worker.py`, `worker_async.py`.

**Key behaviors:**
- `_detect_function_name()`: regex-based scan of student code to find what they actually named their function; handles mismatch between `function_name` in request and actual definition
- `_find_defined_functions()` + `_best_candidate()`: multi-candidate resolution (prefers name containing expected, then last-defined)
- `_is_infrastructure_failure()`: determines if all TCs errored due to OOM/fork limits (not student fault)
- Distinguishes three types of failures:
  - `security_error`: blocked before Judge0, returned immediately
  - `needs_requeue=True`: infra failure, worker should retry
  - Normal grading result: stored as final
- `GradingResult.needs_requeue` is the signal the worker uses for retry decisions
- Deletes Judge0 submission after successful grading (keeps PG table small)
- The `if __name__ == "__main__"` block is example usage, not executed in production

---

### `reconciler.py` — Crash Recovery Daemon
**What it does:** Background process running two recovery loops:
1. **Worker-crash scan** (every 60s): walks `PROCESSING_QUEUE`, requeues jobs whose `INFLIGHT` key has expired
2. **Deadline enforcement** (continuous): listens to Redis keyspace notifications for expired `PENDING_DEADLINE_*` keys; writes `system_error` result for any submission that never got graded within 2 hours  
**Why it exists:** "At-least-once delivery" guarantee. Without it, a crashed worker leaves jobs orphaned forever. Without deadline enforcement, students would see an infinite spinner.  
**Critical path:** Yes for reliability. Not on the per-request hot path.  
**Depends on:** `core/job_queue.py`, `core/log.py`, `redis`.  
**Depended on by:** Docker Compose `reconciler` service CMD.

**Key behaviors:**
- Uses `nx=True` (SET if not exists) when writing expired results — prevents overwriting a real result that finished concurrently
- Requeues crashed jobs with `retry_count += 1` (respects existing retry budget)
- Requires `CONFIG SET notify-keyspace-events Ex` on Redis (enables expired key events)
- 100ms poll loop: most of the time spent idle, wakes on pub/sub messages
- Single instance — all state is in Redis, no sharding needed

---

### `core/job_queue.py` — Redis Priority Queue
**What it does:** Implements all Redis operations for the job queue. Two-priority queue (retry > normal), in-flight tracking, result storage, SSE notification, idempotency cleanup.  
**Why it exists:** Centralizes all Redis key names, TTLs, and queue semantics in one place.  
**Critical path:** Yes. All job lifecycle transitions go through this.  
**Depends on:** `redis` library only.  
**Depended on by:** `api.py`, `worker.py`, `worker_async.py`, `reconciler.py`.

**Key behaviors:**
- Priority: BLPOP checks `judge0:jobs:retry` *before* `judge0:jobs:normal` — retry jobs always served first
- Atomicity: `ack()` uses a Lua script (GET + LREM + DEL in one round-trip) to prevent TOCTOU race with reconciler
- `store_result()`: pipeline of SETEX + DEL (pending deadline) + optional DEL (idem key on system_error) + PUBLISH (SSE notification)
- `is_at_capacity()`: combines retry + normal queue depths for admission control
- Redis keys defined as module-level constants — single source of truth for all consumers

**Redis key layout:**
| Key                                   | Purpose                  | TTL          |
| ------------------------------------- | ------------------------ | ------------ |
| `judge0:jobs:normal`                  | New jobs FIFO list       | None         |
| `judge0:jobs:retry`                   | Retry jobs FIFO list     | None         |
| `judge0:jobs:processing`              | In-flight jobs list      | None         |
| `judge0:inflight:{ticket_id}`         | Crash-detection sentinel | 300s (5 min) |
| `judge0:result:{ticket_id}`           | Final grading result     | 7200s (2h)   |
| `judge0:notify:{ticket_id}`           | Pub/sub channel (SSE)    | Ephemeral    |
| `judge0:idem:{hash}`                  | Idempotency key          | 7200s (2h)   |
| `judge0:pending_deadline:{ticket_id}` | Deadline sentinel        | 7200s (2h)   |

---

### `core/harness_builder.py` — Language Harness Code Generator
**What it does:** Generates complete, compilable source code for each language that wraps student code. The harness runs all test cases in parallel (fork/poll for Python/C/C++, threads for Java), writes delimited JSON output per TC, then prints `DONE`.  
**Why it exists:** The harness is the key innovation. Instead of submitting N separate jobs to Judge0 (one per TC), the harness submits a single job that runs all TCs in parallel inside one sandbox. This reduces Judge0 Puma load by ~100×.  
**Critical path:** Yes. Called for every submission.  
**Depends on:** `harnesses/` directory (reads template files at runtime), `json`, `uuid`, `pathlib`.  
**Depended on by:** `autograder.py`.

**Key behaviors:**
- `session_id` = 12-char UUID hex — unique per grading call, used in delimiter
- `delim` = `@@TC_RESULT__{session_id}__` — collision-resistant output delimiter
- `MAX_PARALLEL_TCS = 200` — batching prevents `O(N)` peak concurrent processes
- Python: single `.format()` call with escaped student code; sentinel `\x00STUDENT_SOURCE_RAW\x00` avoids double-escaping of raw student code for `_STUDENT_SOURCE`
- C/C++: `_build_c_parallel_runner()` / `_build_cpp_parallel_runner()` generate inline batch loops with `fork()` + `poll()` — not a template substitution but programmatic C code generation
- Java: `_build_java_parallel_runner()` generates inline Java code using `AtomicReference<TCResult>[]` + `Thread[]`; `freshStudentClass()` creates a new ClassLoader per TC to reset static state
- Fix-tagged comments throughout explain historical bug fixes (Fix 1.x through Fix 4.x, FIX-4 through FIX-15)

---

### `core/judge0_client.py` — Judge0 HTTP Client
**What it does:** HTTP client for submitting code to Judge0 and receiving results via webhook callback. Includes a circuit breaker, embedded HTTP server for receiving callbacks, exponential backoff on 5xx errors, and a poll fallback if the callback never arrives.  
**Why it exists:** Centralizes all Judge0 communication in one place; the callback mechanism eliminates polling (from ~160 Puma calls/job to 2).  
**Critical path:** Yes. The bridge between our system and Judge0.  
**Depends on:** `requests`, `http.server`, `threading`, `concurrent.futures`.  
**Depended on by:** `autograder.py`, `api.py` (imports `_judge0_breaker` for health check).

**Key behaviors:**
- `CallbackServer`: embedded `HTTPServer` with a `ThreadPoolExecutor(32)` — handles up to 32 concurrent webhooks without spawning unbounded threads
- Race condition safety: `register()` checks the buffer before blocking; early arrivals are stored and signal immediately
- `_post_with_retry()`: 3 attempts, backoff 1s → 4s on 5xx; 4xx errors not retried (client fault)
- `_CircuitBreaker`: opens after 10 consecutive failures, stays open 30s; module-level singleton shared across all `Judge0Client` instances
- Global time limit formula: `ceil(N/200) * per_tc_limit + 5s overhead` (vs old `N * per_tc + overhead`)
- Memory limit sent to Judge0: 4GB RLIMIT_AS (for Rosetta 2 / Mac Docker Desktop compatibility)
- `delete_submission()`: best-effort DELETE after successful grading
- `_poll_fallback()`: if callback never fires after timeout, polls GET for 60s before giving up

**LANGUAGE_IDS mapping:**
| Language | Judge0 ID |
| -------- | --------- |
| python   | 71        |
| c        | 50        |
| cpp      | 54        |
| java     | 62        |

---

### `core/output_parser.py` — Harness Output Parser
**What it does:** Parses the structured output from a harness run. Extracts individual TC blocks using the session-scoped delimiter, compares `got` vs `expected`, assigns final verdict statuses, handles missing/crashed TCs, and detects global TLE.  
**Why it exists:** Comparison is done *outside* the sandbox (Fix 4.1) to prevent students from forging PASS verdicts by writing fake delimiter blocks to their output.  
**Critical path:** Yes. Every completed grading job passes through here.  
**Depends on:** `re`, `json` (stdlib only).  
**Depended on by:** `autograder.py`.

**Key behaviors:**
- `_HARNESS_STATUSES = {"TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT"}` — harness can NEVER legitimately emit PASS/FAIL; those are assigned only here
- `OUTPUT` status from harness → comparison done here → assigned `PASS` or `FAIL`
- Float-tolerant comparison: relative tolerance 1e-6, absolute 1e-9; NaN never equals itself
- Missing TC (not in output): if `global_tle` → status=`TLE`; else → status=`MISSING`
- JSON parse failure in TC block: scans for harness statuses in raw text (never assigns PASS/FAIL from raw text — prevents verdict forgery)
- `global_tle = DONE marker not in stdout` — if harness was killed mid-run, no DONE is printed

---

### `core/log.py` — Structured Logging
**What it does:** Configures `structlog` for JSON output. One-time initialization, re-entrant safe.  
**Why it exists:** JSON log lines are machine-parseable by log aggregators (CloudWatch, Datadog, etc.); structured fields make log querying easy.  
**Critical path:** Supporting (used by every production component).  
**Depends on:** `structlog`, `logging` (stdlib).  
**Depended on by:** `api.py`, `worker.py`, `worker_async.py`, `autograder.py`, `reconciler.py`.

---

### `security/security.py` — Code Security Checker
**What it does:** Static analysis of student code before it reaches Judge0. Two strategies: Python AST walking (catches aliased imports, `__import__`, dunder access, infinite loops), regex patterns for C/C++/Java (catches `system()`, `fork()`, socket headers, inline assembly, `syscall()`).  
**Why it exists:** Defense in depth. Judge0's isolate sandbox is the real security boundary, but blocking obvious violations early reduces sandbox load and gives better error messages to students.  
**Critical path:** Yes. Called for every submission before harness building.  
**Depends on:** `ast`, `re` (stdlib only).  
**Depended on by:** `autograder.py`.

**Blocked Python modules:** `os`, `subprocess`, `socket`, `ctypes`, `signal`, `importlib`, `multiprocessing`, `threading`, `pty`, `tty`, `termios`, `fcntl`, `select`, `mmap`, `resource`, `pwd`, `grp`, `syslog`, `posix`, `posixpath`, `nt`, `io`, `pathlib`, `sys`, `builtins`

**Special cases:**
- `InfiniteLoop` → returns TLE for all TCs without hitting Judge0 (saves time)
- `SyntaxError` → returns ERROR for all TCs (not a "blocked" security violation)
- Delimiter injection → blocked immediately
- `sanitize_for_injection()` (separate function): strips trailing whitespace (Python) or escapes `*/` (C/C++/Java) before template injection

---

### `harnesses/python_harness.py` — Python Harness Template
**What it does:** Python source code template that, when filled in, creates a self-contained grading program. It forks N children (one per TC), each running the student's code with a SIGALRM timer. The parent uses `select.poll()` to collect results.  
**Why it exists:** A template is easier to maintain than generating Python code as strings from Python code. The blank fields (`{session_id}`, `{student_code}`, etc.) are filled by `HarnessBuilder._build_python()`.  
**Critical path:** Yes. Template is read from disk at runtime.  
**Depended on by:** `core/harness_builder.py`.

**Security measures inside harness:**
- `builtins.open` monkey-patched to `_safe_open` (blocks FD access, blocks reading harness file)
- `signal.signal` monkey-patched to `_safe_signal` (blocks SIGALRM override)
- `sys.exit`, `builtins.exit`, `builtins.quit` → raise `SystemExit("__HARNESS_BLOCKED__")`
- Child processes strip dangerous modules from `sys.modules` after fork
- Stdio mode: `exec()` uses explicit `__builtins__` whitelist (Fix 1.6)
- `_can_fork_n()`: scans `/proc` for same-UID processes to check RLIMIT_NPROC

---

### `harnesses/c_harness.c` — C Harness Template
**What it does:** C source code template. Uses `fork()` + `poll()` for parallel TC execution. Each child sets its own `alarm()`, runs the student function, writes a `TCResult` struct to a pipe, and calls `_exit(0)`.  
**Why it exists:** Same rationale as Python harness — one parallel execution, one Judge0 job.  
**Depended on by:** `core/harness_builder.py`.

**Security measures:** Child process closes all FDs > 2 except the result pipe; `setrlimit(RLIMIT_NPROC, 1)` blocks further forks; `setrlimit(RLIMIT_AS, mem_limit)` enforces memory limit; TLE handler writes result and calls `_exit()`; `json_escape()` prevents malformed JSON from raw output; PASS/FAIL from harness rejected by `OutputParser`.

---

### `harnesses/cpp_harness.cpp` — C++ Harness Template
**What it does:** C++ version of the C harness. Nearly identical structure. Adds: `std::bad_alloc` caught as MLE, `std::exception` caught for error reporting, `std::ostringstream` for `auto` return type capture, `std::cout` and `printf` both captured via fd dup.  
**Depended on by:** `core/harness_builder.py`.

---

### `harnesses/java_harness.java` — Java Harness Template
**What it does:** Java version using threads instead of forks (Java has no `fork()`). Thread-local `PrintStream` (TL_OUT) and `InputStream` (TL_IN) dispatch to per-thread capture streams — prevents cross-thread stdout/stdin contamination. Uses reflection to call student methods. Uses `freshStudentClass()` with a per-TC ClassLoader to reset static state between TCs.  
**Depended on by:** `core/harness_builder.py`.

**Security measures:** `SecurityManager` blocks `System.exit()` (Java 8–17; Java 21+ daemon threads abandoned when main returns); `killThread()` uses `interrupt()` then `stop()` (deprecated but safe in throwaway JVM context); student code wrapped as `static class Student { ... }`.

---

### `docker-compose.yml` — Local Development Deployment
**What it does:** Defines all 7 services for local dev: `cgroupv1-init`, `server` (Judge0), `workers` (Judge0 sandboxes), `api` (Flask), `grading_worker` (Python async), `reconciler`, `db` (PostgreSQL), `redis`.  
**Why it exists:** One-command local environment: `docker compose up`.  
**Critical path:** Infrastructure definition for the entire system.

**Key design decisions visible in this file:**
- `cgroupv1-init` runs privileged to mount cgroup v1 controllers (Judge0's isolate needs them)
- `server`: `RAILS_MAX_THREADS=25, WEB_CONCURRENCY=8` → 200 total Puma HTTP slots → supports 100 concurrent grading jobs in callback mode
- `workers`: `mem_limit=8g`, `ulimits.nproc=65536, nofile=65536`; scale with `--scale workers=N`
- `grading_worker`: `WORKER_CONCURRENCY=48`, `CALLBACK_PORT=0` (OS-assigned); scale with `--scale grading_worker=N`
- `redis`: `maxmemory 4gb, allkeys-lru, appendonly yes`; conditional `--requirepass` to avoid Redis 6 empty-password bug
- `db`: `max_connections=500, shared_buffers=512MB`
- Secrets block commented out — shows how to upgrade from env vars to Docker secrets

---

### `judge0.conf` — Judge0 Configuration
**What it does:** Configures the Judge0 Rails application. Database credentials, Redis credentials, performance limits.  
**Why it exists:** Judge0 reads this file at startup via `env_file:` in Docker Compose.

**Critical settings:**
- `MAX_RUNNERS=2` — max simultaneous sandboxes per `workers` replica
- `MAX_PROCESSES_AND_OR_THREADS=220` — allows harness to fork 200 children + overhead (default was 60)
- `MAX_MAX_PROCESSES_AND_OR_THREADS=500` — ceiling for per-submission override
- `OPEN_FILES_LIMIT=65536` — matches container ulimit for harness fork/pipe usage
- `SOURCE_CODE_SIZE_LIMIT=524288` — 512 KB (1000-TC harness easily exceeds default 64 KB)
- `MAX_MEMORY_LIMIT=4194304` — 4 GB (needed for Rosetta 2 JIT on Mac)
- `JUDGE0_TELEMETRY_ENABLE=false` — disables Judge0 telemetry

---

### `Dockerfile` — Python Service Image
**What it does:** Single Docker image used by `api`, `grading_worker`, and `reconciler` services. Based on `python:3.12-slim`. Default CMD is gunicorn for the API; Docker Compose overrides CMD for workers.  
**Why it exists:** All three Python services share the same codebase and dependencies.

**Key choices:**
- `UID 1001` non-root user
- `gunicorn` with `gevent` worker class — SSE streams handled as greenlets (not OS threads)
- `--worker-connections=1000` — each gunicorn process handles 1000 concurrent greenlets
- `--timeout=1900` — longer than `SSE_TIMEOUT_S=1800` so gunicorn never kills a live stream

---

### `main.go` — Load Testing Tool
**What it does:** Concurrent load tester written in Go. Two modes: (1) **direct mode** — builds Python harnesses and submits directly to Judge0; (2) **Flask stack mode** — submits student code to the Flask API and reads results via SSE. Generates JSON reports and optionally kicks off the Excel report generator.  
**Why it exists:** Go's goroutine model makes it trivial to run 1000 concurrent virtual users. Python's GIL makes this hard.  
**Not in production path.** Only used for load testing and performance characterization.  
**Depends on:** Question bank JSON files, optionally `generate_ec2_report.py` and `collect_ec2_metrics.py`.

**Notable internals:** Contains a full standalone Python harness template (as a Go string constant) that is simpler than the production Python harness — used only in direct mode. Has its own `CallbackServer`, `Judge0Client`, `FlaskClient`, output parser, and metrics collection.

---

### `collect_ec2_metrics.py` — Live Metrics Collector
**What it does:** Background metrics collector that reads system state from Linux `/proc` filesystem (no psutil). Captures CPU %, RAM, disk I/O, network I/O, TCP connections, Redis queue depth, Docker container stats. Writes one JSON object per line to a `.jsonl` file.  
**Why it exists:** Provides the time-series data for Sheet 2 of the Excel load test report. Designed to run alongside a load test.  
**Not in production path.** Dev/ops tooling only.  
**Depends on:** `redis` (optional), `subprocess` (for `docker stats`), `/proc` filesystem.  
**Depended on by:** `main.go` spawns it as a subprocess when `--auto-metrics` flag is set; `generate_ec2_report.py` reads its output.

---

### `generate_ec2_report.py` — Excel Report Generator
**What it does:** Generates a multi-sheet Excel workbook from the load test JSON output. 4 sheets: system info, load test metrics (with live metrics if provided), per-user TC results, scenario comparison.  
**Why it exists:** Makes load test results human-readable for non-engineers; enables Excel-based analysis.  
**Not in production path.** Dev/ops tooling only.  
**Depends on:** `openpyxl`, JSON load test reports, optionally metrics JSONL.  
**Depended on by:** `main.go` (spawned as subprocess after each test run).

---

### `generate_cumulative_report.py` — Cumulative Report Generator
**What it does:** Generates a cumulative Excel report aggregating multiple load test runs for comparison.  
**Why it exists:** Track performance trends across multiple test sessions.  
**Not in production path.** Dev/ops tooling only.

---

### `reconciler.py` → (covered above)

### `requirements-api.txt`
`flask>=3.0`, `redis>=5.0`, `gunicorn>=22.0`, `gevent>=24.0`, `requests>=2.28`, `structlog>=24.0`, `pydantic>=2.0`, `setuptools>=68`. Installed in the Docker image.

### `requirements-report.txt`
`openpyxl>=3.1`, `redis>=5.0`. Installed on the host machine only (not in Docker).

### `.env` / `.env.example`
Contains real secrets (committed in `.env`, which is unusual — presumably only safe because this repo is private). `.env.example` is the template. Docker Compose substitutes `${REDIS_PASSWORD}`, `${POSTGRES_PASSWORD}`, `${RAILS_SECRET_KEY_BASE}` from `.env`.

### `cleanup.sh`
Post-load-test maintenance script. 5 steps: truncate PG submissions table, VACUUM, Redis AOF rewrite, truncate Docker container logs, optional full Docker prune. Called manually after load tests to reclaim disk space.

### `deploy-ec2.sh`
EC2 deployment script (not read above, but inferred from name and EC2-specific docker-compose variants).

### `run_load_tests.sh`
Shell script that runs multiple load test scenarios sequentially. Produces the `.json` and `.log` files that fill the root directory.

### `judge0_autograder/__init__.py`
Re-exports all public types for use as a library: `Autograder`, `Submission`, `GradingResult`, `Judge0Config`, `Judge0Result`, `CallbackServer`, `TestCase`, `HarnessConfig`, `ParsedSubmission`, `TCResult`.

### Root `__init__.py`
Empty file. Makes the root directory importable as a Python package, though nothing currently imports from it. Effectively a no-op.

### `go.mod`
`module loadtest`, `go 1.22.2`. Minimal Go module file for `main.go`.

---

## 6. Dependency Relationships

```
                        ┌─────────────────┐
                        │   api.py        │
                        └────────┬────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                   ▼
    core/job_queue.py    core/harness_builder.py  core/judge0_client.py
                                                  (circuit breaker only)
              │
              └──────── (all workers, reconciler import this)

worker.py ──────────────────────────────────────────────────────►
    │  imports: core/job_queue, core/judge0_client,              │
    │           core/harness_builder, autograder, core/log       │
    │                                                            │
worker_async.py ─── imports worker.py (helpers) ────────────────┘

autograder.py
    imports: core/harness_builder
             core/output_parser
             core/judge0_client
             security/security

core/harness_builder.py
    reads files: harnesses/python_harness.py
                 harnesses/c_harness.c
                 harnesses/cpp_harness.cpp
                 harnesses/java_harness.java

reconciler.py
    imports: core/job_queue, core/log

security/security.py
    imports: ast, re (stdlib only — no internal deps)

core/output_parser.py
    imports: re, json (stdlib only)

core/log.py
    imports: structlog, logging

judge0_autograder/__init__.py
    re-exports from: autograder, core/judge0_client, core/harness_builder,
                     core/output_parser

main.go (load tester)
    subprocess: collect_ec2_metrics.py, generate_ec2_report.py

generate_ec2_report.py
    reads: *.json (load test results), *.jsonl (metrics)

collect_ec2_metrics.py
    reads: /proc/*, Redis queue
```

---

## 7. Critical Files and Why They Matter

| File                           | Why Critical                                                            |
| ------------------------------ | ----------------------------------------------------------------------- |
| `api.py`                       | Only entry point for submissions; idempotency gate; SSE delivery        |
| `worker_async.py`              | The production worker — without it, jobs sit in Redis forever           |
| `autograder.py`                | Orchestrates the entire grading pipeline for every job                  |
| `core/harness_builder.py`      | Generates the code that actually runs student submissions               |
| `core/judge0_client.py`        | The only bridge to the sandbox execution engine                         |
| `core/job_queue.py`            | All Redis operations; wrong behavior here = lost jobs                   |
| `core/output_parser.py`        | All grading verdicts are assigned here; bugs = wrong scores             |
| `security/security.py`         | All security checks happen here; bypass = sandbox escape                |
| `harnesses/*.py/.c/.cpp/.java` | The harness templates; missing or corrupt = all submissions fail        |
| `reconciler.py`                | Without it: crashed-worker jobs never get graded; students wait forever |
| `docker-compose.yml`           | The deployment blueprint for all services                               |
| `judge0.conf`                  | Misconfigured = Judge0 won't allow harness parallelism                  |
| `Dockerfile`                   | The image all Python services run in                                    |

---

## 8. Supporting Files and Their Purpose

| File                            | Purpose                                                                                   |
| ------------------------------- | ----------------------------------------------------------------------------------------- |
| `worker.py`                     | Shared helpers for `worker_async.py`; also usable as standalone sync worker for debugging |
| `core/log.py`                   | Structured JSON logging for all services                                                  |
| `judge0_autograder/__init__.py` | Public library interface for external consumers                                           |
| `.env` / `.env.example`         | Secrets management for local dev                                                          |
| `secrets/`                      | Docker secrets support (production hardening path)                                        |
| `requirements-api.txt`          | Python dependency pinning for the Docker image                                            |

---

## 9. Potentially Unused, Legacy, or Redundant Files

### Definitely Data/Artifacts (not code, not needed in production):
- `load_test_*.json`, `load_test_report_*.json`, `results_ec2_*.json` — historical test run outputs
- `run_load_tests_*.log` — historical log files
- `load_test_1000u_mixed_30tc.xlsx` — stray Excel file in root (should be in `Reports/`)
- `metrics.jsonl` — live metrics from last run
- `question_bank.json`, `question_bank_*.json` — load test problem banks (not used in production)
- `session.log` — log file
- `Reports/` and `Reports_backup_20260528_033003/` — report artifacts, not code
- `full_conversation.txt` — developer LLM conversation log, not code

### Documents/Specs (reference material):
- `IO_Type_Detection_Spec.docx` — design spec
- `Judge0_Harness_Architecture.pptx` — architecture presentation

### Possibly Redundant Docker Compose Files:
- `docker-compose.ec2.w3mr3.yml`, `docker-compose.ec2.w4mr2.yml`, `docker-compose.ec2.w4mr3.yml`, `docker-compose.ec2.w5mr2.yml` — these are scaling variants (`wNmrM` = N workers × M max runners). Evidence they're used: named explicitly in `run_load_tests.sh`. All are valid — they're configuration variants, not duplicates.

**Evidence of use for `worker.py` as a runnable process:** The Docker Compose file only shows `python worker_async.py` as the CMD. The synchronous `worker.py` loop is never invoked by Docker. However, `worker.py` is still critical as a module (its functions are imported). If you wanted to run a synchronous worker for debugging, you'd `python worker.py` manually.

### Root `__init__.py`:
Empty file. No file in the codebase imports from the root package. **Likely unused as a package init.** Could be a leftover from an earlier structure when the root was meant to be a Python package. Safe to delete, but harmless to keep.

### EC2 config variants (`judge0.ec2.conf`, `judge0.ec2.mr3.conf`):
These are alternative `judge0.conf` files for EC2 deployments with different runner counts. They're valid and used when deploying via the EC2-specific compose files.

---

## 10. Questions and Uncertainties Found During Analysis

1. **`worker.py` `run_worker()` vs `worker_async.py`**: The sync worker exists and is maintained, but the Docker Compose only uses the async worker. When (if ever) is the sync worker used in production? Could be useful as a simpler debug fallback.

2. **`judge0_autograder/__init__.py` consumers**: This package init re-exports all types. What external system imports from it? It exists as a library interface, but there's no calling code visible in this repo. Is there a separate platform (LMS, API gateway) that does `from judge0_autograder import Autograder`?

3. **`collect_ec2_metrics.py` is listed in requirements-report.txt dependencies but requires `/proc` filesystem** — it can only run on Linux. On Mac it would partially work (no `/proc`). The Docker stats portion works on Mac. This is fine as it's a dev tool, but worth noting.

4. **The `.env` file is committed** (appears in git). This contains actual password values. While likely intentional for a private dev/test repo, it's a security risk if ever made public. The `.gitignore` doesn't exclude `.env`.

5. **`main.go` contains a duplicate Python harness template** (the `pythonHarnessTemplate` Go string constant). This is a simplified version of `harnesses/python_harness.py` used only in direct Judge0 mode load testing. These two implementations can diverge. The load tester's harness still does PASS/FAIL comparison *inside* the sandbox (old behavior), while the production harness uses OUTPUT + external comparison (Fix 4.1). This means load test results in direct mode don't exactly match production behavior.

6. **Java harness uses `Thread.stop()`** which is deprecated since Java 1.2 and throws `UnsupportedOperationException` in Java 21+. The harness explicitly catches this. What version of Java does Judge0 1.13.0 run? If it's Java 21, thread stop is silently ignored, and a thread that ignores `interrupt()` will run until the JVM exits (which it will, since the threads are daemon threads). The comment in the code acknowledges this.

7. **Circuit breaker module-level singleton `_judge0_breaker`**: all `Judge0Client` instances share one circuit breaker. In the async worker with 48 concurrent jobs, if one batch of failures trips the breaker, *all* concurrent jobs immediately fail fast. This is correct for rate-limiting behavior but means the circuit breaker state is global across all concurrent grading coroutines.

8. **`generate_cumulative_report.py` vs `generate_ec2_report.py --compare`**: Both produce comparison views of multiple load test runs. The `--compare` flag in `generate_ec2_report.py` generates Sheet 4 for comparison. `generate_cumulative_report.py` appears to serve a similar purpose. It's unclear if both are maintained to the same standard or if one supersedes the other.

9. **`full_conversation.txt`** — this file appears to be an LLM conversation export. At potentially large size it could slow `git status` or `docker build`. It should probably be excluded via `.gitignore` or `.dockerignore`.

10. **`deploy-ec2.sh`** was not read but inferred to handle EC2-specific deployment (copying files, setting up systemd services, etc.). Its contents would clarify the production deployment story.

---

## 11. Recommended Reading Order for New Developers

Follow this order to build a mental model from the data layer up to the API surface:

### Phase 1: Understand the Queue (30 min)
1. `core/job_queue.py` — understand all Redis keys, TTLs, and operations. This is the "nervous system."
2. `docker-compose.yml` — understand what services run and how they connect.
3. `judge0.conf` — understand what parameters control Judge0 behavior.

### Phase 2: Understand the Execution Engine (45 min)
4. `harnesses/python_harness.py` — read the template as if it were student-facing code. Understand fork/poll/pipe mechanism.
5. `core/harness_builder.py` — understand how the template gets filled in. Focus on `_build_python()` first, then `_build_c_parallel_runner()`.
6. `core/judge0_client.py` — understand submission, callback server, circuit breaker.
7. `core/output_parser.py` — understand delimiter protocol and verdict assignment.

### Phase 3: Understand the Grading Pipeline (30 min)
8. `security/security.py` — understand what's blocked and why.
9. `autograder.py` — read `Autograder.grade()` top-to-bottom. This is the main pipeline.

### Phase 4: Understand the Queue Workers (20 min)
10. `worker.py` — read `run_worker()` and the helpers (`job_to_submission`, `result_to_dict`).
11. `worker_async.py` — read `process_job()` and `run_worker_async()`. Understand semaphore + backoff.
12. `reconciler.py` — understand both recovery mechanisms.

### Phase 5: Understand the API (20 min)
13. `api.py` — read `submit()`, `results_stream()`, `health()` in order.

### Phase 6: Understand the Load Testing Infrastructure (optional)
14. `main.go` — focus on `runUser()`, `runUserFlask()`, and `main()`.
15. `collect_ec2_metrics.py` — skim the metric collection logic.
16. `generate_ec2_report.py` — skim the sheet generation for context.

### Phase 7: The Other Harnesses (optional, after Python is understood)
17. `harnesses/c_harness.c` — compare structure with Python. Fork/poll is identical in concept.
18. `harnesses/java_harness.java` — understand thread-local dispatch and ClassLoader tricks.

---

## Summary Table: File Criticality at a Glance

| File                               | Category                       | Criticality       |
| ---------------------------------- | ------------------------------ | ----------------- |
| `api.py`                           | Entry point                    | **Critical**      |
| `worker_async.py`                  | Worker                         | **Critical**      |
| `autograder.py`                    | Orchestrator                   | **Critical**      |
| `reconciler.py`                    | Reliability daemon             | **Critical**      |
| `core/job_queue.py`                | Queue                          | **Critical**      |
| `core/harness_builder.py`          | Code generation                | **Critical**      |
| `core/judge0_client.py`            | Sandbox client                 | **Critical**      |
| `core/output_parser.py`            | Verdict assignment             | **Critical**      |
| `security/security.py`             | Security                       | **Critical**      |
| `harnesses/*.py/.c/.cpp/.java`     | Templates                      | **Critical**      |
| `docker-compose.yml`               | Infrastructure                 | **Critical**      |
| `judge0.conf`                      | Configuration                  | **Critical**      |
| `Dockerfile`                       | Container                      | **Critical**      |
| `worker.py`                        | Shared helpers + sync fallback | **Supporting**    |
| `core/log.py`                      | Logging                        | **Supporting**    |
| `judge0_autograder/__init__.py`    | Library interface              | **Supporting**    |
| `requirements-api.txt`             | Dependencies                   | **Supporting**    |
| `main.go`                          | Load testing                   | **Dev/Ops tool**  |
| `collect_ec2_metrics.py`           | Metrics collection             | **Dev/Ops tool**  |
| `generate_ec2_report.py`           | Report generation              | **Dev/Ops tool**  |
| `generate_cumulative_report.py`    | Report generation              | **Dev/Ops tool**  |
| `cleanup.sh`                       | Maintenance                    | **Dev/Ops tool**  |
| `deploy-ec2.sh`                    | Deployment                     | **Dev/Ops tool**  |
| `run_load_tests.sh`                | Load test runner               | **Dev/Ops tool**  |
| `docker-compose.ec2*.yml`          | EC2 configs                    | **Dev/Ops infra** |
| `judge0.ec2*.conf`                 | EC2 Judge0 configs             | **Dev/Ops infra** |
| `*.json` (test data, results)      | Artifacts                      | **Data**          |
| `*.log`, `*.jsonl`, `*.xlsx`       | Artifacts                      | **Data**          |
| `Reports/`, `Reports_backup_*/`    | Artifacts                      | **Data**          |
| Root `__init__.py`                 | Empty                          | **Unused**        |
| `full_conversation.txt`            | Dev notes                      | **Not code**      |
| `IO_Type_Detection_Spec.docx`      | Design spec                    | **Reference**     |
| `Judge0_Harness_Architecture.pptx` | Architecture                   | **Reference**     |
| `secrets/*.example`                | Templates                      | **Reference**     |
| `.env.example`                     | Secret template                | **Reference**     |