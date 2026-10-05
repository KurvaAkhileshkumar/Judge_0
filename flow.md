# Judge0 Autograder — System Flow

> Code-verified walkthrough of the entire grading platform, from HTTP request to
> result delivery. Every Redis key, status token, env var, and delimiter below is
> read straight from the source on the EC2 deployment.

---

## 1. The core idea

Instead of sending Judge0 one job **per test case** (N jobs), the system wraps the
student's code + **all N test cases** into a **single self-contained program** (the
"harness"), submits it as **one** Judge0 job, runs every test case **in parallel**
inside that one sandbox, and parses the N results back out of one stdout blob.

```
 Student code  +  N test cases
        │
        ▼
 HarnessBuilder  ──►  ONE runnable program (student code + parallel TC runner + inline test data)
        │
        ▼
 Judge0 sandbox  ──►  one stdout blob with N delimited TC results
        │
        ▼
 OutputParser  ──►  {score, total, tc_results[...]}
        │
        ▼
 Redis result + pub/sub  ──►  SSE stream / webhook / poll
```

This cuts Judge0 API traffic ~100× and makes wall-clock ≈ `max(tc_time)` instead of
`sum(tc_time)`, because test cases run concurrently (fork/threads/workers).

Time model: **global limit = per_tc_limit_s + overhead**, not `N × per_tc_limit_s`.

---

## 2. Service topology (`docker-compose.ec2.yml`)

Single EC2 box (c5.xlarge: 4 vCPU, ~7.6 GB RAM, Ubuntu 22.04, ap-south-1). Seven
services on the default bridge network:

| Service | Image | Role | Count | mem_limit |
|---|---|---|---|---|
| `server` | `judge0-node20:1.13.1` | Judge0 API (Puma/Rails), port **2358** | 1 | — |
| `workers` | `judge0-node20:1.13.1` | Judge0 Resque isolate sandboxes | **3** (`--scale workers=3`) | 1.5g |
| `api` | custom Python 3.12 | Flask grading API (Gunicorn+gevent), port **5000** | 1 | — |
| `grading_worker` | custom Python 3.12 | **async worker** — builds harness, submits, parses | 1 | — |
| `reconciler` | custom Python 3.12 | crash recovery + deadline enforcement | 1 | — |
| `db` | postgres:13 | Judge0 PostgreSQL | 1 | 400m |
| `redis` | redis:6.0 | queue + results + pub/sub (bound to 127.0.0.1) | 1 | 512m |

**Concurrency knobs:** total sandboxes = `workers × MAX_RUNNERS`. Current config is
`workers=3 × MAX_RUNNERS=4`. Judge0 Puma HTTP slots = `WEB_CONCURRENCY=2 ×
RAILS_MAX_THREADS=16 = 32`.

**Images:**
- `Dockerfile` — `python:3.12-slim`, non-root `appuser`, deps from
  `requirements-api.txt` (flask, redis, gunicorn, gevent, requests, structlog,
  pydantic, packaging). Default CMD runs `api:app` under gevent (2 workers, 1000
  connections, 1900s timeout).
- `Dockerfile.judge0-node20` — extends `judge0/judge0:1.13.1`, adds the Node.js
  **20.17.0** binary as `/usr/local/bin/node20` (SHA256-verified) so modern JS and
  per-worker memory limits work. Stock judge0 only ships Node 12. Registered as a
  new language via `sql/register_node20.sql`.

---

## 3. End-to-end flow

```
Student frontend
   │  POST /api/grade   (Bearer student token)
   ▼
edwisely_api.py  ──verify token──►  Edwisely auth server
   │  POST /submit  (+ callback_url)
   ▼
api.py  ──validate (pydantic)──► idempotency check ──► admission control
   │  enqueue QueuedJob
   ▼
Redis  judge0:jobs:normal  (+ pending_deadline TTL)
   │  BLPOP
   ▼
worker_async.py  (grading_worker)
   │  Autograder.grade():
   │    1. HarnessBuilder.build()
   │    2. SecurityChecker.check()
   │    3. Judge0Client.submit_and_wait()  ──► server (2358) ──► workers (isolate)
   │    4. parse_judge0_response()
   ▼
Redis  judge0:result:{ticket}  +  PUBLISH judge0:notify:{ticket}
   │                                   │
   │ (webhook)                         │ (SSE)
   ▼                                   ▼
edwisely webhook  /api/webhook/result   api.py  GET /results/stream/{ticket}
   │                                   │
   ▼                                   ▼
student polls GET /api/result/{ticket}   student receives SSE event
```

Three independent result-delivery paths exist (any/all may be used):
1. **SSE** — `GET /results/stream/{ticket_id}` on api.py (pub/sub backed).
2. **Webhook** — worker POSTs to `callback_url` when set.
3. **Polling** — `GET /api/result/{ticket_id}` on edwisely_api.py.

---

## 4. Submission API — `api.py` (port 5000)

The core stateless grading engine. Redis-backed. Structured JSON logging via
structlog (`core/log.py`, `LOG_LEVEL` env, logs `http_request`/`http_response` on
every call).

### `POST /submit`

Validated by pydantic `_SubmitRequest`:

| Field | Rule |
|---|---|
| `student_id`, `assessment_id` | required str |
| `language` | one of `python / c / cpp / java / javascript` |
| `student_code` | required str |
| `test_cases` | 1–500 items; each has `expected` + (`inputs` **or** `stdin_text`) |
| `mode` | `function` (default) or `stdio` |
| `function_name` | valid identifier `[A-Za-z_][A-Za-z0-9_]*`, default `solve` |
| `per_tc_limit_s` | 1–30, default 2 |
| `memory_limit_mb` | 16–3500, default 256 |
| `param_types` | optional `list[str]` (function mode, C/C++/Java) |
| `return_type` | default `auto` |
| `callback_url` | optional, must start `http(s)://` |
| `idem_key` | optional hex idempotency key |

Cross-validation: `function` mode ⇒ all TCs use `inputs`; `stdio` mode ⇒ all TCs use
`stdin_text`.

Processing order:
1. **Idempotency** — `judge0:idem:{idem_key}` (TTL 7200s). Hit ⇒ `200 {status:
   "duplicate"}` with existing ticket.
2. **Admission control** — `queue.is_at_capacity(MAX_QUEUE_DEPTH=5000)` ⇒ `429`.
3. **Enqueue** — new UUID ticket, build `QueuedJob`, `queue.enqueue(job)`, store idem
   key, return `202 {ticket_id, status: "queued"}`.

### `GET /results/stream/<ticket_id>` (SSE)

Headers: `text/event-stream`, `no-cache`, `X-Accel-Buffering: no`, keep-alive.
- First checks `judge0:result:{ticket}` — if present, emits `event: result` and
  closes (avoids race).
- Else subscribes to `judge0:notify:{ticket}`, polls at 1s, emits `: heartbeat`
  every second, delivers the result on publish.
- On `SSE_TIMEOUT_S` (default 1800s) emits a `system_error` result and closes.

### `GET /health`

Checks Redis + Judge0 + circuit breaker. Returns `ok` / `degraded` / `error`.
Optional timing-safe bearer auth via `HEALTH_TOKEN` (`hmac.compare_digest`). `503`
if Redis down, `401` on token mismatch.

---

## 5. Frontend bridge — `edwisely_api.py` (port 8000)

- **`POST /api/grade`** — requires `Authorization: Bearer <token>`; verifies against
  `EDWISELY_AUTH_URL` (returns `student_id`, `assessment_id`). Injects
  `callback_url = {WEBHOOK_BASE_URL}/api/webhook/result`, forwards to
  `{JUDGE0_EC2_URL}/submit`. Propagates `202/200/400/401/429/502/503`.
- **`POST /api/webhook/result`** — Judge0 side calls this on completion; stores into
  in-memory `_results[ticket_id]` (hook point for DB write / WS push). *No signature
  validation currently.*
- **`GET /api/result/<ticket_id>`** — poll; `200 {status:"done", score, total,
  tc_results[...]}` or `202 {status:"pending"}`.
- **`GET /api/health`** — Judge0 reachability.

> Note: `_results` is an in-memory dict — results live in the edwisely_api process
> only and should be moved to a DB for durability/scale.

---

## 6. Queue & job lifecycle — `core/job_queue.py`

`PriorityJobQueue` over Redis. Keys:

| Key | Purpose | TTL |
|---|---|---|
| `judge0:jobs:normal` | new submissions (RPUSH / BLPOP, FIFO) | — |
| `judge0:jobs:retry` | retried jobs (polled first) | — |
| `judge0:jobs:processing` | in-flight list (visibility pattern) | — |
| `judge0:inflight:{ticket}` | worker heartbeat / crash timeout | 300s |
| `judge0:pending_deadline:{ticket}` | max-wait deadline trigger | 7200s |
| `judge0:result:{ticket}` | stored result JSON | 7200s |
| `judge0:notify:{ticket}` | SSE pub/sub channel | — |
| `judge0:idem:{key}` | idempotency lock | 7200s |

`QueuedJob` dataclass: `ticket_id, student_id, submitted_at, payload, retry_count=0,
idem_key=""`. `payload` carries `language, student_code, test_cases, mode,
function_name, per_tc_limit_s, memory_limit_mb, param_types, return_type,
callback_url`.

**Priority** = BLPOP key order: `BLPOP judge0:jobs:retry judge0:jobs:normal timeout`
— retry queue drains first; FIFO within each.

Key methods:
- `enqueue(job)` — RPUSH to normal + set `pending_deadline` TTL (atomic pipeline).
- `dequeue(timeout=30)` — BLPOP; on hit, RPUSH to processing + set `inflight` key.
  Catches `redis.TimeoutError`/`ConnectionError` → returns `None` (no crash-restart
  storm — this is the recent hardening fix).
- `requeue(job)` — `retry_count++`, RPUSH to retry, reset deadline TTL.
- `ack(job)` — Lua `_ACK_LUA` atomically `GET inflight → LREM processing → DEL
  inflight` (removes TOCTOU race).
- `store_result(ticket, result, idem_key)` — SETEX result, DEL pending_deadline, DEL
  idem_key if `system_error`, PUBLISH notify — one pipeline.
- `get_result(ticket)`, `depths()`, `is_at_capacity(max_depth)`.

Constants: `RESULT_TTL_S=7200`, `INFLIGHT_TTL_S=300`, `MAX_JOB_WAIT_S=7200`.

---

## 7. Worker — `worker_async.py` (active; `worker.py` is the deprecated sync version)

asyncio loop; blocking I/O offloaded via `asyncio.to_thread`. `WORKER_CONCURRENCY`
(default 48) bounds concurrent jobs via a semaphore; thread pool = concurrency + 4.

Loop: `dequeue(5)` → `asyncio.create_task(process_job(...))` → reap done tasks. Each
`process_job`:
1. `async with sem:` → `grader.grade(submission, retry_count)` in a thread.
2. On infra failure (`needs_requeue=True`): compute backoff
   `5 × 3^retry_count` s (5 / 15 / 45) — **sleep outside the semaphore** so it doesn't
   hold a slot — then requeue + ack. At `MAX_RETRY_COUNT` (default 3): store
   `system_error`, no sleep, ack.
3. Else store result + ack. Every exception path still calls `ack` (no stuck jobs).

Extras:
- **Webhook** — if `payload.callback_url` set, `_fire_webhook` POSTs the result with
  3 retries / backoff; logs `webhook_delivered` / `_server_error` / `_failed` /
  `_gave_up`. Never blocks the job.
- **Resque flush** — on retry exhaustion, one-shot `_flush_resque_queue` deletes
  `resque:queue:default` + `resque:failed` and the stuck Judge0 submission, breaking
  OOM-kill cascades.
- Graceful shutdown awaits in-flight tasks on SIGTERM/SIGINT.

Env: `REDIS_*`, `JUDGE0_URL` (default `http://localhost:2358`), `JUDGE0_API_KEY`,
`MAX_RETRY_COUNT=3`, `WORKER_CONCURRENCY=48`, `CALLBACK_HOST/PORT`.

---

## 8. Orchestration — `autograder.py` (`Autograder.grade`)

1. **Build harness** → `HarnessBuilder(HarnessConfig(...))`; captures `session_id`,
   `delim`, `student_code_start_line`.
2. **Security** → `security.check(code, language, delim)`:
   - infinite loop → all TCs `TLE` (never hits Judge0)
   - syntax error → all TCs `CE`
   - real violation (e.g. `import os`) → all TCs `ERROR` + `security_error`
3. **Function detection** (function mode) → `_detect_function_name`: uses the
   requested name if defined, else best candidate (name containing the expected, or
   last-defined), skipping `main/int/void/...`. None found → all TCs `ERROR`.
4. **Sanitize** → `sanitize_for_injection`.
5. **Submit** → `judge0.submit_and_wait(source, language, per_tc_limit_s, tc_count,
   memory_limit_mb)`. Retriable exceptions (5xx, timeout, breaker open, Judge0 status
   12) → return `needs_requeue=True`.
6. **Parse** → `parse_judge0_response(stdout, status, session_id, total_tc_count,
   expected_values, compile_output, student_code_start_line)`.
7. **Language rewrites** — Java `NoSuchMethodException` → CE; JS `Compilation Error:`
   detail → CE.
8. **Infra-failure guard** → `_is_infrastructure_failure`: empty results, or all
   `ERROR` with infra keywords (`fork() failed`, `RLIMIT_NPROC`, `EMFILE`, `Cannot
   allocate memory`, `out of memory`, …) → `needs_requeue=True`.
9. **Cleanup** → best-effort `judge0.delete_submission(token)`.

`GradingResult` carries: `submission` (`ParsedSubmission`), `judge0_raw`,
`harness_code`, `security_error`, `system_error`, `needs_requeue`.

---

## 9. Harness builder — `core/harness_builder.py`

`SUPPORTED_LANGUAGES = [python, c, cpp, java, javascript]`, `MAX_PARALLEL_TCS = 200`.

`build()` dispatches to `_build_<lang>()`, which: loads `harnesses/<lang>_harness.<ext>`,
serializes test data to language literals, escapes + embeds student code (sentinel
approach to avoid brace clashes), fills placeholders, and records
`student_code_start_line` for error attribution.

**Modes:**
- **function** — student defines `solve(...)`; harness calls it with positional
  `inputs`; captures the return value.
- **stdio** — student program reads stdin / writes stdout; harness feeds
  `stdin_text` and captures stdout per TC. (Python stores student source as a string
  and `exec`s it per child so it doesn't run at import with empty stdin.)

**Type handling:**
- C/C++: per-position int→`long long` upgrade when a value overflows 32-bit.
- Java: all-or-nothing int→`long` upgrade (reflection needs exact types). Literals
  autoboxed via `(Object)(...)`.
- `_c_literal` / `_java_literal` serialize bool/None/str/int/float correctly.

**Expected values are never embedded in the harness** (Fix 4.1) — comparison happens
outside the sandbox in OutputParser, so student code can't forge verdicts.

---

## 10. Harness runtime & output protocol (`harnesses/*`)

Every harness emits each TC framed by a session-unique delimiter:

```
@@TC_RESULT__{session_id}__START_{n}
{"status": "...", "got": "...", "detail": "..."}
@@TC_RESULT__{session_id}__END_{n}
...
@@TC_RESULT__{session_id}__DONE
```

`DONE` present ⇒ harness finished. Absent ⇒ global TLE (an earlier TC blew the global
limit). `session_id` = 12 hex chars.

**Harness-emitted statuses:** `OUTPUT` (ran, awaiting comparison), `TLE`, `MLE`,
`SEGV`, `FPE`, `ERROR`. Harnesses may **never** emit `PASS`/`FAIL` — those are
assigned only by the parser; a harness-emitted PASS/FAIL is downgraded to `ERROR`.

**Per-language execution:**

| Lang | Parallelism | Per-TC timeout | Crash detect | Output capture |
|---|---|---|---|---|
| Python | `os.fork()` + `select.poll()` | `signal.alarm` per child | poll EOF + `WIFSIGNALED` | fake `sys.stdout` |
| C | `fork()` + `poll()` | `alarm()` in child + parent backstop | poll EOF + `WIFSIGNALED` | `tmpfile()`/dup |
| C++ | `fork()` + `poll()` | `alarm()` in child | poll EOF + `WIFSIGNALED` | `tmpfile()`/dup, try/catch (`bad_alloc`→MLE) |
| Java | threads + `join(deadline)` | `join(remaining_ms)` | catch `OOM`/exception | `ByteArrayOutputStream` + ThreadLocal dispatch |
| JavaScript | `worker_threads` pool | `setTimeout` + `worker.terminate()` | `worker.on('error')`, `ERR_WORKER_OUT_OF_MEMORY` | `process.stdout.write` override |

Notes:
- **Python** strips dangerous modules in each child, hardens `open`/`signal`/
  `sys.exit`, and in stdio mode `exec`s student code with a whitelisted builtins set.
- **C/C++** generate a batched fork+poll runner; `poll()` avoids `select()`'s
  FD_SETSIZE cap; signal → status mapping SIGSEGV→SEGV, SIGFPE→FPE, SIGKILL→MLE.
- **Java** uses ThreadLocal stdout/stdin dispatch (avoids `System.setOut` contention),
  a fresh ClassLoader per TC (no static bleed), and interrupt→stop kill escalation;
  `_preprocess_java_student_code` promotes all helper classes to static nested classes.
- **JavaScript** uses `worker_threads` (not child_process — far lighter), a syntax
  pre-check via `vm.Script` (SyntaxError → CE), and per-worker
  `resourceLimits.maxOldGenerationSizeMb`.

---

## 11. Judge0 client — `core/judge0_client.py`

**Callback-first** (not polling): submits with `wait=false` and a `callback_url`,
then blocks on a small embedded `CallbackServer` (ThreadPoolExecutor, 32 workers)
until Judge0 PUTs/POSTs the result to `/result`. ~2 Judge0 calls per submission.

- **Submit:** `POST /submissions?base64_encoded=true&wait=false` (source base64).
- **Delete:** `DELETE /submissions/{token}` (best-effort cleanup — keeps the
  submissions table small).
- **Fallback poll:** `GET /submissions/{token}?...fields=stdout,stderr,compile_output,
  status,time,memory` for up to 60s if the callback times out.

Language IDs: `python 71, c 50, cpp 54, java 62, javascript 1001` (Node 20). Auth
header `X-Auth-Token` (if key set).

Payload highlights: `cpu_time_limit = ceil(tc_count / 200) × per_tc_limit_s +
overhead`, `wall_time_limit = cpu + 2`, per-process + per-thread time/memory limits
on, `number_of_processes = 200 + 20` (JS 200+120), `memory_limit` 4 GB (Py/C/C++) or
8 GB (Java/JS), C compiled with `-Werror=int-conversion`.

Resilience:
- `_post_with_retry` — 3 attempts, exponential backoff on 5xx/network only (4xx
  raised immediately), 120s timeout.
- **Circuit breaker** (singleton `_judge0_breaker`) — opens after 10 consecutive
  errors for 30s; open ⇒ fail fast with `RuntimeError`.

Status map (`JUDGE0_STATUS`): 1 In Queue, 2 Processing, 3 Accepted, 4 Wrong Answer,
5 TLE, 6 Compilation Error, 7–11 Runtime Error (SIGSEGV/SIGFPE/SIGABRT/NZEC/Other),
12 Internal Error (**retriable**), 13 Exec Format Error. Returns a `Judge0Result`
(`stdout, stderr, status_str, status_id, compile_output, time_taken_s, memory_kb,
token`).

---

## 12. Output parser — `core/output_parser.py`

Parses the stdout blob into per-TC results.
- Detects `DONE` (absent → `global_tle`). Regex-extracts each
  `START_(\d+)\n…END_\1` block (`re.DOTALL`).
- Missing TC numbers → `TLE` (if global_tle) or `MISSING` (crash before reaching it).
- **Comparison** (`_num_equal`): exact string match first; numeric tolerance only
  when expected looks like a float (contains `.`/`e`) — `abs_err ≤ max(1e-9, |b|×1e-6)`.
  Integer expected (`"3"`) compared exactly (`"3.0" ≠ "3"`).
- **Compile errors** (`_adjust_compile_output`): rewrites compiler diagnostics to be
  student-relative — shifts line numbers by `student_code_start_line`, strips harness
  frames (`run_tc_child`), renames `student_stdio_main`→`main`, maps
  "undefined reference to student_stdio_main" → "'main' function not found", normalizes
  gcc-14 unicode quotes.

Produces `ParsedSubmission(tc_results[TCResult], global_tle, score, total,
partial_execution)`. `TCResult = {tc_num, status, got, expected, detail, warning}`.
Final statuses: `PASS / FAIL / TLE / MLE / SEGV / FPE / ERROR / CE / MISSING`.

---

## 13. Security — `security/security.py`

`SecurityChecker.check(code, language, delim)` runs **before** Judge0. Common checks:
reject null bytes, `MAX_CODE_LENGTH = 10_000` chars, and delimiter-spoofing attempts.

- **Python** — AST walk (catches aliased/dynamic imports). Blocks ~25 modules
  (`os, subprocess, socket, ctypes, signal, importlib, multiprocessing, threading,
  pty, resource, sys, builtins, …`), dangerous builtins (`__import__, exec, eval,
  compile, globals, …`), and dunder escapes (`__class__, __subclasses__, __globals__,
  …`). Flags trivial self-recursion and `while True` with no exit.
- **C/C++/Java** — regex: `system/popen/exec*/fork/ptrace/syscall`, socket/net
  includes, `dlopen/dlsym`, `mmap`, `signal(SIGALRM)`, inline `asm`, absolute-path
  file opens. Infinite-loop flag only if the whole submission has no `break`/`return`.
- **JavaScript** — blocks `child_process/fs/net/http/os/vm/worker_threads/…`,
  `eval`/`new Function`, `process.exit/binding/kill`, dynamic `require(var)`.

Returns `SecurityCheckResult(passed, violations[{rule, detail, line}])`.

---

## 14. Failure recovery — `reconciler.py`

Two recovery mechanisms:
1. **Crash recovery (visibility timeout)** — scans `judge0:jobs:processing` every
   `RECONCILER_SCAN_INTERVAL_S` (60s). For each entry: if `inflight` key still exists,
   skip; if a result exists, LREM the stale entry; else LREM and requeue to retry
   (`retry_count++`). Handles workers that died mid-job.
2. **Deadline enforcement** — enables keyspace notifications (`notify-keyspace-events
   Ex`), subscribes to `__keyevent@{db}__:expired`. When a
   `judge0:pending_deadline:{ticket}` expires (job waited > `MAX_JOB_WAIT_S` = 7200s
   without being picked up), writes a `system_error` result (`SET … NX`) and
   PUBLISHes notify — so the student sees a failure instead of an infinite spinner.

---

## 15. Operations

**Deploy** (`deploy-ec2.sh`, idempotent, 6 stages): disk check → install Docker →
clone/pull repo → generate `.env` secrets (`openssl rand`) → build images (judge0
base, node20, Python services) → `docker compose up -d --scale workers=3` → run
`register_node20.sql` → health checks (`/system_info` on 2358, `/health` on 5000).

**Cleanup** (`cleanup.sh`, after load tests; `--full` weekly): `TRUNCATE submissions
CASCADE` + `VACUUM ANALYZE`, Redis `BGREWRITEAOF`, truncate Docker container logs;
`--full` adds `docker system prune` + `journalctl --vacuum-time=3d`. Color-coded disk
warnings at 85% / 95%.

**Daily S3 log dump + purge** (`dump_judge0_docker_logs_to_s3.py`, ~18:29 UTC /
23:59 IST): gzips and uploads `api`, `grading_worker`, `workers` container logs to
`s3://edwisely-logs/judge0/docker-logs/YYYY-MM-DD/…`, truncating locally **only on
upload success**. Then purges Judge0's `submissions` table older than
`SUBMISSIONS_RETENTION_DAYS = 1` (real results already live in the app DB via webhook)
and VACUUMs. `server`/`reconciler` use Docker log rotation instead.

**judge0.ec2.conf** key values: `MAX_QUEUE_SIZE=200`, `MAX_RUNNERS=4`,
`INTERVAL=0.1`, `MAX_PROCESSES_AND_OR_THREADS=220` (`MAX_MAX…=500`),
`SOURCE_CODE_SIZE_LIMIT=524288`, `MAX_MEMORY_LIMIT=16777216` (16 GB ceiling; actual
bounded by container `mem_limit`), defaults `CPU_TIME_LIMIT=5 / WALL_TIME_LIMIT=10 /
MEMORY_LIMIT=256000`.

---

## 16. Redis key reference

| Key | Set by | Read by | TTL |
|---|---|---|---|
| `judge0:jobs:normal` | api enqueue | worker dequeue | — |
| `judge0:jobs:retry` | requeue / reconciler | worker dequeue (first) | — |
| `judge0:jobs:processing` | dequeue | reconciler, ack | — |
| `judge0:inflight:{ticket}` | dequeue | reconciler, ack | 300s |
| `judge0:pending_deadline:{ticket}` | enqueue/requeue | reconciler (expiry) | 7200s |
| `judge0:result:{ticket}` | store_result / reconciler | api, edwisely_api | 7200s |
| `judge0:notify:{ticket}` | store_result / reconciler | SSE stream | — |
| `judge0:idem:{key}` | api submit | api submit | 7200s |

---

## 17. Adding a new language

1. Add `harnesses/<lang>_harness.<ext>` implementing the parallel TC runner + the
   `@@TC_RESULT__{session_id}__START/END/DONE` protocol (statuses OUTPUT/TLE/MLE/
   SEGV/FPE/ERROR only).
2. Add `_build_<lang>()` in `core/harness_builder.py` (load template, serialize test
   data, embed student code, track `student_code_start_line`) and add the id to
   `SUPPORTED_LANGUAGES`.
3. Add the Judge0 `language_id` to `LANGUAGE_IDS` in `core/judge0_client.py` (register
   it in Judge0's DB if custom, like `register_node20.sql`).
4. Add the language's validation rules to `security/security.py` and literal
   serialization if the type system needs it.
5. Add the language to the `_SubmitRequest` enum in `api.py`.
