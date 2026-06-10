This is a large topic — I'll cover every layer from HTTP request to SSE result, with full internal detail. No tools needed; I have everything from the audit.

---

# Complete Harness + Submission + Result Flow — Every Granular Detail

---

## Part 1: The Mental Model First

The system has one central idea: **instead of submitting N separate jobs to Judge0 (one per test case), it wraps ALL test cases into a single program (the "harness") that runs them all in parallel and emits structured output.** This reduces Judge0 API traffic by 100×.

```
Student Code
     +
Test Cases (N items)
     │
     ▼
HarnessBuilder
     │
     ▼
One complete program in Python/C/C++/Java
(student code + harness runner + all N test inputs embedded)
     │
     ▼
Submitted to Judge0 as ONE job
     │
     ▼
Judge0 sandbox runs it → one stdout blob with N TC results
     │
     ▼
OutputParser extracts all N results from stdout
     │
     ▼
{score, tc_results[...]} stored in Redis → pushed via SSE
```

---

## Part 2: The Submission — `POST /submit`

### What the client sends

```json
{
  "student_id":      "s123",
  "assessment_id":   "a456",
  "language":        "python",
  "student_code":    "def solve(a, b):\n    return a + b",
  "test_cases": [
    { "inputs": [2, 3],   "expected": 5 },
    { "inputs": [10, 20], "expected": 30 }
  ],
  "mode":            "function",
  "function_name":   "solve",
  "per_tc_limit_s":  2,
  "memory_limit_mb": 256,
  "param_types":     ["int", "int"],
  "return_type":     "int"
}
```

For stdio mode:
```json
{
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "2 3\n",   "expected": "5" },
    { "stdin_text": "10 20\n", "expected": "30" }
  ]
}
```

### What `api.py:submit()` does — step by step

**Step 1: Pydantic validation.**  
`_SubmitRequest.model_validate(body)` runs. It validates:
- language is one of `python/c/cpp/java`
- `function_name` matches `[A-Za-z_][A-Za-z0-9_]*`
- `mode` is `function` or `stdio`
- `per_tc_limit_s` is 1–30
- `memory_limit_mb` is 16–3500
- test_cases not empty, not > 500
- Cross-validation: function mode requires `inputs` in each TC; stdio mode requires `stdin_text`

**Step 2: Idempotency check.**
```python
code_hash = sha256(student_code)
raw       = f"{student_id}:{assessment_id}:{code_hash}"
idem_key  = "judge0:idem:" + sha256(raw)
```
Redis `GET judge0:idem:{key}`. If it exists → return the existing `ticket_id` with HTTP 200, status `"duplicate"`. The student gets the same ticket from a previous identical submission without re-running.

**Step 3: Admission control.**
```python
queue.is_at_capacity(5000)
# → r.llen("judge0:jobs:retry") + r.llen("judge0:jobs:normal") >= 5000
```
If true → 429.

**Step 4: Build QueuedJob and enqueue.**
```python
ticket_id = str(uuid.uuid4())   # e.g. "a1b2c3d4-..."

payload = {
    "language":        "python",
    "student_code":    "def solve...",
    "test_cases":      [{"expected": 5, "inputs": [2,3], "stdin_text": None}, ...],
    "mode":            "function",
    "function_name":   "solve",
    "per_tc_limit_s":  2,
    "memory_limit_mb": 256,
    "param_types":     ["int","int"],
    "return_type":     "int",
}

job = QueuedJob(
    ticket_id    = ticket_id,
    student_id   = "s123",
    submitted_at = time.time(),    # float epoch
    payload      = payload,
    retry_count  = 0,
    idem_key     = "judge0:idem:abc...",
)
```

`queue.enqueue(job)` does a Redis **pipeline** (atomic pair):
```
RPUSH  judge0:jobs:normal  <json-serialized QueuedJob>
SETEX  judge0:pending_deadline:{ticket_id}  7200  "1"
```

The `pending_deadline` key is the "dead man's switch" — if it expires (2h) with no result stored, the reconciler writes a system_error. TTL reset on each requeue.

**Step 5: Store idempotency key and return.**
```
SETEX  judge0:idem:{hash}  7200  "{ticket_id}"
```
Returns `202 {"ticket_id": "a1b2c3d4-...", "status": "queued"}`.

---

## Part 3: The Queue — What Sits in Redis

The `QueuedJob` serialized to JSON looks exactly like:
```json
{
  "ticket_id":    "a1b2c3d4-...",
  "student_id":   "s123",
  "submitted_at": 1749500000.123,
  "payload": {
    "language":        "python",
    "student_code":    "def solve(a, b):\n    return a + b",
    "test_cases":      [{"expected": 5, "inputs": [2, 3], "stdin_text": null}],
    "mode":            "function",
    "function_name":   "solve",
    "per_tc_limit_s":  2,
    "memory_limit_mb": 256,
    "param_types":     ["int", "int"],
    "return_type":     "int"
  },
  "retry_count": 0,
  "idem_key":    "judge0:idem:abc..."
}
```

This entire JSON string is pushed as a single element into the Redis list `judge0:jobs:normal`. Redis is just storing strings — the queue is a Redis List, FIFO (RPUSH at tail, BLPOP from head).

**Two queues, one priority:**
- `judge0:jobs:retry` — failed jobs waiting to be retried
- `judge0:jobs:normal` — fresh submissions

Worker always checks retry first via `BLPOP judge0:jobs:retry judge0:jobs:normal 5`. BLPOP checks keys left-to-right — any item in retry queue is always served before any normal job, no matter how many normal jobs are waiting.

---

## Part 4: The Worker — `worker_async.py`

### Startup sequence

```python
r = redis.Redis(host=REDIS_HOST, port=6379, password=..., decode_responses=False)
queue = PriorityJobQueue(r)

cb_server = CallbackServer()
cb_server.start(port=0)        # OS picks a free port, e.g. 54321
# cb_server is a tiny HTTP server running in a daemon thread
# Judge0 will PUT results to http://{this_container_ip}:54321/result

callback_host = socket.gethostbyname(socket.gethostname())  # container's Docker bridge IP

grader = Autograder(judge0_cfg, callback_server=cb_server, callback_host=callback_host)

executor = ThreadPoolExecutor(max_workers=52)   # 48 + 4
loop = asyncio.new_event_loop()
loop.set_default_executor(executor)
loop.run_until_complete(run_worker_async(queue, grader))
```

### The async dequeue loop

```python
sem = asyncio.Semaphore(48)   # max 48 concurrent grading coroutines
tasks = set()

while running:
    job = await asyncio.to_thread(queue.dequeue, 5)
    # ↑ This calls queue.dequeue() in a thread-pool thread.
    # queue.dequeue() does: r.blpop([RETRY_QUEUE, NORMAL_QUEUE], timeout=5)
    # If timeout → returns None → loop continues (checks 'running' flag)
    # If job found:
    #   1. Parse JSON → QueuedJob
    #   2. RPUSH job into PROCESSING_QUEUE
    #   3. SETEX judge0:inflight:{ticket_id} 300 <raw_json>
    #   All in one pipeline round-trip (atomic)
    
    if job is None:
        continue

    # Reap completed tasks
    tasks -= {t for t in tasks if t.done()}
    
    task = asyncio.create_task(process_job(job, queue, grader, sem))
    tasks.add(task)
```

**Why asyncio.to_thread?** `r.blpop()` is a blocking call (blocks the OS thread for up to 5s). Running it in a thread-pool thread means the event loop can continue dispatching other coroutines while one thread is blocked waiting for Redis.

### `process_job()` — inside the semaphore

```python
async with sem:   # blocks here if 48 jobs already running
    submission = job_to_submission(job)
    # ↑ Reconstructs Submission dataclass from job.payload dict

    result = await asyncio.to_thread(grader.grade, submission, job.retry_count)
    # ↑ grader.grade() is blocking (does HTTP to Judge0, waits for callback)
    # Runs in a thread-pool thread so other coroutines can run concurrently

    if result.needs_requeue:
        if job.retry_count >= 3:
            # Give up → store system_error
            await asyncio.to_thread(queue.store_result, ticket_id, {system_error: ...}, job.idem_key)
            await asyncio.to_thread(_flush_resque_queue, queue.r)
            await asyncio.to_thread(queue.ack, job)
        else:
            backoff = 5.0 * (3 ** job.retry_count)  # 5s, 15s, 45s
            # NOTE: falls through OUTSIDE the semaphore for the sleep
    else:
        await asyncio.to_thread(queue.store_result, ticket_id, result_to_dict(result), job.idem_key)
        await asyncio.to_thread(queue.ack, job)

# Outside semaphore: semaphore slot freed BEFORE sleeping
if _requeue_backoff_s is not None:
    await asyncio.sleep(_requeue_backoff_s)   # doesn't hold semaphore slot
    await asyncio.to_thread(queue.requeue, job)
    await asyncio.to_thread(queue.ack, job)
```

**Why sleep outside the semaphore?** If 48 jobs all fail simultaneously and sleep inside the semaphore, no new jobs can start for the full backoff duration. Moving sleep outside means the slot is freed immediately for other jobs.

### `job_to_submission()` — deserializing the queue payload

```python
def job_to_submission(job: QueuedJob) -> Submission:
    p = job.payload
    return Submission(
        student_id      = job.student_id,
        language        = p["language"],          # "python"
        student_code    = p["student_code"],       # "def solve(a,b): return a+b"
        test_cases      = [
            TestCase(
                expected   = tc["expected"],       # 5
                inputs     = tc.get("inputs"),     # [2, 3] for function mode
                stdin_text = tc.get("stdin_text"), # None for function mode
            )
            for tc in p["test_cases"]
        ],
        mode            = p.get("mode", "function"),
        function_name   = p.get("function_name", "solve"),
        per_tc_limit_s  = p.get("per_tc_limit_s", 2),
        memory_limit_mb = p.get("memory_limit_mb", 256),
        param_types     = p.get("param_types"),    # ["int", "int"]
        return_type     = p.get("return_type", "auto"),
    )
```

---

## Part 5: The Autograder Pipeline — `autograder.py:grade()`

This is the brain. Seven stages, executed sequentially for each job.

### Stage 1: Build HarnessConfig + create session_id

```python
config = HarnessConfig(
    student_code    = submission.student_code,
    test_cases      = submission.test_cases,
    language        = "python",
    mode            = "function",
    per_tc_limit_s  = 2,
    memory_limit_mb = 256,
    function_name   = "solve",
    param_types     = ["int", "int"],
    return_type     = "int",
)
builder = HarnessBuilder(config)
# builder.session_id = "a3f9c2e1b8d4"   (12-char UUID hex, unique per grading call)
# builder.delim      = "@@TC_RESULT__a3f9c2e1b8d4__"
```

The `session_id` and `delim` are critical. The delimiter is what separates each test case's output in the harness stdout. It's session-scoped so even if a student prints `@@TC_RESULT__` in their output, they can't forge it without knowing the current session_id.

### Stage 2: Security check

```python
sec = self.security.check(
    student_code  = submission.student_code,
    language      = "python",
    session_delim = builder.delim   # "@@TC_RESULT__a3f9c2e1b8d4__"
)
```

**For Python — AST walk:**
```python
tree = ast.parse(student_code)
checker = _PythonASTChecker()
checker.visit(tree)
```
The AST walker catches:
- `import os` → `visit_Import`: checks `alias.name.split(".")[0]` against BLOCKED_MODULES
- `import os as operating_system` → same node, alias.asname logged in violation message
- `from os import path` → `visit_ImportFrom`
- `__import__("os")` → `visit_Call`: func.id in BLOCKED_BUILTINS
- `os.system(...)` → `visit_Call`: func is Attribute, func.value.id in BLOCKED_MODULES
- `().__class__.__bases__` → `visit_Attribute`: attr in BLOCKED_DUNDER
- `while True: pass` → `visit_While`: test is const-True AND `_body_can_exit(body)` is False
- `def solve(n): return solve(n)` → `visit_FunctionDef`: single-stmt body that calls itself

Special violation handling:
- `InfiniteLoop` → return TLE for ALL TCs immediately, never reach Judge0
- `SyntaxError` (from `ast.parse` failing) → return ERROR for all TCs
- Any other violation → return `security_error` (blocked, not graded)
- `DelimiterInjection` → student code contains the session delimiter → blocked

**For C/C++/Java — regex scan:**
```python
_check_regex(code, _C_BLOCKED)
# Patterns: r"\bsystem\s*\(", r"\bfork\s*\(", r"\bsyscall\s*\(", 
#           r'#\s*include\s*[<"]\s*sys/socket', r"\b__asm\b", etc.
```

### Stage 3: Auto-detect function name (function mode only)

```python
actual_fn = _detect_function_name(
    code          = "def solve(a, b):\n    return a+b",
    language      = "python",
    expected      = "solve",
)
```

This uses regex to search the code for the function definition:
- Python: `re.search(r"(?:\bdef\s+solve\s*\(|^\s*solve\s*=\s*(?:lambda\b|\w))", code)`
- If found → return `"solve"` (expected name is present, use it)
- If NOT found (student named their function differently):
  - `_find_defined_functions()` scans all definitions
  - Filters out keywords: `main, int, void, bool, char, ...`
  - If one candidate: return it
  - If multiple: `_best_candidate()` — prefers names containing "solve" as substring, breaks ties by closest length

Example: student wrote `def sum_values(a,b): return a+b`. Expected: `solve`.
- `_find_defined_functions` → `["sum_values"]`
- Returns `"sum_values"`
- `config.function_name` updated to `"sum_values"` → harness calls `sum_values(a, b)`

### Stage 4: Sanitize code

```python
config.student_code = sanitize_for_injection(student_code, language)
```
- Python: strips trailing whitespace per line (prevents indentation issues in template)
- C/C++/Java: replaces `*/` with `* /` (prevents student code from closing harness's block comments)

### Stage 5: Build harness

```python
harness_code = builder.build()
# Routes to _build_python() / _build_c() / _build_cpp() / _build_java()
```

This is the most complex part. Covered in depth in Part 6.

### Stage 6: Submit to Judge0 and wait

```python
judge0_result = self.judge0.submit_and_wait(
    source_code     = harness_code,
    language        = "python",
    per_tc_limit_s  = 2,
    tc_count        = 2,            # number of test cases
    memory_limit_mb = 256,
)
```

Full Judge0 flow in Part 7.

### Stage 7: Parse output and return

```python
parsed = parse_judge0_response(
    judge0_stdout   = judge0_result.stdout,
    judge0_status   = judge0_result.status_str,
    session_id      = builder.session_id,
    total_tc_count  = 2,
    expected_values = ["5", "30"],    # str(tc.expected).strip() for each TC
    compile_output  = judge0_result.compile_output,
)
```

Full parsing flow in Part 8.

---

## Part 6: The Harness Builder — Deep Dive

### The Delimiter Protocol (shared by all languages)

Every harness produces stdout in this exact format:
```
<anything the student might print — ignored>
@@TC_RESULT__a3f9c2e1b8d4__START_1
{"status": "OUTPUT", "got": "5", "detail": ""}
@@TC_RESULT__a3f9c2e1b8d4__END_1
@@TC_RESULT__a3f9c2e1b8d4__START_2
{"status": "OUTPUT", "got": "30", "detail": ""}
@@TC_RESULT__a3f9c2e1b8d4__END_2
@@TC_RESULT__a3f9c2e1b8d4__DONE
```

Note the harness emits `"OUTPUT"` status — it never emits `PASS` or `FAIL`. The comparison against expected values happens in `OutputParser` (outside the sandbox), not inside the harness. This is "Fix 4.1" — prevents students from forging verdicts.

### Python Harness — Function Mode

**Template placeholders filled:**
```
{session_id}      → "a3f9c2e1b8d4"
{mode}            → "function"
{student_code}    → the sanitized student code (inserted at module level)
{student_code_raw}→ repr() of original student code (for _STUDENT_SOURCE)
{test_cases_json} → [{"input": [2, 3]}, {"input": [10, 20]}]
{per_tc_limit_s}  → 2
{memory_limit_mb} → 256
{function_name}   → "solve"
```

**The generated harness looks like:**
```python
import os, sys, io, signal, select, resource, traceback, json, builtins, time

MODE  = "function"
DELIM = "@@TC_RESULT__a3f9c2e1b8d4__"

# Security monkey-patches
_real_open   = open
_real_signal = signal.signal
_HARNESS_FILE = __file__

def _safe_open(file, mode="r", *args, **kwargs):
    if isinstance(file, int):
        raise PermissionError("Direct file descriptor access not allowed")
    if os.path.abspath(str(file)) == os.path.abspath(_HARNESS_FILE):
        raise PermissionError("Access denied")
    return _real_open(file, mode, *args, **kwargs)
builtins.open = _safe_open   # student's open() calls go through this

def _safe_signal(signum, handler):
    if signum == signal.SIGALRM: return   # block SIGALRM override attempts
    return _real_signal(signum, handler)
signal.signal = _safe_signal

def _safe_exit(*args): raise SystemExit("__HARNESS_BLOCKED__")
sys.exit = builtins.exit = builtins.quit = _safe_exit

# ══ STUDENT CODE at module level (function mode) ══
def solve(a, b):
    return a + b

# ══ HARNESS RUNNER ══
_STUDENT_SOURCE = 'def solve(a, b):\n    return a + b'   # repr() of original

def _child_run_function(tc):
    actual = solve(*tc["input"])   # {function_name} was replaced with "solve"
    got_s  = str(actual).strip()
    return {"status": "OUTPUT", "got": got_s}

def _child_run(tc, write_fd, per_tc_limit_s, mem_limit_mb):
    # Strip dangerous modules from sys.modules AFTER fork
    for _m in ('os', 'subprocess', 'socket', 'signal', 'resource', ...):
        sys.modules.pop(_m, None)
    
    # Per-child SIGALRM
    def _tle(s, f): raise TimeoutError("TLE")
    _real_signal(signal.SIGALRM, _tle)
    signal.alarm(per_tc_limit_s)
    
    result = None
    try:
        result = _child_run_function(tc)   # MODE == "function"
        signal.alarm(0)
    except TimeoutError:
        result = {"status": "TLE", "detail": "Exceeded 2s"}
    except MemoryError:
        result = {"status": "MLE", "detail": "Memory limit exceeded"}
    except Exception:
        result = {"status": "ERROR", "detail": <last 2 traceback lines>}
    
    data = json.dumps(result).encode()
    os.write(write_fd, data)
    os.close(write_fd)
    os._exit(0)   # skip atexit, no cleanup

def _run_all_parallel(test_cases, per_tc_limit_s, memory_limit_mb):
    n = 2
    # Preflight: check RLIMIT_NOFILE and RLIMIT_NPROC
    
    results = {}
    for batch_start in range(0, n, 200):  # batches of 200
        batch_tcs = test_cases[batch_start : batch_start+200]
        jobs = []
        
        for b_i, tc in enumerate(batch_tcs):
            g_i = batch_start + b_i
            r_fd, w_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(r_fd)
                _child_run(tc, w_fd, 2, 256)   # in child
                os._exit(0)
            else:
                os.close(w_fd)
                jobs.append((pid, r_fd, g_i))
        
        # Collect via poll()
        poller = select.poll()
        for _, r_fd, _ in jobs: poller.register(r_fd, select.POLLIN)
        deadline = time.monotonic() + 2 + 5
        
        while pending_fds:
            ready = poller.poll(remaining_ms)
            for r_fd, event in ready:
                chunk = os.read(r_fd, 65536)
                if chunk: bufs[idx].append(chunk)
                else:  # EOF = child finished
                    raw = b"".join(bufs[idx])
                    results[idx] = json.loads(raw)
        
        # Kill any still-running children (TLE)
    
    # Emit results in order
    for i in range(n):
        result = results.get(i, {"status": "ERROR", ...})
        sys.stdout.write(f"{DELIM}START_{i+1}\n")
        sys.stdout.write(json.dumps(result) + "\n")
        sys.stdout.write(f"{DELIM}END_{i+1}\n")
        sys.stdout.flush()
    sys.stdout.write(f"{DELIM}DONE\n")
    sys.stdout.flush()

_TEST_CASES      = [{"input": [2, 3]}, {"input": [10, 20]}]
_PER_TC_LIMIT_S  = 2
_MEMORY_LIMIT_MB = 256
_run_all(_TEST_CASES, _PER_TC_LIMIT_S, _MEMORY_LIMIT_MB)
```

**What happens at runtime inside Judge0:**
1. Python interpreter runs the harness file
2. Student's `solve` function is defined at module level
3. `_run_all_parallel()` is called
4. For N=2 TCs (fits in one batch): both `os.fork()` calls happen immediately
5. Child 0: alarm(2), calls `solve(2, 3)` → returns 5 → writes `{"status":"OUTPUT","got":"5"}` to pipe → `_exit(0)`
6. Child 1: alarm(2), calls `solve(10, 20)` → returns 30 → writes `{"status":"OUTPUT","got":"30"}` to pipe → `_exit(0)`
7. Parent: `poll()` on both pipe read-ends simultaneously, reads each child's result as it arrives
8. Both results arrive (in whatever order, fastest first), stored in `results` dict
9. Emits in order: TC1 result, TC2 result, then DONE

**Total wall time = max(time_for_TC1, time_for_TC2)**, not sum.

### Python Harness — Stdio Mode

**Key differences from function mode:**

`test_cases_json` becomes: `[{"stdin_text": "2 3\n"}, {"stdin_text": "10 20\n"}]`

The student code is NOT executed at module level. Instead:
```python
# module_level_code = "# stdio mode: student code runs only in child processes via exec()"
```
The student source is stored raw:
```python
_STUDENT_SOURCE = 'a, b = map(int, input().split())\nprint(a + b)\n'
```

In each child, `_child_run_stdio()` is called:
```python
def _child_run_stdio(tc):
    fake_stdin  = io.StringIO(tc.get("stdin_text", ""))  # "2 3\n"
    fake_stdout = io.StringIO()
    sys.stdin   = fake_stdin
    sys.stdout  = fake_stdout
    
    # Whitelisted builtins — exec() cannot escape via __builtins__
    _safe_builtins = {k:v for k,v in __builtins__.__dict__.items()
                      if k in {'print', 'input', 'range', 'len', 'int', ...}}
    
    ns = {
        "__name__":     "__main__",
        "__builtins__": _safe_builtins,
        "open":         _safe_open,
        "exit":         _safe_exit,
    }
    exec(compile(_STUDENT_SOURCE, "<student>", "exec"), ns)
    
    got = fake_stdout.getvalue().strip()
    return {"status": "OUTPUT", "got": got}
```

**What happens:**
1. Fork N children simultaneously
2. Each child: replace `sys.stdin` with `io.StringIO("2 3\n")`, `sys.stdout` with `io.StringIO()`
3. `exec(_STUDENT_SOURCE, ns)` — runs the student program in a restricted namespace
4. `input()` reads from the fake stdin → returns `"2 3"`
5. `print(a + b)` writes to the fake stdout
6. `fake_stdout.getvalue().strip()` → `"5"` → written to pipe

**Why exec and not a subprocess?** Faster. No process creation, no file I/O, no Python startup overhead. The fake stdin/stdout redirect is pure Python, works inside the existing forked child.

**The `__import__` in whitelisted builtins** allows `import bisect`, `import math` etc. inside student code, since those stdlib modules are harmless. The AST checker already blocked dangerous imports before reaching this point.

### C Harness — Function Mode

`_build_c()` generates the complete C source by filling `c_harness.c` template + generating inline C code for the parallel runner.

**The TC runner is NOT from the template** — it's generated programmatically by `_build_c_parallel_runner()`. For N=2 TCs with `param_types=["int","int"]`, `return_type="int"`:

`tc_params_comma` = `"int p0, int p1, "` (params + trailing comma when params exist)

`call_solve_and_capture` = (from `_build_c_call("int")`):
```c
int ret = solve(p0, p1);
snprintf(result.got, sizeof(result.got), "%d", (int)ret);
```

The generated child function signature:
```c
static void run_tc_child(int pipe_fd, int p0, int p1, int per_tc_limit_s, int memory_limit_mb) {
    // apply memory limit via RLIMIT_AS
    // block fork via RLIMIT_NPROC = 1
    // close all FDs except stdin/stdout/stderr/pipe_fd
    // set SIGALRM handler
    // alarm(2)
    
    TCResult result;
    memset(&result, 0, sizeof(result));
    
    int ret = solve(p0, p1);
    snprintf(result.got, sizeof(result.got), "%d", (int)ret);
    
    alarm(0);
    strncpy(result.status, "OUTPUT", sizeof(result.status)-1);
    
    write(pipe_fd, &result, sizeof(TCResult));
    close(pipe_fd);
    _exit(0);
}
```

**The `tc_runner_body` for BATCH 0 (TC1 and TC2):**
```c
// Preflight: RLIMIT_NOFILE check

TCResult *_results = (TCResult*)calloc(2, sizeof(TCResult));

/* BATCH 0: TCs 1..2 */
{
    pid_t _pids[2];
    int   _fds[2];
    int   _done[2];
    // memset all to 0

    /* Phase 1: Fork both children simultaneously */
    {
        int _pfd[2];
        pipe(_pfd);
        pid_t _p = fork();
        if (_p == 0) {
            close(_pfd[0]);
            run_tc_child(_pfd[1], 2, 3, 2, 256);   // TC1 inputs
        }
        close(_pfd[1]);
        _pids[0] = _p;
        _fds[0]  = _pfd[0];
    }
    {
        int _pfd[2];
        pipe(_pfd);
        pid_t _p = fork();
        if (_p == 0) {
            close(_pfd[0]);
            run_tc_child(_pfd[1], 10, 20, 2, 256);  // TC2 inputs
        }
        close(_pfd[1]);
        _pids[1] = _p;
        _fds[1]  = _pfd[0];
    }

    /* Phase 2: Collect via poll() */
    alarm(2 + 2);   // global safety alarm
    struct pollfd _pfds[2];
    // set up pollfd for each pipe read-end
    
    while (_pending > 0 && !_global_tle) {
        poll(_pfds, 2, 4000);   // (2+2)*1000 ms
        for each ready fd:
            read(_pfds[i].fd, &_results[g_i], sizeof(TCResult));
            waitpid(_pids[i], &_st, 0);
            // if child wrote < sizeof(TCResult): check WIFSIGNALED → SEGV/MLE/FPE
    }
    // Kill any survivors → TLE
    alarm(0);
}

/* Phase 3: Print results */
for i in 0..1:
    json_escape(_results[i].status, ...)
    json_escape(_results[i].got, ...)
    printf("@@TC_RESULT__a3f9c2e1b8d4__START_%d\n", i+1);
    printf("{\n  \"status\": \"%s\",\n  \"got\": \"%s\",\n  \"detail\": \"%s\"\n}\n", ...);
    printf("@@TC_RESULT__a3f9c2e1b8d4__END_%d\n", i+1);
    fflush(stdout);

free(_results);
printf("@@TC_RESULT__a3f9c2e1b8d4__DONE\n");
fflush(stdout);
```

**Key C-specific details:**
- Test case input values are embedded as **C literals** in the fork code. `2` → `2`, `"hello"` → `"hello"`, `True` → `1`, `3.14` → `3.14`
- The `TCResult` struct is passed through the pipe as raw binary (`sizeof(TCResult)` bytes). The parent reads exactly `sizeof(TCResult)` bytes — if fewer bytes arrive (child crashed before writing), the parent checks `WIFSIGNALED()` to determine SEGV/MLE/FPE
- `poll()` has no FD_SETSIZE=1024 limit (unlike `select()`). Works for 200+ parallel TCs
- `calloc()` on heap for `_results` — avoids stack overflow from VLA for large N
- SIGALRM in child: child sets its own alarm, writes TLE result, `_exit(0)`. Parent's alarm is a safety backstop only

**C Harness — Stdio Mode**

`call_solve_and_capture` = result of `_build_c_stdio_call()`:
```c
{
    int _sp[2];
    pipe(_sp);
    // Feed _stdin_text into pipe, close write end (student gets EOF)
    write(_sp[1], _stdin_text, strlen(_stdin_text));
    close(_sp[1]);
    
    int _saved_in = dup(STDIN_FILENO);
    dup2(_sp[0], STDIN_FILENO);   // fd 0 → read end of pipe
    close(_sp[0]);
    
    FILE* _tmp = tmpfile();
    int _saved_out = dup(STDOUT_FILENO);
    dup2(fileno(_tmp), STDOUT_FILENO);  // fd 1 → tmpfile
    
    student_stdio_main(0, NULL);   // #define main student_stdio_main
    fflush(stdout);
    
    // Restore fds
    dup2(_saved_in, STDIN_FILENO);  close(_saved_in);
    dup2(_saved_out, STDOUT_FILENO); close(_saved_out);
    
    // Read captured output
    fread(_cbuf, 1, MAX_OUTPUT-1, _tmp);
    fclose(_tmp);
    // strip trailing whitespace
    strncpy(result.got, _cbuf, sizeof(result.got)-1);
}
```

The `#define main student_stdio_main` trick: the builder prepends this define to the student code, and follows with `#undef main`. So the student's `int main()` becomes `int student_stdio_main()`. The harness has its own real `int main(void)` that is unaffected.

**Stdio TC parameters:** `tc_params_comma = "const char* _stdin_text, "` so `run_tc_child` receives the stdin as a C string. Fork calls become:
```c
run_tc_child(_pfd[1], "2 3\n", 2, 256);   // TC1
run_tc_child(_pfd[1], "10 20\n", 2, 256); // TC2
```

### Java Harness — Function Mode

Java can't fork. It uses **one thread per TC** with a shared deadline.

The harness wraps student code as an inner class:
```java
public class Harness {
    static class Student {
        public int solve(int a, int b) {
            return a + b;
        }
    }
    // ... thread dispatch, result collection
```

**ThreadLocal stream dispatch** — the key innovation for parallel TC isolation:
```java
// DISPATCH_STREAM is set as System.out once at startup
static final ThreadLocal<PrintStream> TL_OUT = new ThreadLocal<>();
static final PrintStream DISPATCH_STREAM = new PrintStream(new OutputStream() {
    @Override public void write(int b) {
        PrintStream s = TL_OUT.get();  // routes to THIS thread's stream
        if (s != null) s.write(b);
    }
});
System.setOut(DISPATCH_STREAM);
```

Every worker thread sets `TL_OUT.set(capture)` before running. Any `System.out.println()` in student code routes through `DISPATCH_STREAM` → `TL_OUT.get()` → that thread's capture stream. Two threads can call `System.out.println()` simultaneously without interfering.

**`launchFunctionTC()` for TC1 (inputs=[2,3]):**
```java
Thread t = new Thread(() -> {
    TCResult result = new TCResult();
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream capture = new PrintStream(baos);
    TL_OUT.set(capture);
    TL_IN.set(null);  // no stdin in function mode
    
    try {
        forceGC();
        long memBefore = MX.getHeapMemoryUsage().getUsed();
        
        Class<?> _studentClass = freshStudentClass();  // new ClassLoader per TC!
        Class<?>[] paramClasses = resolveParamClasses(new String[]{"int","int"});
        Method m = _studentClass.getDeclaredMethod("solve", paramClasses);
        m.setAccessible(true);
        Constructor<?> _ctor = _studentClass.getDeclaredConstructor();
        _ctor.setAccessible(true);
        Object retVal = m.invoke(_ctor.newInstance(), new Object[]{(Object)(2), (Object)(3)});
        
        // Check memory usage
        long memUsedMb = (MX.getHeapMemoryUsage().getUsed() - memBefore) / (1024*1024);
        if (memUsedMb > 256) { result.status="MLE"; ... }
        
        String returned = retVal != null ? retVal.toString().trim() : "null";
        result.got    = returned;   // "5"
        result.status = "OUTPUT";
        
    } catch (OutOfMemoryError e) { result.status = "MLE"; ... }
    catch (InvocationTargetException e) { /* unwrap cause */ }
    catch (Exception e) { result.status = "ERROR"; }
    finally { TL_OUT.remove(); TL_IN.remove(); }
    
    resultRef.set(result);
});
t.setDaemon(true);  // JVM exits even if thread still running
return t;
```

**`freshStudentClass()`** — why it's needed:
```java
ClassLoader _loader = new ClassLoader(parent) {
    @Override
    protected Class<?> loadClass(String name, boolean resolve) {
        if ("Harness$Student".equals(name)) {
            return defineClass(name, STUDENT_CLASS_BYTES, 0, STUDENT_CLASS_BYTES.length);
        }
        return super.loadClass(name, resolve);
    }
};
return _loader.loadClass("Harness$Student");
```

Without this: if the student has `static int[] dp = new int[N]`, that static array persists across ALL test cases (class-level state). TC2 inherits TC1's dp. By loading a fresh class per TC, static fields are zeroed out for each TC independently.

**The `tc_runner_body` for N=2 TCs:**
```java
AtomicReference<TCResult>[] _resultRefs = new AtomicReference[2];
Thread[] _threads = new Thread[2];
for (int i = 0; i < 2; i++) _resultRefs[i] = new AtomicReference<>(null);

// Phase 1: Create threads (not started yet)
{
    Object[] _in0 = { (Object)(2), (Object)(3) };
    _threads[0] = launchFunctionTC(_in0, paramTypes, functionName, memoryLimitMb, _resultRefs[0]);
}
{
    Object[] _in1 = { (Object)(10), (Object)(20) };
    _threads[1] = launchFunctionTC(_in1, paramTypes, functionName, memoryLimitMb, _resultRefs[1]);
}

// Phase 2: Start ALL threads simultaneously — t=0 for both
for (int i = 0; i < 2; i++) _threads[i].start();

// Phase 3: Join with shared deadline
long _deadline = System.currentTimeMillis() + 2000 + 500;
for (int i = 0; i < 2; i++) {
    long _remaining = _deadline - System.currentTimeMillis();
    _threads[i].join(Math.max(1L, _remaining));   // min 1ms (join(0) = wait forever!)
    if (_threads[i].isAlive()) {
        boolean _dead = killThread(_threads[i]);
        _resultRefs[i].set(new TCResult() {{ status="TLE"; detail="Exceeded 2s"; }});
    }
}

// Phase 4: Print results
for (int i = 0; i < 2; i++) {
    TCResult _r = _resultRefs[i].get();
    printResult(i+1, _r);   // uses ORIGINAL_OUT, not DISPATCH_STREAM
}
```

**Join(max(1L, remaining)) fix:** Old code used `join(_remaining)` which could call `join(0)` meaning "wait forever". The fix ensures minimum 1ms join — if remaining time is 0 or negative, we still poll the thread once (1ms) and then check isAlive.

### Java Harness — Stdio Mode

Uses `launchStdioTC(stdinInput, resultRef)` instead of `launchFunctionTC`:
```java
InputStream fakeIn = new ByteArrayInputStream("2 3\n".getBytes());
TL_IN.set(fakeIn);   // DISPATCH_STDIN routes read() to this

Method m = _studentClass.getMethod("main", String[].class);
m.invoke(null, (Object) new String[]{});  // static method, null receiver
```

`DISPATCH_STDIN` routes all `System.in.read()` calls to the per-thread `TL_IN`, same pattern as stdout.

---

## Part 7: Judge0 Client — Submission and Callback

### Building the payload

```python
global_limit_s = math.ceil(max(tc_count, 1) / 200) * per_tc_limit_s + 5
# For N=2 TCs, per_tc=2s: ceil(2/200)*2+5 = 1*2+5 = 7s
# For N=500 TCs, per_tc=2s: ceil(500/200)*2+5 = 3*2+5 = 11s

payload = {
    "source_code":   base64.b64encode(harness_code.encode()).decode(),
    "language_id":   71,       # Python
    "cpu_time_limit": 7,
    "wall_time_limit": 9,      # +2s wall buffer
    "memory_limit":   4194304, # 4 GB (RLIMIT_AS, for Rosetta 2 compatibility)
    "stdin":          "",
    "base64_encoded": True,
    "enable_per_process_and_thread_time_limit":   True,  # no cgroups needed
    "enable_per_process_and_thread_memory_limit": True,
    "number_of_processes": 220,   # harness forks up to 200 children + overhead
    "callback_url": "http://172.17.0.5:54321/result",  # this worker's callback URL
}
```

**Why memory_limit=4GB:** On Mac Docker Desktop, Rosetta 2's JIT needs gigabytes of virtual address space. Setting RLIMIT_AS to 4GB prevents spurious MLE kills. On Linux/EC2, actual physical memory is bounded by the container's `mem_limit=1g`.

**Why `enable_per_process_and_thread_*=True`:** These flags make Judge0 use RLIMIT_CPU/RLIMIT_AS per-process instead of cgroup limits. Required on Mac (no cgroup v1). On Linux this is valid too — the harness sets its own RLIMIT_AS per child anyway.

**Why `number_of_processes=220`:** Judge0's sandbox default is 60. The harness forks up to 200 children per batch. This overrides the per-submission process limit.

### Circuit breaker check

Before the POST:
```python
if _judge0_breaker.is_open():
    raise RuntimeError("Judge0 circuit breaker open...")
```

The breaker is module-level singleton — shared across ALL `Judge0Client` instances (all 48 concurrent coroutines). Opens after 10 consecutive 5xx/connection errors, stays open 30s.

### The HTTP POST

```python
resp = requests.post(
    "http://server:2358/submissions?base64_encoded=true&wait=false",
    json=payload,
    headers=headers,
    timeout=120,   # queue drain can take >10s at 1000 concurrent users
)
# Response: {"token": "abc123-..."}
token = resp.json()["token"]
```

**`wait=false`:** Don't wait for execution to complete. Judge0 returns a token immediately and queues the job in its Resque queue.

**Internal Judge0 flow:**
1. Rails/Puma receives the POST
2. Validates the submission, creates a record in PostgreSQL
3. Enqueues to Resque (Redis `resque:queue:default`)
4. Returns `{"token": "abc123-..."}`
5. A Resque worker (the `workers` Docker service running `./scripts/workers`) picks up the job
6. Resque worker calls isolate to compile and run the harness in a Linux sandbox
7. When done, Rails fires a PUT to `callback_url` with the full result JSON

### The Callback Server — waiting for the result

```python
evt = self.callback_server.register(token)
# register() atomically:
#   - creates threading.Event
#   - stores event in _events[token]
#   - IF result already in _results[token] (early arrival), pre-sets event

fired = evt.wait(timeout=7+120)   # global_limit + 120s buffer for queue drain
```

The 120s extra buffer: at 1000 concurrent users, Judge0's Resque queue can hold a job for 60-90s before execution even starts. Without this buffer, the callback timeout fires before the job finishes, causing spurious SYSTEM_ERRORs.

**Race condition handled:** Judge0 can fire the callback webhook **before** `register()` is called. For example, a compilation error resolves in ~100ms — faster than Python's GIL round-trip after `requests.post()` returns. The `CallbackServer._deliver()` buffers the result. `register()` checks the buffer:

```python
def register(self, token):
    evt = threading.Event()
    with self._lock:
        self._events[token] = evt
        if token in self._results:
            evt.set()   # already arrived → signal immediately
    return evt
```

**The callback HTTP handler:**
```python
class _Handler(BaseHTTPRequestHandler):
    def do_PUT(self): self._handle()
    def do_POST(self): self._handle()
    
    def _handle(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        self.send_response(200)   # respond immediately — Judge0 doesn't retry
        self.end_headers()
        
        payload = json.loads(body)
        token = payload.get("token", "")
        server_ref._deliver(token, payload)   # signal waiting coroutine
```

The server uses a `ThreadPoolExecutor(32)` for connection handling — under 1000 concurrent webhooks, this prevents spawning 1000 OS threads (which would consume ~8GB of stack space on Linux).

**Poll fallback:** If callback never fires within the timeout:
```python
poll_result = self._poll_fallback(token, global_limit_s)
# Polls GET /submissions/{token} every 5s for up to 60s
# Returns result if status not in (1=InQueue, 2=Processing)
# Returns None if still pending → raises TimeoutError → worker requeues
```

### Parsing the webhook payload

```python
def _parse_webhook_payload(data: dict) -> Judge0Result:
    status_id = data.get("status", {}).get("id", 11)
    return Judge0Result(
        stdout         = base64.b64decode(data.get("stdout", "") or "").decode(errors="replace"),
        stderr         = base64.b64decode(data.get("stderr", "") or "").decode(errors="replace"),
        status_str     = JUDGE0_STATUS.get(status_id, "Unknown"),  # "Accepted", "TLE", etc.
        status_id      = status_id,
        compile_output = base64.b64decode(data.get("compile_output", "") or "").decode(errors="replace"),
        time_taken_s   = float(data.get("time")) if data.get("time") else None,
        memory_kb      = data.get("memory"),
        token          = token,
    )
```

Stdout is base64-encoded by Judge0 (because it may contain binary). Decoded here into a Python string.

---

## Part 8: Output Parser — Extracting TC Results

### Top-level: `parse_judge0_response()`

First checks Judge0-level failures:
```python
if judge0_status == "Time Limit Exceeded":
    # Judge0's own time limit fired (harness exceeded global_limit_s)
    # No harness output at all → ALL TCs = TLE
    return ParsedSubmission(tc_results=[TCResult(i, "TLE", ...) for i in 1..N])

if judge0_status in ("Compilation Error", "Internal Error"):
    # compile_output has the error message
    # ALL TCs = ERROR with compile error detail (trimmed to 500 chars)
    return ParsedSubmission(tc_results=[TCResult(i, "ERROR", detail=compile_output) for i in 1..N])

# Otherwise: parse harness output
parser = OutputParser(stdout, session_id, N, expected_values)
return parser.parse()
```

### `OutputParser.parse()`

```python
def parse(self) -> ParsedSubmission:
    result = ParsedSubmission(tc_results=[], total=N)
    found_tcs = set()
    
    # Check DONE marker presence
    result.global_tle = (f"{self.delim}DONE" not in self.raw)
    
    # Extract all TC blocks with regex
    pattern = re.compile(
        rf"{re.escape(self.delim)}START_(\d+)\n(.*?){re.escape(self.delim)}END_\1",
        re.DOTALL   # . matches newlines
    )
    
    for match in pattern.finditer(self.raw):
        tc_num  = int(match.group(1))    # 1-indexed
        content = match.group(2).strip() # JSON content between START and END
        found_tcs.add(tc_num)
        result.tc_results.append(self._parse_tc_block(tc_num, content))
    
    # Handle missing TCs
    for i in range(1, N+1):
        if i not in found_tcs:
            if result.global_tle:
                result.tc_results.append(TCResult(i, "TLE", detail="TC not reached — global TLE"))
            else:
                result.tc_results.append(TCResult(i, "MISSING", detail="TC not reached — crash"))
    
    result.tc_results.sort(key=lambda r: r.tc_num)
    result.score = sum(1 for r in result.tc_results if r.status == "PASS")
    return result
```

### `_parse_tc_block()` — the critical verdict logic

```python
def _parse_tc_block(self, tc_num, content):
    try:
        data = json.loads(content)
        # {"status": "OUTPUT", "got": "5", "detail": ""}
        
        status = data.get("status", "ERROR")
        
        # SECURITY: reject PASS/FAIL from harness — harness NEVER emits them legitimately
        # A student who writes fake TCResult structs to the pipe in C
        # could try to inject status="PASS" — this rejects it
        if status not in _HARNESS_STATUSES:  # {"TLE","MLE","SEGV","FPE","ERROR","OUTPUT"}
            status = "ERROR"
        
        if status == "OUTPUT":
            # This is the normal case — harness emits "OUTPUT", we compare here
            expected_str = str(self.expected_values[tc_num-1]).strip()
            got_str      = str(data.get("got", "")).strip()
            
            # Comparison: exact string match OR float-tolerant
            passed = (got_str == expected_str) or _num_equal(got_str, expected_str)
            
            return TCResult(
                tc_num   = tc_num,
                status   = "PASS" if passed else "FAIL",
                got      = got_str,       # "5"
                expected = expected_str,  # "5"
                detail   = data.get("detail", ""),
                warning  = data.get("warning", ""),
            )
        
        # For TLE/MLE/SEGV/FPE/ERROR — pass through directly
        return TCResult(tc_num=tc_num, status=status, got=..., detail=...)
    
    except json.JSONDecodeError:
        # Student corrupted the harness output (printed garbage between delimiters)
        # Scan raw text for harness statuses — never assign PASS/FAIL from raw text
        status = "ERROR"
        for s in _HARNESS_STATUSES:
            if s in content:
                status = s; break
        return TCResult(tc_num=tc_num, status=status, detail=f"Output parse error: {content[:100]}")
```

**Float-tolerant comparison:**
```python
def _num_equal(a_str, b_str):
    a, b = float(a_str), float(b_str)
    if a != a or b != b: return False   # NaN check
    return abs(a-b) <= max(1e-9, abs(b)*1e-6)
    # "0.30000000000000004" vs "0.3" → passes (relative error < 1e-6)
```

---

## Part 9: Storing Result + Infrastructure Failure Detection

### Infrastructure failure detection (before storing)

```python
if _is_infrastructure_failure(parsed):
    # All TCs are ERROR with keywords: "fork() failed", "RLIMIT_NPROC",
    # "Cannot allocate memory", "calloc failed", etc.
    # OR: parsed.tc_results is empty (harness produced no output)
    # → Not the student's fault. Worker requeues.
    return GradingResult(needs_requeue=True, ...)
```

### `queue.store_result()` — one Redis pipeline call

```python
def store_result(self, ticket_id, result, idem_key=""):
    result_json = json.dumps(result)
    # e.g.: '{"score":2,"total":2,"tc_results":[{"tc_num":1,"status":"PASS",...}]}'
    
    pipe = self.r.pipeline()
    pipe.setex(f"judge0:result:{ticket_id}", 7200, result_json)
    pipe.delete(f"judge0:pending_deadline:{ticket_id}")   # job done, cancel timeout
    if idem_key and "system_error" in result:
        pipe.delete(idem_key)   # release idempotency so student can resubmit
    pipe.publish(f"judge0:notify:{ticket_id}", result_json)  # triggers SSE
    pipe.execute()   # all 3-4 ops in one round-trip
```

The `PUBLISH` on `judge0:notify:{ticket_id}` is the trigger for SSE delivery. Any subscriber on that channel receives the result JSON immediately.

### `queue.ack()` — atomic Lua script

```lua
-- KEYS[1] = "judge0:inflight:{ticket_id}"
-- KEYS[2] = "judge0:jobs:processing"
local raw = redis.call('GET', KEYS[1])
if raw then
    redis.call('LREM', KEYS[2], 1, raw)   -- remove from PROCESSING list
    redis.call('DEL',  KEYS[1])            -- delete inflight key
end
return raw
```

This is atomic — no window between the GET and LREM where the reconciler could race.

### Judge0 submission cleanup

```python
self.judge0.delete_submission(judge0_result.token)
# → DELETE http://server:2358/submissions/{token}
# Best-effort, never raises. Keeps PostgreSQL submissions table from growing.
```

---

## Part 10: SSE Delivery — The Client Gets Their Result

The client opened `GET /results/stream/{ticket_id}` immediately after submit (or even before the result arrives — the SSE stream waits).

### `results_stream()` in `api.py`

```python
def _generate():
    # Check if result already stored (handles race: result arrived before stream opened)
    existing = r.get(f"judge0:result:{ticket_id}")
    if existing:
        payload = existing.decode()
        yield f"event: result\ndata: {payload}\n\n"
        return
    
    # Subscribe to pub/sub channel
    pubsub = r.pubsub()
    pubsub.subscribe(f"judge0:notify:{ticket_id}")
    
    deadline = time.monotonic() + 1800   # 30 min timeout
    try:
        while time.monotonic() < deadline:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            # get_message with timeout=1.0 blocks up to 1 second
            
            if msg and msg["type"] == "message":
                data = msg["data"].decode()
                yield f"event: result\ndata: {data}\n\n"
                return   # stream closes after first result event
            
            # Keep connection alive through load balancers
            yield ": heartbeat\n\n"
        
        # 30-min timeout
        yield f"event: result\ndata: {json.dumps({'system_error': 'Timed out'})}\n\n"
    finally:
        pubsub.unsubscribe(...)
        pubsub.close()

return Response(_generate(), mimetype="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
```

**What the client receives over the wire (raw HTTP stream):**
```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no

: heartbeat

: heartbeat

: heartbeat

event: result
data: {"score":2,"total":2,"global_tle":false,"tc_results":[{"tc_num":1,"status":"PASS","got":"5","expected":"5","detail":"","warning":""},{"tc_num":2,"status":"PASS","got":"30","expected":"30","detail":"","warning":""}],"time_taken_s":0.123,"memory_kb":8192}

```

The `X-Accel-Buffering: no` header disables nginx proxy buffering so heartbeats and events reach the client immediately without nginx holding them in a buffer.

**Why gunicorn uses gevent:** SSE is a long-lived connection (up to 30 min). With normal sync gunicorn workers, each SSE stream occupies a worker thread. With `--workers=2 --worker-connections=1000`, each gunicorn process can handle 1000 concurrent SSE streams as gevent greenlets — cooperatively switching when waiting on Redis pub/sub. No OS threads, no 8GB RAM spike.

---

## Complete Data Flow Summary (One Submission, Two TCs)

```
CLIENT
  POST /submit → api.py validates → enqueue QueuedJob to Redis normal queue
  202 {ticket_id}
  
  GET /results/stream/{ticket_id} → SSE stream opens → subscribes to pub/sub channel

WORKER (worker_async.py)
  BLPOP → dequeues job → moves to PROCESSING_QUEUE + sets inflight TTL

AUTOGRADER
  SecurityChecker.check() → no violations → proceed
  _detect_function_name() → "solve" found → use as-is
  sanitize_for_injection() → strip trailing whitespace
  HarnessBuilder.build() → template.format() → complete Python source (200+ lines)
    contains: student code, 2 fork calls, poll loop, delimiter protocol
    test inputs embedded: [2,3] and [10,20]
    session_id: "a3f9c2e1b8d4"

JUDGE0 CLIENT
  base64(harness_code) → POST /submissions?wait=false
  → Judge0 returns {token: "abc123"}
  → register token with CallbackServer → threading.Event created

JUDGE0 SANDBOX (isolate)
  Python interpreter runs harness
  fork() × 2 → 2 children simultaneously
  Child 1: alarm(2), call solve(2,3)=5, write {"status":"OUTPUT","got":"5"} to pipe, _exit(0)
  Child 2: alarm(2), call solve(10,20)=30, write {"status":"OUTPUT","got":"30"} to pipe, _exit(0)
  Parent: poll() collects both results in ~0ms
  Parent: prints @@TC_RESULT__a3f9c2e1b8d4__START_1...END_1, START_2...END_2, DONE
  
JUDGE0 → PUT http://172.17.0.5:54321/result {token, stdout(b64), status, time, memory}

CALLBACK SERVER
  Handler decodes body → _deliver("abc123", payload)
  → threading.Event.set() → unblocks waiting coroutine

JUDGE0 CLIENT
  evt.wait() returns → pop_result("abc123") → parse payload
  Judge0Result: stdout="@@TC_RESULT__...START_1\n{...}\n...DONE\n", status_str="Accepted"

AUTOGRADER
  _is_infrastructure_failure() → False (TCs present, no resource errors)
  parse_judge0_response() → OutputParser.parse()
    regex extracts TC1 block: {"status":"OUTPUT","got":"5"}
    status=="OUTPUT" → compare "5" == "5" → PASS
    regex extracts TC2 block: {"status":"OUTPUT","got":"30"}
    compare "30" == "30" → PASS
    DONE marker found → global_tle=False
  ParsedSubmission: score=2, total=2, tc_results=[PASS, PASS]
  
  delete_submission("abc123") → DELETE /submissions/abc123 (cleanup)
  
  GradingResult: score=2/2, no system_error, no security_error, needs_requeue=False

WORKER
  result_to_dict(result) → {"score":2,"total":2,"tc_results":[...]}
  queue.store_result(ticket_id, result_dict, idem_key)
    → SETEX judge0:result:{ticket_id} 7200 "{...}"
    → DEL judge0:pending_deadline:{ticket_id}
    → PUBLISH judge0:notify:{ticket_id} "{...}"
  queue.ack(job) → Lua: LREM PROCESSING_QUEUE + DEL inflight key

API (SSE stream)
  pubsub.get_message() → receives published result JSON
  yield "event: result\ndata: {score:2,total:2,...}\n\n"
  stream closes

CLIENT
  receives SSE event → parses JSON → shows student "2/2 PASS"
```

That is every layer, every data structure, every Redis key, every network call — from the moment a student hits submit to the moment they see their score.