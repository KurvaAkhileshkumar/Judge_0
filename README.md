# Classroom Autograder — Harness-Based Single Job Architecture

## What This Solves

| Problem | Solution |
|---|---|
| N jobs per submission (queue explosion) | 1 job per submission via harness |
| No per-TC time limit in single job | signal.alarm (Python) / fork+alarm (C/C++) / Thread.join (Java) |
| Segfault kills entire job (C/C++) | fork() per TC — child crash isolated, parent continues |
| Student overrides signal handler | Blocked via safe_signal wrapper + Judge0 global limit as fallback |
| Student reads test cases from harness | open() blocked + unique session delimiter |
| Student poisons output with print() | Unique delimiter only your parser knows |
| Student calls sys.exit() | Blocked at runtime |
| Memory limit shared across TCs | Per-child RLIMIT_AS (C/C++) / Runtime delta (Java) |
| Partial output on global TLE | Parser handles MISSING TCs explicitly |
| Output parse corruption | JSON blocks between unique delimiters |

---

## Architecture

```
Submission
    │
    ▼
SecurityChecker          ← Static code analysis before anything runs
    │ blocked → return SecurityError
    ▼
HarnessBuilder           ← Generates complete source file per language
    │
    ▼
Judge0Client             ← Submits, sets global_limit = (per_tc × N) + overhead
    │
    ▼
OutputParser             ← Extracts TC blocks by unique delimiter, handles partial output
    │
    ▼
GradingResult            ← Per-TC: PASS/FAIL/TLE/MLE/SEGV/FPE/ERROR/MISSING
```

---

## Per-Language Strategy

### Python
- **Timeout**: `signal.SIGALRM` — OS-level interrupt, fires inside `solve()` even in infinite loop
- **Signal override**: Replaced `signal.signal` with safe wrapper that ignores SIGALRM override attempts
- **File read**: Replaced `open()` with safe version that blocks harness file access
- **sys.exit()**: Replaced with exception-raising version
- **Memory**: `resource.getrusage()` delta per TC

### C
- **Timeout**: `fork()` per TC + `setitimer(ITIMER_REAL)` in parent + `waitpid()` with EINTR detection
- **Segfault**: Child crashes, parent detects via `WIFSIGNALED(status)` + `WTERMSIG == SIGSEGV`
- **Memory**: `RLIMIT_AS` set on child process before `solve()` runs
- **Isolation**: Every TC is a separate child process — complete isolation

### C++
- Same fork() strategy as C
- Additionally catches `std::bad_alloc` (OOM inside solve) in child before writing result
- Redirects `std::cout` in child to capture return-value-based output

### Java
- **Timeout**: `Thread.join(timeoutMs)` — if thread still alive after timeout, `interrupt()` + mark TLE
- **Memory**: `Runtime.totalMemory() - freeMemory()` delta (approximate)
- **System.exit()**: Blocked via `SecurityManager.checkExit()`
- **Limitation**: Tight infinite loops in Java may not respond to `Thread.interrupt()` — Judge0 global limit is the safety net

---

## Time Limit Logic

```
per_tc_limit_s  = 2s   (your config per problem)
tc_count        = 10
overhead_s      = 5    (harness startup, imports, JVM warmup for Java)

Judge0 global limit = (2 × 10) + 5 = 25s

Inside harness:
  TC1: signal.alarm(2) → solve() → 0.3s → PASS → alarm cancelled
  TC2: signal.alarm(2) → solve() → 2.0s → TLE fired → move to TC3
  TC3: signal.alarm(2) → solve() → 0.8s → PASS → alarm cancelled
  ...
  Total time: ~5s (well under 25s global)

If student bypasses per-TC timer:
  TC2 hangs forever → Judge0 global fires at 25s → entire job killed
  Parser sees MISSING for TC2 onward
```

---

## File Structure

```
autograder/
├── autograder.py              ← Main entry point
├── core/
│   ├── harness_builder.py     ← Generates filled harness per language
│   ├── output_parser.py       ← Parses structured harness output
│   └── judge0_client.py       ← Submits to Judge0, polls, normalizes status
├── harnesses/
│   ├── python_harness.py      ← Python harness template
│   ├── c_harness.c            ← C harness template (fork-based)
│   ├── cpp_harness.cpp        ← C++ harness template (fork-based)
│   └── java_harness.java      ← Java harness template (thread-based)
└── security/
    └── security.py            ← Static code analysis + sanitization
```

---

## Quick Start

```python
from autograder import Autograder, Submission
from core.harness_builder import TestCase
from core.judge0_client import Judge0Config

grader = Autograder(Judge0Config(
    base_url = "https://judge0.yourdomain.com",
    api_key  = "your-key",
))

result = grader.grade(Submission(
    student_id   = "student_001",
    language     = "python",
    student_code = "def solve(a, b):\n    return a + b",
    test_cases   = [
        TestCase(inputs=[2, 3],   expected=5),
        TestCase(inputs=[10, 20], expected=30),
    ],
    per_tc_limit_s  = 2,
    memory_limit_mb = 128,
))

print(result.summary())
# Student: student_001
# Score:   2/2
#   TC1: PASS
#   TC2: PASS
```

---

## Known Limitations

1. **Java tight loops**: `Thread.interrupt()` may not break native tight loops. Judge0 global limit handles this but all remaining TCs show as MISSING.
2. **Memory tracking**: Java memory delta is approximate due to GC. C/C++ `RLIMIT_AS` is hard.
3. **Function-based only**: Student must write `solve()`, not stdin/stdout programs. Problem statements must specify this.
4. **Static security checks**: Regex-based, not AST-based. Sophisticated bypass attempts may get through to Judge0's sandbox (which is the real security boundary anyway).
