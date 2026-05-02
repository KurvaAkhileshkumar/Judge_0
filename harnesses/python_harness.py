"""
Python Harness — v3  (Parallel Execution)
==========================================
All test cases now execute simultaneously via os.fork().

Architecture
────────────
Sequential (old):  TC1 → TC2 → TC3 → ... → TCn   time = sum(TC_times)
Parallel   (new):  TC1 ─┐
                   TC2 ─┼─ all start at t=0       time = max(TC_times)
                   TC3 ─┘

Mechanism
─────────
1. FORK PHASE   — one child process per TC, all forked simultaneously.
                  Children inherit parent memory (solve/student code).
2. EXECUTE PHASE— each child runs its single TC with its own SIGALRM timer.
                  If child TLEs, its alarm handler writes TLE result and exits.
                  If child segfaults, pipe closes (EOF) and parent detects it.
3. COLLECT PHASE— parent uses select() to wait on all pipes simultaneously.
                  Results arrive as children finish (fastest first).
4. EMIT PHASE   — results printed in original TC order using DELIM protocol.

Fallback
────────
If RLIMIT_NPROC is too low to fork N children, automatically falls back to
the sequential mode (v2 behaviour) and sets warning in each result.

Global time limit
─────────────────
Old (sequential): Judge0 limit = N × per_tc_limit_s + overhead
New (parallel):   Judge0 limit = per_tc_limit_s + overhead  (10× smaller)
"""

import os
import sys
import io
import signal
import select
import resource
import traceback
import json
import builtins
import time

MODE  = "{mode}"         # "function" or "stdio"
DELIM = "@@TC_RESULT__{session_id}__"

# ── SECURITY: save real references before monkey-patching ──────────────
_real_open   = open
_real_signal = signal.signal
_HARNESS_FILE = __file__

# ── SECURITY: block harness file read ──────────────────────────────────
def _safe_open(file, mode="r", *args, **kwargs):
    try:
        if os.path.abspath(str(file)) == os.path.abspath(_HARNESS_FILE):
            raise PermissionError("Access denied")
    except PermissionError:
        raise
    except Exception:
        raise PermissionError("Access denied")
    return _real_open(file, mode, *args, **kwargs)

builtins.open = _safe_open

# ── SECURITY: block signal handler override ────────────────────────────
_signal_override_attempted = False

def _safe_signal(signum, handler):
    global _signal_override_attempted
    if signum == signal.SIGALRM:
        _signal_override_attempted = True
        return
    return _real_signal(signum, handler)

signal.signal = _safe_signal

# ── SECURITY: block sys.exit ───────────────────────────────────────────
def _safe_exit(*args):
    raise SystemExit("__HARNESS_BLOCKED__")

sys.exit      = _safe_exit
builtins.exit = _safe_exit
builtins.quit = _safe_exit

# ════════════════════════════════════════════════════════════════════════
# STUDENT CODE INJECTED BELOW
# For function mode: defines solve() which children call directly (inherited via fork).
# For stdio mode:    code is NOT executed here at module level.
#                    The real source is stored in _STUDENT_SOURCE and exec()'d
#                    inside each child process with fake stdin/stdout per TC.
# ════════════════════════════════════════════════════════════════════════

{student_code}

# ════════════════════════════════════════════════════════════════════════
# TEST RUNNER — student code ends above
# ════════════════════════════════════════════════════════════════════════

# Raw student source stored for stdio re-exec in child processes.
_STUDENT_SOURCE = """{student_code_raw}"""

def _safe_tb():
    lines = traceback.format_exc().strip().splitlines()
    return " | ".join(lines[-2:])


# FIX-10: float comparison with relative+absolute epsilon tolerance.
# Strict string equality fails for 0.1+0.2 → "0.30000000000000004" ≠ "0.3".
# Only applies when BOTH got and expected parse as finite floats.
def _num_equal(a_str, b_str):
    try:
        a, b = float(a_str), float(b_str)
        if a != a or b != b:   # NaN — never equal
            return False
        diff = abs(a - b)
        return diff <= max(1e-9, abs(b) * 1e-6)
    except (ValueError, TypeError):
        return False

# ── CHILD PROCESS LOGIC ────────────────────────────────────────────────
# These functions run INSIDE forked children.
# Children inherit parent memory via fork() — solve() is already available.
# Each child has its own SIGALRM countdown. When it fires, TLE result is
# written to the pipe and the child exits cleanly.

def _child_run(tc, write_fd, per_tc_limit_s, mem_limit_mb):
    """
    Entry point for every child process (both function and stdio mode).
    Sets up:
      - Per-child memory limit via RLIMIT_AS
      - Per-child SIGALRM timeout via _real_signal (bypasses parent's monkey-patch)
    Then runs the TC and writes exactly one JSON result to write_fd.
    """

    # Apply per-child memory limit
    if mem_limit_mb > 0:
        try:
            rlim = mem_limit_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (rlim, rlim))
        except Exception:
            pass

    # Per-child TLE: use _real_signal (parent replaced signal.signal with _safe_signal)
    def _tle(s, f):
        raise TimeoutError("TLE")

    _real_signal(signal.SIGALRM, _tle)
    signal.alarm(per_tc_limit_s)

    result = None
    try:
        if MODE == "stdio":
            result = _child_run_stdio(tc)
        else:
            result = _child_run_function(tc)
        signal.alarm(0)
    except TimeoutError:
        result = {{"status": "TLE", "detail": f"Exceeded {{per_tc_limit_s}}s"}}
    except MemoryError:
        signal.alarm(0)
        result = {{"status": "MLE", "detail": "Memory limit exceeded"}}
    except SystemExit as e:
        signal.alarm(0)
        msg = "Called sys.exit()" if "__HARNESS_BLOCKED__" in str(e) else "SystemExit"
        result = {{"status": "ERROR", "detail": msg}}
    except Exception:
        signal.alarm(0)
        result = {{"status": "ERROR", "detail": _safe_tb()}}

    # Write result to pipe as JSON bytes, then close and exit.
    # Parent detects EOF when this process exits.
    try:
        data = json.dumps(result).encode()
        os.write(write_fd, data)
    except Exception:
        pass
    finally:
        os.close(write_fd)

    os._exit(0)  # skip atexit handlers — child is done


def _child_run_function(tc):
    """
    Runs solve(*inputs) and compares return value to expected.
    solve() is inherited from parent via fork — no re-exec needed.
    """
    actual   = solve(*tc["input"])
    got_s    = str(actual).strip()
    exp_s    = str(tc["expected"]).strip()
    # FIX-10: try numeric tolerance before strict equality
    passed   = (got_s == exp_s) or _num_equal(got_s, exp_s)
    return {{
        "status":   "PASS" if passed else "FAIL",
        "got":      got_s,
        "expected": exp_s,
    }}


def _child_run_stdio(tc):
    """
    Re-executes the student source in a fresh namespace per TC.
    Replaces sys.stdin/stdout in the child — safe because it's a forked process.
    """
    fake_stdin  = io.StringIO(tc.get("stdin_text", ""))
    fake_stdout = io.StringIO()
    sys.stdin   = fake_stdin
    sys.stdout  = fake_stdout

    ns = {{"__name__": "__main__", "open": _safe_open, "exit": _safe_exit, "quit": _safe_exit}}
    exec(compile(_STUDENT_SOURCE, "<student>", "exec"), ns)

    got      = fake_stdout.getvalue().strip()
    expected = str(tc["expected"]).strip()
    passed   = (got == expected) or _num_equal(got, expected)
    return {{
        "status":   "PASS" if passed else "FAIL",
        "got":      got,
        "expected": expected,
    }}


# ── NPROC CHECK ────────────────────────────────────────────────────────

def _can_fork_n(n):
    """
    Check RLIMIT_NPROC to confirm we can safely fork n children.
    Returns True if parallel is safe, False to fall back to sequential.
    """
    try:
        soft, _ = resource.getrlimit(resource.RLIMIT_NPROC)
        if soft == resource.RLIM_INFINITY:
            return True
        # count current threads/processes in this process group
        current = len(os.listdir('/proc/self/task'))
        return (current + n + 2) <= soft  # +2 safety margin
    except Exception:
        return True  # assume ok if we can't check


# ── PARALLEL RUNNER ────────────────────────────────────────────────────

def _run_all_parallel(test_cases, per_tc_limit_s, memory_limit_mb):
    """
    Forks one child per TC simultaneously.
    Uses select() to collect results as children finish.
    Total job time = max(TC_times)  not  sum(TC_times).
    """
    _real_stdout = sys.stdout

    n            = len(test_cases)
    # FIX-1: each child gets the FULL per-TC limit, not total÷N.
    # Dividing by N (e.g. 256MB÷10=25MB) was far too restrictive and caused
    # legitimate solutions to OOM. Judge0 enforces the limit per-process via
    # enable_per_process_and_thread_memory_limit, so each child is already
    # capped at memory_limit_mb independently by isolate.
    mem_per_child = memory_limit_mb

    # ── FORK PHASE: launch all children at t=0 ──────────────────────────
    jobs = []   # (pid, read_fd, tc_index)

    for i, tc in enumerate(test_cases):
        r_fd, w_fd = os.pipe()
        try:
            pid = os.fork()
        except OSError:
            # RLIMIT_NPROC hit mid-fork — kill forked children and fall back
            os.close(r_fd)
            os.close(w_fd)
            for p, rf, _ in jobs:
                try:
                    os.kill(p, signal.SIGKILL)
                    os.waitpid(p, 0)
                    os.close(rf)
                except Exception:
                    pass
            _run_all_sequential(test_cases, per_tc_limit_s, memory_limit_mb,
                                warning="fork() failed mid-launch (RLIMIT_NPROC). "
                                        "Sequential fallback used.")
            return

        if pid == 0:
            # CHILD: close read end, run TC, write result, exit
            os.close(r_fd)
            _child_run(tc, w_fd, per_tc_limit_s, mem_per_child)
            os._exit(0)  # unreachable but defensive
        else:
            # PARENT: close write end, track this child
            os.close(w_fd)
            jobs.append((pid, r_fd, i))

    # ── COLLECT PHASE: gather results using select() ─────────────────────
    # Deadline = per_tc_limit_s + 1.5s grace for process/OS overhead.
    # Children handle their own TLE internally via SIGALRM, so they will
    # always write a result within per_tc_limit_s.  The 1.5s is a safety net
    # for the unlikely case where a child's alarm handler itself stalls.
    deadline      = time.monotonic() + per_tc_limit_s + 1.5
    results       = {{}}                    # tc_index -> result dict
    chunks        = {{i: [] for _, _, i in jobs}}  # tc_index -> list of bytes
    fd_to_job     = {{r_fd: (pid, i) for pid, r_fd, i in jobs}}
    pending_fds   = set(fd_to_job.keys())

    while pending_fds:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            # Global deadline passed — kill everything still running
            for r_fd in list(pending_fds):
                pid, idx = fd_to_job[r_fd]
                try:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                except Exception:
                    pass
                try:
                    os.close(r_fd)
                except Exception:
                    pass
                results[idx] = {{"status": "TLE",
                                 "detail": f"Exceeded {{per_tc_limit_s}}s (global deadline)"}}
            pending_fds.clear()
            break

        try:
            readable, _, _ = select.select(list(pending_fds), [], [], remaining)
        except Exception:
            break

        if not readable:
            # Timeout from select() — no child made progress within remaining time
            for r_fd in list(pending_fds):
                pid, idx = fd_to_job[r_fd]
                try:
                    os.kill(pid, signal.SIGKILL)
                    os.waitpid(pid, 0)
                except Exception:
                    pass
                try:
                    os.close(r_fd)
                except Exception:
                    pass
                results[idx] = {{"status": "TLE",
                                 "detail": f"Exceeded {{per_tc_limit_s}}s"}}
            pending_fds.clear()
            break

        for r_fd in readable:
            pid, idx = fd_to_job[r_fd]
            try:
                chunk = os.read(r_fd, 65536)
            except Exception:
                chunk = b""

            if chunk:
                chunks[idx].append(chunk)
            else:
                # EOF — child closed its write end (finished or crashed)
                pending_fds.discard(r_fd)
                try:
                    os.close(r_fd)
                except Exception:
                    pass
                try:
                    _, exit_status = os.waitpid(pid, 0)
                except Exception:
                    exit_status = 0

                raw = b"".join(chunks.get(idx, []))
                if raw:
                    try:
                        results[idx] = json.loads(raw)
                    except Exception:
                        results[idx] = {{"status": "ERROR",
                                         "detail": f"Result parse failed: {{raw[:80]!r}}"}}
                else:
                    # Child wrote nothing — likely crashed before writing
                    import os as _os
                    if _os.WIFSIGNALED(exit_status):
                        sig = _os.WTERMSIG(exit_status)
                        status_map = {{
                            signal.SIGSEGV: ("SEGV", "Segmentation fault"),
                            signal.SIGFPE:  ("FPE",  "Floating point exception"),
                            signal.SIGKILL: ("MLE",  "Memory limit exceeded (SIGKILL)"),
                        }}
                        st, detail = status_map.get(sig, ("ERROR", f"Killed by signal {{sig}}"))
                        results[idx] = {{"status": st, "detail": detail}}
                    else:
                        results[idx] = {{"status": "ERROR",
                                         "detail": "Child exited without writing result"}}

    # ── EMIT PHASE: print results in original TC order ────────────────────
    for i in range(n):
        result = results.get(i, {{"status": "ERROR", "detail": "Result not collected"}})
        if _signal_override_attempted:
            result["warning"] = "signal_override_attempted"
        _real_stdout.write(f"{{DELIM}}START_{{i+1}}\n")
        _real_stdout.write(json.dumps(result) + "\n")
        _real_stdout.write(f"{{DELIM}}END_{{i+1}}\n")
        _real_stdout.flush()

    _real_stdout.write(f"{{DELIM}}DONE\n")
    _real_stdout.flush()


# ── SEQUENTIAL FALLBACK ────────────────────────────────────────────────
# Used when RLIMIT_NPROC is too restrictive for parallel execution.

def _run_all_sequential(test_cases, per_tc_limit_s, memory_limit_mb, warning=None):
    """Original v2 sequential implementation used as fallback."""
    _real_stdout = sys.stdout

    for i, tc in enumerate(test_cases):
        result = _run_tc_sequential(tc, per_tc_limit_s, memory_limit_mb)
        if warning:
            result["warning"] = warning
        if _signal_override_attempted:
            result["warning"] = result.get("warning", "") + " | signal_override_attempted"
        _real_stdout.write(f"{{DELIM}}START_{{i+1}}\n")
        _real_stdout.write(json.dumps(result) + "\n")
        _real_stdout.write(f"{{DELIM}}END_{{i+1}}\n")
        _real_stdout.flush()

    _real_stdout.write(f"{{DELIM}}DONE\n")
    _real_stdout.flush()


def _run_tc_sequential(tc, per_tc_limit_s, memory_limit_mb):
    """Single TC execution — sequential fallback mode."""
    import resource as _res

    mem_before = 0
    try:
        mem_before = _res.getrusage(_res.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        pass

    def _tle(s, f):
        raise TimeoutError("TLE")

    _real_signal(signal.SIGALRM, _tle)
    signal.alarm(per_tc_limit_s)

    try:
        if MODE == "stdio":
            fake_stdin  = io.StringIO(tc.get("stdin_text", ""))
            fake_stdout = io.StringIO()
            real_stdin  = sys.stdin
            real_stdout = sys.stdout
            sys.stdin   = fake_stdin
            sys.stdout  = fake_stdout
            ns = {{"__name__": "__main__", "open": _safe_open,
                  "exit": _safe_exit, "quit": _safe_exit}}
            exec(compile(_STUDENT_SOURCE, "<student>", "exec"), ns)
            signal.alarm(0)
            sys.stdin  = real_stdin
            sys.stdout = real_stdout
            got      = fake_stdout.getvalue().strip()
            expected = str(tc["expected"]).strip()
            passed   = (got == expected) or _num_equal(got, expected)
            return {{"status": "PASS" if passed else "FAIL",
                     "got": got, "expected": expected}}
        else:
            actual   = solve(*tc["input"])
            signal.alarm(0)
            mem_used = 0
            try:
                mem_after = _res.getrusage(_res.RUSAGE_SELF).ru_maxrss / 1024
                mem_used  = mem_after - mem_before
            except Exception:
                pass
            if memory_limit_mb and mem_used > memory_limit_mb:
                return {{"status": "MLE",
                         "detail": f"Used ~{{int(mem_used)}}MB, limit {{memory_limit_mb}}MB"}}
            got_s  = str(actual).strip()
            exp_s  = str(tc["expected"]).strip()
            passed = (got_s == exp_s) or _num_equal(got_s, exp_s)
            return {{"status": "PASS" if passed else "FAIL",
                     "got": got_s, "expected": exp_s}}

    except TimeoutError:
        return {{"status": "TLE", "detail": f"Exceeded {{per_tc_limit_s}}s"}}
    except MemoryError:
        signal.alarm(0)
        return {{"status": "MLE", "detail": "Memory limit exceeded"}}
    except SystemExit as e:
        signal.alarm(0)
        msg = "Called sys.exit()" if "__HARNESS_BLOCKED__" in str(e) else "SystemExit"
        return {{"status": "ERROR", "detail": msg}}
    except Exception:
        signal.alarm(0)
        lines = traceback.format_exc().strip().splitlines()
        return {{"status": "ERROR", "detail": " | ".join(lines[-2:])}}


# ── ENTRY POINT ────────────────────────────────────────────────────────

def _run_all(test_cases, per_tc_limit_s, memory_limit_mb):
    """
    Dispatcher: use parallel execution if RLIMIT_NPROC allows it,
    otherwise fall back to sequential with a warning.
    """
    if _can_fork_n(len(test_cases)):
        _run_all_parallel(test_cases, per_tc_limit_s, memory_limit_mb)
    else:
        _run_all_sequential(test_cases, per_tc_limit_s, memory_limit_mb,
                            warning="RLIMIT_NPROC too low for parallel — sequential fallback")


_TEST_CASES      = {test_cases_json}
_PER_TC_LIMIT_S  = {per_tc_limit_s}
_MEMORY_LIMIT_MB = {memory_limit_mb}

_run_all(_TEST_CASES, _PER_TC_LIMIT_S, _MEMORY_LIMIT_MB)
