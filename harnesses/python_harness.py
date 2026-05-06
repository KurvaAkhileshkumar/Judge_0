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
3. COLLECT PHASE— parent uses poll() to wait on all pipes simultaneously.
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
    Runs solve(*inputs) and returns the actual output.
    Fix 4.1: No expected comparison inside sandbox — OutputParser does it.
    """
    actual = solve(*tc["input"])
    got_s  = str(actual).strip()
    return {{
        "status": "OUTPUT",
        "got":    got_s,
    }}


def _child_run_stdio(tc):
    """
    Re-executes the student source in a fresh namespace per TC.
    Fix 1.6: __builtins__ is restricted to an explicit whitelist so
    exec() cannot access blocked builtins via ns["__builtins__"].__dict__.
    Fix 4.1: returns raw output; OutputParser does comparison.
    """
    # Fix 1.6: whitelist of safe builtins for exec() namespace
    _safe_builtins = {{
        k: v for k, v in __builtins__.__dict__.items()
        if k in {{
            'print', 'input', 'range', 'len', 'int', 'float', 'str', 'bool',
            'list', 'dict', 'set', 'tuple', 'frozenset', 'bytes', 'bytearray',
            'enumerate', 'zip', 'map', 'filter', 'sorted', 'reversed',
            'sum', 'min', 'max', 'abs', 'round', 'divmod', 'pow',
            'isinstance', 'issubclass', 'type', 'repr', 'chr', 'ord',
            'hex', 'bin', 'oct', 'id', 'hash', 'iter', 'next', 'any', 'all',
            'StopIteration', 'ValueError', 'TypeError', 'IndexError',
            'KeyError', 'AttributeError', 'NameError', 'RuntimeError',
            'OverflowError', 'ZeroDivisionError', 'RecursionError',
            'MemoryError', 'Exception', 'BaseException',
            'True', 'False', 'None', 'NotImplemented', 'Ellipsis',
            'object', 'super', 'property', 'staticmethod', 'classmethod',
        }}
    }}
    fake_stdin  = io.StringIO(tc.get("stdin_text", ""))
    fake_stdout = io.StringIO()
    sys.stdin   = fake_stdin
    sys.stdout  = fake_stdout

    ns = {{
        "__name__":     "__main__",
        "__builtins__": _safe_builtins,  # Fix 1.6
        "open":         _safe_open,
        "exit":         _safe_exit,
        "quit":         _safe_exit,
    }}
    exec(compile(_STUDENT_SOURCE, "<student>", "exec"), ns)

    got = fake_stdout.getvalue().strip()
    # Fix 4.1: return raw output; OutputParser does comparison externally
    return {{
        "status": "OUTPUT",
        "got":    got,
    }}


# ── NPROC CHECK ────────────────────────────────────────────────────────

def _can_fork_n(n):
    """
    Check RLIMIT_NPROC to confirm we can safely fork n children.

    RLIMIT_NPROC is a per-UID kernel limit counted system-wide — not
    per-process, not per-container.  All sandboxes sharing the same UID
    compete for the same pool.  The old code only counted threads in THIS
    process (/proc/self/task), so it returned True while other sandboxes
    had already consumed most of the quota → fork() failed mid-launch.

    Fix: scan /proc for ALL processes owned by this UID.  Inside an isolate
    PID namespace this may undercount (other sandboxes are in separate
    namespaces), so the primary guard is ulimits.nproc=65536 in
    docker-compose.yml which raises the per-container ceiling high enough
    that a single harness with 1,000 children never comes close to it.
    """
    try:
        soft, _ = resource.getrlimit(resource.RLIMIT_NPROC)
        if soft == resource.RLIM_INFINITY:
            return True, -1
        uid = os.getuid()
        current = 0
        try:
            for entry in os.listdir('/proc'):
                if not entry.isdigit():
                    continue
                try:
                    with open(f'/proc/{{entry}}/status') as f:
                        for line in f:
                            if line.startswith('Uid:'):
                                if int(line.split()[1]) == uid:
                                    current += 1
                                break
                except OSError:
                    pass
        except OSError:
            # /proc listing blocked — fall back to this process's thread count
            current = len(os.listdir('/proc/self/task'))
        available = soft - current - 2  # -2 safety margin
        return available >= n, available
    except Exception:
        return True, -1  # assume ok if we can't check


# ── PARALLEL RUNNER ────────────────────────────────────────────────────

def _run_all_parallel(test_cases, per_tc_limit_s, memory_limit_mb):
    """
    Forks one child per TC simultaneously.
    Uses select.poll() to collect results as children finish.
    select.poll() has no FD_SETSIZE=1024 limit unlike select.select().
    Total job time = max(TC_times)  not  sum(TC_times).
    """
    _real_stdout = sys.stdout

    n            = len(test_cases)
    # FIX-1: each child gets the FULL per-TC limit, not total÷N.
    # Dividing by N (e.g. 256MB÷10=25MB) was far too restrictive and caused
    # legitimate solutions to OOM. Judge0 enforces the limit per-process via
    # enable_per_process_and_thread_memory_limit, so each child is already
    # capped at memory_limit_mb independently by isolate.
    mem_per_child    = memory_limit_mb
    MAX_PARALLEL_TCS = 200  # caps peak RSS to MAX_PARALLEL_TCS x memory_limit_mb

    # Pre-flight: with batching, at most min(n, MAX_PARALLEL_TCS) pipes open at once.
    # Runs before any fork: if the limit is too low, ALL TCs get ERROR before any child starts,
    # so _is_infrastructure_failure() sees 100% ERROR and the worker requeues the job.
    try:
        _soft_fd, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        _need_fd = min(n, MAX_PARALLEL_TCS) * 2 + 64
        if _soft_fd != resource.RLIM_INFINITY and _soft_fd < _need_fd:
            _reason = f"EMFILE: open-file limit too low -- need {{_need_fd}} fds, have {{_soft_fd}}"
            for _i in range(n):
                _real_stdout.write(f"{{DELIM}}START_{{_i+1}}\n")
                _real_stdout.write(json.dumps({{"status": "ERROR", "detail": _reason}}) + "\n")
                _real_stdout.write(f"{{DELIM}}END_{{_i+1}}\n")
                _real_stdout.flush()
            _real_stdout.write(f"{{DELIM}}DONE\n")
            _real_stdout.flush()
            return
    except Exception:
        pass

    # ── BATCHED FORK + COLLECT ────────────────────────────────────────────
    # Each batch forks up to MAX_PARALLEL_TCS children simultaneously,
    # collects their results, then moves to the next batch.
    # This caps peak concurrent processes (and peak RSS) to MAX_PARALLEL_TCS.
    results       = {{}}   # global tc_index -> result dict
    pipe_failures = {{}}   # global tc_index -> error string
    fork_aborted  = False

    for batch_start in range(0, n, MAX_PARALLEL_TCS):
        if fork_aborted:
            break
        batch_end = min(batch_start + MAX_PARALLEL_TCS, n)
        batch_tcs = test_cases[batch_start:batch_end]

        jobs   = []   # (pid, read_fd, global_tc_index)
        chunks = {{}}  # global_tc_index -> list of bytes

        for b_i, tc in enumerate(batch_tcs):
            g_i = batch_start + b_i
            try:
                r_fd, w_fd = os.pipe()
            except OSError as e:
                pipe_failures[g_i] = f"pipe() failed: {{e}}"
                continue
            try:
                pid = os.fork()
            except OSError as e:
                os.close(r_fd)
                os.close(w_fd)
                reason = (
                    f"fork() failed at TC{{g_i+1}} ({{e}}). "
                    f"Process/memory limit exhausted — TCs {{g_i+1}}–{{n}} not executed. "
                    f"Fix: set ulimits.nproc=65536 in docker-compose.yml"
                )
                for j in range(g_i, n):
                    pipe_failures[j] = reason
                fork_aborted = True
                break

            if pid == 0:
                os.close(r_fd)
                _child_run(tc, w_fd, per_tc_limit_s, mem_per_child)
                os._exit(0)
            else:
                os.close(w_fd)
                jobs.append((pid, r_fd, g_i))
                chunks[g_i] = []

        # ── COLLECT PHASE for this batch ──────────────────────────────────
        deadline    = time.monotonic() + per_tc_limit_s + 1.5
        fd_to_job   = {{r_fd: (pid, idx) for pid, r_fd, idx in jobs}}
        pending_fds = set(fd_to_job.keys())

        poller = select.poll()
        for r_fd in pending_fds:
            poller.register(r_fd, select.POLLIN)

        while pending_fds:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Global deadline passed — kill everything still running
                for r_fd in list(pending_fds):
                    pid, idx = fd_to_job[r_fd]
                    try:
                        poller.unregister(r_fd)
                    except Exception:
                        pass
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
                # poll() timeout is milliseconds; max(1,...) avoids poll(0) = instant return
                ready = poller.poll(max(1, int(remaining * 1000)))
            except Exception:
                break

            if not ready:
                # Timeout from poll() — no child made progress within remaining time
                for r_fd in list(pending_fds):
                    pid, idx = fd_to_job[r_fd]
                    try:
                        poller.unregister(r_fd)
                    except Exception:
                        pass
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

            for r_fd, event in ready:
                pid, idx = fd_to_job[r_fd]
                try:
                    chunk = os.read(r_fd, 65536)
                except Exception:
                    chunk = b""

                if chunk:
                    chunks[idx].append(chunk)
                else:
                    # EOF — child closed its write end (finished or crashed)
                    try:
                        poller.unregister(r_fd)
                    except Exception:
                        pass
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
    # Merge in any TCs that failed at pipe()/fork() before the collect loop
    for idx, err in pipe_failures.items():
        results[idx] = {{"status": "ERROR", "detail": err}}

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


# Sequential fallback intentionally removed.
# With N test cases sequential takes N × per_tc_limit_s which always
# exceeds the Judge0 global time limit.  Resource exhaustion is now
# reported immediately as ERROR for the affected TCs.


# ── ENTRY POINT ────────────────────────────────────────────────────────

def _run_all(test_cases, per_tc_limit_s, memory_limit_mb):
    """
    Dispatcher: use parallel execution if RLIMIT_NPROC allows it.

    Sequential fallback is intentionally removed.  With N test cases,
    sequential takes N × per_tc_limit_s seconds which always exceeds the
    Judge0 global time limit (per_tc_limit_s + 5s overhead).  Falling back
    to sequential would guarantee every TC beyond the first few hits TLE —
    worse than reporting the root cause immediately.

    If parallel is not possible, all TCs are marked ERROR with a clear
    message pointing to the fix (ulimits.nproc in docker-compose.yml).
    """
    can_fork, available = _can_fork_n(len(test_cases))
    if can_fork:
        _run_all_parallel(test_cases, per_tc_limit_s, memory_limit_mb)
    else:
        reason = (
            f"RLIMIT_NPROC too low: need {{len(test_cases)}} slots, "
            f"only {{available}} available. "
            f"Fix: set ulimits.nproc=65536 in docker-compose.yml and "
            f"MAX_PROCESSES_PER_WORKER=1100 in judge0.conf"
        )
        _real_stdout = sys.stdout
        for i in range(len(test_cases)):
            _real_stdout.write(f"{{DELIM}}START_{{i+1}}\n")
            _real_stdout.write(json.dumps({{"status": "ERROR", "detail": reason}}) + "\n")
            _real_stdout.write(f"{{DELIM}}END_{{i+1}}\n")
            _real_stdout.flush()
        _real_stdout.write(f"{{DELIM}}DONE\n")
        _real_stdout.flush()


_TEST_CASES      = {test_cases_json}
_PER_TC_LIMIT_S  = {per_tc_limit_s}
_MEMORY_LIMIT_MB = {memory_limit_mb}

_run_all(_TEST_CASES, _PER_TC_LIMIT_S, _MEMORY_LIMIT_MB)
