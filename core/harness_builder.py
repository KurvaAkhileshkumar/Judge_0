"""
harness_builder.py — v3
─────────────────────────
Generates filled harness code for each language.

v3 changes:
  - C/C++ tc_runner_body: generates parallel fork + poll() code
    (fork all → poll() → collect → print)
  - Java tc_runner_body: launches all threads simultaneously,
    joins them with a shared deadline
  - Python: parallel logic is inside the harness template itself
  - Global time limit: per_tc_limit_s + overhead  (not N × per_tc + overhead)
    This is a 10× reduction for 10-TC problems.
"""

import json
import uuid
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED_LANGUAGES = ["python", "c", "cpp", "java"]

# Maximum TCs run in parallel within one batch.
# Caps peak RSS to MAX_PARALLEL_TCS x memory_limit_mb (worst case).
# Wall time for N TCs = ceil(N / MAX_PARALLEL_TCS) x per_tc_limit_s + overhead.
MAX_PARALLEL_TCS = 200

# Harness templates sit in harnesses/ next to the core/ package directory.
# Path(__file__) makes this work whether the repo is cloned, on PYTHONPATH,
# or installed via pip — no dependency on the current working directory.
_HARNESSES_DIR = Path(__file__).parent.parent / "harnesses"


@dataclass
class TestCase:
    """
    Represents one test case. Two modes:

    FUNCTION mode (mode="function"):
      inputs   = positional args passed to solve()   e.g. [2, 3]
      expected = expected return value               e.g. 5

    STDIO mode (mode="stdio"):
      stdin_text = raw string fed to student's stdin  e.g. "2 3\n"
      expected   = expected stdout output             e.g. "5"
    """
    expected:   Any
    inputs:     list[Any] = None
    stdin_text: str       = None


@dataclass
class HarnessConfig:
    student_code:    str
    test_cases:      list[TestCase]
    language:        str
    mode:            str = "function"
    per_tc_limit_s:  int = 2
    memory_limit_mb: int = 256
    function_name:   str = "solve"
    param_types:     list[str] = None
    return_type:     str = "auto"


class HarnessBuilder:

    def __init__(self, config: HarnessConfig):
        self.cfg        = config
        self.session_id = uuid.uuid4().hex[:12]
        self.delim      = f"@@TC_RESULT__{self.session_id}__"

    def build(self) -> str:
        lang = self.cfg.language.lower()
        if lang not in SUPPORTED_LANGUAGES:
            raise ValueError(f"Unsupported language: {lang}")
        return getattr(self, f"_build_{lang}")()

    # ─────────────────────────────────────────────────────────────────
    # PYTHON
    # Parallel logic lives entirely inside the template.
    # Builder only fills in the data (test cases, limits, student code).
    # ─────────────────────────────────────────────────────────────────
    def _build_python(self) -> str:
        template = (_HARNESSES_DIR / "python_harness.py").read_text()

        if self.cfg.mode == "stdio":
            tc_dicts = [
                {"stdin_text": tc.stdin_text, "expected": tc.expected}
                for tc in self.cfg.test_cases
            ]
        else:
            tc_dicts = [
                {"input": tc.inputs, "expected": tc.expected}
                for tc in self.cfg.test_cases
            ]

        # Escape { and } in student code so .format() doesn't misinterpret them
        # as format placeholders. After .format(), {{ becomes { in the output.
        student_code_escaped = (
            self.cfg.student_code
                .replace("{", "{{")
                .replace("}", "}}")
        )

        # For stdio mode: student code must NOT run at module level in the parent
        # process (it calls input() which would fail with empty stdin).
        # Children exec() from _STUDENT_SOURCE with fake stdin/stdout.
        # For function mode: student code defines solve() which must be in the
        # parent namespace so it is inherited by forked children.
        if self.cfg.mode == "stdio":
            module_level_code = "# stdio mode: student code runs only in child processes via exec()"
        else:
            module_level_code = student_code_escaped

        # _STUDENT_SOURCE must hold the EXACT original code (unescaped).
        # We use a sentinel placeholder to avoid passing the raw code through
        # .format() where {  } would cause KeyError.  We substitute it AFTER
        # .format() is done.
        _SENTINEL = "\x00STUDENT_SOURCE_RAW\x00"
        template_patched = template.replace("{student_code_raw}", _SENTINEL)

        filled = template_patched.format(
            session_id      = self.session_id,
            mode            = self.cfg.mode,
            student_code    = module_level_code,
            test_cases_json = json.dumps(tc_dicts),
            per_tc_limit_s  = self.cfg.per_tc_limit_s,
            memory_limit_mb = self.cfg.memory_limit_mb,
        )

        # Now substitute the sentinel with the real (unescaped) student code
        return filled.replace(_SENTINEL, self.cfg.student_code)

    # ─────────────────────────────────────────────────────────────────
    # C
    # ─────────────────────────────────────────────────────────────────
    def _build_c(self) -> str:
        template     = (_HARNESSES_DIR / "c_harness.c").read_text()
        param_types  = self.cfg.param_types or []
        params       = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
        args         = ", ".join(f"p{i}" for i in range(len(param_types)))
        return_type  = self.cfg.return_type if self.cfg.return_type != "auto" else "int"

        # FIX-2: tc_params_comma carries a trailing ", " only when params
        # exist so the child function signature becomes valid C with zero args:
        #   run_tc_child(int pipe_fd, const char* expected, ...)   ← 0 params
        #   run_tc_child(int pipe_fd, int p0, const char* expected,...)← 1+ params
        tc_params_comma = (params + ", ") if params else ""

        return template.format(
            delim                  = self.delim,
            student_code           = self.cfg.student_code,
            tc_params_comma        = tc_params_comma,
            tc_args                = args,
            call_solve_and_capture = self._build_c_call(return_type),
            tc_runner_body         = self._build_c_parallel_runner(),
        )

    # FIX-5: printf format string and cast mapped per C type.
    # The old code cast EVERY non-void return to (int) and used "%d".
    # float/double → truncated to int (wrong answer), char* → UB crash.
    _C_PRINTF_FMT: dict = {
        "int":                 ("%d",    ""),
        "long":                ("%ld",   ""),
        "long long":           ("%lld",  ""),
        "unsigned int":        ("%u",    ""),
        "unsigned long":       ("%lu",   ""),
        "unsigned long long":  ("%llu",  ""),
        "float":               ("%.9g",  "(double)"),   # promote float→double
        "double":              ("%.9g",  ""),
        "long double":         ("%.9Lg", ""),
        "char":                ("%c",    ""),
        "short":               ("%d",    "(int)"),
        "unsigned short":      ("%u",    "(unsigned int)"),
        "size_t":              ("%zu",   ""),
        "ssize_t":             ("%zd",   ""),
    }

    def _build_c_call(self, return_type: str) -> str:
        fn   = self.cfg.function_name
        pt   = self.cfg.param_types or []
        args = ", ".join(f"p{i}" for i in range(len(pt)))

        if return_type == "void":
            # Capture whatever the function prints to stdout via tmpfile dup
            return f"""
    char buf[MAX_OUTPUT];
    memset(buf, 0, sizeof(buf));
    FILE* tmp = tmpfile();
    int old_fd = dup(STDOUT_FILENO);
    dup2(fileno(tmp), STDOUT_FILENO);
    {fn}({args});
    fflush(stdout);
    dup2(old_fd, STDOUT_FILENO);
    close(old_fd);
    fseek(tmp, 0, SEEK_SET);
    fread(buf, 1, MAX_OUTPUT - 1, tmp);
    fclose(tmp);
    /* FIX-14: strip ALL trailing whitespace, not just one '\\n' */
    int _len = (int)strlen(buf);
    while (_len > 0 && (buf[_len-1] == '\\n' || buf[_len-1] == '\\r' ||
                        buf[_len-1] == ' '   || buf[_len-1] == '\\t'))
        buf[--_len] = '\\0';
    strncpy(result.got, buf, sizeof(result.got) - 1);
"""

        # char* / const char*
        if return_type in ("char*", "const char*", "char *", "const char *"):
            return f"""
    {return_type} ret = {fn}({args});
    strncpy(result.got, ret ? ret : "(null)", sizeof(result.got) - 1);
"""

        # All numeric types — look up the right format and cast
        fmt, cast = self._C_PRINTF_FMT.get(return_type, ("%d", "(int)"))
        return f"""
    {return_type} ret = {fn}({args});
    snprintf(result.got, sizeof(result.got), "{fmt}", {cast}ret);
"""

    def _build_c_parallel_runner(self) -> str:
        """
        Generates inline C code that:
          1. Processes TCs in batches of MAX_PARALLEL_TCS (caps peak RSS)
          2. Within each batch: forks all children simultaneously, then
             uses poll() to collect results (no FD_SETSIZE limit)
          3. Kills remaining children on per-batch deadline
          4. Prints all results in TC order after all batches complete
        """
        n     = len(self.cfg.test_cases)
        ps    = self.cfg.per_tc_limit_s
        mem_c = self.cfg.memory_limit_mb if self.cfg.memory_limit_mb else 0
        d     = self.delim

        lines = []

        # Pre-flight: with batching, at most min(n, MAX_PARALLEL_TCS) pipes open at once.
        _need = min(n, MAX_PARALLEL_TCS) * 2 + 64
        lines.append(f"""
    /* Pre-flight: RLIMIT_NOFILE >= {_need} (max {min(n, MAX_PARALLEL_TCS)}x2 pipes + 64).
     * Batching caps concurrent fds; emits all TCs as ERROR if limit is too low. */
    {{
        struct rlimit _fd_rl;
        getrlimit(RLIMIT_NOFILE, &_fd_rl);
        if (_fd_rl.rlim_cur != RLIM_INFINITY && _fd_rl.rlim_cur < {_need}UL) {{
            for (int _i = 1; _i <= {n}; _i++) {{
                printf("%sSTART_%d\\n", DELIM, _i);
                printf("{{\\"status\\":\\"ERROR\\",\\"detail\\":\\"EMFILE: open-file limit too low -- need {_need} fds, have %lu\\"}}\\n",
                       (unsigned long)_fd_rl.rlim_cur);
                printf("%sEND_%d\\n", DELIM, _i);
            }}
            printf("%sDONE\\n", DELIM);
            fflush(stdout);
            return 1;
        }}
    }}
""")

        # Global result array holds all N TC results across all batches.
        lines.append(f"""
    /* Batched execution: {n} TCs in batches of up to {MAX_PARALLEL_TCS} */
    TCResult  _results[{n}];
    memset(_results, 0, sizeof(_results));
""")

        batches = [
            self.cfg.test_cases[i : i + MAX_PARALLEL_TCS]
            for i in range(0, n, MAX_PARALLEL_TCS)
        ]
        for batch_idx, batch in enumerate(batches):
            batch_start = batch_idx * MAX_PARALLEL_TCS
            bsz         = len(batch)

            lines.append(f"""
    /* BATCH {batch_idx}: TCs {batch_start+1}..{batch_start+bsz} */
    {{
        pid_t _pids[{bsz}];
        int   _fds[{bsz}];
        int   _done[{bsz}];
        memset(_pids, 0, sizeof(_pids));
        memset(_fds,  0, sizeof(_fds));
        memset(_done, 0, sizeof(_done));
        _global_tle = 0;  /* reset from any previous batch alarm */""")

            lines.append("        /* Phase 1: Fork batch children */")
            for b_i, tc in enumerate(batch):
                g_i        = batch_start + b_i
                args_str   = ", ".join(str(v) for v in (tc.inputs or []))
                args_comma = (args_str + ", ") if args_str else ""
                expected   = str(tc.expected).replace('"', '\\"')
                lines.append(f"""        {{
            int _pfd[2];
            if (pipe(_pfd) == 0) {{
                pid_t _p = fork();
                if (_p == 0) {{
                    close(_pfd[0]);
                    run_tc_child(_pfd[1], {args_comma}"{expected}", {ps}, {mem_c});
                }}
                close(_pfd[1]);
                _pids[{b_i}] = _p;
                _fds[{b_i}]  = _pfd[0];
            }} else {{
                _pids[{b_i}] = -1;
                strcpy(_results[{g_i}].status, "ERROR");
                strcpy(_results[{g_i}].detail, "pipe() failed");
                _done[{b_i}] = 1;
            }}
        }}""")

            lines.append(f"""
        /* Phase 2: Collect via poll() */
        alarm({ps} + 2);
        {{
            struct pollfd _pfds[{bsz}];
            int _pending = 0;
            for (int _i = 0; _i < {bsz}; _i++) {{
                if (!_done[_i]) {{
                    _pfds[_i].fd     = _fds[_i];
                    _pfds[_i].events = POLLIN;
                    _pending++;
                }} else {{
                    _pfds[_i].fd     = -1;
                    _pfds[_i].events = 0;
                }}
                _pfds[_i].revents = 0;
            }}
            while (_pending > 0 && !_global_tle) {{
                int _r;
                do {{
                    _r = poll(_pfds, {bsz}, ({ps} + 2) * 1000);
                }} while (_r == -1 && errno == EINTR && !_global_tle);
                if (_r <= 0) break;
                for (int _i = 0; _i < {bsz}; _i++) {{
                    int _gi = {batch_start} + _i;
                    if (!_done[_i] && (_pfds[_i].revents & (POLLIN | POLLHUP))) {{
                        ssize_t _nb = read(_pfds[_i].fd, &_results[_gi], sizeof(TCResult));
                        close(_pfds[_i].fd);
                        _pfds[_i].fd = -1;
                        _done[_i] = 1;
                        _pending--;
                        if (_pids[_i] <= 0) {{
                            strcpy(_results[_gi].status, "ERROR");
                            strcpy(_results[_gi].detail, "fork() failed - no process slots");
                        }} else {{
                            int _st; waitpid(_pids[_i], &_st, 0);
                            if (_nb != (ssize_t)sizeof(TCResult)) {{
                                if (WIFSIGNALED(_st)) {{
                                    int _sig = WTERMSIG(_st);
                                    if      (_sig == SIGSEGV) {{ strcpy(_results[_gi].status, "SEGV"); strcpy(_results[_gi].detail, "Segmentation fault"); }}
                                    else if (_sig == SIGFPE)  {{ strcpy(_results[_gi].status, "FPE");  strcpy(_results[_gi].detail, "Division by zero"); }}
                                    else if (_sig == SIGKILL) {{ strcpy(_results[_gi].status, "MLE");  strcpy(_results[_gi].detail, "Memory limit exceeded"); }}
                                    else                      {{ snprintf(_results[_gi].status, 16, "ERROR"); snprintf(_results[_gi].detail, 200, "Signal %d", _sig); }}
                                }} else {{
                                    strcpy(_results[_gi].status, "ERROR");
                                    strcpy(_results[_gi].detail, "No output from child");
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            for (int _i = 0; _i < {bsz}; _i++) {{
                int _gi = {batch_start} + _i;
                if (!_done[_i] && _pids[_i] > 0) {{
                    kill(_pids[_i], SIGKILL);
                    waitpid(_pids[_i], NULL, 0);
                    if (_pfds[_i].fd != -1) {{ close(_pfds[_i].fd); _pfds[_i].fd = -1; }}
                    strcpy(_results[_gi].status, "TLE");
                    snprintf(_results[_gi].detail, sizeof(_results[_gi].detail), "Exceeded {ps}s");
                }}
            }}
            alarm(0);
        }}
    }} /* end BATCH {batch_idx} */
""")

        lines.append(f"""
    /* Phase 3: Print results in original TC order */
    for (int _i = 0; _i < {n}; _i++) {{
        char _je_got[8193], _je_exp[8193], _je_det[2049], _je_st[33];
        json_escape(_results[_i].status,   _je_st,  sizeof(_je_st));
        json_escape(_results[_i].got,      _je_got, sizeof(_je_got));
        json_escape(_results[_i].expected, _je_exp, sizeof(_je_exp));
        json_escape(_results[_i].detail,   _je_det, sizeof(_je_det));
        printf("{d}START_%d\\n", _i + 1);
        printf("{{\\n");
        printf("  \\"status\\": \\"%s\\",\\n",   _je_st);
        printf("  \\"got\\": \\"%s\\",\\n",      _je_got);
        printf("  \\"expected\\": \\"%s\\",\\n", _je_exp);
        printf("  \\"detail\\": \\"%s\\"\\n",    _je_det);
        printf("}}\\n");
        printf("{d}END_%d\\n", _i + 1);
        fflush(stdout);
    }}""")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # C++

    # ─────────────────────────────────────────────────────────────────
    def _build_cpp(self) -> str:
        template    = (_HARNESSES_DIR / "cpp_harness.cpp").read_text()
        param_types = self.cfg.param_types or []
        params      = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
        args        = ", ".join(f"p{i}" for i in range(len(param_types)))

        # FIX-2: same trailing-comma logic as C (see _build_c)
        tc_params_comma = (params + ", ") if params else ""

        return template.format(
            delim                  = self.delim,
            student_code           = self.cfg.student_code,
            tc_params_comma        = tc_params_comma,
            tc_args                = args,
            call_solve_and_capture = self._build_cpp_call(),
            tc_runner_body         = self._build_cpp_parallel_runner(),
        )

    def _build_cpp_call(self) -> str:
        fn   = self.cfg.function_name
        rt   = self.cfg.return_type
        args = ", ".join(f"p{i}" for i in range(len(self.cfg.param_types or [])))
        if rt == "void":
            # void functions: rely on oss capturing any cout output
            return f"{fn}({args});"
        # FIX (cpp void-only skip): `auto` used to fall through to the no-capture
        # branch, silently discarding non-void return values.  Now only `void`
        # skips capture; everything else (including `auto`) uses `oss << ret`.
        return f"""
        auto ret = {fn}({args});
        oss << ret;
"""

    def _build_cpp_parallel_runner(self) -> str:
        """Same structure as C parallel runner (poll-based) but uses std::cout."""
        n     = len(self.cfg.test_cases)
        ps    = self.cfg.per_tc_limit_s
        mem_c = self.cfg.memory_limit_mb if self.cfg.memory_limit_mb else 0
        d     = self.delim

        lines = []

        _need = min(n, MAX_PARALLEL_TCS) * 2 + 64
        lines.append(f"""
    /* Pre-flight: RLIMIT_NOFILE >= {_need} (max {min(n, MAX_PARALLEL_TCS)}x2 pipes + 64).
     * Batching caps concurrent fds; emits all TCs as ERROR if limit is too low. */
    {{
        struct rlimit _fd_rl;
        getrlimit(RLIMIT_NOFILE, &_fd_rl);
        if (_fd_rl.rlim_cur != RLIM_INFINITY && _fd_rl.rlim_cur < {_need}UL) {{
            for (int _i = 1; _i <= {n}; _i++) {{
                std::cout << DELIM << "START_" << _i << "\\n";
                std::cout << "{{\\"status\\":\\"ERROR\\",\\"detail\\":\\"EMFILE: open-file limit too low -- need {_need} fds, have "
                          << (unsigned long)_fd_rl.rlim_cur << "\\"}}" << "\\n";
                std::cout << DELIM << "END_" << _i << "\\n";
            }}
            std::cout << DELIM << "DONE\\n";
            std::cout.flush();
            return 1;
        }}
    }}
""")

        lines.append(f"""
    /* Batched execution: {n} TCs in batches of up to {MAX_PARALLEL_TCS} */
    TCResult  _results[{n}];
    memset(_results, 0, sizeof(_results));
""")

        batches = [
            self.cfg.test_cases[i : i + MAX_PARALLEL_TCS]
            for i in range(0, n, MAX_PARALLEL_TCS)
        ]
        for batch_idx, batch in enumerate(batches):
            batch_start = batch_idx * MAX_PARALLEL_TCS
            bsz         = len(batch)

            lines.append(f"""
    /* BATCH {batch_idx}: TCs {batch_start+1}..{batch_start+bsz} */
    {{
        pid_t _pids[{bsz}];
        int   _fds[{bsz}];
        int   _done[{bsz}];
        memset(_pids, 0, sizeof(_pids));
        memset(_fds,  0, sizeof(_fds));
        memset(_done, 0, sizeof(_done));
        _global_tle = 0;  /* reset from any previous batch alarm */""")

            lines.append("        /* Phase 1: Fork batch children */")
            for b_i, tc in enumerate(batch):
                g_i        = batch_start + b_i
                args_str   = ", ".join(str(v) for v in (tc.inputs or []))
                args_comma = (args_str + ", ") if args_str else ""
                expected   = str(tc.expected).replace('"', '\\"')
                lines.append(f"""        {{
            int _pfd[2];
            if (pipe(_pfd) == 0) {{
                pid_t _p = fork();
                if (_p == 0) {{
                    close(_pfd[0]);
                    run_tc_child(_pfd[1], {args_comma}"{expected}", {ps}, {mem_c});
                }}
                close(_pfd[1]);
                _pids[{b_i}] = _p;
                _fds[{b_i}]  = _pfd[0];
            }} else {{
                _pids[{b_i}] = -1;
                strcpy(_results[{g_i}].status, "ERROR");
                strcpy(_results[{g_i}].detail, "pipe() failed");
                _done[{b_i}] = 1;
            }}
        }}""")

            lines.append(f"""
        /* Phase 2: Collect via poll() */
        alarm({ps} + 2);
        {{
            struct pollfd _pfds[{bsz}];
            int _pending = 0;
            for (int _i = 0; _i < {bsz}; _i++) {{
                if (!_done[_i]) {{
                    _pfds[_i].fd     = _fds[_i];
                    _pfds[_i].events = POLLIN;
                    _pending++;
                }} else {{
                    _pfds[_i].fd     = -1;
                    _pfds[_i].events = 0;
                }}
                _pfds[_i].revents = 0;
            }}
            while (_pending > 0 && !_global_tle) {{
                int _r;
                do {{
                    _r = poll(_pfds, {bsz}, ({ps} + 2) * 1000);
                }} while (_r == -1 && errno == EINTR && !_global_tle);
                if (_r <= 0) break;
                for (int _i = 0; _i < {bsz}; _i++) {{
                    int _gi = {batch_start} + _i;
                    if (!_done[_i] && (_pfds[_i].revents & (POLLIN | POLLHUP))) {{
                        ssize_t _nb = read(_pfds[_i].fd, &_results[_gi], sizeof(TCResult));
                        close(_pfds[_i].fd);
                        _pfds[_i].fd = -1;
                        _done[_i] = 1;
                        _pending--;
                        if (_pids[_i] <= 0) {{
                            strcpy(_results[_gi].status, "ERROR");
                            strcpy(_results[_gi].detail, "fork() failed - no process slots");
                        }} else {{
                            int _st; waitpid(_pids[_i], &_st, 0);
                            if (_nb != (ssize_t)sizeof(TCResult)) {{
                                if (WIFSIGNALED(_st)) {{
                                    int _sig = WTERMSIG(_st);
                                    if      (_sig == SIGSEGV) {{ strcpy(_results[_gi].status, "SEGV"); strcpy(_results[_gi].detail, "Segmentation fault"); }}
                                    else if (_sig == SIGFPE)  {{ strcpy(_results[_gi].status, "FPE");  strcpy(_results[_gi].detail, "Division by zero"); }}
                                    else if (_sig == SIGKILL) {{ strcpy(_results[_gi].status, "MLE");  strcpy(_results[_gi].detail, "Memory limit exceeded"); }}
                                    else                      {{ snprintf(_results[_gi].status, 16, "ERROR"); snprintf(_results[_gi].detail, 200, "Signal %d", _sig); }}
                                }} else {{
                                    strcpy(_results[_gi].status, "ERROR");
                                    strcpy(_results[_gi].detail, "No output from child");
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            for (int _i = 0; _i < {bsz}; _i++) {{
                int _gi = {batch_start} + _i;
                if (!_done[_i] && _pids[_i] > 0) {{
                    kill(_pids[_i], SIGKILL);
                    waitpid(_pids[_i], NULL, 0);
                    if (_pfds[_i].fd != -1) {{ close(_pfds[_i].fd); _pfds[_i].fd = -1; }}
                    strcpy(_results[_gi].status, "TLE");
                    snprintf(_results[_gi].detail, sizeof(_results[_gi].detail), "Exceeded {ps}s");
                }}
            }}
            alarm(0);
        }}
    }} /* end BATCH {batch_idx} */
""")

        lines.append(f"""
    /* Phase 3: Print results in order */
    for (int _i = 0; _i < {n}; _i++) {{
        char _je_got[8193], _je_exp[8193], _je_det[2049], _je_st[33];
        json_escape(_results[_i].status,   _je_st,  sizeof(_je_st));
        json_escape(_results[_i].got,      _je_got, sizeof(_je_got));
        json_escape(_results[_i].expected, _je_exp, sizeof(_je_exp));
        json_escape(_results[_i].detail,   _je_det, sizeof(_je_det));
        std::cout << "{d}START_" << (_i+1) << std::endl;
        std::cout << "{{" << std::endl;
        std::cout << "  \\"status\\": \\"" << _je_st  << "\\"," << std::endl;
        std::cout << "  \\"got\\": \\"" << _je_got << "\\"," << std::endl;
        std::cout << "  \\"expected\\": \\"" << _je_exp << "\\"," << std::endl;
        std::cout << "  \\"detail\\": \\"" << _je_det << "\\"" << std::endl;
        std::cout << "}}" << std::endl;
        std::cout << "{d}END_" << (_i+1) << std::endl;
        std::cout.flush();
    }}""")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # JAVA

    # ─────────────────────────────────────────────────────────────────
    def _build_java(self) -> str:
        # Java has too many literal { } braces for Python .format() — use replace() instead.
        template = (_HARNESSES_DIR / "java_harness.java").read_text()

        inner_class = (
            "\n    static class Student {\n" +
            textwrap.indent(self.cfg.student_code, "        ") +
            "\n    }\n"
        )

        ptypes = self.cfg.param_types or []
        if ptypes:
            items = ", ".join(f'"{t}"' for t in ptypes)
            param_types_array = "new String[]{" + items + "}"
        else:
            param_types_array = "new String[]{}"

        replacements = {
            "{delim}":                       self.delim,
            "{mode}":                        self.cfg.mode,
            "{student_code_as_inner_class}": inner_class,
            "{per_tc_limit_ms}":             str(self.cfg.per_tc_limit_s * 1000),
            "{memory_limit_mb}":             str(self.cfg.memory_limit_mb),
            "{function_name}":               self.cfg.function_name,
            "{param_types_array}":           param_types_array,
            "{tc_runner_body}":              self._build_java_parallel_runner(),
        }

        result = template
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)
        return result

    def _build_java_parallel_runner(self) -> str:
        """
        Generates Java code that:
          1. Builds an AtomicReference<TCResult> per TC
          2. Calls launchFunctionTC / launchStdioTC to create Thread objects
          3. Starts ALL threads simultaneously
          4. Joins ALL threads with a shared deadline
          5. Kills any still-alive threads
          6. Prints results in TC order
        """
        n   = len(self.cfg.test_cases)
        ps  = self.cfg.per_tc_limit_s
        pms = ps * 1000

        lines = []

        lines.append(f"""
        /* ── PARALLEL EXECUTION: {n} TCs simultaneously ──────────────── */
        @SuppressWarnings("unchecked")
        AtomicReference<TCResult>[] _resultRefs = new AtomicReference[{n}];
        Thread[] _threads = new Thread[{n}];
        for (int _i = 0; _i < {n}; _i++) {{
            _resultRefs[_i] = new AtomicReference<>(null);
        }}
""")

        # Build launch calls
        lines.append("        /* Phase 1: Create all threads (not started yet) */")
        for i, tc in enumerate(self.cfg.test_cases):
            expected = str(tc.expected).replace('"', '\\"')
            if self.cfg.mode == "stdio":
                stdin = (tc.stdin_text or "").replace('"', '\\"').replace("\n", "\\n")
                lines.append(f"""
        _threads[{i}] = launchStdioTC("{stdin}", "{expected}", _resultRefs[{i}]);""")
            else:
                inputs_arr = ", ".join(
                    f'(Object)({v})' if isinstance(v, (int, float)) else f'"{v}"'
                    for v in (tc.inputs or [])
                )
                lines.append(f"""
        {{
            Object[] _in{i} = {{ {inputs_arr} }};
            _threads[{i}] = launchFunctionTC(_in{i}, "{expected}",
                paramTypes, functionName, memoryLimitMb, _resultRefs[{i}]);
        }}""")

        # Start all
        lines.append(f"""
        /* Phase 2: Start ALL threads simultaneously — t=0 for all */
        for (int _i = 0; _i < {n}; _i++) {{
            _threads[_i].start();
        }}
""")

        # Join all with shared deadline
        lines.append(f"""
        /* Phase 3: Join all threads with shared deadline
         *
         * FIX-3: the old code used "if (_remaining > 0) join(_remaining)".
         * When TC[0] was a TLE (used the full deadline), _remaining became 0
         * for TC[1]..TC[N-1].  Those threads may have already finished, but
         * with _remaining<=0 we skipped join() and went straight to isAlive()
         * before the thread had a chance to mark its result — so correct
         * completions were killed and reported TLE.
         *
         * Fix: always call join(Math.max(1, _remaining)).
         *   join(0) in Java means "wait forever" — never use it here.
         *   join(1) is a 1ms poll: if the thread already finished it returns
         *   immediately; if it is still alive we kill it.  Threads that
         *   completed while waiting for TC[0] are correctly reported PASS/FAIL.
         */
        long _deadline = System.currentTimeMillis() + {pms} + 500;
        for (int _i = 0; _i < {n}; _i++) {{
            long _remaining = _deadline - System.currentTimeMillis();
            try {{ _threads[_i].join(Math.max(1L, _remaining)); }} catch (InterruptedException ignored) {{}}
            if (_threads[_i].isAlive()) {{
                boolean _dead = killThread(_threads[_i]);
                if (_resultRefs[_i].get() == null) {{
                    TCResult _r = new TCResult();
                    _r.status = "TLE";
                    _r.detail = "Exceeded {ps}s" + (_dead ? "" : " (unkillable)");
                    _resultRefs[_i].set(_r);
                }}
            }}
        }}
""")

        # Print all results
        lines.append(f"""
        /* Phase 4: Print results in original TC order */
        for (int _i = 0; _i < {n}; _i++) {{
            TCResult _r = _resultRefs[_i].get();
            if (_r == null) {{
                _r = new TCResult();
                _r.status = "ERROR";
                _r.detail = "Thread produced no result";
            }}
            printResult(_i + 1, _r);
        }}""")

        return "\n".join(lines)
