"""
harness_builder.py — v3
─────────────────────────
Generates filled harness code for each language.

v3 changes:
  - C/C++ tc_runner_body: generates parallel fork + select() code
    (fork all → select() → collect → print)
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
from typing import Any

SUPPORTED_LANGUAGES = ["python", "c", "cpp", "java"]


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
        template = open("harnesses/python_harness.py").read()

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
        template     = open("harnesses/c_harness.c").read()
        param_types  = self.cfg.param_types or []
        params       = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
        args         = ", ".join(f"p{i}" for i in range(len(param_types)))
        return_type  = self.cfg.return_type if self.cfg.return_type != "auto" else "int"

        return template.format(
            delim                  = self.delim,
            student_code           = self.cfg.student_code,
            tc_params              = params,
            tc_args                = args,
            call_solve_and_capture = self._build_c_call(return_type),
            tc_runner_body         = self._build_c_parallel_runner(),
        )

    def _build_c_call(self, return_type: str) -> str:
        fn   = self.cfg.function_name
        pt   = self.cfg.param_types or []
        args = ", ".join(f"p{i}" for i in range(len(pt)))
        if return_type == "void":
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
    int len = strlen(buf);
    if (len > 0 && buf[len-1] == '\\n') buf[len-1] = '\\0';
    strncpy(result.got, buf, sizeof(result.got) - 1);
"""
        else:
            return f"""
    {return_type} ret = {fn}({args});
    snprintf(result.got, sizeof(result.got), "%d", (int)ret);
"""

    def _build_c_parallel_runner(self) -> str:
        """
        Generates inline C code that:
          1. Forks all N children simultaneously (each with its own alarm)
          2. Uses select() to collect results as children complete
          3. Kills remaining children on global deadline
          4. Prints all results in TC order
        """
        n      = len(self.cfg.test_cases)
        ps     = self.cfg.per_tc_limit_s
        mem    = self.cfg.memory_limit_mb
        mem_c  = mem // n if mem else 0
        d      = self.delim

        lines = []

        # ── Data arrays ────────────────────────────────────────────────
        lines.append(f"""
    /* ── PARALLEL EXECUTION: {n} TCs simultaneously ─────────────────── */
    pid_t     _pids[{n}];
    int       _fds[{n}];
    TCResult  _results[{n}];
    int       _done[{n}];
    memset(_pids,    0, sizeof(_pids));
    memset(_fds,     0, sizeof(_fds));
    memset(_results, 0, sizeof(_results));
    memset(_done,    0, sizeof(_done));
""")

        # ── Fork phase ─────────────────────────────────────────────────
        lines.append("    /* Phase 1: Fork all children simultaneously */")
        for i, tc in enumerate(self.cfg.test_cases):
            args_str = ", ".join(str(v) for v in (tc.inputs or []))
            expected = str(tc.expected).replace('"', '\\"')
            lines.append(f"""    {{
        int _pfd{i}[2];
        if (pipe(_pfd{i}) == 0) {{
            pid_t _p = fork();
            if (_p == 0) {{
                close(_pfd{i}[0]);
                run_tc_child(_pfd{i}[1], {args_str}, "{expected}", {ps}, {mem_c});
            }}
            close(_pfd{i}[1]);
            _pids[{i}] = _p;
            _fds[{i}]  = _pfd{i}[0];
        }} else {{
            _pids[{i}] = -1;
            strcpy(_results[{i}].status, "ERROR");
            strcpy(_results[{i}].detail, "pipe() failed");
            _done[{i}] = 1;
        }}
    }}""")

        # ── Collect phase via select() ─────────────────────────────────
        # Global safety alarm: per_tc_limit_s + 2s grace.
        # Primary enforcement is inside each child via alarm().
        lines.append(f"""
    /* Phase 2: Collect results using select() — fastest child first */
    alarm({ps} + 2);  /* parent safety alarm */
    {{
        int _pending = {n};
        while (_pending > 0 && !_global_tle) {{
            fd_set _rfds;
            FD_ZERO(&_rfds);
            int _maxfd = -1;
            for (int _i = 0; _i < {n}; _i++) {{
                if (!_done[_i] && _pids[_i] > 0) {{
                    FD_SET(_fds[_i], &_rfds);
                    if (_fds[_i] > _maxfd) _maxfd = _fds[_i];
                }}
            }}
            if (_maxfd < 0) break;

            struct timeval _tv = {{ {ps} + 2, 0 }};
            int _sel = select(_maxfd + 1, &_rfds, NULL, NULL, &_tv);

            if (_sel <= 0) break;  /* timeout or error */

            for (int _i = 0; _i < {n}; _i++) {{
                if (!_done[_i] && _pids[_i] > 0 && FD_ISSET(_fds[_i], &_rfds)) {{
                    ssize_t _n = read(_fds[_i], &_results[_i], sizeof(TCResult));
                    close(_fds[_i]);
                    int _st; waitpid(_pids[_i], &_st, 0);
                    _done[_i] = 1;
                    _pending--;

                    if (_n != sizeof(TCResult)) {{
                        if (WIFSIGNALED(_st)) {{
                            int _sig = WTERMSIG(_st);
                            if      (_sig == SIGSEGV) {{ strcpy(_results[_i].status, "SEGV"); strcpy(_results[_i].detail, "Segmentation fault"); }}
                            else if (_sig == SIGFPE)  {{ strcpy(_results[_i].status, "FPE");  strcpy(_results[_i].detail, "Division by zero"); }}
                            else if (_sig == SIGKILL) {{ strcpy(_results[_i].status, "MLE");  strcpy(_results[_i].detail, "Memory limit exceeded"); }}
                            else                      {{ snprintf(_results[_i].status, 16, "ERROR"); snprintf(_results[_i].detail, 200, "Signal %d", _sig); }}
                        }} else {{
                            strcpy(_results[_i].status, "ERROR");
                            strcpy(_results[_i].detail, "No output from child");
                        }}
                    }}
                }}
            }}
        }}

        /* Kill any still-running children (safety alarm fired or select timeout) */
        for (int _i = 0; _i < {n}; _i++) {{
            if (!_done[_i] && _pids[_i] > 0) {{
                kill(_pids[_i], SIGKILL);
                waitpid(_pids[_i], NULL, 0);
                close(_fds[_i]);
                strcpy(_results[_i].status, "TLE");
                snprintf(_results[_i].detail, sizeof(_results[_i].detail),
                         "Exceeded {ps}s");
            }}
        }}
        alarm(0);  /* cancel safety alarm */
    }}""")

        # ── Print phase ────────────────────────────────────────────────
        lines.append(f"""
    /* Phase 3: Print results in original TC order */
    for (int _i = 0; _i < {n}; _i++) {{
        printf("{d}START_%d\\n", _i + 1);
        printf("{{\\n");
        printf("  \\"status\\": \\"%s\\",\\n",   _results[_i].status);
        printf("  \\"got\\": \\"%s\\",\\n",      _results[_i].got);
        printf("  \\"expected\\": \\"%s\\",\\n", _results[_i].expected);
        printf("  \\"detail\\": \\"%s\\"\\n",    _results[_i].detail);
        printf("}}\\n");
        printf("{d}END_%d\\n", _i + 1);
        fflush(stdout);
    }}""")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # C++
    # ─────────────────────────────────────────────────────────────────
    def _build_cpp(self) -> str:
        template    = open("harnesses/cpp_harness.cpp").read()
        param_types = self.cfg.param_types or []
        params      = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
        args        = ", ".join(f"p{i}" for i in range(len(param_types)))

        return template.format(
            delim                  = self.delim,
            student_code           = self.cfg.student_code,
            tc_params              = params,
            tc_args                = args,
            call_solve_and_capture = self._build_cpp_call(),
            tc_runner_body         = self._build_cpp_parallel_runner(),
        )

    def _build_cpp_call(self) -> str:
        fn   = self.cfg.function_name
        rt   = self.cfg.return_type
        args = ", ".join(f"p{i}" for i in range(len(self.cfg.param_types or [])))
        if rt in ("void", "auto"):
            return f"{fn}({args});"
        return f"""
        auto ret = {fn}({args});
        oss << ret;
"""

    def _build_cpp_parallel_runner(self) -> str:
        """Same structure as C parallel runner but uses std::cout."""
        n     = len(self.cfg.test_cases)
        ps    = self.cfg.per_tc_limit_s
        mem   = self.cfg.memory_limit_mb
        mem_c = mem // n if mem else 0
        d     = self.delim

        lines = []

        lines.append(f"""
    /* ── PARALLEL EXECUTION: {n} TCs simultaneously ─────────────────── */
    pid_t    _pids[{n}];
    int      _fds[{n}];
    TCResult _results[{n}];
    int      _done[{n}];
    memset(_pids,    0, sizeof(_pids));
    memset(_fds,     0, sizeof(_fds));
    memset(_results, 0, sizeof(_results));
    memset(_done,    0, sizeof(_done));
""")

        lines.append("    /* Phase 1: Fork all children simultaneously */")
        for i, tc in enumerate(self.cfg.test_cases):
            args_str = ", ".join(str(v) for v in (tc.inputs or []))
            expected = str(tc.expected).replace('"', '\\"')
            lines.append(f"""    {{
        int _pfd{i}[2];
        if (pipe(_pfd{i}) == 0) {{
            pid_t _p = fork();
            if (_p == 0) {{
                close(_pfd{i}[0]);
                run_tc_child(_pfd{i}[1], {args_str}, "{expected}", {ps}, {mem_c});
            }}
            close(_pfd{i}[1]);
            _pids[{i}] = _p;
            _fds[{i}]  = _pfd{i}[0];
        }} else {{
            _pids[{i}] = -1;
            strcpy(_results[{i}].status, "ERROR");
            strcpy(_results[{i}].detail, "pipe() failed");
            _done[{i}] = 1;
        }}
    }}""")

        lines.append(f"""
    /* Phase 2: Collect via select() */
    alarm({ps} + 2);
    {{
        int _pending = {n};
        while (_pending > 0 && !_global_tle) {{
            fd_set _rfds;
            FD_ZERO(&_rfds);
            int _maxfd = -1;
            for (int _i = 0; _i < {n}; _i++) {{
                if (!_done[_i] && _pids[_i] > 0) {{
                    FD_SET(_fds[_i], &_rfds);
                    if (_fds[_i] > _maxfd) _maxfd = _fds[_i];
                }}
            }}
            if (_maxfd < 0) break;
            struct timeval _tv = {{ {ps} + 2, 0 }};
            int _sel = select(_maxfd + 1, &_rfds, NULL, NULL, &_tv);
            if (_sel <= 0) break;

            for (int _i = 0; _i < {n}; _i++) {{
                if (!_done[_i] && _pids[_i] > 0 && FD_ISSET(_fds[_i], &_rfds)) {{
                    ssize_t _n = read(_fds[_i], &_results[_i], sizeof(TCResult));
                    close(_fds[_i]);
                    int _st; waitpid(_pids[_i], &_st, 0);
                    _done[_i] = 1;
                    _pending--;
                    if (_n != (ssize_t)sizeof(TCResult)) {{
                        if (WIFSIGNALED(_st)) {{
                            int _sig = WTERMSIG(_st);
                            if      (_sig == SIGSEGV) {{ strcpy(_results[_i].status, "SEGV"); strcpy(_results[_i].detail, "Segmentation fault"); }}
                            else if (_sig == SIGFPE)  {{ strcpy(_results[_i].status, "FPE");  strcpy(_results[_i].detail, "Division by zero"); }}
                            else if (_sig == SIGKILL) {{ strcpy(_results[_i].status, "MLE");  strcpy(_results[_i].detail, "Memory limit exceeded"); }}
                            else                      {{ strcpy(_results[_i].status, "ERROR"); snprintf(_results[_i].detail, 255, "Signal %d", _sig); }}
                        }} else {{
                            strcpy(_results[_i].status, "ERROR");
                            strcpy(_results[_i].detail, "No output from child");
                        }}
                    }}
                }}
            }}
        }}
        for (int _i = 0; _i < {n}; _i++) {{
            if (!_done[_i] && _pids[_i] > 0) {{
                kill(_pids[_i], SIGKILL);
                waitpid(_pids[_i], NULL, 0);
                close(_fds[_i]);
                strcpy(_results[_i].status, "TLE");
                snprintf(_results[_i].detail, sizeof(_results[_i].detail), "Exceeded {ps}s");
            }}
        }}
        alarm(0);
    }}""")

        lines.append(f"""
    /* Phase 3: Print results in order */
    for (int _i = 0; _i < {n}; _i++) {{
        std::cout << "{d}START_" << (_i+1) << std::endl;
        std::cout << "{{" << std::endl;
        std::cout << "  \\"status\\": \\"" << _results[_i].status << "\\"," << std::endl;
        std::cout << "  \\"got\\": \\"" << _results[_i].got << "\\"," << std::endl;
        std::cout << "  \\"expected\\": \\"" << _results[_i].expected << "\\"," << std::endl;
        std::cout << "  \\"detail\\": \\"" << _results[_i].detail << "\\"" << std::endl;
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
        template = open("harnesses/java_harness.java").read()

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
        /* Phase 3: Join all threads with shared deadline */
        long _deadline = System.currentTimeMillis() + {pms} + 500; /* +500ms grace */
        for (int _i = 0; _i < {n}; _i++) {{
            long _remaining = _deadline - System.currentTimeMillis();
            if (_remaining > 0) {{
                try {{ _threads[_i].join(_remaining); }} catch (InterruptedException ignored) {{}}
            }}
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
