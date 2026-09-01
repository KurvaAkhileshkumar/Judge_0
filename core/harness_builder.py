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
        self.cfg                   = config
        self.session_id            = uuid.uuid4().hex[:12]
        self.delim                 = f"@@TC_RESULT__{self.session_id}__"
        self.student_code_start_line = 0  # set by _build_java(); used by output_parser

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
            # Fix 4.1: omit expected — OutputParser does comparison externally.
            tc_dicts = [
                {"stdin_text": tc.stdin_text}
                for tc in self.cfg.test_cases
            ]
        else:
            # Fix 4.1: omit expected — OutputParser does comparison externally.
            tc_dicts = [
                {"input": tc.inputs}
                for tc in self.cfg.test_cases
            ]

        # For stdio mode: student code must NOT run at module level in the parent
        # process (it calls input() which would fail with empty stdin).
        # Children exec() from _STUDENT_SOURCE with fake stdin/stdout.
        # For function mode: exec with '<student>' as the filename so that runtime
        # tracebacks show student-relative line numbers — the same mechanism stdio
        # mode uses. _safe_tb() already knows how to extract them from '<student>' frames.
        if self.cfg.mode == "stdio":
            module_level_code = "# stdio mode: student code runs only in child processes via exec()"
        else:
            # repr() produces a safe Python string literal (handles backslashes,
            # quotes, newlines, and dict-literal braces). The result is passed as
            # a VALUE to .format() — values are never scanned for {placeholder}
            # patterns, so no brace-escaping is needed.
            module_level_code = f"exec(compile({repr(self.cfg.student_code)}, '<student>', 'exec'), globals())"

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
            function_name   = self.cfg.function_name,
        )

        # Substitute sentinel with repr() of student code — safe against triple-quotes,
        # backslashes, and any other characters that would break a string literal.
        return filled.replace(_SENTINEL, repr(self.cfg.student_code))

    # ─────────────────────────────────────────────────────────────────
    # C
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _c_string_escape(s: str) -> str:
        """Escape a Python string for safe embedding inside a C string literal."""
        return (s
            .replace('\\', '\\\\')
            .replace('"',  '\\"')
            .replace('\n', '\\n')
            .replace('\r', '\\r')
            .replace('\t', '\\t')
            .replace('\0', '\\0')
        )

    _C_INT_MAX =  2**31 - 1
    _C_INT_MIN = -2**31

    @staticmethod
    def _c_literal(val, language: str = "c") -> str:
        """
        Serialize a Python input value as a C or C++ literal.

        Handles the type mapping that str() gets wrong:
          Python True/False → C: 1/0,  C++: true/false
          Python None       → 0  (NULL-equivalent for numeric/pointer params)
          Python str        → "quoted and escaped string literal"
          Python int/float  → str(val)  (works directly in C/C++)
          Python list/dict  → raises ValueError (use stdio mode for arrays)

        Integer range rule:
          Values that fit in 32-bit signed int → bare literal (e.g. 42)
          Values outside int range             → LL suffix   (e.g. 3000000000LL)
          This prevents implicit narrowing when the harness passes the literal
          to a run_tc_child() whose parameter was auto-upgraded to long long.
        """
        if val is None:
            return "0"
        if isinstance(val, bool):   # must check before int — bool is subclass of int
            if language == "cpp":
                return "true" if val else "false"
            return "1" if val else "0"
        if isinstance(val, str):
            escaped = (val
                .replace('\\', '\\\\')
                .replace('"',  '\\"')
                .replace('\n', '\\n')
                .replace('\r', '\\r')
                .replace('\t', '\\t')
            )
            return f'"{escaped}"'
        if isinstance(val, int):
            if val > HarnessBuilder._C_INT_MAX or val < HarnessBuilder._C_INT_MIN:
                return str(val) + 'LL'
            return str(val)
        if isinstance(val, float):
            return str(val)
        raise ValueError(
            f"Input value {val!r} (type {type(val).__name__}) cannot be passed as a "
            f"C/C++ function argument. Use stdio mode for array or complex inputs."
        )

    _JAVA_INT_MAX =  2**31 - 1
    _JAVA_INT_MIN = -2**31

    @staticmethod
    def _java_literal(val) -> str:
        """
        Serialize a Python input value as a Java Object expression for reflection.

        Handles the type mapping that the old isinstance(v, (int,float)) check got wrong:
          Python True/False → true/false  (bool must be checked BEFORE int)
          Python None       → null
          Python int        → (Object)(42)    autoboxes to Integer
                           or (Object)(3e9L)  autoboxes to Long (values > INT_MAX)
          Python float      → (Object)(1.5)   autoboxes to Double
          Python str        → "escaped string literal"
          Python list/dict  → raises ValueError (use stdio mode)

        Integer range rule:
          Values that fit in Java int  → no suffix   → autoboxes to Integer
          Values outside Java int range → L suffix   → autoboxes to Long
          Without the L suffix, javac rejects literals > 2^31-1 with
          "integer number too large" because Java treats unsuffixed integer
          literals as int regardless of magnitude.
        """
        if val is None:
            return "null"
        if isinstance(val, bool):   # must check before int — bool is subclass of int
            return "true" if val else "false"
        if isinstance(val, int):
            if HarnessBuilder._JAVA_INT_MIN <= val <= HarnessBuilder._JAVA_INT_MAX:
                return f"(Object)({val})"       # autoboxes to Integer
            return f"(Object)({val}L)"          # autoboxes to Long
        if isinstance(val, float):
            return f"(Object)({val})"
        if isinstance(val, str):
            escaped = (val
                .replace('\\', '\\\\')
                .replace('"',  '\\"')
                .replace('\n', '\\n')
                .replace('\t', '\\t')
            )
            return f'"{escaped}"'
        raise ValueError(
            f"Input value {val!r} (type {type(val).__name__}) cannot be passed as a "
            f"Java function argument. Use stdio mode for array or complex inputs."
        )

    def _effective_param_types(self, language: str) -> list:
        """
        Return param_types with "int" auto-upgraded when any test-case input
        value exceeds the 32-bit signed int range [−2³¹, 2³¹−1].

        Why this matters
        ────────────────
        • C/C++: run_tc_child() is declared with the user-supplied param types.
          If the type is "int" but the literal is 3000000000LL, the compiler
          narrows silently → wrong result (e.g. 705032704 instead of 3000000000).
        • Java: resolveParamClasses() maps "int" → int.class.  When the input
          autoboxes to Long (because _java_literal adds an L suffix), passing a
          Long to an int parameter via reflection throws IllegalArgumentException.

        C/C++ policy — upgrade per position:
          Only positions whose value overflows int are upgraded to "long long".
          Implicit widening in C lets the other int positions widen at the call
          site without issue.

        Java policy — upgrade all-or-nothing:
          Java reflection requires an EXACT type match for getDeclaredMethod().
          Upgrading only some positions would produce a mixed signature like
          (int.class, long.class) that matches no real method.  So: if ANY
          "int" position needs upgrading, ALL "int" positions are upgraded to
          "long" — the common case where the user meant to write "long" for all.
        """
        INT_MAX = 2**31 - 1
        INT_MIN = -2**31
        types = list(self.cfg.param_types or [])

        if language in ("c", "cpp"):
            # Per-position upgrade — C allows implicit widening at the call site.
            for tc in self.cfg.test_cases:
                for i, val in enumerate(tc.inputs or []):
                    if i >= len(types):
                        break
                    if types[i] == "int" and isinstance(val, int) and not isinstance(val, bool):
                        if val > INT_MAX or val < INT_MIN:
                            types[i] = "long long"
        else:
            # Java — all-or-nothing: if any "int" position overflows, upgrade all.
            needs_upgrade = False
            for tc in self.cfg.test_cases:
                for i, val in enumerate(tc.inputs or []):
                    if i < len(types) and types[i] == "int":
                        if isinstance(val, int) and not isinstance(val, bool):
                            if val > INT_MAX or val < INT_MIN:
                                needs_upgrade = True
                                break
                if needs_upgrade:
                    break
            if needs_upgrade:
                types = ["long" if t == "int" else t for t in types]

        return types

    def _build_c(self) -> str:
        template    = (_HARNESSES_DIR / "c_harness.c").read_text()
        param_types = self._effective_param_types("c")
        return_type = self.cfg.return_type if self.cfg.return_type != "auto" else "int"

        # Sentinel approach: replace {student_code} BEFORE calling .format() so
        # that Python's str.format() never sees the student code's { } characters.
        # Old approach (replace "{" → "{{") was wrong: double-braces in a format()
        # VALUE are NOT unescaped — they remain as "{{" in the output, producing
        # invalid C like "class Counter {{" instead of "class Counter {".
        _SENTINEL = "\x00STUDENT_C_RAW\x00"

        if self.cfg.mode == "stdio":
            # Rename student's main() to avoid conflict with harness main().
            # #define precedes the student block; #undef follows it so the
            # harness's own "int main(void)" at the bottom stays as-is.
            student_raw = (
                "#define main student_stdio_main\n" +
                self.cfg.student_code +
                "\n#undef main"
            )
            tc_params_comma        = "const char* _stdin_text, "
            call_solve_and_capture = self._build_c_stdio_call()
        else:
            params      = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
            args        = ", ".join(f"p{i}" for i in range(len(param_types)))
            # FIX-2: trailing ", " only when params exist so zero-arg functions
            # don't produce "int pipe_fd, , int per_tc" — a C syntax error.
            tc_params_comma        = (params + ", ") if params else ""
            call_solve_and_capture = self._build_c_call(return_type)
            student_raw = self.cfg.student_code

        # Lines in the template before {student_code} = harness header line count.
        # For stdio mode, student_raw starts with "#define main …\n",
        # so real student code begins one line later.
        prefix_lines = template[:template.index("{student_code}")].count("\n")
        self.student_code_start_line = prefix_lines + 1 + (1 if self.cfg.mode == "stdio" else 0)

        template_patched = template.replace("{student_code}", _SENTINEL)
        result = template_patched.format(
            delim                  = self.delim,
            tc_params_comma        = tc_params_comma,
            tc_args                = "",
            call_solve_and_capture = call_solve_and_capture,
            tc_runner_body         = self._build_c_parallel_runner(),
        )
        return result.replace(_SENTINEL, student_raw)

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

    def _build_c_stdio_call(self) -> str:
        """
        C stdio mode: pipe _stdin_text into the child's stdin, call
        student_stdio_main(0, NULL), capture stdout into result.got.
        Uses tmpfile so both printf() and any other fd-1 writes are captured.
        """
        return r"""
    /* stdio mode: redirect stdin from _stdin_text, capture stdout via tmpfile */
    {
        int _sp[2];
        if (pipe(_sp) != 0) {
            strcpy(result.status, "ERROR");
            strcpy(result.detail, "pipe() failed for stdin redirect");
        } else {
            /* Feed TC input into pipe; close write end so student gets EOF */
            write(_sp[1], _stdin_text, strlen(_stdin_text));
            close(_sp[1]);

            int _saved_in  = dup(STDIN_FILENO);
            dup2(_sp[0], STDIN_FILENO);
            close(_sp[0]);

            /* Capture stdout (printf/puts/fwrite all go to fd 1) */
            FILE* _tmp       = tmpfile();
            int   _saved_out = dup(STDOUT_FILENO);
            dup2(fileno(_tmp), STDOUT_FILENO);

            /* Call through a K&R-style (no-prototype) function pointer so that
             * gcc does not prototype-check the call site.  This allows both
             *   int main(void)             and
             *   int main(int argc, char**)
             * to compile without "too few arguments" errors.  The student's
             * code reads input via stdin (redirected above) so argc/argv are
             * irrelevant in this context. */
            ((int(*)())student_stdio_main)();
            fflush(stdout);

            dup2(_saved_in,  STDIN_FILENO);  close(_saved_in);
            dup2(_saved_out, STDOUT_FILENO); close(_saved_out);

            char _cbuf[MAX_OUTPUT];
            memset(_cbuf, 0, sizeof(_cbuf));
            fseek(_tmp, 0, SEEK_SET);
            fread(_cbuf, 1, MAX_OUTPUT - 1, _tmp);
            fclose(_tmp);

            /* Strip trailing whitespace */
            int _cl = (int)strlen(_cbuf);
            while (_cl > 0 && (_cbuf[_cl-1] == '\n' || _cbuf[_cl-1] == '\r' ||
                                _cbuf[_cl-1] == ' '  || _cbuf[_cl-1] == '\t'))
                _cbuf[--_cl] = '\0';

            strncpy(result.got, _cbuf, sizeof(result.got) - 1);
        }
    }
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
    /* Fix 1.2: heap-allocate results — stack VLA for N={n} TCs is
     * {n} x {'{n}232'} bytes = {n*9232//1024} KB, which overflows the default
     * 8 MB RLIMIT_STACK and triggers a silent segfault.  calloc() puts it
     * on the heap and returns NULL on failure so we can emit a clean error.
     *
     * Batched execution: {n} TCs in batches of up to {MAX_PARALLEL_TCS} */
    TCResult *_results = (TCResult*)calloc({n}, sizeof(TCResult));
    if (!_results) {{
        for (int _ei = 1; _ei <= {n}; _ei++) {{
            printf("%sSTART_%d\\n", DELIM, _ei);
            printf("{{\\"status\\":\\"ERROR\\",\\"detail\\":\\"calloc failed: out of memory\\"}}\\n");
            printf("%sEND_%d\\n", DELIM, _ei);
        }}
        printf("%sDONE\\n", DELIM);
        fflush(stdout);
        return 1;
    }}
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
                g_i = batch_start + b_i
                if self.cfg.mode == "stdio":
                    stdin_esc  = self._c_string_escape(tc.stdin_text or "")
                    args_comma = f'"{stdin_esc}", '
                else:
                    args_str   = ", ".join(self._c_literal(v, "c") for v in (tc.inputs or []))
                    args_comma = (args_str + ", ") if args_str else ""
                lines.append(f"""        {{
            int _pfd[2];
            if (pipe(_pfd) == 0) {{
                pid_t _p = fork();
                if (_p == 0) {{
                    close(_pfd[0]);
                    run_tc_child(_pfd[1], {args_comma}{ps}, {mem_c});
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
        char _je_got[8193], _je_det[2049], _je_st[33];
        json_escape(_results[_i].status, _je_st,  sizeof(_je_st));
        json_escape(_results[_i].got,    _je_got, sizeof(_je_got));
        json_escape(_results[_i].detail, _je_det, sizeof(_je_det));
        printf("{d}START_%d\\n", _i + 1);
        printf("{{\\n");
        printf("  \\"status\\": \\"%s\\",\\n",   _je_st);
        printf("  \\"got\\": \\"%s\\",\\n",      _je_got);
        printf("  \\"detail\\": \\"%s\\"\\n",    _je_det);
        printf("}}\\n");
        printf("{d}END_%d\\n", _i + 1);
        fflush(stdout);
    }}
    free(_results);""")  # Fix 1.2: free heap allocation

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # C++

    # ─────────────────────────────────────────────────────────────────
    def _build_cpp(self) -> str:
        template    = (_HARNESSES_DIR / "cpp_harness.cpp").read_text()
        param_types = self._effective_param_types("cpp")

        # Sentinel approach — same rationale as _build_c: brace-escaping student
        # code with "{{" is wrong because format() does NOT unescape "{{" in VALUES,
        # only in the template string itself.
        _SENTINEL = "\x00STUDENT_CPP_RAW\x00"

        if self.cfg.mode == "stdio":
            student_raw = (
                "#define main student_stdio_main\n" +
                self.cfg.student_code +
                "\n#undef main"
            )
            tc_params_comma        = "const char* _stdin_text, "
            call_solve_and_capture = self._build_cpp_stdio_call()
        else:
            params      = ", ".join(f"{t} p{i}" for i, t in enumerate(param_types))
            args        = ", ".join(f"p{i}" for i in range(len(param_types)))
            # FIX-2: same trailing-comma logic as C
            tc_params_comma        = (params + ", ") if params else ""
            call_solve_and_capture = self._build_cpp_call()
            student_raw = self.cfg.student_code

        prefix_lines = template[:template.index("{student_code}")].count("\n")
        self.student_code_start_line = prefix_lines + 1 + (1 if self.cfg.mode == "stdio" else 0)

        template_patched = template.replace("{student_code}", _SENTINEL)
        result = template_patched.format(
            delim                  = self.delim,
            tc_params_comma        = tc_params_comma,
            tc_args                = "",
            call_solve_and_capture = call_solve_and_capture,
            tc_runner_body         = self._build_cpp_parallel_runner(),
        )
        return result.replace(_SENTINEL, student_raw)

    def _build_cpp_call(self) -> str:
        fn   = self.cfg.function_name
        rt   = self.cfg.return_type
        args = ", ".join(f"p{i}" for i in range(len(self.cfg.param_types or [])))
        if rt == "void":
            return f"{fn}({args});"
        if rt == "auto":
            # Stream the return value directly — works for any non-void return type
            # in C++11/14/17.  Users with a void function must pass return_type="void"
            # explicitly; auto+void would produce a compile error (oss << void).
            # The old if constexpr approach required C++17, but Judge0's default
            # GCC standard is gnu++14.
            return f"""
        oss << {fn}({args});"""
        return f"""
        auto ret = {fn}({args});
        oss << ret;
"""

    def _build_cpp_stdio_call(self) -> str:
        """
        C++ stdio mode: pipe _stdin_text into the child's stdin, call
        student_stdio_main(0, nullptr), capture stdout via fd dup so both
        printf() and std::cout output are captured.
        """
        return r"""
    /* C++ stdio mode: redirect stdin, capture all stdout (printf + cout) */
    {
        int _sp[2];
        if (pipe(_sp) != 0) {
            strncpy(result.status, "ERROR",  sizeof(result.status) - 1);
            strncpy(result.detail, "pipe() failed for stdin redirect", sizeof(result.detail) - 1);
        } else {
            write(_sp[1], _stdin_text, strlen(_stdin_text));
            close(_sp[1]);

            int _saved_in = dup(STDIN_FILENO);
            dup2(_sp[0], STDIN_FILENO);
            close(_sp[0]);

            /* Capture at fd level — catches printf, puts, cout, cerr→stdout, etc. */
            FILE* _tmp       = tmpfile();
            int   _saved_out = dup(STDOUT_FILENO);
            dup2(fileno(_tmp), STDOUT_FILENO);

            /* Call through a reinterpret_cast to a no-arg function pointer.
             * This suppresses the "too few arguments" error when the student
             * declares int main(int argc, char** argv) — both signatures are
             * accepted.  argc/argv are irrelevant since stdin is redirected. */
            reinterpret_cast<int(*)()>(student_stdio_main)();

            /* Flush C and C++ output buffers before restoring fd */
            fflush(stdout);
            std::cout.flush();

            dup2(_saved_in,  STDIN_FILENO);  close(_saved_in);
            dup2(_saved_out, STDOUT_FILENO); close(_saved_out);

            char _cbuf[8192];
            memset(_cbuf, 0, sizeof(_cbuf));
            fseek(_tmp, 0, SEEK_SET);
            fread(_cbuf, 1, sizeof(_cbuf) - 1, _tmp);
            fclose(_tmp);

            std::string got_str(_cbuf);
            while (!got_str.empty() &&
                   (got_str.back() == '\n' || got_str.back() == '\r' ||
                    got_str.back() == ' '  || got_str.back() == '\t'))
                got_str.pop_back();

            strncpy(result.got, got_str.c_str(), sizeof(result.got) - 1);
        }
    }
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
    /* Fix 1.2: heap-allocate results — same rationale as C runner.
     * Fix 4.1: no expected in result; OutputParser does comparison.
     * Batched execution: {n} TCs in batches of up to {MAX_PARALLEL_TCS} */
    TCResult *_results = (TCResult*)calloc({n}, sizeof(TCResult));
    if (!_results) {{
        for (int _ei = 1; _ei <= {n}; _ei++) {{
            std::cout << DELIM << "START_" << _ei << "\\n";
            std::cout << "{{\\"status\\":\\"ERROR\\",\\"detail\\":\\"calloc failed: out of memory\\"}}" << "\\n";
            std::cout << DELIM << "END_" << _ei << "\\n";
        }}
        std::cout << DELIM << "DONE" << std::endl;
        return 1;
    }}
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
                g_i = batch_start + b_i
                if self.cfg.mode == "stdio":
                    stdin_esc  = self._c_string_escape(tc.stdin_text or "")
                    args_comma = f'"{stdin_esc}", '
                else:
                    args_str   = ", ".join(self._c_literal(v, "cpp") for v in (tc.inputs or []))
                    args_comma = (args_str + ", ") if args_str else ""
                lines.append(f"""        {{
            int _pfd[2];
            if (pipe(_pfd) == 0) {{
                pid_t _p = fork();
                if (_p == 0) {{
                    close(_pfd[0]);
                    run_tc_child(_pfd[1], {args_comma}{ps}, {mem_c});
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
    /* Phase 3: Print results (Fix 4.1: no expected field) */
    for (int _i = 0; _i < {n}; _i++) {{
        char _je_got[8193], _je_det[2049], _je_st[33];
        json_escape(_results[_i].status, _je_st,  sizeof(_je_st));
        json_escape(_results[_i].got,    _je_got, sizeof(_je_got));
        json_escape(_results[_i].detail, _je_det, sizeof(_je_det));
        std::cout << "{d}START_" << (_i+1) << std::endl;
        std::cout << "{{" << std::endl;
        std::cout << "  \\"status\\": \\"" << _je_st  << "\\"," << std::endl;
        std::cout << "  \\"got\\": \\"" << _je_got << "\\"," << std::endl;
        std::cout << "  \\"detail\\": \\"" << _je_det << "\\"" << std::endl;
        std::cout << "}}" << std::endl;
        std::cout << "{d}END_" << (_i+1) << std::endl;
        std::cout.flush();
    }}
    free(_results);  /* Fix 1.2: free heap allocation */""")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────
    # JAVA

    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _preprocess_java_student_code(student_code: str) -> tuple:
        """
        Split student code into (extra_imports, class_body).

        Handles three distinct shapes of student code:

        1. No class declaration — raw method/field definitions (rare):
           Return code as-is; it is embedded directly inside Student {}.

        2. Single class (most common):
           Strip the outer class declaration and embed only the body.

        3. Multiple top-level classes:
           Students sometimes write helper classes before (or after) their
           solution class, e.g.:

               class Node { int val; Node next; ... }
               public class Solution {
                   public static void main(String[] args) { ... }
               }

           The OLD code used `.search()` which matched the FIRST class
           declaration and extracted only its body, losing every subsequent
           class — including the one that contains main().  Result:
           NoSuchMethodException: Main$Student.main(String[]).

           FIX: collect ALL top-level class declarations via brace-counting,
           identify the PRIMARY class (the one with main(), else the public
           class, else the last class), convert every helper class to a
           `static` nested class prepended inside Student, then append the
           primary class body. Because all helpers become static nested
           classes of Student, references like `new Node()` inside main()
           resolve to Main$Student$Node — the same loader, no access error.
        """
        import re

        # ── Step 1: extract import statements ─────────────────────────────
        lines = student_code.split('\n')
        import_lines = []
        other_lines = []
        for line in lines:
            stripped = line.strip()
            if re.match(r'^import\s+[\w.*]+\s*;', stripped):
                import_lines.append(stripped)
            else:
                other_lines.append(line)

        remaining = '\n'.join(other_lines)
        extra_imports = '\n'.join(import_lines)

        # ── Step 2: find ALL top-level type declarations ───────────────────
        # Matches class / interface / enum / record — not just "class".
        # A previous version matched only "class", so a top-level
        # `interface Payment { ... }` (or enum/record) preceding the solution
        # class was silently dropped, and every class that `implements` it
        # failed to compile with "cannot find symbol: Payment".
        # Regex: optional modifiers, the type keyword, name, optional generic
        # params, optional record components, optional extends/implements/
        # permits clause, then the opening "{".
        _type_decl_re = re.compile(
            r'(?:(?:public|protected|private|abstract|final|static|sealed|non-sealed)\s+)*'
            r'(?:class|interface|enum|record)\s+\w+'
            r'(?:<[^>]*>)?'                          # generic params <T>, <A, B>
            r'(?:\s*\([^)]*\))?'                     # record header components
            r'(?:\s+extends\s+[\w.<>,\[\]\s]+?)?'    # extends A / A, B (interfaces)
            r'(?:\s+implements\s+[\w.<>,\[\]\s]+?)?' # implements X, Y
            r'(?:\s+permits\s+[\w.,\s]+?)?'          # sealed permits list
            r'\s*\{',
        )
        _kind_re = re.compile(r'\b(class|interface|enum|record)\b')

        classes = []      # list of dicts: {header, body, kind, is_public}
        search_from = 0
        while True:
            m = _type_decl_re.search(remaining, search_from)
            if not m:
                break
            header_text = remaining[m.start():m.end()]
            body_start  = m.end()
            depth = 1
            i = body_start
            while i < len(remaining) and depth > 0:
                if   remaining[i] == '{': depth += 1
                elif remaining[i] == '}': depth -= 1
                i += 1
            km     = _kind_re.search(header_text)
            kind   = km.group(1) if km else 'class'
            # Modifiers appear before the type keyword (class/interface/…).
            prefix = header_text[:km.start()] if km else header_text
            classes.append({
                'header':    header_text,
                'body':      remaining[body_start : i - 1],
                'kind':      kind,
                'is_public': 'public' in prefix,
            })
            # jump past this declaration's closing brace so inner types
            # are NOT picked up as separate top-level entries
            search_from = i

        # ── Step 3: no type found → return raw code ───────────────────────
        if not classes:
            return extra_imports, remaining

        # ── Step 4: single declaration → strip wrapper, return body ───────
        if len(classes) == 1:
            return extra_imports, classes[0]['body']

        # ── Step 5: multiple types → find primary, nest helpers ────────────
        def _has_main(body: str) -> bool:
            return bool(re.search(
                r'public\s+static\s+void\s+main\s*\(\s*String', body))

        # Priority: type with main() > public class > last class > last decl.
        # Interfaces/enums/records are never chosen as primary unless nothing
        # else qualifies — the primary holds main()/the target method.
        primary_idx = None
        for idx, cls in enumerate(classes):
            if _has_main(cls['body']):
                primary_idx = idx
                break
        if primary_idx is None:
            for idx, cls in enumerate(classes):
                if cls['is_public'] and cls['kind'] == 'class':
                    primary_idx = idx
                    break
        if primary_idx is None:
            for idx in range(len(classes) - 1, -1, -1):
                if classes[idx]['kind'] == 'class':
                    primary_idx = idx
                    break
        if primary_idx is None:
            primary_idx = len(classes) - 1

        primary = classes[primary_idx]
        helpers = [cls for idx, cls in enumerate(classes) if idx != primary_idx]

        # Convert each helper to a static nested type inside _Harness_.
        # Top-level types cannot carry 'static'; nested interfaces/enums are
        # implicitly static, so adding it is harmless and lets helper classes
        # be referenced from the primary body without an enclosing instance.
        parts = []
        for helper in helpers:
            header = helper['header']
            km     = _kind_re.search(header)
            prefix = header[:km.start()] if km else header
            if 'static' not in prefix:
                header = 'static ' + header
            parts.append(header + helper['body'] + '\n}')

        # Primary body comes last (after all helper definitions)
        parts.append(primary['body'])
        return extra_imports, '\n'.join(parts)

    def _build_java(self) -> str:
        # Java has too many literal { } braces for Python .format() — use replace() instead.
        template = (_HARNESSES_DIR / "java_harness.java").read_text()

        extra_imports, class_body = self._preprocess_java_student_code(self.cfg.student_code)

        inner_class = (
            "\n    static class _Harness_ {\n" +
            textwrap.indent(class_body, "        ") +
            "\n    }\n"
        )

        ptypes = self._effective_param_types("java")
        if ptypes:
            items = ", ".join(f'"{t}"' for t in ptypes)
            param_types_array = "new String[]{" + items + "}"
        else:
            param_types_array = "new String[]{}"

        replacements = {
            "{delim}":             self.delim,
            "{mode}":              self.cfg.mode,
            "{per_tc_limit_ms}":   str(self.cfg.per_tc_limit_s * 1000),
            "{memory_limit_mb}":   str(self.cfg.memory_limit_mb),
            "{function_name}":     self.cfg.function_name,
            "{param_types_array}": param_types_array,
            "{tc_runner_body}":    self._build_java_parallel_runner(),
            "{extra_imports}":     extra_imports,
            # Process student code LAST so none of the above replacements
            # can match a placeholder string that happens to appear inside
            # student code (e.g. a Java string literal "\{per_tc_limit_ms}").
            "{student_code_as_inner_class}": inner_class,
        }

        result = template
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)

        # Calculate where student code body starts in the generated harness.
        # javac error lines and stack frames reference this file — we subtract the
        # offset so students see line numbers relative to their own code.
        try:
            marker = "static class Student {"
            idx = result.index(marker)
            # +1 for the { line itself, +1 again because body starts on next line
            self.student_code_start_line = result[:idx + len(marker)].count("\n") + 2
        except ValueError:
            self.student_code_start_line = 0

        # Fill the runtime constant used by studentDetailMsg() in the Java harness.
        result = result.replace("{student_code_start_line}", str(self.student_code_start_line))

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

        # Build launch calls (Fix 4.1: expected removed; OutputParser does comparison)
        lines.append("        /* Phase 1: Create all threads (not started yet) */")
        for i, tc in enumerate(self.cfg.test_cases):
            if self.cfg.mode == "stdio":
                stdin = (tc.stdin_text or "").replace('"', '\\"').replace("\n", "\\n")
                lines.append(f"""
        _threads[{i}] = launchStdioTC("{stdin}", _resultRefs[{i}]);""")
            else:
                inputs_arr = ", ".join(
                    self._java_literal(v) for v in (tc.inputs or [])
                )
                lines.append(f"""
        {{
            Object[] _in{i} = {{ {inputs_arr} }};
            _threads[{i}] = launchFunctionTC(_in{i},
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
