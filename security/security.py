"""
security.py — v2
─────────────────
FIX 4: AST-based analysis for Python, regex for C/C++/Java.

Python AST checks:
  Walks the actual parse tree, not raw text.
  Catches aliased imports, from-imports, __import__() calls,
  exec()/eval() calls, attribute access on blocked modules,
  dangerous builtins — all of which bypass naive regex.

  What AST still cannot catch:
    exec("import os")   ← string content is opaque to AST
    importlib tricks    ← caught by Call node check on importlib
  These fall through to Judge0's sandbox (the real security boundary).

C / C++ / Java:
  Regex is sufficient because:
  - These languages compile; you cannot exec() a runtime string
    into native dangerous code the way Python can
  - Structural patterns (system(), #include <socket>) are reliably
    detectable by regex without aliasing risk
"""

import re
import ast
from dataclasses import dataclass, field
from typing import List, Tuple, Optional


@dataclass
class SecurityViolation:
    rule:    str
    detail:  str
    lineno:  Optional[int] = None


@dataclass
class SecurityCheckResult:
    passed:     bool
    violations: List[SecurityViolation] = field(default_factory=list)

    @property
    def reason(self) -> str:
        if not self.violations:
            return ""
        v = self.violations[0]
        loc = f" (line {v.lineno})" if v.lineno else ""
        return f"{v.rule}{loc}: {v.detail}"


# ── Infinite loop helpers (used by _PythonASTChecker) ────────────────────

def _is_const_true(node: ast.expr) -> bool:
    """Return True if node is the literal True or 1."""
    if isinstance(node, ast.Constant):
        return node.value in (True, 1)
    return False


def _body_can_exit(stmts: list) -> bool:
    """
    Return True if any statement in `stmts` contains a break, return, or
    raise that could terminate the enclosing while loop.

    Does NOT recurse into nested function/class/lambda definitions because
    a `return` inside `def f(): return 1` does not exit the outer loop.
    """
    for stmt in stmts:
        if isinstance(stmt, (ast.Break, ast.Return, ast.Raise)):
            return True
        # Skip nested scopes — their exits don't affect the outer loop
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.ClassDef, ast.Lambda)):
            continue
        # ExceptHandler is not a statement — handle it directly
        if isinstance(stmt, ast.ExceptHandler):
            if _body_can_exit(stmt.body):
                return True
            continue
        # Recurse into control-flow sub-bodies (if/for/while/try/with)
        # Note: handlers is a list of ExceptHandler, handled above via Try.handlers
        for _attr in ("body", "orelse", "finalbody"):
            sub = getattr(stmt, _attr, None)
            if isinstance(sub, list) and _body_can_exit(sub):
                return True
        # ast.Try.handlers is List[ExceptHandler] — recurse into each
        handlers = getattr(stmt, "handlers", None)
        if isinstance(handlers, list):
            for h in handlers:
                if isinstance(h, ast.ExceptHandler) and _body_can_exit(h.body):
                    return True
    return False


# ── PYTHON AST VISITOR ───────────────────────────────────────────────────

class _PythonASTChecker(ast.NodeVisitor):
    """
    Walks the Python AST and raises _Violation on dangerous patterns.

    Catches what regex cannot:
      import os as o              → Import node, alias.name = "os"
      from os import path         → ImportFrom node, module = "os"
      from os import *            → ImportFrom node
      __import__("os")            → Call node, func.id = "__import__"
      importlib.import_module("os")→ Attribute node check
      signal.signal(...)          → Attribute call on blocked module
    """

    BLOCKED_MODULES = {
        "os", "subprocess", "socket", "ctypes", "signal",
        "importlib", "multiprocessing", "threading",
        "pty", "tty", "termios", "fcntl", "select",
        "mmap", "resource", "pwd", "grp", "syslog",
        "posix", "posixpath", "nt",
        # FIX-13a: io.open / io.FileIO bypass builtins.open monkey-patch.
        # The harness imports `io` before student code; blocking the student
        # from re-importing it closes the bypass without touching the harness.
        "io",
        # FIX-13b: pathlib.Path.read_text() / .write_text() bypass open().
        "pathlib",
        # sys: sys.modules['os'] gives access to already-loaded dangerous modules
        # without an import statement, bypassing all Import/ImportFrom AST checks.
        # Example: def solve(n): return sys.modules['os'].system('...')
        "sys",
        # builtins: import builtins; builtins.open = real_open bypasses
        # the harness monkey-patch of builtins.open with _safe_open.
        "builtins",
    }

    BLOCKED_BUILTINS = {
        "__import__", "exec", "eval", "compile",
        "globals", "locals", "vars", "dir",
        "breakpoint", "memoryview",
    }

    BLOCKED_DUNDER = {
        "__class__", "__bases__", "__subclasses__",
        "__globals__", "__builtins__", "__code__",
        "__import__",
    }

    def __init__(self):
        self.violations: List[SecurityViolation] = []

    def _add(self, rule: str, detail: str, node):
        lineno = getattr(node, "lineno", None)
        self.violations.append(SecurityViolation(rule, detail, lineno))

    # ── Import / ImportFrom ──────────────────────────────────────────────
    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            top = alias.name.split(".")[0]
            if top in self.BLOCKED_MODULES:
                self._add(
                    "BlockedImport",
                    f"Module '{alias.name}' is not allowed"
                    + (f" (aliased as '{alias.asname}')" if alias.asname else ""),
                    node
                )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ""
        top    = module.split(".")[0]
        if top in self.BLOCKED_MODULES:
            names = ", ".join(a.name for a in node.names)
            self._add(
                "BlockedImport",
                f"'from {module} import {names}' is not allowed",
                node
            )
        self.generic_visit(node)

    # ── Call nodes ──────────────────────────────────────────────────────
    def visit_Call(self, node: ast.Call):
        func = node.func

        # __import__("os"), exec(...), eval(...), compile(...)
        if isinstance(func, ast.Name):
            if func.id in self.BLOCKED_BUILTINS:
                self._add(
                    "BlockedBuiltin",
                    f"'{func.id}()' is not allowed",
                    node
                )

        # importlib.import_module(...)
        # signal.signal(...)  ← attribute call on blocked module
        elif isinstance(func, ast.Attribute):
            if isinstance(func.value, ast.Name):
                if func.value.id in self.BLOCKED_MODULES:
                    self._add(
                        "BlockedModuleCall",
                        f"'{func.value.id}.{func.attr}()' is not allowed",
                        node
                    )

        self.generic_visit(node)

    # ── Attribute access ─────────────────────────────────────────────────
    def visit_Attribute(self, node: ast.Attribute):
        # Catch dunder attribute access used for introspection escapes
        # e.g. ().__class__.__bases__[0].__subclasses__()
        if node.attr in self.BLOCKED_DUNDER:
            self._add(
                "BlockedDunder",
                f"Attribute access '*.{node.attr}' is not allowed",
                node
            )
        self.generic_visit(node)

    # ── open() as a Name ────────────────────────────────────────────────
    def visit_Name(self, node: ast.Name):
        # Student calling open() directly — caught here as a name reference
        # (actual call is caught in visit_Call, but this catches assignments
        # like `f = open` followed by `f(...)`)
        if node.id == "open" and isinstance(node.ctx, ast.Load):
            # Allow — our harness replaces open() with safe version anyway.
            # We catch it at the Call level instead.
            pass
        self.generic_visit(node)

    # ── Direct infinite recursion detection ─────────────────────────────
    def visit_FunctionDef(self, node: ast.FunctionDef):
        # Detect the simplest case: entire function body is a single statement
        # that unconditionally calls itself — e.g. `def solve(n): return solve(n)`.
        # Only flag single-statement bodies; multi-statement bodies may have a
        # base case somewhere (we leave those for Judge0 to time out).
        real_body = [s for s in node.body if not isinstance(s, ast.Expr)
                     or not isinstance(s.value, ast.Constant)]
        if len(real_body) == 1:
            stmt = real_body[0]
            call_node = None
            if isinstance(stmt, ast.Return) and isinstance(stmt.value, ast.Call):
                call_node = stmt.value
            elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                call_node = stmt.value
            if call_node is not None:
                func = call_node.func
                # Direct self-call: solve(...) or self.solve(...) etc.
                called_name = (
                    func.id if isinstance(func, ast.Name) else
                    func.attr if isinstance(func, ast.Attribute) else None
                )
                if called_name == node.name:
                    self._add(
                        "InfiniteLoop",
                        f"'{node.name}()' calls itself unconditionally — guaranteed TLE",
                        node,
                    )
        self.generic_visit(node)

    # async def has a separate AST node but identical structure
    visit_AsyncFunctionDef = visit_FunctionDef

    # ── Infinite loop detection ──────────────────────────────────────────
    def visit_While(self, node: ast.While):
        # Flag while True / while 1 only when the body has no way out.
        # We walk the body but do NOT recurse into nested function/class
        # definitions — a `return` inside `def f(): return 1` does NOT
        # exit the outer while loop.
        if _is_const_true(node.test) and not _body_can_exit(node.body):
            self._add(
                "InfiniteLoop",
                "while True/1 with no break, return, or raise — guaranteed TLE",
                node,
            )
        self.generic_visit(node)


def _check_python_ast(code: str) -> List[SecurityViolation]:
    """
    Parse student code into AST and walk it.
    Returns list of violations found.
    Handles SyntaxError gracefully — returns a syntax violation.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return [SecurityViolation("SyntaxError", str(e), e.lineno)]

    checker = _PythonASTChecker()
    checker.visit(tree)
    return checker.violations


_INFINITE_LOOP_RE = re.compile(
    r"\b(?:"
    r"while\s*\(\s*(?:1|true)\s*\)"   # while(1) / while(true)
    r"|for\s*\(\s*;\s*;\s*\)"          # for(;;)
    r"|do\s*\{[^}]*\}\s*while\s*\(\s*(?:1|true)\s*\)"  # do { } while(1/true)
    r")",
    re.IGNORECASE | re.DOTALL,
)

def _check_c_infinite_loops(code: str) -> List[SecurityViolation]:
    """
    Detect unconditional infinite loops in C/C++/Java where the ENTIRE
    submission has no break or return statement — guaranteeing TLE.
    If there IS a break/return somewhere, the loop may be intentional
    (e.g. while(1) { ... break; }) so we leave it for Judge0 to judge.
    """
    m = _INFINITE_LOOP_RE.search(code)
    if not m:
        return []
    # Only flag if there is truly no exit path in the whole submission
    has_exit = bool(re.search(r"\b(?:break|return)\b", code))
    if has_exit:
        return []
    lineno = code[:m.start()].count("\n") + 1
    loop_text = m.group(0).strip()
    return [SecurityViolation(
        "InfiniteLoop",
        f"{loop_text} with no break/return in submission — guaranteed TLE",
        lineno,
    )]


# ── C / C++ REGEX CHECKS ────────────────────────────────────────────────

_C_BLOCKED: List[Tuple[str, str]] = [
    (r"\bsystem\s*\(",               "system() not allowed"),
    (r"\bpopen\s*\(",                "popen() not allowed"),
    (r"\bexecv[ep]?\s*\(",           "exec() family not allowed"),
    (r"\bexecl[ep]?\s*\(",           "exec() family not allowed"),
    (r"\bfork\s*\(",                 "fork() not allowed"),
    (r"\bptrace\s*\(",               "ptrace() not allowed"),
    (r"\bprctl\s*\(",                "prctl() not allowed"),
    (r'#\s*include\s*[<"]\s*sys/socket\s*[>"]',  "socket headers not allowed"),
    (r'#\s*include\s*[<"]\s*netinet',             "network headers not allowed"),
    (r'#\s*include\s*[<"]\s*arpa',                "network headers not allowed"),
    (r'#\s*include\s*[<"]\s*sys/ptrace',          "ptrace header not allowed"),
    (r"\bdlopen\s*\(",               "dynamic loading not allowed"),
    (r"\bdlsym\s*\(",                "dynamic loading not allowed"),
    (r"\bmmap\s*\(",                 "mmap() not allowed"),
    (r"\bsignal\s*\(\s*SIGALRM",     "SIGALRM override not allowed"),
    (r"\bsetitimer\s*\(",            "setitimer() not allowed"),
    # FIX-15: inline assembly can invoke arbitrary syscalls, bypassing all
    # regex pattern checks above. Block all GCC/Clang/MSVC asm syntax.
    (r"\b(?:asm|__asm__|__asm)\b",   "inline assembly not allowed"),
    # syscall(): raw kernel invocation bypasses every blocked libc wrapper
    # (fork, execve, socket, etc.) since the harness blocks the C-library
    # functions but not the underlying syscall() entry point.
    (r"\bsyscall\s*\(",              "syscall() not allowed"),
    # fopen/open for file access — sandbox filesystem is limited but readable
    (r"\bfopen\s*\(",                "fopen() not allowed"),
    (r'\bopen\s*\(\s*"/',            'open() with absolute path not allowed'),
]

# ── JAVA REGEX CHECKS ───────────────────────────────────────────────────

_JAVA_BLOCKED: List[Tuple[str, str]] = [
    (r"\bRuntime\s*\.\s*getRuntime\s*\(",   "Runtime.getRuntime() not allowed"),
    (r"\bProcessBuilder\b",                  "ProcessBuilder not allowed"),
    (r"\bSystem\s*\.\s*exit\s*\(",           "System.exit() not allowed"),
    (r"\bjava\s*\.\s*io\s*\.\s*File\b",     "java.io.File not allowed"),
    (r"\bjava\s*\.\s*net\b",                 "java.net not allowed"),
    (r"\bClass\s*\.\s*forName\s*\(",         "Class.forName() not allowed"),
    (r"\bThread\s*\.\s*currentThread\s*\(",  "Thread manipulation not allowed"),
    (r"\bClassLoader\b",                     "ClassLoader not allowed"),
    (r"\bReflection\b",                      "Reflection not allowed"),
    (r"\bjava\s*\.\s*lang\s*\.\s*reflect",   "java.lang.reflect not allowed"),
    (r"\bUnsafe\b",                          "sun.misc.Unsafe not allowed"),
    # File access via constructors that accept a String path — java.io.File is
    # blocked but FileReader(String), FileInputStream(String), etc. are not.
    (r"\bFileReader\s*\(",                   "java.io.FileReader not allowed"),
    (r"\bFileWriter\s*\(",                   "java.io.FileWriter not allowed"),
    (r"\bFileInputStream\s*\(",              "java.io.FileInputStream not allowed"),
    (r"\bFileOutputStream\s*\(",             "java.io.FileOutputStream not allowed"),
    # java.nio file access
    (r"\bjava\s*\.\s*nio\b",                 "java.nio not allowed"),
    (r"\bFiles\s*\.\s*read",                 "java.nio.file.Files.read*() not allowed"),
    (r"\bPaths\s*\.\s*get\s*\(",             "java.nio.file.Paths.get() not allowed"),
]


# ── MAIN CHECKER CLASS ───────────────────────────────────────────────────

class SecurityChecker:

    MAX_CODE_LENGTH = 10_000

    def check(
        self,
        student_code: str,
        language:     str,
        session_delim: str,
    ) -> SecurityCheckResult:

        violations: List[SecurityViolation] = []

        # 1. Null bytes / binary content
        if "\x00" in student_code:
            violations.append(SecurityViolation(
                "BinaryContent", "Null bytes detected in submission"
            ))
            return SecurityCheckResult(False, violations)

        # 2. Code length
        if len(student_code) > self.MAX_CODE_LENGTH:
            violations.append(SecurityViolation(
                "CodeTooLong",
                f"Submission exceeds {self.MAX_CODE_LENGTH} character limit "
                f"({len(student_code)} chars)"
            ))
            return SecurityCheckResult(False, violations)

        # 3. Delimiter injection — student trying to spoof TC output
        if session_delim in student_code:
            violations.append(SecurityViolation(
                "DelimiterInjection",
                "Output delimiter injection attempt detected"
            ))
            return SecurityCheckResult(False, violations)

        # 4. Language-specific checks
        lang = language.lower()
        if lang == "python":
            violations.extend(_check_python_ast(student_code))
        elif lang in ("c", "cpp"):
            violations.extend(_check_regex(student_code, _C_BLOCKED))
            violations.extend(_check_c_infinite_loops(student_code))
        elif lang == "java":
            violations.extend(_check_regex(student_code, _JAVA_BLOCKED))
            violations.extend(_check_c_infinite_loops(student_code))

        if violations:
            return SecurityCheckResult(False, violations)

        return SecurityCheckResult(True)


def _check_regex(
    code: str,
    patterns: List[Tuple[str, str]],
) -> List[SecurityViolation]:
    violations = []
    for pattern, reason in patterns:
        m = re.search(pattern, code, re.IGNORECASE | re.MULTILINE)
        if m:
            # Estimate line number from match position
            lineno = code[:m.start()].count("\n") + 1
            violations.append(SecurityViolation("BlockedPattern", reason, lineno))
    return violations


# ── SANITIZATION ─────────────────────────────────────────────────────────

def sanitize_for_injection(student_code: str, language: str) -> str:
    """
    Final cleanup before injecting into harness template string.
    Does NOT modify code semantics — only prevents template breakage.
    """
    if language == "python":
        # Strip trailing whitespace per line — prevents indentation issues
        return "\n".join(line.rstrip() for line in student_code.split("\n"))

    elif language in ("c", "cpp", "java"):
        # Prevent */ from closing harness block comments prematurely
        return student_code.replace("*/", "* /")

    return student_code
