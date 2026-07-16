"""
output_parser.py
────────────────
Parses structured harness output into clean result objects.

Handles:
- Partial output (Judge0 global limit fires mid-harness)
- Student printing garbage between TC blocks
- Missing TC blocks (crash before delimiter printed)
- JSON parse errors in TC blocks
- Detecting DONE marker absence (global TLE)
"""

import re
import json
from dataclasses import dataclass, field
from typing import Optional


VALID_STATUSES = {"PASS", "FAIL", "TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT", "CE", "MISSING"}

# Statuses a harness is ALLOWED to emit. PASS and FAIL are never emitted by
# the harness — they are assigned only by OutputParser after comparison.
# Accepting PASS/FAIL from harness output would allow students to forge
# verdicts by writing fake TCResult structs to the result pipe.
_HARNESS_STATUSES = {"TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT"}


def _num_equal(a_str: str, b_str: str) -> bool:
    """Numeric tolerance comparison — only for float-typed expected values.

    If the expected string (b_str) is a plain integer (digits only, optional
    leading minus), float comparison is skipped entirely so that "3.0" is NOT
    accepted when the expected output is "3".  The caller's exact string match
    (got_str == expected_str) is the only path to PASS for integer outputs.

    When expected IS float-like (contains '.' or 'e'/'E' exponent notation),
    relative+absolute epsilon tolerance is applied (1e-6 relative, 1e-9 absolute).
    """
    # Plain integer expected → never accept a float representation.
    # "-3", "0", "42" must not match "-3.0", "0.0", "42.0".
    if re.fullmatch(r"-?\d+", b_str):
        return False
    try:
        a, b = float(a_str), float(b_str)
        if a != a or b != b:   # NaN check
            return False
        return abs(a - b) <= max(1e-9, abs(b) * 1e-6)
    except (ValueError, TypeError):
        return False


@dataclass
class TCResult:
    tc_num:   int
    status:   str                   # PASS | FAIL | TLE | MLE | SEGV | FPE | ERROR | MISSING | CE
    got:      str       = ""
    expected: str       = ""
    detail:   str       = ""
    warning:  str       = ""        # e.g. signal_override_attempted


@dataclass
class ParsedSubmission:
    tc_results:         list[TCResult]
    global_tle:         bool = False   # True if DONE marker never appeared
    score:              int  = 0       # number of PASS results
    total:              int  = 0
    partial_execution:  bool = False   # True if some TCs are MISSING


class OutputParser:

    def __init__(self, raw_output: str, session_id: str, total_tc_count: int,
                 expected_values: list = None):
        self.raw             = raw_output
        self.delim           = f"@@TC_RESULT__{session_id}__"
        self.total_tc_count  = total_tc_count
        self.expected_values = expected_values or []

    def parse(self) -> ParsedSubmission:
        result    = ParsedSubmission(tc_results=[], total=self.total_tc_count)
        found_tcs = set()

        # Check for DONE marker — absence means global TLE or crash
        done_marker  = f"{self.delim}DONE"
        result.global_tle = done_marker not in self.raw

        # Extract TC blocks using unique delimiter
        pattern = re.compile(
            rf"{re.escape(self.delim)}START_(\d+)\n(.*?){re.escape(self.delim)}END_\1",
            re.DOTALL
        )

        for match in pattern.finditer(self.raw):
            tc_num  = int(match.group(1))
            content = match.group(2).strip()
            found_tcs.add(tc_num)

            tc_result = self._parse_tc_block(tc_num, content)
            result.tc_results.append(tc_result)

        # Identify TCs that never produced output
        # If global TLE fired → mark them TLE (not MISSING)
        # because the cause is time, not a code crash
        for i in range(1, self.total_tc_count + 1):
            if i not in found_tcs:
                if result.global_tle:
                    status = "TLE"
                    detail = "TC not reached — earlier TC caused global time limit exceeded"
                else:
                    status = "MISSING"
                    detail = "TC not reached — likely crash or error in earlier TC"
                result.tc_results.append(TCResult(
                    tc_num = i,
                    status = status,
                    detail = detail,
                ))

        # Sort by TC number
        result.tc_results.sort(key=lambda r: r.tc_num)

        # Score
        result.score          = sum(1 for r in result.tc_results if r.status == "PASS")
        result.partial_execution = len(found_tcs) < self.total_tc_count

        return result

    def _parse_tc_block(self, tc_num: int, content: str) -> TCResult:
        """Parse a single TC block. Content should be JSON."""
        try:
            data = json.loads(content)

            status = data.get("status", "ERROR")
            if status not in _HARNESS_STATUSES:
                # Reject PASS/FAIL from harness: harness never emits them legitimately.
                # Receiving them means student wrote a fake result to the pipe.
                status = "ERROR"

            # Fix 4.1: harnesses emit OUTPUT; comparison done here with expected_values
            if status == "OUTPUT":
                if self.expected_values:
                    tc_idx       = tc_num - 1
                    expected_str = str(self.expected_values[tc_idx]).strip() \
                                   if tc_idx < len(self.expected_values) else ""
                    got_str      = str(data.get("got", "")).strip()
                    passed       = (got_str == expected_str) or _num_equal(got_str, expected_str)
                    return TCResult(
                        tc_num   = tc_num,
                        status   = "PASS" if passed else "FAIL",
                        got      = got_str,
                        expected = expected_str,
                        detail   = str(data.get("detail", "")),
                        warning  = str(data.get("warning", "")),
                    )
                else:
                    # No expected values provided — treat as configuration error
                    status = "ERROR"

            return TCResult(
                tc_num   = tc_num,
                status   = status,
                got      = str(data.get("got", "")),
                expected = str(data.get("expected", "")),
                detail   = str(data.get("detail", "")),
                warning  = str(data.get("warning", "")),
            )

        except json.JSONDecodeError:
            # Content wasn't valid JSON — student may have corrupted output.
            # Only scan for harness-emitted statuses — never assign PASS/FAIL
            # from raw text, as that would let a student forge a PASS verdict
            # by injecting the literal string "PASS" into corrupted output.
            status = "ERROR"
            for s in _HARNESS_STATUSES:
                if s in content:
                    status = s
                    break

            return TCResult(
                tc_num = tc_num,
                status = status,
                detail = f"Output parse error — raw: {content[:100]}",
            )


# Harness function names that must never appear in student-facing error messages.
# These are the C/C++ harness runner functions; errors inside them are cascades
# from broken student code and are meaningless to students.
_HARNESS_FN_SYMS = re.compile(r'\b(run_tc_child)\b')

# The alias the C/C++ harness injects for the student's main():
#   #define main student_stdio_main
# Errors *inside* student_stdio_main belong to the student.
# Cascade references to it OUTSIDE any "In function" block
# (e.g. "'student_stdio_main' was not declared in this scope") are harness noise.
_STUDENT_ALIAS_SYMS = re.compile(r'\b(student_stdio_main)\b')

# Errors that fire when the student never defined main().
# gcc (C):    'student_stdio_main' undeclared (first use…)     — K&R-cast form
# gcc/ld (C): undefined reference to `student_stdio_main'      — old direct-call form
# g++ (C++):  'student_stdio_main' was not declared in this scope
_MISSING_MAIN_RE = re.compile(
    r"(?:"
    r"[`'\"]student_stdio_main[`'\"] undeclared"
    r"|"
    r"\bundefined reference to [`'\"]student_stdio_main"
    r"|"
    r"[`'\"]student_stdio_main[`'\"] was not declared in this scope"
    r")"
)

# gcc/g++ "In function '…':" context lines.
_IN_FN_RE = re.compile(r"^.*: In function '([^']*)':")

# gcc/g++ source-context lines: "   65 | int foo() {"
# These show the student's literal source code — they must NEVER be filtered,
# even if they contain 'student_stdio_main' (the macro-renamed version of main).
_SRC_CONTEXT_RE = re.compile(r'^\s+\d+\s*\|')


def _adjust_compile_output(raw: str, student_code_start_line: int) -> str:
    """
    Rewrite compiler-reported line numbers from harness-absolute to
    student-relative, and strip errors that reference harness internals.

    Handles javac, gcc, and g++ output formats:
      Java:  'Main.java:52: error: …'       → 'line 6: error: …'
      C/C++: '/tmp/sol.c:63:10: error: …'   → 'line 4: error: …'

    Also adjusts source-context lines emitted by modern gcc/g++:
      '  42 | int student_stdio_main(void) {'  →  '   1 | int main(void) {'

    Filtering rules for C/C++ harness noise
    ─────────────────────────────────────────
    • "In function 'run_tc_child':" → skip the entire error group (cascade).
    • "In function 'student_stdio_main':" → student's own main(); show as
      "In function 'main':" and include the group.
    • Error/note HEADER lines outside any "In function" block that reference
      student_stdio_main (e.g. "'student_stdio_main' was not declared") → skip.
    • Source-context lines ("   N | code...") that contain student_stdio_main
      are KEPT and the alias is renamed to "main".  These lines show the
      student's actual code; filtering them would hide the error location.
      BUG in old code: the source-context line `65 | int student_stdio_main(...`
      triggered skip=True, silencing all subsequent error messages.

    Note: modern gcc (≥14) on some distros emits Unicode typographic single
    quotes (U+2018/U+2019) around function names in "In function '…':" lines
    instead of the ASCII apostrophe.  Normalize them so regexes still match.
    """
    if student_code_start_line <= 1:
        return raw
    # Normalize typographic quotes (U+2018 LEFT, U+2019 RIGHT) → ASCII apostrophe.
    # gcc ≥14 on some systems uses these instead of the plain apostrophe in
    # diagnostic messages like: In function ‘student_stdio_main’:
    raw = raw.replace('\u2018', "'").replace('\u2019', "'")

    # Pre-scan: detect "undefined reference to student_stdio_main" BEFORE the
    # per-line loop, because that loop’s skip=True (from run_tc_child’s error
    # group) would hide this linker error before _STUDENT_ALIAS_SYMS could catch
    # it.  This error means the student never defined main().
    if _MISSING_MAIN_RE.search(raw):
        return (
            "error: ‘main’ function not found in your code.\n"
            "       Define: int main(void) { ... }"
        )

    offset = student_code_start_line - 1
    is_java = "Main.java:" in raw

    def _fix_header(m):
        return f"line {max(1, int(m.group(1)) - offset)}:"

    def _fix_context(m):
        return f"{m.group(1)}{max(1, int(m.group(2)) - offset)}{m.group(3)}"

    out  = []
    skip = False   # True = inside a harness function's error group

    for line in raw.splitlines():
        in_fn_m = _IN_FN_RE.match(line)

        if in_fn_m:
            fn_name = in_fn_m.group(1)
            if _HARNESS_FN_SYMS.search(fn_name):
                # run_tc_child — skip entire group; pop any orphaned preamble.
                skip = True
                while out and _IN_FN_RE.match(out[-1]):
                    out.pop()
                continue
            else:
                # Student function (student_stdio_main) or genuine helper.
                # Either way, include the group (rename alias below).
                skip = False
                # fall through to rename + append

        elif skip:
            # Inside a harness error group — skip until next "In function".
            continue

        elif _STUDENT_ALIAS_SYMS.search(line):
            # Line OUTSIDE any "In function" block that mentions student_stdio_main.
            #
            # Two sub-cases:
            #   1. Source-context lines ("   N | code..."):
            #      The macro renamed the student's 'main' to 'student_stdio_main'
            #      in the compiled file, so this line is genuine student code.
            #      Keep it — just rename for display clarity.
            #   2. Other error/note header lines (cascade from broken student_stdio_main):
            #      Meaningless to the student — skip.
            #   Note: linker "undefined reference to 'student_stdio_main'" is
            #   intercepted by the _MISSING_MAIN_RE pre-scan above, so it never
            #   reaches this branch.
            if _SRC_CONTEXT_RE.match(line):
                pass   # source context — rename alias below, then append
            else:
                skip = True
                while out and _IN_FN_RE.match(out[-1]):
                    out.pop()
                continue

        # Rename the harness alias to the student-facing name in ALL kept lines
        # (error headers, "In function" lines, and source context).
        line = line.replace('student_stdio_main', 'main')

        # Adjust error-header line numbers:  main.cpp:42:5: error: …
        if is_java:
            line = re.sub(r'\bMain\.java:(\d+):', _fix_header, line)
        else:
            line = re.sub(r'\S+\.(?:c|cpp|cc|cxx|cs):(\d+)(?::\d+)?:', _fix_header, line)

        # Adjust source-context line numbers:  "  42 | int foo() {"
        line = re.sub(r'^(\s+)(\d+)(\s*\|)', _fix_context, line)

        out.append(line)

    return '\n'.join(out)


def parse_judge0_response(
    judge0_stdout:            str,
    judge0_status:            str,   # Judge0's own status: "Accepted", "Time Limit Exceeded", etc.
    session_id:               str,
    total_tc_count:           int,
    expected_values:          list = None,  # Fix 4.1: passed from autograder; harness emits OUTPUT
    compile_output:           str  = None,  # Compiler error message from Judge0 (status_id=6)
    student_code_start_line:  int  = 0,     # Harness line where student body begins (Java only)
) -> ParsedSubmission:
    """
    Top-level parser.
    Handles Judge0-level failures before even trying to parse harness output.
    """

    # Judge0 itself TLE'd — no harness output at all
    if judge0_status == "Time Limit Exceeded":
        results = [
            TCResult(
                tc_num = i,
                status = "TLE",
                detail = "Judge0 global time limit exceeded — infinite loop likely"
            )
            for i in range(1, total_tc_count + 1)
        ]
        sub = ParsedSubmission(tc_results=results, total=total_tc_count)
        sub.global_tle = True
        return sub

    # Judge0 compile error — no output at all.
    # Use status="CE" so the backend maps it to (3, "Compilation Error") via
    # _JUDGE0_TC_STATUS, giving the student the correct label + error text.
    if judge0_status in ("Compilation Error", "Internal Error"):
        if compile_output:
            adjusted = _adjust_compile_output(
                compile_output.strip(), student_code_start_line
            )
            if not adjusted.strip():
                # All compiler output was filtered as harness noise.
                # This happens when the student's function has the wrong
                # signature and the error is inside run_tc_child.
                adjusted = (
                    "error: Compilation failed.\n"
                    "       Check your function name, parameter types, and return type."
                )
            detail = f"Compilation Error:\n{adjusted[:3000]}"
        else:
            detail = f"Judge0: {judge0_status}"
        results = [
            TCResult(tc_num=i, status="CE", detail=detail)
            for i in range(1, total_tc_count + 1)
        ]
        return ParsedSubmission(tc_results=results, total=total_tc_count)

    # Normal case — parse harness output
    parser = OutputParser(judge0_stdout or "", session_id, total_tc_count, expected_values)
    return parser.parse()
