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


VALID_STATUSES = {"PASS", "FAIL", "TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT", "CE"}

# Statuses a harness is ALLOWED to emit. PASS and FAIL are never emitted by
# the harness — they are assigned only by OutputParser after comparison.
# Accepting PASS/FAIL from harness output would allow students to forge
# verdicts by writing fake TCResult structs to the result pipe.
_HARNESS_STATUSES = {"TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT"}


def _num_equal(a_str: str, b_str: str) -> bool:
    """Float-tolerant comparison: relative tolerance 1e-6, absolute 1e-9."""
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

# Symbols that the harness injects as aliases for student code.
# In stdio mode: #define main student_stdio_main — so the student's main()
# is compiled under this name.  Errors *inside* it belong to the student;
# cascade references to it (e.g. "'student_stdio_main' was not declared")
# are harness noise and must be dropped.
_STUDENT_ALIAS_SYMS = re.compile(r'\b(student_stdio_main)\b')

# Pattern that matches gcc/g++ "In function '…':" context lines.
_IN_FN_RE = re.compile(r"^.*: In function '([^']*)':")


def _adjust_compile_output(raw: str, student_code_start_line: int) -> str:
    """
    Rewrite compiler-reported line numbers from harness-absolute to student-relative,
    and strip any errors / context that reference harness-internal symbols.

    Handles javac, gcc, and g++ output formats:
      Java:  'Main.java:52: error: …'       → 'line 6: error: …'
      C/C++: '/tmp/sol.c:63:10: error: …'   → 'line 4: error: …'

    Also adjusts source-context lines emitted by modern gcc/g++:
      '  42 | #include<bit'  →  '   1 | #include<bit'

    Filtering rules for C/C++ harness noise:
      • "In function 'run_tc_child':" → skip the entire error group;
        stay in skip mode until the next "In function" (or end of output).
      • "In function 'student_stdio_main':" → student's own main(), renamed to
        "main" in the output so the student recognises it.
      • Any line outside an "In function" block that contains a harness alias
        (e.g. "'student_stdio_main' was not declared") → cascade error, skip.
    """
    if student_code_start_line <= 1:
        return raw
    offset = student_code_start_line - 1
    is_java = "Main.java:" in raw

    def _fix_header(m):
        return f"line {max(1, int(m.group(1)) - offset)}:"

    def _fix_context(m):
        return f"{m.group(1)}{max(1, int(m.group(2)) - offset)}{m.group(3)}"

    out  = []
    skip = False   # True = we're inside a harness function's error group

    for line in raw.splitlines():
        in_fn_m = _IN_FN_RE.match(line)

        if in_fn_m:
            fn_name = in_fn_m.group(1)
            if _HARNESS_FN_SYMS.search(fn_name):
                # Harness runner function (run_tc_child) — skip entire group.
                # Retroactively remove any orphaned "In function" preamble.
                skip = True
                while out and _IN_FN_RE.match(out[-1]):
                    out.pop()
                continue
            elif _STUDENT_ALIAS_SYMS.search(fn_name):
                # Student's main() renamed by the #define wrapper — show it as
                # "main" so the student recognises their own function.
                line = line.replace('student_stdio_main', 'main')
                skip = False
                # fall through to process and add to out
            else:
                # Genuine student helper function — include as-is.
                skip = False
                # fall through

        elif skip:
            # We are inside a harness function's error group.
            # Keep skipping EVERYTHING (errors, context lines, notes) until
            # the next "In function" block — which the in_fn_m branch above handles.
            continue

        elif _STUDENT_ALIAS_SYMS.search(line):
            # A line outside any "In function" block that references the student
            # alias — this is a cascade reference, not a real student error.
            skip = True
            while out and _IN_FN_RE.match(out[-1]):
                out.pop()
            continue

        # Adjust error-header line numbers: main.cpp:42:5: error: …
        if is_java:
            line = re.sub(r'\bMain\.java:(\d+):', _fix_header, line)
        else:
            line = re.sub(r'\S+\.(?:c|cpp|cc|cxx|cs):(\d+)(?::\d+)?:', _fix_header, line)

        # Adjust source-context line numbers: "  42 | #include<bit"
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
            detail = f"Compilation Error:\n{adjusted[:500]}"
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
