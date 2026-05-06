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


VALID_STATUSES = {"PASS", "FAIL", "TLE", "MLE", "SEGV", "FPE", "ERROR", "OUTPUT"}


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
    status:   str                   # PASS | FAIL | TLE | MLE | SEGV | FPE | ERROR | MISSING
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
            if status not in VALID_STATUSES:
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
            # Content wasn't valid JSON — student may have corrupted output
            # Try to detect status from raw text as fallback
            status = "ERROR"
            for s in VALID_STATUSES:
                if s in content:
                    status = s
                    break

            return TCResult(
                tc_num = tc_num,
                status = status,
                detail = f"Output parse error — raw: {content[:100]}",
            )


def parse_judge0_response(
    judge0_stdout:  str,
    judge0_status:  str,   # Judge0's own status: "Accepted", "Time Limit Exceeded", etc.
    session_id:     str,
    total_tc_count: int,
    expected_values: list = None,   # Fix 4.1: passed from autograder; harness emits OUTPUT
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

    # Judge0 compile error — no output at all
    if judge0_status in ("Compilation Error", "Internal Error"):
        results = [
            TCResult(
                tc_num = i,
                status = "ERROR",
                detail = f"Judge0: {judge0_status}"
            )
            for i in range(1, total_tc_count + 1)
        ]
        return ParsedSubmission(tc_results=results, total=total_tc_count)

    # Normal case — parse harness output
    parser = OutputParser(judge0_stdout or "", session_id, total_tc_count, expected_values)
    return parser.parse()
