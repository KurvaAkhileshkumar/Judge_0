"""
autograder.py
─────────────
Main entry point. Ties together:
  HarnessBuilder → SecurityChecker → Judge0Client → OutputParser

Usage:
    grader = Autograder(judge0_config)
    result = grader.grade(submission)
    print(result.summary())
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dataclasses import dataclass
from typing import Any, Optional

from core.harness_builder import HarnessBuilder, HarnessConfig, TestCase
from core.output_parser   import OutputParser, ParsedSubmission, parse_judge0_response
from core.judge0_client   import Judge0Client, Judge0Config, Judge0Result
from security.security    import SecurityChecker, sanitize_for_injection


@dataclass
class Submission:
    student_id:      str
    language:        str                # "python" | "c" | "cpp" | "java"
    student_code:    str
    test_cases:      list[TestCase]
    mode:            str = "function"   # "function" | "stdio"
    function_name:   str = "solve"
    per_tc_limit_s:  int = 2
    memory_limit_mb: int = 256
    # Required for C/C++/Java function mode
    param_types:     list[str] = None
    return_type:     str = "auto"


@dataclass
class GradingResult:
    student_id:     str
    language:       str
    submission:     ParsedSubmission
    judge0_raw:     Judge0Result
    harness_code:   str              # for debugging
    security_error: str = ""         # set if blocked before Judge0

    def summary(self) -> str:
        if self.security_error:
            return f"[BLOCKED] {self.security_error}"

        lines = [
            f"Student: {self.student_id}",
            f"Score:   {self.submission.score}/{self.submission.total}",
            f"Global TLE: {self.submission.global_tle}",
            "",
        ]
        for r in self.submission.tc_results:
            warn = f" ⚠ {r.warning}" if r.warning else ""
            detail = f" ({r.detail})" if r.detail and r.status != "PASS" else ""
            lines.append(f"  TC{r.tc_num}: {r.status}{detail}{warn}")

        if self.judge0_raw:
            lines.append(f"\nJudge0 time: {self.judge0_raw.time_taken_s}s")
            lines.append(f"Judge0 mem:  {self.judge0_raw.memory_kb}KB")

        return "\n".join(lines)


class Autograder:

    def __init__(self, judge0_config: Judge0Config):
        self.judge0   = Judge0Client(judge0_config)
        self.security = SecurityChecker()

    def grade(self, submission: Submission) -> GradingResult:

        # ── 1. Build harness (to get session_id / delim) ────────────────
        config = HarnessConfig(
            student_code    = submission.student_code,
            test_cases      = submission.test_cases,
            language        = submission.language,
            mode            = submission.mode,
            per_tc_limit_s  = submission.per_tc_limit_s,
            memory_limit_mb = submission.memory_limit_mb,
            function_name   = submission.function_name,
            param_types     = submission.param_types,
            return_type     = submission.return_type,
        )
        builder = HarnessBuilder(config)

        # ── 2. Security check ────────────────────────────────────────────
        sec = self.security.check(
            submission.student_code,
            submission.language,
            builder.delim
        )
        if not sec.passed:
            empty_sub = ParsedSubmission(
                tc_results = [],
                total      = len(submission.test_cases),
                score      = 0,
            )
            return GradingResult(
                student_id     = submission.student_id,
                language       = submission.language,
                submission     = empty_sub,
                judge0_raw     = None,
                harness_code   = "",
                security_error = sec.reason,
            )

        # ── 3. Sanitize and build harness ────────────────────────────────
        config.student_code = sanitize_for_injection(
            submission.student_code,
            submission.language
        )
        harness_code = builder.build()

        # ── 4. Submit to Judge0 ──────────────────────────────────────────
        judge0_result = self.judge0.submit_and_wait(
            source_code     = harness_code,
            language        = submission.language,
            per_tc_limit_s  = submission.per_tc_limit_s,
            tc_count        = len(submission.test_cases),
            memory_limit_mb = submission.memory_limit_mb,
        )

        # ── 5. Parse output ──────────────────────────────────────────────
        parsed = parse_judge0_response(
            judge0_stdout  = judge0_result.stdout,
            judge0_status  = judge0_result.status_str,
            session_id     = builder.session_id,
            total_tc_count = len(submission.test_cases),
        )

        return GradingResult(
            student_id   = submission.student_id,
            language     = submission.language,
            submission   = parsed,
            judge0_raw   = judge0_result,
            harness_code = harness_code,
        )


# ── Example usage ────────────────────────────────────────────────────────────

if __name__ == "__main__":

    judge0_cfg = Judge0Config(
        base_url = "https://judge0.yourdomain.com",
        api_key  = "your-api-key-here",
    )

    grader = Autograder(judge0_cfg)

    # ── Example 1: Python — FUNCTION mode ────────────────────────────────
    python_function = Submission(
        student_id   = "student_001",
        language     = "python",
        mode         = "function",
        student_code = "def solve(a, b):\n    return a + b",
        test_cases   = [
            TestCase(inputs=[2, 3],   expected=5),
            TestCase(inputs=[10, 20], expected=30),
        ],
        per_tc_limit_s  = 2,
        memory_limit_mb = 128,
    )

    # ── Example 2: Python — STDIO mode ───────────────────────────────────
    # Student writes a full program that reads from stdin and prints to stdout
    python_stdio = Submission(
        student_id   = "student_002",
        language     = "python",
        mode         = "stdio",
        student_code = """
a, b = map(int, input().split())
print(a + b)
""",
        test_cases   = [
            TestCase(stdin_text="2 3\n",   expected="5"),
            TestCase(stdin_text="10 20\n", expected="30"),
        ],
        per_tc_limit_s  = 2,
        memory_limit_mb = 128,
    )

    # ── Example 3: C — FUNCTION mode ─────────────────────────────────────
    c_function = Submission(
        student_id   = "student_003",
        language     = "c",
        mode         = "function",
        student_code = "int solve(int a, int b) { return a + b; }",
        test_cases   = [
            TestCase(inputs=[2, 3],   expected=5),
            TestCase(inputs=[10, 20], expected=30),
        ],
        param_types     = ["int", "int"],
        return_type     = "int",
        per_tc_limit_s  = 2,
        memory_limit_mb = 128,
    )

    # ── Example 4: Java — FUNCTION mode ──────────────────────────────────
    java_function = Submission(
        student_id   = "student_004",
        language     = "java",
        mode         = "function",
        student_code = "public int solve(int a, int b) { return a + b; }",
        test_cases   = [
            TestCase(inputs=[3, 4], expected=7),
        ],
        param_types     = ["int", "int"],
        per_tc_limit_s  = 2,
        memory_limit_mb = 128,
    )

    # ── Example 5: Java — STDIO mode ─────────────────────────────────────
    java_stdio = Submission(
        student_id   = "student_005",
        language     = "java",
        mode         = "stdio",
        student_code = """
import java.util.Scanner;
public static void main(String[] args) {
    Scanner sc = new Scanner(System.in);
    int a = sc.nextInt(), b = sc.nextInt();
    System.out.println(a + b);
}
""",
        test_cases   = [
            TestCase(stdin_text="3 4\n", expected="7"),
        ],
        per_tc_limit_s  = 2,
        memory_limit_mb = 128,
    )

    # ── Example 6: AST security catching aliased import ──────────────────
    from security.security import SecurityChecker
    checker = SecurityChecker()

    blocked_code = "import os as operating_system\ndef solve(a, b): return a + b"
    result = checker.check(blocked_code, "python", "@@TC_RESULT__fake__")
    print(f"Aliased import caught: {not result.passed}")
    print(f"Reason: {result.reason}")
    # Output:
    # Aliased import caught: True
    # Reason: BlockedImport (line 1): Module 'os' is not allowed (aliased as 'operating_system')

    print("\nAutograder initialized. Connect to Judge0 to run submissions.")

