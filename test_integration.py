"""
Integration test — submits real solutions from question_bank.json to Judge0 CE
Uses: https://ce.judge0.com (free public endpoint, rate-limited)
"""
import sys, os, json, time

sys.path.insert(0, os.path.dirname(__file__))
from autograder import Autograder, Submission
from core.harness_builder import TestCase
from core.judge0_client   import Judge0Config
from security.security    import SecurityChecker

JUDGE0_URL = "https://ce.judge0.com"

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

results = []

def check(name, ok, detail=""):
    icon = PASS if ok else FAIL
    print(f"  [{icon}] {name}" + (f"  →  {detail}" if detail else ""))
    results.append((name, ok, detail))

grader = Autograder(Judge0Config(
    base_url        = JUDGE0_URL,
    api_key         = None,
    poll_interval_s = 1.0,
    max_polls       = 60,
))

with open("question_bank.json") as f:
    bank = json.load(f)

sc = SecurityChecker()

print(f"\n  Judge0 Integration Test — {JUDGE0_URL}")
print(f"  {'='*56}\n")

# Test a representative subset to avoid rate-limiting:
# accepted + wrong_answer + tle from first 2 problems
# syntax_error is handled locally by security checker

for prob in bank["problems"][:2]:           # two_sum + fibonacci
    print(f"\n  Problem: {prob['title']} ({prob['id']})")
    print(f"  {'-'*50}")
    for sol in prob["solutions"]:
        expected = sol["expected_harness_statuses"]

        # Syntax errors blocked before Judge0
        if sol["type"] == "syntax_error":
            r = sc.check(sol["source_code"], "python", "@@FAKE__")
            ok = not r.passed
            check(f"{sol['id']}: BLOCKED_BY_AST (no Judge0 call)", ok,
                  r.reason if ok else "NOT blocked")
            continue

        # Skip TLE for integration test to save time on public API
        if sol["type"] == "time_limit_exceeded":
            print(f"  [SKIP] {sol['id']}: TLE solution skipped (saves API time)")
            continue

        tcs = [TestCase(stdin_text=tc["stdin_text"], expected=tc["expected"])
               for tc in prob["test_cases"]]

        submission = Submission(
            student_id      = sol["id"],
            language        = prob["language"],
            mode            = "stdio",
            student_code    = sol["source_code"],
            test_cases      = tcs,
            per_tc_limit_s  = prob["per_tc_limit_s"],
            memory_limit_mb = prob["memory_limit_mb"],
        )

        t0 = time.monotonic()
        try:
            result = grader.grade(submission)
        except Exception as e:
            check(f"{sol['id']}: grader raised exception", False, str(e))
            continue
        elapsed = time.monotonic() - t0

        if result.security_error:
            actual = ["BLOCKED"]
        else:
            actual = [r.status for r in result.submission.tc_results]

        ok = (actual == expected)
        score_str = f"{result.submission.score}/{result.submission.total}" if not result.security_error else "0/0"
        j0_time = f"J0={result.judge0_raw.time_taken_s}s" if result.judge0_raw else ""
        detail = f"elapsed={elapsed:.1f}s score={score_str} {j0_time} actual={actual}"
        check(f"{sol['id']}", ok,
              detail if ok else f"expected={expected} | {detail}")

        time.sleep(0.5)  # be polite to public API

# Summary
print(f"\n  {'='*56}")
total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed
print(f"\n  Total: {total}  Passed: {passed}  Failed: {failed}")
if failed:
    print("\n  FAILURES:")
    for name, ok, detail in results:
        if not ok:
            print(f"    [FAIL] {name}  →  {detail}")
print()
sys.exit(0 if failed == 0 else 1)
