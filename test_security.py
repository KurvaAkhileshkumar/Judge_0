"""
test_security.py
─────────────────
Tests for the SecurityChecker covering all specified patterns.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from security.security import SecurityChecker, sanitize_for_injection

checker = SecurityChecker()
DELIM = "@@TC_RESULT__fakesess12__"

results = []

def check(label, code, language, expected_pass, should_contain=None):
    result = checker.check(code, language, DELIM)
    ok = result.passed == expected_pass
    status = "PASS" if ok else "FAIL"
    reason = result.reason if not result.passed else "(clean)"
    if should_contain and not result.passed:
        if should_contain not in result.reason:
            ok = False
            status = "FAIL"
            reason += f" [expected '{should_contain}' in reason]"
    results.append((status, label, reason))
    return ok

# ── Python tests ────────────────────────────────────────────────────────────

check("Python: blocked import (import os)",
      "import os\ndef solve(a, b): return a + b",
      "python", expected_pass=False, should_contain="BlockedImport")

check("Python: aliased import (import os as operating_system)",
      "import os as operating_system\ndef solve(a, b): return a + b",
      "python", expected_pass=False, should_contain="BlockedImport")

check("Python: from import (from subprocess import run)",
      "from subprocess import run\ndef solve(a, b): return a + b",
      "python", expected_pass=False, should_contain="BlockedImport")

check("Python: blocked builtin (exec(...))",
      'exec("print(1)")\ndef solve(a, b): return a + b',
      "python", expected_pass=False, should_contain="BlockedBuiltin")

check("Python: dunder access (().__class__.__bases__)",
      "().__class__.__bases__\ndef solve(a, b): return a + b",
      "python", expected_pass=False, should_contain="BlockedDunder")

check("Python: clean code (should pass)",
      "def solve(a, b): return a + b",
      "python", expected_pass=True)

# ── C tests ─────────────────────────────────────────────────────────────────

check("C: blocked system()",
      "int main() { system(\"ls\"); return 0; }",
      "c", expected_pass=False, should_contain="system()")

check("C: blocked fork()",
      "int main() { pid_t p = fork(); return 0; }",
      "c", expected_pass=False, should_contain="fork()")

# ── Java tests ───────────────────────────────────────────────────────────────

check("Java: blocked Runtime.getRuntime()",
      "class S { void f() { Runtime.getRuntime().exec(\"ls\"); } }",
      "java", expected_pass=False, should_contain="Runtime.getRuntime()")

check("Java: blocked System.exit()",
      "class S { void f() { System.exit(0); } }",
      "java", expected_pass=False, should_contain="System.exit()")

# ── Delimiter injection ──────────────────────────────────────────────────────

check("Delimiter injection attempt",
      f"def solve(a, b):\n    print('{DELIM}START_1')\n    return 0",
      "python", expected_pass=False, should_contain="DelimiterInjection")

# ── Syntax error ─────────────────────────────────────────────────────────────

check("Python: syntax error detection",
      "def two_sum(nums, target)\n    seen = {}\n    return seen\n",
      "python", expected_pass=False, should_contain="SyntaxError")

# ── Code too long ─────────────────────────────────────────────────────────────

long_code = "x = 1\n" * 3000  # well over 10000 chars
check("Code too long (>10000 chars)",
      long_code,
      "python", expected_pass=False, should_contain="CodeTooLong")

# ── Print results ────────────────────────────────────────────────────────────

print()
print("=" * 70)
print("SECURITY CHECKER TEST RESULTS")
print("=" * 70)

passed = 0
failed = 0
for status, label, reason in results:
    icon = "OK  " if status == "PASS" else "FAIL"
    print(f"  [{icon}] {label}")
    if status == "FAIL":
        print(f"         Reason: {reason}")
    failed += (1 if status == "FAIL" else 0)
    passed += (1 if status == "PASS" else 0)

print()
print(f"  Total: {passed + failed}  Passed: {passed}  Failed: {failed}")
print("=" * 70)

# Also test sanitize_for_injection
print()
print("Sanitize tests:")
code = "def f():\n    x = 1   \n    y = 2  \n"
sanitized = sanitize_for_injection(code, "python")
# Each line should have trailing spaces stripped; leading indentation preserved
lines = sanitized.split("\n")
trailing_ok = all(not line.endswith(" ") for line in lines)
assert trailing_ok, f"Trailing spaces not stripped: {lines}"
print("  [OK  ] Python trailing whitespace stripped")

c_code = "/* */ int solve() { return 1; /* end */ }"
sanitized_c = sanitize_for_injection(c_code, "c")
assert "*/" not in sanitized_c, "*/  not replaced"
print("  [OK  ] C comment terminator sanitized")

sys.exit(1 if failed > 0 else 0)
