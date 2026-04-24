"""
test_harness.py
────────────────
Tests the HarnessBuilder for Python stdio and function mode.
Must be run from /Users/akhilesh/Edwisely/judge0_3 (so harnesses/ is found).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
from core.harness_builder import HarnessBuilder, HarnessConfig, TestCase

print("=" * 70)
print("HARNESS BUILDER TEST RESULTS")
print("=" * 70)

# Load question_bank.json
with open("question_bank.json") as f:
    bank = json.load(f)

prob = bank["problems"][0]  # two_sum
sol_accepted = next(s for s in prob["solutions"] if s["type"] == "accepted")

tc_list = [
    TestCase(stdin_text=tc["stdin_text"], expected=tc["expected"])
    for tc in prob["test_cases"]
]

# ── Test 1: Python STDIO harness ─────────────────────────────────────────────
print("\nTest 1: Python STDIO harness (two_sum accepted)")
cfg_stdio = HarnessConfig(
    student_code    = sol_accepted["source_code"],
    test_cases      = tc_list,
    language        = "python",
    mode            = "stdio",
    per_tc_limit_s  = 2,
    memory_limit_mb = 256,
)
builder_stdio = HarnessBuilder(cfg_stdio)
harness_stdio = builder_stdio.build()

delim = builder_stdio.delim
session_id = builder_stdio.session_id

# Verify delimiter is in the harness
assert delim in harness_stdio, f"Delimiter '{delim}' not found in harness"
print(f"  [OK  ] Delimiter present: {delim[:30]}...")

# Verify student code is in the harness
assert "seen" in harness_stdio, "Student code not injected"
print("  [OK  ] Student code injected")

# Verify test cases are embedded
assert "stdin_text" in harness_stdio, "Test cases not serialized"
assert prob["test_cases"][0]["expected"] in harness_stdio, "Expected values not in harness"
print("  [OK  ] Test cases embedded")

# Verify mode is set
assert 'MODE  = "stdio"' in harness_stdio or 'MODE = "stdio"' in harness_stdio, "Mode not set"
print("  [OK  ] Mode set to 'stdio'")

# Verify per_tc_limit_s is embedded
assert "_PER_TC_LIMIT_S  = 2" in harness_stdio or "_PER_TC_LIMIT_S = 2" in harness_stdio, \
    "per_tc_limit_s not embedded"
print("  [OK  ] per_tc_limit_s embedded")

# Print first 50 lines
print("\n--- First 50 lines of STDIO harness ---")
lines = harness_stdio.split("\n")
for i, line in enumerate(lines[:50], 1):
    print(f"  {i:3d}: {line}")
print(f"  ... ({len(lines)} total lines)")


# ── Test 2: Python FUNCTION harness ─────────────────────────────────────────
print("\nTest 2: Python FUNCTION harness (simple add)")

tc_func = [
    TestCase(inputs=[2, 3], expected=5),
    TestCase(inputs=[10, 20], expected=30),
]
cfg_func = HarnessConfig(
    student_code    = "def solve(a, b): return a + b",
    test_cases      = tc_func,
    language        = "python",
    mode            = "function",
    per_tc_limit_s  = 2,
    memory_limit_mb = 128,
)
builder_func = HarnessBuilder(cfg_func)
harness_func = builder_func.build()

delim_func = builder_func.delim
assert delim_func in harness_func, "Delimiter not in function harness"
print(f"  [OK  ] Delimiter present: {delim_func[:30]}...")
assert "def solve(a, b): return a + b" in harness_func, "Student code not in function harness"
print("  [OK  ] Student code injected")
assert '"input"' in harness_func or "'input'" in harness_func, "Function inputs not serialized"
print("  [OK  ] Function inputs serialized")
assert 'MODE  = "function"' in harness_func or 'MODE = "function"' in harness_func, "Mode not set"
print("  [OK  ] Mode set to 'function'")


# ── Test 3: Verify session IDs are unique ───────────────────────────────────
print("\nTest 3: Session IDs are unique")
builders = [HarnessBuilder(cfg_stdio) for _ in range(5)]
ids = {b.session_id for b in builders}
assert len(ids) == 5, "Session IDs not unique!"
print(f"  [OK  ] 5 unique session IDs generated: {list(ids)[:2]}...")


# ── Test 4: Unsupported language raises ValueError ───────────────────────────
print("\nTest 4: Unsupported language raises ValueError")
try:
    cfg_bad = HarnessConfig(
        student_code="x=1", test_cases=[], language="ruby", mode="stdio"
    )
    HarnessBuilder(cfg_bad).build()
    print("  [FAIL] Should have raised ValueError")
    sys.exit(1)
except ValueError as e:
    print(f"  [OK  ] ValueError raised: {e}")


print()
print("=" * 70)
print("All harness builder tests PASSED")
print("=" * 70)
