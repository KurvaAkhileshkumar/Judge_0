"""
Full audit script — runs without Judge0 or Docker.
Tests: security checker, harness builder, output parser, local harness execution.
"""
import sys, os, json, subprocess, time, textwrap

sys.path.insert(0, os.path.dirname(__file__))
from core.harness_builder import HarnessBuilder, HarnessConfig, TestCase
from core.output_parser   import OutputParser, ParsedSubmission, parse_judge0_response
from security.security    import SecurityChecker, sanitize_for_injection

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"

results = []

def check(name, ok, detail=""):
    icon = PASS if ok else FAIL
    print(f"  [{icon}] {name}" + (f"  →  {detail}" if detail else ""))
    results.append((name, ok, detail))

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# ── 1. SECURITY CHECKER ───────────────────────────────────────────────────
section("1. SECURITY CHECKER")

sc    = SecurityChecker()
DELIM = "@@TC_RESULT__fake__"

cases = [
    ("import os",                          "python", False, "BlockedImport"),
    ("import os as o",                     "python", False, "aliased import"),
    ("from subprocess import run",         "python", False, "from-import"),
    ("import sys\ndef solve(a,b):return a+b", "python", True,  "sys is allowed"),
    ("exec('x=1')",                        "python", False, "exec blocked"),
    ("eval('1+1')",                        "python", False, "eval blocked"),
    ("().__class__.__bases__",             "python", False, "dunder access"),
    ("def solve(a,b):\n return a+b",       "python", True,  "clean code"),
    ("def two_sum(n)\n pass",              "python", False, "syntax error"),
    ("x"*10_001,                           "python", False, "code too long"),
    (DELIM + "\ncode",                     "python", False, "delimiter injection"),
    ("int solve(int a){system(\"ls\");}",  "c",      False, "system() blocked"),
    ("int f(){fork();}",                   "c",      False, "fork() blocked"),
    ("int f(){setitimer(0,0,0);}",         "c",      False, "setitimer blocked"),
    ("int f(){signal(SIGALRM,h);}",        "c",      False, "SIGALRM override blocked"),
    ("int f(){return 1;}",                 "c",      True,  "clean C code"),
    ("void f(){Runtime.getRuntime();}",    "java",   False, "Runtime blocked"),
    ("void f(){System.exit(0);}",          "java",   False, "System.exit blocked"),
    ("void f(){return;}",                  "java",   True,  "clean Java code"),
]

for code, lang, expect_pass, label in cases:
    r = sc.check(code, lang, DELIM)
    ok = (r.passed == expect_pass)
    detail = "" if ok else f"expected passed={expect_pass}, got {r.passed}, reason={r.reason}"
    check(f"security: {label}", ok, detail)

# sanitize_for_injection
san = sanitize_for_injection("def f():\n    x = 1  ", "python")
check("sanitize: python trailing spaces stripped", san == "def f():\n    x = 1")

san_c = sanitize_for_injection("/* test */ code", "c")
check("sanitize: C code unmodified when no */", san_c == "/* test */ code")

san_c2 = sanitize_for_injection("int f() { /* end */ }", "c")
check("sanitize: C */ replaced", "* /" in san_c2)


# ── 2. HARNESS BUILDER ────────────────────────────────────────────────────
section("2. HARNESS BUILDER")

# Python stdio harness
cfg = HarnessConfig(
    student_code    = "a, b = map(int, input().split())\nprint(a + b)",
    test_cases      = [TestCase(stdin_text="2 3\n", expected="5"),
                       TestCase(stdin_text="10 20\n", expected="30")],
    language        = "python",
    mode            = "stdio",
    per_tc_limit_s  = 2,
    memory_limit_mb = 128,
)
b = HarnessBuilder(cfg)
code = b.build()
check("harness builder: python stdio builds without error", True)
check("harness builder: delimiter in output", b.delim in code)
check("harness builder: session_id in delimiter", b.session_id in b.delim)
check("harness builder: test cases embedded", "2 3" in code and "10 20" in code)
check("harness builder: mode set to stdio", 'MODE  = "stdio"' in code or 'MODE = "stdio"' in code)
check("harness builder: DONE marker pattern present", "DONE" in code)

# Python function harness
cfg2 = HarnessConfig(
    student_code    = "def solve(a, b):\n    return a + b",
    test_cases      = [TestCase(inputs=[2,3], expected=5)],
    language        = "python",
    mode            = "function",
    per_tc_limit_s  = 2,
    memory_limit_mb = 128,
)
b2 = HarnessBuilder(cfg2)
code2 = b2.build()
check("harness builder: python function mode builds", True)
check("harness builder: function mode has solve() call", "solve" in code2)

# Session IDs are unique
b3 = HarnessBuilder(cfg)
check("harness builder: unique session IDs", b.session_id != b3.session_id)

# Unsupported language
try:
    bad = HarnessConfig(student_code="x", test_cases=[], language="brainfuck", mode="stdio")
    HarnessBuilder(bad).build()
    check("harness builder: unsupported language raises error", False, "no error raised")
except ValueError:
    check("harness builder: unsupported language raises ValueError", True)


# ── 3. OUTPUT PARSER ─────────────────────────────────────────────────────
section("3. OUTPUT PARSER")

SID = "test123abc"
D   = f"@@TC_RESULT__{SID}__"

# Normal output with PASS/FAIL/TLE
raw = (
    f"{D}START_1\n{{\"status\":\"PASS\",\"got\":\"5\",\"expected\":\"5\"}}\n{D}END_1\n"
    f"{D}START_2\n{{\"status\":\"FAIL\",\"got\":\"3\",\"expected\":\"5\"}}\n{D}END_2\n"
    f"{D}START_3\n{{\"status\":\"TLE\",\"detail\":\"Exceeded 2s\"}}\n{D}END_3\n"
    f"{D}DONE\n"
)
p = OutputParser(raw, SID, 3).parse()
check("parser: score=1 from PASS/FAIL/TLE", p.score == 1, f"got {p.score}")
check("parser: global_tle=False when DONE present", not p.global_tle)
check("parser: TC1 PASS",  p.tc_results[0].status == "PASS")
check("parser: TC2 FAIL",  p.tc_results[1].status == "FAIL")
check("parser: TC3 TLE",   p.tc_results[2].status == "TLE")

# Global TLE — no DONE marker
raw_tle = (
    f"{D}START_1\n{{\"status\":\"PASS\"}}\n{D}END_1\n"
)
p2 = OutputParser(raw_tle, SID, 3).parse()
check("parser: global_tle=True when DONE absent", p2.global_tle)
check("parser: missing TC2/TC3 marked TLE (not MISSING)", all(
    r.status == "TLE" for r in p2.tc_results if r.tc_num in (2,3)
))

# MISSING TC (DONE present but TC2 never printed)
raw_miss = (
    f"{D}START_1\n{{\"status\":\"PASS\"}}\n{D}END_1\n"
    f"{D}DONE\n"
)
p3 = OutputParser(raw_miss, SID, 2).parse()
check("parser: missing TC with DONE present → MISSING status", p3.tc_results[1].status == "MISSING")

# Judge0-level TLE
p4 = parse_judge0_response("", "Time Limit Exceeded", SID, 2)
check("parser: Judge0 TLE → all TCs TLE", all(r.status == "TLE" for r in p4.tc_results))
check("parser: Judge0 TLE → global_tle=True", p4.global_tle)

# Compilation error
p5 = parse_judge0_response("", "Compilation Error", SID, 2)
check("parser: Compilation Error → all TCs ERROR", all(r.status == "ERROR" for r in p5.tc_results))

# Bad JSON inside TC block
raw_bad = f"{D}START_1\nnot-json-at-all\n{D}END_1\n{D}DONE\n"
p6 = OutputParser(raw_bad, SID, 1).parse()
check("parser: bad JSON in TC block → ERROR status", p6.tc_results[0].status == "ERROR")


# ── 4. LOCAL HARNESS EXECUTION ────────────────────────────────────────────
section("4. LOCAL HARNESS EXECUTION (all 15 solutions × 3 problems)")

with open("question_bank.json") as f:
    bank = json.load(f)

def run_harness_local(prob, sol):
    """Build harness, write to temp file, run with python3, return parsed result."""
    tcs = [TestCase(stdin_text=tc["stdin_text"], expected=tc["expected"])
           for tc in prob["test_cases"]]
    cfg = HarnessConfig(
        student_code    = sol["source_code"],
        test_cases      = tcs,
        language        = "python",
        mode            = "stdio",
        per_tc_limit_s  = prob["per_tc_limit_s"],
        memory_limit_mb = prob["memory_limit_mb"],
    )
    builder = HarnessBuilder(cfg)
    harness_code = builder.build()

    tmp = f"/tmp/_harness_{builder.session_id}.py"
    with open(tmp, "w") as f:
        f.write(harness_code)

    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            [sys.executable, tmp],
            capture_output=True, text=True,
            timeout=prob["per_tc_limit_s"] * len(prob["test_cases"]) + 8
        )
        stdout = proc.stdout
    except subprocess.TimeoutExpired:
        stdout = ""
    finally:
        os.unlink(tmp)
    elapsed = time.monotonic() - t0

    parsed = parse_judge0_response(
        judge0_stdout  = stdout,
        judge0_status  = "Accepted",
        session_id     = builder.session_id,
        total_tc_count = len(prob["test_cases"]),
    )
    return parsed, elapsed

all_exec_pass = True
tle_times = []

for prob in bank["problems"]:
    for sol in prob["solutions"]:
        expected = sol["expected_harness_statuses"]

        # Security/syntax_error solutions are blocked before harness runs
        if expected == ["BLOCKED_BY_AST"] or sol["type"] == "syntax_error":
            r = SecurityChecker().check(sol["source_code"], "python", "@@FAKE__")
            blocked = not r.passed
            ok = blocked
            check(f"{prob['id']} / {sol['id']}: BLOCKED_BY_AST", ok,
                  f"reason={r.reason}" if blocked else "NOT blocked by security checker")
            if not ok:
                all_exec_pass = False
            continue

        parsed, elapsed = run_harness_local(prob, sol)
        actual = [r.status for r in parsed.tc_results]

        if sol["type"] == "time_limit_exceeded":
            tle_times.append(elapsed)

        ok = (actual == expected)
        detail = f"elapsed={elapsed:.2f}s  actual={actual}" if ok else f"expected={expected} actual={actual} elapsed={elapsed:.2f}s"
        check(f"{prob['id']} / {sol['id']}", ok, detail)
        if not ok:
            all_exec_pass = False

# Parallelism check: TLE solutions should finish in ~per_tc_limit_s, NOT N×per_tc_limit_s
if tle_times:
    avg_tle = sum(tle_times) / len(tle_times)
    parallel_ok = avg_tle < 3.5  # parallel: ~2s; sequential: ~8s
    check(f"parallelism: TLE wall time avg={avg_tle:.2f}s (should be ~2s, not ~8s)", parallel_ok)


# ── 5. SUMMARY ────────────────────────────────────────────────────────────
section("SUMMARY")
total   = len(results)
passed  = sum(1 for _, ok, _ in results if ok)
failed  = total - passed
print(f"\n  Total checks : {total}")
print(f"  Passed       : {passed}")
print(f"  Failed       : {failed}")

if failed:
    print("\n  FAILURES:")
    for name, ok, detail in results:
        if not ok:
            print(f"    [FAIL] {name}  →  {detail}")

print()
sys.exit(0 if failed == 0 else 1)
