#!/usr/bin/env python3
"""
test_harness.py
───────────────
Comprehensive harness test: all 4 languages × both modes (function + stdio)
with correct, wrong-answer, TLE, compilation-error, and runtime-error cases.

Usage:
    python test_harness.py
    python test_harness.py --url http://localhost:5001
    python test_harness.py --lang python          # single language
    python test_harness.py --mode function        # single mode
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error

API_BASE = "http://localhost:5001"
SSE_TIMEOUT = 120   # seconds to wait for a result

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Test case definitions ─────────────────────────────────────────────────────

TESTS = [

    # ══════════════════════════════════════════════════════════════════
    # PYTHON — FUNCTION MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "Python / function / correct — add two ints",
        "language": "python", "mode": "function",
        "function_name": "solve", "param_types": None, "return_type": "auto",
        "student_code": "def solve(a, b):\n    return a + b\n",
        "test_cases": [
            {"inputs": [2, 3],   "expected": "5"},
            {"inputs": [0, 0],   "expected": "0"},
            {"inputs": [-1, 1],  "expected": "0"},
            {"inputs": [100, 200], "expected": "300"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "Python / function / wrong answer — subtract instead of add",
        "language": "python", "mode": "function",
        "function_name": "solve", "param_types": None, "return_type": "auto",
        "student_code": "def solve(a, b):\n    return a - b\n",
        "test_cases": [
            {"inputs": [2, 3], "expected": "5"},
            {"inputs": [5, 5], "expected": "10"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "Python / function / syntax error",
        "language": "python", "mode": "function",
        "function_name": "solve", "param_types": None, "return_type": "auto",
        "student_code": "def solve(a, b)\n    return a + b\n",
        "test_cases": [{"inputs": [1, 2], "expected": "3"}],
        "expect": "ALL_ERROR",
    },
    {
        "name": "Python / function / infinite loop (static TLE)",
        "language": "python", "mode": "function",
        "function_name": "solve", "param_types": None, "return_type": "auto",
        "student_code": "def solve(a, b):\n    while True:\n        pass\n",
        "test_cases": [{"inputs": [1, 2], "expected": "3"}],
        "expect": "ALL_TLE",
    },
    {
        "name": "Python / function / runtime error (division by zero)",
        "language": "python", "mode": "function",
        "function_name": "solve", "param_types": None, "return_type": "auto",
        "student_code": "def solve(a, b):\n    return a // b\n",
        "test_cases": [
            {"inputs": [10, 2], "expected": "5"},
            {"inputs": [10, 0], "expected": "error"},
        ],
        "expect": "MIXED",  # TC1 PASS, TC2 ERROR
    },

    # ══════════════════════════════════════════════════════════════════
    # PYTHON — STDIO MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "Python / stdio / correct — fibonacci iterative",
        "language": "python", "mode": "stdio",
        "student_code": "n = int(input())\na, b = 0, 1\nfor _ in range(n):\n    a, b = b, a + b\nprint(a)\n",
        "test_cases": [
            {"stdin_text": "0\n",  "expected": "0"},
            {"stdin_text": "1\n",  "expected": "1"},
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "Python / stdio / wrong answer",
        "language": "python", "mode": "stdio",
        "student_code": "n = int(input())\nprint(n * 2)\n",
        "test_cases": [
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "Python / stdio / blocked import",
        "language": "python", "mode": "stdio",
        "student_code": "import os\nprint(os.getcwd())\n",
        "test_cases": [{"stdin_text": "\n", "expected": "anything"}],
        "expect": "SECURITY_BLOCKED",
    },

    # ══════════════════════════════════════════════════════════════════
    # C — FUNCTION MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "C / function / correct — add two ints",
        "language": "c", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "int",
        "student_code": "int solve(int a, int b) { return a + b; }",
        "test_cases": [
            {"inputs": [2, 3],   "expected": "5"},
            {"inputs": [0, 0],   "expected": "0"},
            {"inputs": [-1, 1],  "expected": "0"},
            {"inputs": [100, 200], "expected": "300"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C / function / wrong answer",
        "language": "c", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "int",
        "student_code": "int solve(int a, int b) { return a * b; }",
        "test_cases": [
            {"inputs": [2, 3], "expected": "5"},
            {"inputs": [4, 5], "expected": "9"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "C / function / compilation error",
        "language": "c", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "int",
        "student_code": "int solve(int a, int b) { return a + b  /* missing semicolon and brace */",
        "test_cases": [{"inputs": [1, 2], "expected": "3"}],
        "expect": "ALL_ERROR",
    },
    {
        "name": "C / function / correct — double return type",
        "language": "c", "mode": "function",
        "function_name": "solve",
        "param_types": ["double", "double"], "return_type": "double",
        "student_code": "double solve(double a, double b) { return a * b; }",
        "test_cases": [
            {"inputs": [2.0, 3.0],  "expected": "6"},
            {"inputs": [1.5, 2.0],  "expected": "3"},
            {"inputs": [0.1, 0.2],  "expected": "0.02"},
        ],
        "expect": "ALL_PASS",
    },

    # ══════════════════════════════════════════════════════════════════
    # C — STDIO MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "C / stdio / correct — fibonacci with printf",
        "language": "c", "mode": "stdio",
        "student_code": (
            "#include <stdio.h>\n"
            "int main() {\n"
            "    int n; scanf(\"%d\", &n);\n"
            "    long long a = 0, b = 1;\n"
            "    for (int i = 0; i < n; i++) { long long t = a+b; a = b; b = t; }\n"
            "    printf(\"%lld\\n\", a);\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "0\n",  "expected": "0"},
            {"stdin_text": "1\n",  "expected": "1"},
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C / stdio / correct — fibonacci with cout-equivalent (printf only)",
        "language": "c", "mode": "stdio",
        "student_code": (
            "#include <stdio.h>\n"
            "int main() {\n"
            "    int n; scanf(\"%d\", &n);\n"
            "    long long a = 0, b = 1;\n"
            "    for (int i = 0; i < n; i++) { long long t = a+b; a = b; b = t; }\n"
            "    printf(\"%lld\\n\", a);\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "10\n", "expected": "55"},
            {"stdin_text": "15\n", "expected": "610"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C / stdio / runtime error — segfault",
        "language": "c", "mode": "stdio",
        "student_code": (
            "#include <stdio.h>\n"
            "int main() {\n"
            "    int *p = 0;\n"
            "    printf(\"%d\\n\", *p);\n"   # dereference null
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [{"stdin_text": "\n", "expected": "0"}],
        "expect": "HAS_ERROR_OR_SEGV",
    },
    {
        "name": "C / stdio / blocked call — system()",
        "language": "c", "mode": "stdio",
        "student_code": (
            "#include <stdio.h>\n"
            "#include <stdlib.h>\n"
            "int main() {\n"
            "    system(\"echo hello\");\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [{"stdin_text": "\n", "expected": "hello"}],
        "expect": "SECURITY_BLOCKED",
    },

    # ══════════════════════════════════════════════════════════════════
    # C++ — FUNCTION MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "C++ / function / correct — add two ints",
        "language": "cpp", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "int",
        "student_code": "int solve(int a, int b) { return a + b; }",
        "test_cases": [
            {"inputs": [2, 3],   "expected": "5"},
            {"inputs": [0, 0],   "expected": "0"},
            {"inputs": [-5, 5],  "expected": "0"},
            {"inputs": [100, 200], "expected": "300"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C++ / function / correct — string length (auto return)",
        "language": "cpp", "mode": "function",
        "function_name": "solve",
        "param_types": ["std::string"], "return_type": "auto",
        "student_code": '#include <string>\nint solve(std::string s) { return (int)s.size(); }',
        "test_cases": [
            {"inputs": ["hello"], "expected": "5"},
            {"inputs": [""],      "expected": "0"},
            {"inputs": ["ab"],    "expected": "2"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C++ / function / wrong answer",
        "language": "cpp", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "int",
        "student_code": "int solve(int a, int b) { return a - b; }",
        "test_cases": [
            {"inputs": [2, 3], "expected": "5"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "C++ / function / runtime exception — std::out_of_range",
        "language": "cpp", "mode": "function",
        "function_name": "solve",
        "param_types": ["std::string"], "return_type": "auto",
        "student_code": '#include <string>\nchar solve(std::string s) { return s.at(100); }',
        "test_cases": [
            {"inputs": ["hi"], "expected": "x"},
        ],
        "expect": "HAS_ERROR_OR_SEGV",
    },

    # ══════════════════════════════════════════════════════════════════
    # C++ — STDIO MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "C++ / stdio / correct — fibonacci with cout",
        "language": "cpp", "mode": "stdio",
        "student_code": (
            "#include <iostream>\n"
            "using namespace std;\n"
            "int main() {\n"
            "    int n; cin >> n;\n"
            "    long long a = 0, b = 1;\n"
            "    for (int i = 0; i < n; i++) { long long t = a+b; a = b; b = t; }\n"
            "    cout << a << endl;\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "0\n",  "expected": "0"},
            {"stdin_text": "1\n",  "expected": "1"},
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C++ / stdio / correct — fibonacci with printf (Bug 1 regression test)",
        "language": "cpp", "mode": "stdio",
        "student_code": (
            "#include <cstdio>\n"
            "int main() {\n"
            "    int n; scanf(\"%d\", &n);\n"
            "    long long a = 0, b = 1;\n"
            "    for (int i = 0; i < n; i++) { long long t = a+b; a = b; b = t; }\n"
            "    printf(\"%lld\\n\", a);\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "0\n",  "expected": "0"},
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "C++ / stdio / blocked call — system()",
        "language": "cpp", "mode": "stdio",
        "student_code": (
            "#include <cstdlib>\n"
            "#include <iostream>\n"
            "int main() {\n"
            "    system(\"echo hello\");\n"
            "    return 0;\n"
            "}\n"
        ),
        "test_cases": [{"stdin_text": "\n", "expected": "hello"}],
        "expect": "SECURITY_BLOCKED",
    },

    # ══════════════════════════════════════════════════════════════════
    # JAVA — FUNCTION MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "Java / function / correct — add two ints",
        "language": "java", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "auto",
        "student_code": "public int solve(int a, int b) { return a + b; }",
        "test_cases": [
            {"inputs": [2, 3],   "expected": "5"},
            {"inputs": [0, 0],   "expected": "0"},
            {"inputs": [-1, 1],  "expected": "0"},
            {"inputs": [100, 200], "expected": "300"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "Java / function / wrong answer",
        "language": "java", "mode": "function",
        "function_name": "solve",
        "param_types": ["int", "int"], "return_type": "auto",
        "student_code": "public int solve(int a, int b) { return a * b; }",
        "test_cases": [
            {"inputs": [2, 3], "expected": "5"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "Java / function / correct — string reverse",
        "language": "java", "mode": "function",
        "function_name": "solve",
        "param_types": ["String"], "return_type": "auto",
        "student_code": (
            "public String solve(String s) {\n"
            "    return new StringBuilder(s).reverse().toString();\n"
            "}\n"
        ),
        "test_cases": [
            {"inputs": ["hello"], "expected": "olleh"},
            {"inputs": ["abc"],   "expected": "cba"},
            {"inputs": [""],      "expected": ""},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "Java / function / runtime error — ArrayIndexOutOfBounds",
        "language": "java", "mode": "function",
        "function_name": "solve",
        "param_types": ["int"], "return_type": "auto",
        "student_code": (
            "public int solve(int n) {\n"
            "    int[] arr = new int[5];\n"
            "    return arr[n];\n"  # will throw for n >= 5
            "}\n"
        ),
        "test_cases": [
            {"inputs": [0],  "expected": "0"},
            {"inputs": [10], "expected": "0"},  # OOB
        ],
        "expect": "MIXED",
    },

    # ══════════════════════════════════════════════════════════════════
    # JAVA — STDIO MODE
    # ══════════════════════════════════════════════════════════════════
    {
        "name": "Java / stdio / correct — fibonacci",
        "language": "java", "mode": "stdio",
        "student_code": (
            "public static void main(String[] args) throws Exception {\n"
            "    java.util.Scanner sc = new java.util.Scanner(System.in);\n"
            "    int n = sc.nextInt();\n"
            "    long a = 0, b = 1;\n"
            "    for (int i = 0; i < n; i++) { long t = a+b; a = b; b = t; }\n"
            "    System.out.println(a);\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "0\n",  "expected": "0"},
            {"stdin_text": "1\n",  "expected": "1"},
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "ALL_PASS",
    },
    {
        "name": "Java / stdio / wrong answer",
        "language": "java", "mode": "stdio",
        "student_code": (
            "public static void main(String[] args) throws Exception {\n"
            "    java.util.Scanner sc = new java.util.Scanner(System.in);\n"
            "    int n = sc.nextInt();\n"
            "    System.out.println(n * 2);\n"
            "}\n"
        ),
        "test_cases": [
            {"stdin_text": "5\n",  "expected": "5"},
            {"stdin_text": "10\n", "expected": "55"},
        ],
        "expect": "HAS_FAIL",
    },
    {
        "name": "Java / stdio / blocked — Runtime.exec",
        "language": "java", "mode": "stdio",
        "student_code": (
            "public static void main(String[] args) throws Exception {\n"
            "    Runtime.getRuntime().exec(\"echo hello\");\n"
            "}\n"
        ),
        "test_cases": [{"stdin_text": "\n", "expected": "hello"}],
        "expect": "SECURITY_BLOCKED",
    },
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def post_json(url, data):
    body = json.dumps(data).encode()
    req  = urllib.request.Request(url, data=body,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, json.loads(resp.read())


def stream_sse(url, timeout=SSE_TIMEOUT):
    """Read an SSE stream and return the first 'result' event data."""
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    deadline = time.monotonic() + timeout
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            buf = b""
            while time.monotonic() < deadline:
                chunk = resp.read(1024)
                if not chunk:
                    break
                buf += chunk
                lines = buf.split(b"\n")
                buf = lines[-1]
                for line in lines[:-1]:
                    line = line.decode(errors="replace").rstrip()
                    if line.startswith("data:"):
                        data = line[5:].strip()
                        if data:
                            return json.loads(data)
    except Exception as e:
        return {"stream_error": str(e)}
    return {"stream_error": "SSE stream closed without result"}


# ── Verdict helpers ───────────────────────────────────────────────────────────

def check_expectation(result, expect):
    """Return (ok, reason) based on the result dict and expected outcome."""

    if "security_error" in result:
        if expect == "SECURITY_BLOCKED":
            return True, f"blocked: {result['security_error'][:80]}"
        return False, f"unexpected security block: {result['security_error'][:80]}"

    if "system_error" in result:
        return False, f"system_error: {result['system_error'][:80]}"

    if "stream_error" in result:
        return False, f"stream_error: {result['stream_error'][:80]}"

    if expect == "SECURITY_BLOCKED":
        return False, "expected security block but got a result"

    tcs = result.get("tc_results", [])
    if not tcs:
        return False, "no tc_results in response"

    statuses = [r["status"] for r in tcs]

    if expect == "ALL_PASS":
        fails = [r for r in tcs if r["status"] != "PASS"]
        if fails:
            sample = fails[0]
            return False, f"TC{sample['tc_num']} {sample['status']}: got={sample.get('got','')!r} expected={sample.get('expected','')!r} detail={sample.get('detail','')!r}"
        return True, f"all {len(tcs)} TCs passed"

    if expect == "HAS_FAIL":
        if any(s == "FAIL" for s in statuses):
            return True, f"got expected FAIL(s) in {statuses}"
        return False, f"expected at least one FAIL, got {statuses}"

    if expect == "ALL_ERROR":
        non_error = [s for s in statuses if s not in ("ERROR", "MISSING")]
        if non_error:
            return False, f"expected all ERROR, got {statuses}"
        return True, f"all TCs errored as expected"

    if expect == "ALL_TLE":
        non_tle = [s for s in statuses if s != "TLE"]
        if non_tle:
            return False, f"expected all TLE, got {statuses}"
        return True, f"all TCs TLE as expected"

    if expect == "HAS_ERROR_OR_SEGV":
        if any(s in ("ERROR", "SEGV", "FPE", "MLE") for s in statuses):
            return True, f"got expected error/crash in {statuses}"
        return False, f"expected error/crash, got {statuses}"

    if expect == "MIXED":
        unique = set(statuses)
        if len(unique) > 1:
            return True, f"mixed statuses as expected: {statuses}"
        return False, f"expected mixed statuses, got uniform {statuses}"

    return False, f"unknown expect value: {expect!r}"


# ── Main test runner ──────────────────────────────────────────────────────────

def run_test(t, api_base, idx, total):
    name = t["name"]
    lang = t["language"]
    mode = t["mode"]

    print(f"\n{CYAN}[{idx}/{total}]{RESET} {BOLD}{name}{RESET}")

    # Build test_cases list for the API
    api_tcs = []
    for tc in t["test_cases"]:
        if mode == "function":
            api_tcs.append({
                "inputs":   tc["inputs"],
                "expected": tc["expected"],
            })
        else:
            api_tcs.append({
                "stdin_text": tc["stdin_text"],
                "expected":   tc["expected"],
            })

    body = {
        "student_id":      f"test_{idx}",
        "assessment_id":   f"harness_test_{idx}",
        "language":        lang,
        "student_code":    t["student_code"],
        "test_cases":      api_tcs,
        "mode":            mode,
        "function_name":   t.get("function_name", "solve"),
        "per_tc_limit_s":  3,
        "memory_limit_mb": 256,
    }
    if t.get("param_types"):
        body["param_types"] = t["param_types"]
    if t.get("return_type"):
        body["return_type"] = t["return_type"]

    # Submit
    t0 = time.monotonic()
    try:
        status, resp = post_json(f"{api_base}/submit", body)
    except Exception as e:
        print(f"  {RED}SUBMIT FAILED: {e}{RESET}")
        return False

    ticket_id = resp.get("ticket_id", "")
    sub_status = resp.get("status", "")
    print(f"  submitted  ticket={ticket_id[:8]}  status={sub_status}  http={status}")

    # Stream result
    result = stream_sse(f"{api_base}/results/stream/{ticket_id}")
    elapsed = time.monotonic() - t0
    print(f"  elapsed    {elapsed:.2f}s")

    # Check expectation
    ok, reason = check_expectation(result, t["expect"])

    if ok:
        print(f"  {GREEN}PASS  {reason}{RESET}")
    else:
        print(f"  {RED}FAIL  {reason}{RESET}")
        if "tc_results" in result:
            for r in result["tc_results"]:
                icon = "✓" if r["status"] == "PASS" else "✗"
                print(f"    TC{r['tc_num']} {icon} {r['status']:8}  got={str(r.get('got',''))[:40]!r}  exp={str(r.get('expected',''))[:40]!r}  {r.get('detail','')[:60]}")
        elif "security_error" in result:
            print(f"    security_error: {result['security_error']}")
        elif "system_error" in result:
            print(f"    system_error: {result['system_error']}")

    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",      default=API_BASE)
    parser.add_argument("--lang",     default=None, help="Filter by language (python/c/cpp/java)")
    parser.add_argument("--mode",     default=None, help="Filter by mode (function/stdio)")
    parser.add_argument("--json-out", default="test_harness_results.json",
                        help="Save results to this JSON file (default: test_harness_results.json)")
    args = parser.parse_args()

    tests = TESTS
    if args.lang:
        tests = [t for t in tests if t["language"] == args.lang]
    if args.mode:
        tests = [t for t in tests if t["mode"] == args.mode]

    if not tests:
        print("No tests matched the filters.")
        sys.exit(1)

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  Judge0 Harness Test Suite — {len(tests)} tests{RESET}")
    print(f"{BOLD}  API: {args.url}{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")

    run_started = time.time()
    results = []
    detail_rows = []

    for i, t in enumerate(tests, 1):
        t0 = time.monotonic()
        ok = run_test(t, args.url, i, len(tests))
        elapsed = time.monotonic() - t0
        results.append((t["name"], ok))
        detail_rows.append({
            "index":    i,
            "name":     t["name"],
            "language": t["language"],
            "mode":     t["mode"],
            "expect":   t["expect"],
            "passed":   ok,
            "elapsed_s": round(elapsed, 2),
        })

    run_ended = time.time()
    passed = sum(1 for _, ok in results if ok)
    failed = len(results) - passed

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  Results: {passed}/{len(results)} passed{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")

    # Save JSON
    import datetime
    report = {
        "run_at":       datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "api_url":      args.url,
        "total":        len(results),
        "passed":       passed,
        "failed":       failed,
        "duration_s":   round(run_ended - run_started, 1),
        "tests":        detail_rows,
    }
    with open(args.json_out, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"\n  JSON results → {args.json_out}")

    if failed:
        print(f"\n{RED}Failed tests:{RESET}")
        for name, ok in results:
            if not ok:
                print(f"  {RED}✗ {name}{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}All tests passed!{RESET}")


if __name__ == "__main__":
    main()
