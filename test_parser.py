"""
test_parser.py
───────────────
Tests for OutputParser covering PASS/FAIL/TLE, global TLE, and MISSING TC.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import json
from core.output_parser import OutputParser, ParsedSubmission, TCResult, parse_judge0_response

SESSION_ID = "testsession00"
DELIM = f"@@TC_RESULT__{SESSION_ID}__"

def make_tc_block(tc_num, status, got="", expected="", detail=""):
    content = json.dumps({"status": status, "got": got, "expected": expected, "detail": detail})
    return (
        f"{DELIM}START_{tc_num}\n"
        f"{content}\n"
        f"{DELIM}END_{tc_num}\n"
    )

print("=" * 70)
print("OUTPUT PARSER TEST RESULTS")
print("=" * 70)

failures = 0

def check(label, condition, msg=""):
    global failures
    if condition:
        print(f"  [OK  ] {label}")
    else:
        print(f"  [FAIL] {label}" + (f": {msg}" if msg else ""))
        failures += 1

# ── Test 1: Normal PASS/FAIL/TLE results with DONE marker ───────────────────
print("\nTest 1: Normal PASS/FAIL/TLE results with DONE marker")

raw = (
    make_tc_block(1, "PASS", got="5",   expected="5")  +
    make_tc_block(2, "FAIL", got="2 7", expected="1 2") +
    make_tc_block(3, "TLE",  detail="Exceeded 2s")      +
    make_tc_block(4, "PASS", got="2 3", expected="2 3") +
    f"{DELIM}DONE\n"
)

parser = OutputParser(raw, SESSION_ID, total_tc_count=4)
result = parser.parse()

check("Score is 2 (TC1 and TC4 PASS)", result.score == 2, f"got {result.score}")
check("Total is 4", result.total == 4, f"got {result.total}")
check("Global TLE is False", result.global_tle == False, f"got {result.global_tle}")
check("TC1 is PASS", result.tc_results[0].status == "PASS")
check("TC2 is FAIL", result.tc_results[1].status == "FAIL")
check("TC3 is TLE",  result.tc_results[2].status == "TLE")
check("TC4 is PASS", result.tc_results[3].status == "PASS")
check("TC2 got field",      result.tc_results[1].got == "2 7",  f"got '{result.tc_results[1].got}'")
check("TC2 expected field", result.tc_results[1].expected == "1 2", f"got '{result.tc_results[1].expected}'")
check("Partial execution False (all 4 found)", result.partial_execution == False)


# ── Test 2: Global TLE — DONE marker absent ──────────────────────────────────
print("\nTest 2: Global TLE — DONE marker missing (only TC1 completed)")

raw2 = make_tc_block(1, "PASS", got="5", expected="5")  # no DONE, TCs 2-4 missing

parser2 = OutputParser(raw2, SESSION_ID, total_tc_count=4)
result2 = parser2.parse()

check("Global TLE is True (no DONE marker)", result2.global_tle == True, f"got {result2.global_tle}")
check("Score is 1 (only TC1 PASS)", result2.score == 1, f"got {result2.score}")
check("TC1 is PASS", result2.tc_results[0].status == "PASS")
check("TC2 is TLE (global TLE fill)",  result2.tc_results[1].status == "TLE",
      f"got {result2.tc_results[1].status}")
check("TC3 is TLE (global TLE fill)",  result2.tc_results[2].status == "TLE")
check("TC4 is TLE (global TLE fill)",  result2.tc_results[3].status == "TLE")
check("Partial execution True", result2.partial_execution == True)


# ── Test 3: MISSING TC — crash before all TCs, but DONE marker present ───────
print("\nTest 3: MISSING TC — crash between TCs, DONE marker absent")

raw3 = make_tc_block(1, "PASS", got="5", expected="5")
# TC2 never emitted — harness crashed, no DONE either

parser3 = OutputParser(raw3, SESSION_ID, total_tc_count=3)
result3 = parser3.parse()

# Since DONE is absent, global_tle = True; missing TCs become TLE
check("Global TLE True (no DONE)", result3.global_tle == True)
check("TC2 status is TLE (global TLE fill)", result3.tc_results[1].status == "TLE",
      f"got {result3.tc_results[1].status}")
check("TC3 status is TLE (global TLE fill)", result3.tc_results[2].status == "TLE")


# ── Test 3b: MISSING TC with DONE marker ─────────────────────────────────────
print("\nTest 3b: MISSING TC with DONE marker — TC gets MISSING status")

raw3b = (
    make_tc_block(1, "PASS", got="5", expected="5") +
    # TC2 skipped
    make_tc_block(3, "FAIL") +
    f"{DELIM}DONE\n"
)

parser3b = OutputParser(raw3b, SESSION_ID, total_tc_count=3)
result3b = parser3b.parse()

check("Global TLE False (DONE present)", result3b.global_tle == False)
check("TC2 is MISSING (found TCs: 1 and 3, DONE present)",
      result3b.tc_results[1].status == "MISSING",
      f"got {result3b.tc_results[1].status}")
check("TC1 is PASS", result3b.tc_results[0].status == "PASS")
check("TC3 is FAIL", result3b.tc_results[2].status == "FAIL")


# ── Test 4: parse_judge0_response — Judge0-level TLE ─────────────────────────
print("\nTest 4: parse_judge0_response — Judge0 Time Limit Exceeded")

sub4 = parse_judge0_response(
    judge0_stdout  = "",
    judge0_status  = "Time Limit Exceeded",
    session_id     = SESSION_ID,
    total_tc_count = 3,
)

check("All 3 TCs are TLE", all(r.status == "TLE" for r in sub4.tc_results),
      str([r.status for r in sub4.tc_results]))
check("Global TLE True", sub4.global_tle == True)
check("Score is 0", sub4.score == 0, f"got {sub4.score}")


# ── Test 5: parse_judge0_response — Compilation Error ────────────────────────
print("\nTest 5: parse_judge0_response — Compilation Error")

sub5 = parse_judge0_response(
    judge0_stdout  = "",
    judge0_status  = "Compilation Error",
    session_id     = SESSION_ID,
    total_tc_count = 2,
)

check("All 2 TCs are ERROR", all(r.status == "ERROR" for r in sub5.tc_results),
      str([r.status for r in sub5.tc_results]))
check("Score is 0", sub5.score == 0)


# ── Test 6: JSON parse error in TC block ─────────────────────────────────────
print("\nTest 6: JSON parse error in TC block")

raw6 = (
    f"{DELIM}START_1\n"
    "this is not valid json\n"
    f"{DELIM}END_1\n"
    f"{DELIM}DONE\n"
)

parser6 = OutputParser(raw6, SESSION_ID, total_tc_count=1)
result6 = parser6.parse()
check("TC1 is ERROR on bad JSON", result6.tc_results[0].status == "ERROR",
      f"got {result6.tc_results[0].status}")


# ── Test 7: Invalid status in TC block ───────────────────────────────────────
print("\nTest 7: Invalid status string defaults to ERROR")

raw7 = (
    f"{DELIM}START_1\n"
    + json.dumps({"status": "BOGUS_STATUS", "got": "x", "expected": "y"}) + "\n"
    + f"{DELIM}END_1\n"
    + f"{DELIM}DONE\n"
)

parser7 = OutputParser(raw7, SESSION_ID, total_tc_count=1)
result7 = parser7.parse()
check("Bogus status → ERROR", result7.tc_results[0].status == "ERROR",
      f"got {result7.tc_results[0].status}")


# ── Summary ──────────────────────────────────────────────────────────────────
print()
print("=" * 70)
if failures == 0:
    print(f"All output parser tests PASSED")
else:
    print(f"{failures} test(s) FAILED")
print("=" * 70)

sys.exit(1 if failures > 0 else 0)
