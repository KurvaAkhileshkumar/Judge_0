#!/usr/bin/env python3
"""
generate_ec2_report.py
──────────────────────
Generates a rich, colour-coded Excel workbook from one or more
load_test_report.json files produced by the Go load tester (main.go).

Usage:
    # Single report
    python generate_ec2_report.py [report.json] [--metrics metrics.jsonl] [--out report.xlsx]

    # Multi-run scenario comparison (auto-detect all load_test_report*.json)
    python generate_ec2_report.py report.json --compare

    # Or pass explicit comparison files
    python generate_ec2_report.py report.json --compare file2.json file3.json ...

Arguments:
    report.json       — Primary JSON report (default: load_test_report.json)
    --metrics FILE    — JSONL file from collect_ec2_metrics.py (optional)
    --out FILE        — Output .xlsx path (default: primary report stem + .xlsx)
    --workers N       — Judge0 worker containers (default: 3)
    --runners N       — MAX_RUNNERS per worker (default: 2)
    --compare [FILE …]— Add Sheet 4 multi-run comparison; if no FILEs given,
                        auto-detects all load_test_report*.json in the same directory

Sheets produced:
  Sheet 1 — System Info
      EC2 instance spec, Docker service layout (with direct-mode variant),
      Judge0 conf, Flask/gunicorn conf, grading worker, PostgreSQL, Redis,
      load-test run config incl. concurrency mode and result delivery method.

  Sheet 2 — Load Test Metrics
      Concurrency mode analysis (flat-concurrent/ramp/pipelined-batch),
      KPI overview, latency percentiles (P50–P99.9) with severity colouring,
      throughput, score distribution (0%/partial/100%), submission outcomes,
      idempotency cache analysis (warm-cache scenario), status distribution,
      per-problem pass rates, solution-type breakdown, batch analysis,
      and (if --metrics provided) live system metrics time-series with
      conditional colouring on CPU/RAM/queue-depth cells.

  Sheet 3 — User Test Case Results
      Per-user aggregated marks summary table at the top, then a detailed
      per-submission table: User ID, Problem, Solution, Harness Status,
      Score, Total TCs, Marks %, Partial flag, Latency, Error notes, and
      per-TC columns (Status | Got | Expected) up to the widest submission.

  Sheet 4 — Scenario Comparison (only with --compare)
      Side-by-side comparison of multiple load test runs: users, concurrency
      mode, ramp, throughput, latency percentiles, pass/fail/error rates,
      rate-limited count, duplicate count, peak in-flight, and score
      distribution — one column per report file.
"""

import argparse
import datetime
import json
import math
import sys
from pathlib import Path
from datetime import timezone as _tz

_UTC = _tz.utc

def _now_utc() -> str:
    return datetime.datetime.now(_UTC).strftime("%Y-%m-%d %H:%M:%S")

try:
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    print("ERROR: openpyxl not installed.  Run: pip install -r requirements-report.txt")
    sys.exit(1)


# ── Colour palette ────────────────────────────────────────────────────────────

_C = {
    # Header tiers
    "hdr_dark":      "1F4E79",   # dark navy — sheet/section titles
    "hdr_mid":       "2E75B6",   # medium blue — sub-section titles
    "hdr_light":     "BDD7EE",   # light blue — column headers
    # Section dividers
    "section":       "D6E4F0",
    # Outcome colours
    "pass":          "C6EFCE",   # green
    "pass_fg":       "276221",
    "fail":          "FFCCCC",   # pink-red
    "fail_fg":       "9C0006",
    "tle":           "FFEB9C",   # yellow
    "tle_fg":        "9C6500",
    "error":         "FFD966",   # amber
    "error_fg":      "7D5000",
    "blocked":       "D9D9D9",   # grey
    "blocked_fg":    "404040",
    "rate_limited":  "F4B942",   # orange
    "rate_fg":       "6D3B00",
    "sys_error":     "FF6666",   # red
    "sys_fg":        "660000",
    "judge0_err":    "FF6666",
    "judge0_fg":     "660000",
    "unknown":       "F2F2F2",
    # Misc
    "white":         "FFFFFF",
    "alt_row":       "EBF3FB",
    "light_grey":    "F2F2F2",
    "partial":       "FFD966",   # amber for partial marks
    "partial_fg":    "7D5000",
    # CPU / RAM severity
    "cpu_ok":        "C6EFCE",
    "cpu_warn":      "FFEB9C",
    "cpu_crit":      "FFCCCC",
}

# Map harness status → (bg, fg)
_STATUS_STYLE = {
    "PASS":         (_C["pass"],        _C["pass_fg"]),
    "FAIL":         (_C["fail"],        _C["fail_fg"]),
    "TLE":          (_C["tle"],         _C["tle_fg"]),
    "ERROR":        (_C["error"],       _C["error_fg"]),
    "BLOCKED":      (_C["blocked"],     _C["blocked_fg"]),
    "RATE_LIMITED": (_C["rate_limited"],_C["rate_fg"]),
    "SYSTEM_ERROR": (_C["sys_error"],   _C["sys_fg"]),
    "JUDGE0_ERROR": (_C["judge0_err"],  _C["judge0_fg"]),
    "UNKNOWN":      (_C["unknown"],     "000000"),
    "MISSING":      (_C["unknown"],     "000000"),
}


# ── Low-level cell helpers ────────────────────────────────────────────────────

def _fill(hex_color: str) -> PatternFill:
    return PatternFill(fill_type="solid", fgColor=hex_color)


def _font(bold: bool = False, color: str = "000000", size: int = 11) -> Font:
    return Font(bold=bold, color=color, size=size)


def _thin_border() -> Border:
    thin = Side(style="thin", color="BFBFBF")
    return Border(left=thin, right=thin, top=thin, bottom=thin)


def _center_align(wrap: bool = True) -> Alignment:
    return Alignment(horizontal="center", vertical="center", wrap_text=wrap)


def _left_align(wrap: bool = True) -> Alignment:
    return Alignment(horizontal="left", vertical="center", wrap_text=wrap)


def _style(cell, bg: str = _C["white"], fg: str = "000000",
           bold: bool = False, size: int = 11,
           halign: str = "left", wrap: bool = True):
    cell.fill = _fill(bg)
    cell.font = _font(bold=bold, color=fg, size=size)
    cell.border = _thin_border()
    cell.alignment = Alignment(horizontal=halign, vertical="center", wrap_text=wrap)


# ── Mid-level row helpers ─────────────────────────────────────────────────────

def _write_title(ws, row: int, text: str, ncols: int, fontsize: int = 13):
    """Dark-navy full-width title row."""
    cell = ws.cell(row=row, column=1, value=text)
    _style(cell, bg=_C["hdr_dark"], fg="FFFFFF", bold=True, size=fontsize, halign="center")
    ws.row_dimensions[row].height = max(22, fontsize + 10)
    if ncols > 1:
        ws.merge_cells(start_row=row, start_column=1,
                       end_row=row,   end_column=ncols)


def _write_section(ws, row: int, text: str, ncols: int):
    """Blue section-divider row spanning all columns."""
    cell = ws.cell(row=row, column=1, value=f"  {text}")
    _style(cell, bg=_C["section"], fg=_C["hdr_dark"], bold=True)
    if ncols > 1:
        ws.merge_cells(start_row=row, start_column=1,
                       end_row=row,   end_column=ncols)


def _write_col_header(ws, row: int, col: int, text: str, bg: str = _C["hdr_mid"],
                      fg: str = "FFFFFF", colspan: int = 1):
    """Single column-header cell (optionally merged)."""
    cell = ws.cell(row=row, column=col, value=text)
    _style(cell, bg=bg, fg=fg, bold=True, halign="center")
    if colspan > 1:
        ws.merge_cells(start_row=row, start_column=col,
                       end_row=row,   end_column=col + colspan - 1)
    return cell


def _write_kv(ws, row: int, key: str, value, alt: bool = False):
    """Two-column key: value row."""
    bg = _C["alt_row"] if alt else _C["white"]
    c1 = ws.cell(row=row, column=1, value=key)
    _style(c1, bg=bg, bold=True)
    c2 = ws.cell(row=row, column=2, value=value)
    _style(c2, bg=bg)


def _write_data_row(ws, row: int, values: list, alt: bool = False,
                    col_start: int = 1):
    """Plain data row, alternating background."""
    bg = _C["alt_row"] if alt else _C["white"]
    for ci, val in enumerate(values, col_start):
        cell = ws.cell(row=row, column=ci, value=val)
        _style(cell, bg=bg)


def _set_col_widths(ws, widths: list, col_start: int = 1):
    for i, w in enumerate(widths, col_start):
        ws.column_dimensions[get_column_letter(i)].width = w


# ── Derived helpers ───────────────────────────────────────────────────────────

def _throughput(summary: dict) -> float:
    return summary.get("throughput_per_sec", 0.0) or 0.0


def _duration(summary: dict) -> float:
    completed = summary.get("completed", 0) or 1
    tput = _throughput(summary)
    return (completed / tput) if tput > 0 else 0.0


def _pct(numerator, denominator):
    if not denominator:
        return "–"
    return f"{numerator / denominator * 100:.1f}%"


# ── Scenario / concurrency helpers ────────────────────────────────────────────

def _concurrency_label(cfg: dict) -> str:
    """Human-readable concurrency mode derived from config."""
    batch = cfg.get("batch_size", 0) or 0
    ramp  = cfg.get("ramp_up_sec", 0) or 0
    users = cfg.get("users", 0) or 0
    if batch > 0:
        n_batches = math.ceil(users / batch)
        return (f"Pipelined Batch  ({n_batches} batches × {batch} users/batch — "
                f"all batches fire simultaneously)")
    elif ramp > 0:
        cadence_ms = ramp * 1000 / max(users - 1, 1)
        return (f"Flat Concurrent + Ramp-up  ({ramp}s ramp → "
                f"1 user every ~{cadence_ms:.0f} ms)")
    else:
        return "Flat Concurrent  — all users fire simultaneously (no ramp-up)"


def _ramp_cadence_ms(cfg: dict) -> float:
    ramp  = cfg.get("ramp_up_sec", 0) or 0
    users = cfg.get("users", 1) or 1
    return (ramp * 1000 / max(users - 1, 1)) if ramp > 0 and users > 1 else 0.0


def _result_delivery_label(cfg: dict) -> str:
    mode = cfg.get("mode", "")
    if mode == "flask_stack":
        return "SSE  /results/stream/<ticket_id>  (Flask stack)"
    cb = cfg.get("callback_port", 0) or 0
    if cb:
        return f"Callback  (Judge0 → HTTP POST → port {cb})"
    return f"Polling  (interval {cfg.get('poll_interval', '400ms')})"


_PASCAL_TO_SNAKE = {
    "UserID":        "user_id",
    "ProblemID":     "problem_id",
    "SolutionID":    "solution_id",
    "SolutionType":  "solution_type",
    "HarnessStatus": "harness_status",
    "SourceCode":    "source_code",
    "TCDetails":     "tc_details",
    "LatencyMs":     "latency_ms",
    "Judge0Status":  "judge0_status",
    "Error":         "error",
    "GlobalTLE":     "global_tle",
    "Score":         "score",
    "TotalTCs":      "total_tcs",
}

def _normalise_result(r: dict) -> dict:
    """Return a copy of a result dict with PascalCase keys mapped to snake_case.
    Old JSON files (pre-tag) used Go's default PascalCase; new files use proper
    json tags and already have snake_case.  This makes the report handle both."""
    out = {}
    for k, v in r.items():
        out[_PASCAL_TO_SNAKE.get(k, k)] = v
    return out


def _score_distribution(all_subs: list) -> tuple:
    """Returns (full_marks_count, partial_count, zero_count)."""
    full = partial = zero = 0
    for s in all_subs:
        score = s.get("score", 0)
        total = s.get("total_tcs", 0) or 1
        if score >= total:
            full += 1
        elif score > 0:
            partial += 1
        else:
            zero += 1
    return full, partial, zero


def _scenario_label(cfg: dict) -> str:
    """Short one-line label for a report file used in Sheet 4."""
    mode  = cfg.get("mode", "direct")
    users = cfg.get("users", 0)
    ramp  = cfg.get("ramp_up_sec", 0) or 0
    batch = cfg.get("batch_size", 0) or 0
    parts = [f"{users}u"]
    if batch > 0:
        parts.append(f"batch{batch}")
    elif ramp > 0:
        parts.append(f"ramp{ramp}s")
    else:
        parts.append("concurrent")
    parts.append("flask" if mode == "flask_stack" else "direct")
    return "_".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
#  Sheet 1 — System Info
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet1(wb: openpyxl.Workbook, report: dict, workers: int, runners: int):
    ws = wb.active
    ws.title = "System Info"
    ws.sheet_view.showGridLines = False

    cfg     = report.get("config", {})
    summary = report.get("summary", {})
    NCOLS   = 2

    # ── Title ─────────────────────────────────────────────────────────────────
    _write_title(ws, 1, "EC2 LOAD TEST — SYSTEM CONFIGURATION", NCOLS, fontsize=14)
    ws.cell(row=2, column=1, value=(
        f"Generated: {_now_utc()} UTC"
    ))
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=2)
    c2 = ws.cell(row=2, column=1)
    _style(c2, bg=_C["light_grey"], halign="center", bold=False, size=10)

    row  = 3
    alt  = False

    def section(title: str):
        nonlocal row, alt
        row += 1
        _write_section(ws, row, title, NCOLS)
        row += 1
        alt = False

    def kv(key: str, value):
        nonlocal row, alt
        _write_kv(ws, row, key, value, alt)
        row += 1
        alt = not alt

    # ── EC2 Instance ──────────────────────────────────────────────────────────
    section("EC2 Instance")
    kv("Instance Type",               "c5.xlarge")
    kv("vCPUs",                       4)
    kv("RAM",                         "8 GB")
    kv("Storage",                     "EBS (gp2 / gp3)")
    kv("OS",                          "Ubuntu 22.04.5 LTS")
    kv("Kernel",                      "Linux 5.15+ x86_64")
    kv("Region",                      "ap-south-1 (Mumbai)")
    kv("Network Performance",         "Up to 10 Gbps")

    # ── Docker Compose Services ───────────────────────────────────────────────
    section("Docker Services Layout")
    kv("Judge0 API Server (server)",  "judge0/judge0:1.13.1  —  port 2358")
    kv("Judge0 Worker containers",    f"{workers} containers  (--scale workers={workers})")
    kv("MAX_RUNNERS per worker",      runners)
    kv("Total concurrent sandboxes",  f"{workers} × {runners} = {workers * runners}")
    kv("Flask API (api)",             "gunicorn+gevent  —  port 5001→5000")
    kv("Grading Worker (grading_worker)", "worker_async.py  —  WORKER_CONCURRENCY=24")
    kv("Reconciler",                  "reconciler.py  —  scan every 60s")
    kv("PostgreSQL",                  "postgres:13  —  mem_limit=400m  reservation=128m")
    kv("Redis",                       "Latest  —  mem_limit=512m  maxmemory=512mb")
    kv("Memory budget (workers=3)",
       "OS≈1 GB + PG≤0.4 GB + Redis≤0.5 GB + server≈0.4 GB + 3×workers≤3 GB + Flask≈0.3 GB ≈ 5.6 GB")

    # ── Judge0 Configuration ──────────────────────────────────────────────────
    section("Judge0 Configuration (judge0.ec2.conf)")
    kv("MAX_RUNNERS (isolate sandboxes/worker)", runners)
    kv("MAX_QUEUE_SIZE (Resque)",     200)
    kv("CPU_TIME_LIMIT (default)",    "5 s")
    kv("WALL_TIME_LIMIT (default)",   "10 s")
    kv("MEMORY_LIMIT (default)",      "256 000 KB  (256 MB)")
    kv("MAX_MEMORY_LIMIT",            "4 194 304 KB  (4 GB)")
    kv("SOURCE_CODE_SIZE_LIMIT",      "524 288 B  (512 KB)")
    kv("MAX_PROCESSES_PER_WORKER",    1100)
    kv("OPEN_FILES_LIMIT",            65536)
    kv("INTERVAL (Resque idle poll)", "0.1 s")
    kv("RAILS_ENV / RACK_ENV",        "production")
    kv("JUDGE0_TELEMETRY_ENABLE",     False)

    # ── Judge0 API Server (Puma) ──────────────────────────────────────────────
    section("Judge0 API Server (Puma / Rails)")
    kv("WEB_CONCURRENCY (Puma processes)",   2)
    kv("RAILS_MAX_THREADS (threads/process)", 16)
    kv("HTTP slots",                          "2 × 16 = 32 concurrent HTTP requests")
    kv("net.core.somaxconn",                 4096)
    kv("net.ipv4.tcp_max_syn_backlog",       4096)

    # ── Flask API ─────────────────────────────────────────────────────────────
    section("Flask Grading API (gunicorn + gevent)")
    kv("Gunicorn worker class",  "gevent")
    kv("Gunicorn workers",       2)
    kv("Worker connections",     500)
    kv("Bind",                   "0.0.0.0:5000  (host port 5001)")
    kv("Timeout",                "1900 s")
    kv("Result delivery",        "SSE  /results/stream/<ticket_id>")
    kv("MAX_QUEUE_DEPTH",        5000)
    kv("SSE_TIMEOUT_S",          1800)
    kv("Idempotency TTL",        "2 h  (SHA-256 key stored in Redis)")
    kv("Max request body",       "1 MB  (rejects oversized payloads before parsing)")

    # ── Grading Worker ────────────────────────────────────────────────────────
    section("Grading Worker (worker_async.py)")
    kv("WORKER_CONCURRENCY",     24)
    kv("MAX_RETRY_COUNT",        3)
    kv("Retry strategy",         "Natural queue drain  —  no sleep between retries")
    kv("Result TTL",             "2 h in Redis")
    kv("Callback mode",          "Judge0 POSTs back to worker's callback server")

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    section("PostgreSQL 13")
    kv("max_connections",        200)
    kv("shared_buffers",         "128 MB")
    kv("effective_cache_size",   "1 GB  (planner hint, no allocation)")
    kv("wal_buffers",            "16 MB")
    kv("work_mem",               "4 MB")
    kv("checkpoint_completion_target", 0.9)
    kv("mem_limit",              "400 m")
    kv("mem_reservation",        "128 m")

    # ── Redis ─────────────────────────────────────────────────────────────────
    section("Redis")
    kv("maxmemory",              "512 mb")
    kv("mem_limit",              "512 m")
    kv("Queue keys",             "judge0:jobs:normal  |  judge0:jobs:retry")
    kv("Result key prefix",      "judge0:result:<ticket_id>")
    kv("Pub/Sub prefix",         "judge0:notify:<ticket_id>")
    kv("Idempotency prefix",     "judge0:idem:<sha256>")

    # ── Load Test Configuration ───────────────────────────────────────────────
    section("Load Test Configuration")
    kv("Mode",                   cfg.get("mode", "N/A"))
    kv("Target URL",             cfg.get("target", "N/A"))
    kv("Virtual Users",          cfg.get("users", "N/A"))
    kv("Ramp-up Duration",       f"{cfg.get('ramp_up_sec', 0)} s")
    kv("Batch Size",             cfg.get("batch_size") or "0  (flat concurrent — no batching)")
    kv("Concurrency Mode",       _concurrency_label(cfg))
    if _ramp_cadence_ms(cfg) > 0:
        kv("Ramp-up Cadence",    f"1 virtual user launched every ~{_ramp_cadence_ms(cfg):.0f} ms")
    kv("Result Delivery",        _result_delivery_label(cfg))
    kv("Dry Run",                cfg.get("dry_run", False))
    kv("Test Duration",          f"{_duration(summary):.1f} s")
    kv("Go Load Tester",         "main.go  —  Judge0 Harness Load Tester")

    is_flask = cfg.get("mode") == "flask_stack"
    if not is_flask:
        section("Direct Judge0 Mode Notes")
        kv("Result delivery",    "Polling (default) or Callback (-callback-port N)")
        kv("No admission control", "No rate limiting / idempotency in direct mode")
        kv("No Redis queue",     "Harnesses submitted directly to Judge0 /submissions")
        kv("Security check",     "Go-side quick AST pre-check (syntax errors blocked locally)")

    # ── Column widths ─────────────────────────────────────────────────────────
    ws.column_dimensions["A"].width = 46
    ws.column_dimensions["B"].width = 55
    ws.freeze_panes = ws.cell(row=3, column=1)


# ─────────────────────────────────────────────────────────────────────────────
#  Sheet 2 — Load Test Metrics
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet2(wb: openpyxl.Workbook, report: dict, metrics_data: list):
    ws = wb.create_sheet("Load Test Metrics")
    ws.sheet_view.showGridLines = False

    cfg           = report.get("config", {})
    summary       = report.get("summary", {})
    status_counts = report.get("status_counts", {})
    problem_counts= report.get("problem_counts", {})
    type_counts   = report.get("type_counts", {})
    batches       = report.get("batches", [])
    per_problem   = report.get("per_problem_report", [])
    all_results   = [_normalise_result(r) for r in report.get("results", [])]

    NCOLS = 6
    row   = 1

    # ── Title ─────────────────────────────────────────────────────────────────
    _write_title(ws, row, "EC2 LOAD TEST — PERFORMANCE METRICS", NCOLS, fontsize=14)
    row += 1
    ts_cell = ws.cell(row=row, column=1,
                      value=f"Generated: {_now_utc()} UTC"
                            f"   |   Report: {cfg.get('target','N/A')}"
                            f"   |   Users: {cfg.get('users','N/A')}"
                            f"   |   Mode: {cfg.get('mode','N/A')}")
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=NCOLS)
    _style(ts_cell, bg=_C["light_grey"], halign="left", size=10)
    row += 2

    # ── Compute derived stats ─────────────────────────────────────────────────
    lats = sorted(r.get("latency_ms", 0) for r in all_results if "latency_ms" in r)

    def _pctile(p: float) -> int:
        if not lats:
            return summary.get(f"latency_p{int(p)}_ms", 0)
        idx = max(0, int(math.ceil(len(lats) * p / 100)) - 1)
        return lats[idx]

    avg_lat = (sum(lats) / len(lats)) if lats else 0.0
    min_lat = lats[0]  if lats else 0
    max_lat = lats[-1] if lats else 0
    p50     = summary.get("latency_p50_ms", _pctile(50))
    p95     = summary.get("latency_p95_ms", _pctile(95))
    p99     = summary.get("latency_p99_ms", _pctile(99))

    completed   = summary.get("completed", 0)
    total_subs  = summary.get("total_submissions", completed) or 1
    throughput  = _throughput(summary)
    duration    = _duration(summary)

    # ── Section: KPI Overview (2 columns of key-value pairs side by side) ─────
    _write_section(ws, row, "KPI OVERVIEW", NCOLS)
    row += 1

    # Header row
    for ci, h in enumerate(["Metric", "Value", "", "Metric", "Value", ""], 1):
        bg = _C["hdr_light"] if h else _C["white"]
        c = ws.cell(row=row, column=ci, value=h)
        _style(c, bg=bg, bold=True, halign="center", fg=_C["hdr_dark"])
    row += 1

    kpis_left = [
        ("Mode",                    cfg.get("mode", "N/A")),
        ("Target URL",              cfg.get("target", "N/A")),
        ("Virtual Users",           cfg.get("users", "N/A")),
        ("Ramp-up Duration",        f"{cfg.get('ramp_up_sec', 0)} s"),
        ("Batch Size",              cfg.get("batch_size") or "flat concurrent"),
        ("Test Duration",           f"{duration:.1f} s"),
        ("Total Submissions",       summary.get("total_submissions", 0)),
        ("Completed",               completed),
        ("Throughput",              f"{throughput:.2f} req/s"),
        ("Peak In-Flight Jobs",     summary.get("peak_in_flight_jobs", 0)),
    ]
    kpis_right = [
        ("Errors (network/Judge0)", summary.get("errors", 0)),
        ("Blocked (security check)",summary.get("blocked_by_security", 0)),
        ("Rate Limited (HTTP 429)", summary.get("rate_limited_429", 0)),
        ("Duplicate (idempotent)",  summary.get("duplicate_idempotent", 0)),
        ("System Errors (infra)",   summary.get("system_errors", 0)),
        ("Harness Jobs (actual)",   summary.get("harness_jobs", 0)),
        ("Batch-Equivalent Jobs",   summary.get("batch_equivalent", 0)),
        ("Queue Reduction Factor",  summary.get("queue_reduction", "N/A")),
        ("Max Queue Depth (Redis)", summary.get("max_queue_depth", "N/A")),
        ("Result Delivery",         "SSE  /results/stream"),
    ]

    for i, ((lk, lv), (rk, rv)) in enumerate(zip(kpis_left, kpis_right)):
        bg = _C["alt_row"] if i % 2 else _C["white"]
        for ci, val in [(1, lk), (2, lv), (3, ""), (4, rk), (5, rv), (6, "")]:
            c = ws.cell(row=row, column=ci, value=val)
            _style(c, bg=bg, bold=(ci in (1, 4)))
        row += 1
    row += 1

    # ── Section: Latency Analysis ─────────────────────────────────────────────
    _write_section(ws, row, "LATENCY ANALYSIS", NCOLS)
    row += 1

    for ci, h in enumerate(["Percentile", "Latency (ms)", "", "Metric", "Value", ""], 1):
        bg = _C["hdr_light"] if h else _C["white"]
        c = ws.cell(row=row, column=ci, value=h)
        _style(c, bg=bg, bold=True, halign="center", fg=_C["hdr_dark"])
    row += 1

    lat_left = [
        ("P50  (median)",    p50),
        ("P75",              _pctile(75)),
        ("P90",              _pctile(90)),
        ("P95",              p95),
        ("P99",              p99),
        ("P99.9",            _pctile(99.9)),
        ("Min",              min_lat),
        ("Max",              max_lat),
        ("Average",          round(avg_lat, 1)),
    ]
    lat_right = [
        ("Throughput (req/s)",      f"{throughput:.2f}"),
        ("Total Completed",         completed),
        ("Test Duration",           f"{duration:.1f} s"),
        ("Harness Jobs (actual)",   summary.get("harness_jobs", 0)),
        ("Batch-Equivalent Jobs",   summary.get("batch_equivalent", 0)),
        ("Queue Reduction",         summary.get("queue_reduction", "N/A")),
        ("Peak In-Flight",          summary.get("peak_in_flight_jobs", 0)),
        ("Workers × Runners",       f"{report.get('_workers',3)} × {report.get('_runners',2)} = {report.get('_workers',3)*report.get('_runners',2)} sandboxes"),
        ("WORKER_CONCURRENCY",      24),
    ]

    nrows = max(len(lat_left), len(lat_right))
    for i in range(nrows):
        lk, lv = lat_left[i]  if i < len(lat_left)  else ("", "")
        rk, rv = lat_right[i] if i < len(lat_right) else ("", "")
        bg = _C["alt_row"] if i % 2 else _C["white"]
        # colour latency values by severity
        lat_bg = bg
        if isinstance(lv, (int, float)) and lv > 0:
            if   lv > 10000: lat_bg = _C["fail"]
            elif lv > 5000:  lat_bg = _C["tle"]
            elif lv < 1000:  lat_bg = _C["pass"]
        for ci, val in [(1, lk), (2, lv), (3, ""), (4, rk), (5, rv), (6, "")]:
            c = ws.cell(row=row, column=ci, value=val)
            _style(c, bg=lat_bg if ci == 2 else bg, bold=(ci in (1, 4)))
        row += 1
    row += 1

    # ── Section: Submission Outcomes ──────────────────────────────────────────
    _write_section(ws, row, "SUBMISSION OUTCOMES", NCOLS)
    row += 1

    _write_col_header(ws, row, 1, "Outcome",     bg=_C["hdr_light"], fg=_C["hdr_dark"])
    _write_col_header(ws, row, 2, "Count",       bg=_C["hdr_light"], fg=_C["hdr_dark"])
    _write_col_header(ws, row, 3, "% of Total",  bg=_C["hdr_light"], fg=_C["hdr_dark"])
    _write_col_header(ws, row, 4, "Description", bg=_C["hdr_light"], fg=_C["hdr_dark"], colspan=3)
    row += 1

    _OUTCOME_DESC = {
        "PASS":         "All test cases passed — full marks",
        "FAIL":         "One or more TCs wrong answer — partial/no marks",
        "TLE":          "Time limit exceeded — per-TC or global",
        "ERROR":        "Runtime error in student code",
        "BLOCKED":      "Rejected by security checker / AST pre-check (syntax error)",
        "RATE_LIMITED": "HTTP 429 — queue full, submission rejected by admission control",
        "SYSTEM_ERROR": "Infrastructure failure — not the student's fault; retried up to 3×",
        "JUDGE0_ERROR": "Judge0 / network error during submission or polling",
        "UNKNOWN":      "Status could not be determined",
    }
    STATUS_ORDER = ["PASS","FAIL","TLE","ERROR","BLOCKED","RATE_LIMITED","SYSTEM_ERROR","JUDGE0_ERROR","UNKNOWN"]
    all_statuses = sorted(status_counts.items(), key=lambda x: -x[1])
    ordered = [(s, status_counts[s]) for s in STATUS_ORDER if s in status_counts]
    ordered += [(s, c) for s, c in all_statuses if s not in STATUS_ORDER]

    for i, (status, count) in enumerate(ordered):
        bg, fg = _STATUS_STYLE.get(status, (_C["unknown"], "000000"))
        for ci, val in [
            (1, status),
            (2, count),
            (3, _pct(count, completed)),
            (4, _OUTCOME_DESC.get(status, "")),
        ]:
            c = ws.cell(row=row, column=ci, value=val)
            if ci <= 3:
                _style(c, bg=bg, fg=fg, bold=(ci == 1))
            else:
                _style(c, bg=_C["alt_row"] if i % 2 else _C["white"])
        if 4 < NCOLS:
            ws.merge_cells(start_row=row, start_column=4, end_row=row, end_column=NCOLS)
        row += 1
    row += 1

    # ── Section: Per-Problem Analysis ─────────────────────────────────────────
    _write_section(ws, row, "PER-PROBLEM ANALYSIS", NCOLS)
    row += 1

    for ci, h in enumerate(["Problem ID", "Submissions", "% of Total",
                              "Passed", "Pass Rate", "Avg Latency (ms)"], 1):
        _write_col_header(ws, row, ci, h, bg=_C["hdr_light"], fg=_C["hdr_dark"])
    row += 1

    # Build per-problem stats from per_problem_report
    prob_stats: dict[str, dict] = {}
    for prob_rpt in per_problem:
        pid  = prob_rpt["problem_id"]
        subs = prob_rpt.get("submissions", [])
        passed = sum(1 for s in subs if s.get("harness_status") == "PASS")
        lats_p = [s.get("latency_ms", 0) for s in subs if "latency_ms" in s]
        prob_stats[pid] = {
            "count":      len(subs),
            "passed":     passed,
            "avg_lat":    round(sum(lats_p) / len(lats_p), 0) if lats_p else 0,
        }

    total_sub_count = sum(problem_counts.values()) or 1
    for i, (pid, count) in enumerate(sorted(problem_counts.items(), key=lambda x: -x[1])):
        bg = _C["alt_row"] if i % 2 else _C["white"]
        stats = prob_stats.get(pid, {})
        passed   = stats.get("passed", 0)
        pass_pct = _pct(passed, count)
        avg_l    = stats.get("avg_lat", "–")
        for ci, val in [
            (1, pid), (2, count), (3, _pct(count, total_sub_count)),
            (4, passed), (5, pass_pct), (6, avg_l),
        ]:
            c = ws.cell(row=row, column=ci, value=val)
            _style(c, bg=bg, bold=(ci == 1))
        row += 1
    row += 1

    # ── Section: Solution Type Analysis ───────────────────────────────────────
    _write_section(ws, row, "SOLUTION TYPE ANALYSIS", NCOLS)
    row += 1

    for ci, h in enumerate(["Solution Type", "Count", "% of Total",
                              "Expected Behaviour", "", ""], 1):
        bg = _C["hdr_light"] if h else _C["white"]
        _write_col_header(ws, row, ci, h, bg=bg, fg=_C["hdr_dark"])
    row += 1

    _TYPE_DESC = {
        "accepted":            "Should PASS all test cases",
        "wrong_answer":        "Should FAIL some or all test cases",
        "time_limit_exceeded": "Should TLE on all test cases",
        "runtime_error":       "Should ERROR on all test cases",
        "syntax_error":        "Should be BLOCKED by security / AST checker",
        "partial":             "Passes a subset of test cases",
    }
    for i, (stype, count) in enumerate(sorted(type_counts.items(), key=lambda x: -x[1])):
        bg = _C["alt_row"] if i % 2 else _C["white"]
        desc = _TYPE_DESC.get(stype, "")
        for ci, val in [(1, stype), (2, count), (3, _pct(count, total_sub_count)),
                        (4, desc), (5, ""), (6, "")]:
            c = ws.cell(row=row, column=ci, value=val)
            _style(c, bg=bg, bold=(ci == 1))
        if desc:
            ws.merge_cells(start_row=row, start_column=4, end_row=row, end_column=NCOLS)
        row += 1
    row += 1

    # ── Section: Batch Analysis (if present) ──────────────────────────────────
    if batches:
        _write_section(ws, row, "BATCH ANALYSIS", NCOLS)
        row += 1

        for ci, h in enumerate(["Batch ID", "Users", "Passed",
                                  "Accept %", "Duration (s)", "Avg Lat (ms)"], 1):
            _write_col_header(ws, row, ci, h, bg=_C["hdr_light"], fg=_C["hdr_dark"])
        row += 1

        for i, b in enumerate(batches):
            bg = _C["alt_row"] if i % 2 else _C["white"]
            for ci, val in [
                (1, b.get("batch_id", "")),
                (2, b.get("total_users", "")),
                (3, b.get("passed", "")),
                (4, f"{b.get('acceptance_pct', 0):.1f}%"),
                (5, f"{b.get('duration_s', 0):.2f}"),
                (6, round(b.get("avg_latency_ms", 0), 0)),
            ]:
                c = ws.cell(row=row, column=ci, value=val)
                _style(c, bg=bg)
            row += 1
        row += 1

    # ── Section: Concurrency Mode Analysis ───────────────────────────────────
    _write_section(ws, row, "CONCURRENCY MODE ANALYSIS", NCOLS)
    row += 1

    _CONC_MODE_DETAIL = {
        "flat_concurrent_noramp": (
            "Flat Concurrent (no ramp)",
            "All virtual users launch simultaneously. Maximum burst — worst-case for the queue, "
            "Redis, and Judge0 admission control. Use to find saturation point.",
        ),
        "flat_concurrent_ramp": (
            "Flat Concurrent + Ramp-up",
            "Virtual users introduced evenly over the ramp window. Reduces initial spike; "
            "simulates users logging in progressively. Good for steady-state throughput testing.",
        ),
        "pipelined_batch": (
            "Pipelined Batch",
            "Users split into N-sized batches; all batches are fired simultaneously as goroutines. "
            "Within each batch users run concurrently. Useful to test Judge0 at specific concurrency levels.",
        ),
    }

    batch_size = cfg.get("batch_size", 0) or 0
    ramp       = cfg.get("ramp_up_sec", 0) or 0
    users      = cfg.get("users", 0) or 0

    if batch_size > 0:
        mode_key = "pipelined_batch"
    elif ramp > 0:
        mode_key = "flat_concurrent_ramp"
    else:
        mode_key = "flat_concurrent_noramp"

    mode_name, mode_desc = _CONC_MODE_DETAIL[mode_key]

    conc_rows = [
        ("Concurrency Mode",          mode_name),
        ("Description",               mode_desc),
        ("Virtual Users",             users),
        ("Ramp-up Duration",          f"{ramp} s" if ramp else "None (all users start instantly)"),
        ("Ramp-up User Cadence",      f"1 user every ~{_ramp_cadence_ms(cfg):.0f} ms"
                                      if ramp > 0 else "N/A"),
        ("Batch Size",                batch_size if batch_size else "N/A (flat mode)"),
        ("Number of Batches",         math.ceil(users / batch_size) if batch_size else "N/A"),
        ("Result Delivery",           _result_delivery_label(cfg)),
        ("Flask Stack Mode",          "Yes — Flask API → Redis → worker → Judge0 → SSE"
                                      if cfg.get("mode") == "flask_stack"
                                      else "No — Direct Judge0 submissions (no Flask/Redis)"),
        ("Solution Mix",              "Random (accepted + wrong_answer + TLE + error + syntax)"
                                      if not cfg.get("accept_only") else "Accepted only (100% pass target)"),
        ("Dry Run",                   cfg.get("dry_run", False)),
    ]

    for i, (k, v) in enumerate(conc_rows):
        bg = _C["alt_row"] if i % 2 else _C["white"]
        c1 = ws.cell(row=row, column=1, value=k)
        _style(c1, bg=bg, bold=True)
        c2 = ws.cell(row=row, column=2, value=v)
        _style(c2, bg=bg)
        ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=NCOLS)
        row += 1
    row += 1

    # ── Section: Score Distribution ───────────────────────────────────────────
    full_marks, partial_marks, zero_marks = _score_distribution(all_results)

    _write_section(ws, row, "SCORE DISTRIBUTION", NCOLS)
    row += 1

    for ci, h in enumerate(["Category", "Count", "% of Completed",
                              "Description", "", ""], 1):
        bg = _C["hdr_light"] if h else _C["white"]
        _write_col_header(ws, row, ci, h, bg=bg, fg=_C["hdr_dark"])
    row += 1

    dist_rows = [
        ("Full Marks  (100%)",   full_marks,    _C["pass"],    _C["pass_fg"],
         "Score == Total TCs — all test cases passed"),
        ("Partial Marks  (1–99%)", partial_marks, _C["partial"], _C["partial_fg"],
         "1 ≤ Score < Total TCs — some test cases passed"),
        ("Zero Marks  (0%)",     zero_marks,    _C["fail"],    _C["fail_fg"],
         "Score == 0 — no test cases passed (or blocked/error before any TC ran)"),
    ]

    for label, count, bg, fg, desc in dist_rows:
        c1 = ws.cell(row=row, column=1, value=label)
        _style(c1, bg=bg, fg=fg, bold=True)
        c2 = ws.cell(row=row, column=2, value=count)
        _style(c2, bg=bg, fg=fg, halign="center")
        c3 = ws.cell(row=row, column=3, value=_pct(count, completed))
        _style(c3, bg=bg, fg=fg, halign="center")
        c4 = ws.cell(row=row, column=4, value=desc)
        _style(c4, bg=_C["alt_row"] if dist_rows.index((label,count,bg,fg,desc)) % 2 else _C["white"])
        ws.merge_cells(start_row=row, start_column=4, end_row=row, end_column=NCOLS)
        row += 1

    # Accepted-solutions-only analysis
    accept_count = sum(type_counts.values()) if type_counts else 0
    if cfg.get("accept_only") or (accept_count > 0 and all(t == "accepted" for t in type_counts)):
        c = ws.cell(row=row, column=1,
                    value="NOTE: Accept-only run — solution mix limited to 'accepted' solutions; "
                          "expected PASS rate ≈ 100% (unless infra error).")
        _style(c, bg=_C["tle"], bold=True)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=NCOLS)
        row += 1

    row += 1

    # ── Section: Idempotency / Cache Analysis (only if duplicates exist) ──────
    dup_count = summary.get("duplicate_idempotent", 0)
    if dup_count > 0:
        _write_section(ws, row, "IDEMPOTENCY / WARM-CACHE ANALYSIS", NCOLS)
        row += 1

        dup_note = ws.cell(row=row, column=1, value=(
            f"This run had {dup_count} duplicate (idempotent) submissions "
            f"({_pct(dup_count, total_subs)} of total). "
            "A duplicate occurs when the same student_id + assessment_id + SHA-256(code) "
            "is re-submitted within the TTL window (2h). Flask returns HTTP 200 immediately "
            "from Redis — no re-grading occurs. Latency for duplicates reflects only "
            "Redis lookup + SSE stream time, not Judge0 execution time."
        ))
        _style(dup_note, bg=_C["alt_row"], size=10)
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=NCOLS)
        ws.row_dimensions[row].height = 30
        row += 1

        idem_rows = [
            ("Total Submissions",          total_subs),
            ("Duplicate (idempotent) hits",dup_count),
            ("Duplicate %",               _pct(dup_count, total_subs)),
            ("Fresh submissions",          total_subs - dup_count),
            ("Redis key TTL",              "2 h  (SHA-256 of student_id:assessment_id:code_hash)"),
            ("Cache scenario",             "Warm-cache: same code re-submitted within TTL window"),
        ]
        for i, (k, v) in enumerate(idem_rows):
            bg = _C["alt_row"] if i % 2 else _C["white"]
            c1 = ws.cell(row=row, column=1, value=k); _style(c1, bg=bg, bold=True)
            c2 = ws.cell(row=row, column=2, value=v); _style(c2, bg=bg)
            ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=NCOLS)
            row += 1
        row += 1

    # ── Section: Live System Metrics ──────────────────────────────────────────
    if metrics_data:
        _METRICS_COLS = [
            ("timestamp",          "Timestamp"),
            ("cpu_pct",            "CPU %"),
            ("mem_pct",            "RAM %"),
            ("mem_used_gb",        "RAM Used (GB)"),
            ("mem_avail_gb",       "RAM Avail (GB)"),
            ("disk_read_mb_s",     "Disk Read (MB/s)"),
            ("disk_write_mb_s",    "Disk Write (MB/s)"),
            ("net_sent_mb_s",      "Net TX (MB/s)"),
            ("net_recv_mb_s",      "Net RX (MB/s)"),
            ("tcp_established",    "TCP Connections"),
            ("redis_clients",      "Redis Clients"),
            ("redis_mem_used_mb",  "Redis Mem (MB)"),
            ("redis_queue_depth",  "Redis Queue Depth"),
        ]
        present = [
            (k, h) for k, h in _METRICS_COLS
            if any(k in m for m in metrics_data)
        ]
        ncols_m = len(present)

        _write_section(ws, row, "LIVE SYSTEM METRICS (captured during test)", ncols_m)
        row += 1

        # Column headers
        for ci, (k, h) in enumerate(present, 1):
            _write_col_header(ws, row, ci, h, bg=_C["hdr_light"], fg=_C["hdr_dark"])
        row += 1

        for i, m in enumerate(metrics_data):
            bg = _C["alt_row"] if i % 2 else _C["white"]
            for ci, (k, _h) in enumerate(present, 1):
                val = m.get(k, "")
                c = ws.cell(row=row, column=ci, value=val)
                # Severity colouring for CPU and RAM
                cell_bg = bg
                if k == "cpu_pct" and isinstance(val, (int, float)):
                    cell_bg = _C["cpu_crit"] if val >= 90 else (_C["cpu_warn"] if val >= 70 else _C["cpu_ok"])
                elif k == "mem_pct" and isinstance(val, (int, float)):
                    cell_bg = _C["cpu_crit"] if val >= 90 else (_C["cpu_warn"] if val >= 75 else _C["cpu_ok"])
                elif k == "redis_queue_depth" and isinstance(val, int) and val > 150:
                    cell_bg = _C["tle"]
                _style(c, bg=cell_bg)
            row += 1

        # Summary stats row at the bottom
        row += 1
        _write_section(ws, row, "Live Metrics Summary", ncols_m)
        row += 1
        for ci, (k, h) in enumerate(present, 1):
            vals = [m[k] for m in metrics_data if k in m and isinstance(m[k], (int, float))]
            if vals:
                summary_val = (
                    f"avg={sum(vals)/len(vals):.1f}  max={max(vals):.1f}  min={min(vals):.1f}"
                )
            else:
                summary_val = "–"
            c = ws.cell(row=row, column=ci, value=summary_val if ci > 1 else "Summary (avg/max/min)")
            _style(c, bg=_C["light_grey"], bold=(ci == 1), size=9)
        row += 1

    # ── Column widths ─────────────────────────────────────────────────────────
    _set_col_widths(ws, [30, 18, 14, 40, 18, 18])
    ws.freeze_panes = ws.cell(row=3, column=1)


# ─────────────────────────────────────────────────────────────────────────────
#  Sheet 3 — User Test Case Results
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet3(wb: openpyxl.Workbook, report: dict):
    ws = wb.create_sheet("User Test Case Results")
    ws.sheet_view.showGridLines = False

    per_problem = report.get("per_problem_report", [])

    # Flatten all submissions keeping the problem_id context
    all_subs: list[dict] = []
    max_tcs = 0
    for prob_rpt in per_problem:
        pid  = prob_rpt["problem_id"]
        for sub in prob_rpt.get("submissions", []):
            entry = dict(sub)
            entry["_problem_id"] = pid
            all_subs.append(entry)
            max_tcs = max(max_tcs, len(sub.get("test_cases", [])))

    # ── Column layout ─────────────────────────────────────────────────────────
    STATIC = [
        "User ID",
        "Problem ID",
        "Solution ID",
        "Solution Type",
        "Harness Status",
        "Score",
        "Total TCs",
        "Marks %",
        "Partial?",
        "Latency (ms)",
        "Error / Notes",
        "Submitted Code",   # source_code from question bank / JSON report
    ]
    N_STATIC = len(STATIC)       # 12
    TC_COLS  = 3                 # Status | Got | Expected per TC
    TOTAL_COLS = N_STATIC + max_tcs * TC_COLS

    # ── Title ─────────────────────────────────────────────────────────────────
    _write_title(ws, 1, "USER TEST CASE RESULTS & MARKS", TOTAL_COLS, fontsize=14)
    ws.row_dimensions[1].height = 26

    # ── Sub-header: marks legend ──────────────────────────────────────────────
    legend_vals = [
        "Marks Legend:  ",
        "Full marks (100%)",   "",
        "Partial marks (1–99%)", "",
        "No marks (0%)",       "",
        "Score = TCs passed / Total TCs",
    ]
    for ci, val in enumerate(legend_vals, 1):
        c = ws.cell(row=2, column=ci, value=val)
        bg = (
            _C["pass"]    if ci == 2 else
            _C["partial"] if ci == 4 else
            _C["fail"]    if ci == 6 else
            _C["light_grey"]
        )
        _style(c, bg=bg, size=9, bold=(ci in (2, 4, 6)))
    # fill remaining legend cells
    for ci in range(len(legend_vals) + 1, TOTAL_COLS + 1):
        ws.cell(row=2, column=ci).fill = _fill(_C["light_grey"])
    ws.row_dimensions[2].height = 16

    # ── Per-user aggregated marks summary ─────────────────────────────────────
    # Aggregate: for each unique user_id, sum score and total_tcs across all submissions
    user_agg: dict = {}
    for s in all_subs:
        uid = s.get("user_id", "?")
        if uid not in user_agg:
            user_agg[uid] = {
                "score": 0, "total_tcs": 0, "submissions": 0,
                "problems": set(), "statuses": [],
            }
        user_agg[uid]["score"]       += s.get("score", 0)
        user_agg[uid]["total_tcs"]   += s.get("total_tcs", 0)
        user_agg[uid]["submissions"] += 1
        user_agg[uid]["problems"].add(s.get("_problem_id", ""))
        user_agg[uid]["statuses"].append(s.get("harness_status", ""))

    # Section header
    _write_title(ws, 3, "PER-USER MARKS SUMMARY", min(8, TOTAL_COLS), fontsize=12)

    # Column headers for summary
    SUMM_COLS = ["User ID", "Submissions", "Problems Attempted",
                 "Total Score", "Total TCs", "Marks %", "Partial?", "Dominant Status"]
    for ci, h in enumerate(SUMM_COLS, 1):
        _write_col_header(ws, 4, ci, h, bg=_C["hdr_light"], fg=_C["hdr_dark"])
    # Fill remaining columns empty
    for ci in range(len(SUMM_COLS) + 1, TOTAL_COLS + 1):
        c = ws.cell(row=4, column=ci, value="")
        c.fill = _fill(_C["hdr_light"])

    summary_start_data = 5
    for i, (uid, agg) in enumerate(sorted(user_agg.items())):
        data_row = summary_start_data + i
        score    = agg["score"]
        total    = agg["total_tcs"] or 1
        pct      = score / total * 100
        partial  = (0 < score < total)

        if pct >= 100:
            mb, mf = _C["pass"],    _C["pass_fg"]
        elif pct > 0:
            mb, mf = _C["partial"], _C["partial_fg"]
        else:
            mb, mf = _C["fail"],    _C["fail_fg"]

        # Dominant status
        from collections import Counter
        status_ctr = Counter(agg["statuses"])
        dominant   = status_ctr.most_common(1)[0][0] if status_ctr else "–"

        alt_bg = _C["alt_row"] if i % 2 else _C["white"]
        row_vals = [
            uid,
            agg["submissions"],
            len(agg["problems"]),
            score,
            agg["total_tcs"],
            f"{pct:.1f}%",
            "Yes" if partial else "No",
            dominant,
        ]
        for ci, val in enumerate(row_vals, 1):
            c = ws.cell(row=data_row, column=ci, value=val)
            if ci == 6:   # Marks %
                _style(c, bg=mb, fg=mf, bold=True, halign="center")
            elif ci == 7:  # Partial?
                _style(c, bg=mb if partial else alt_bg,
                       fg=mf if partial else "000000", halign="center")
            elif ci == 8:  # Status
                sbg, sfg = _STATUS_STYLE.get(dominant, (_C["unknown"], "000000"))
                _style(c, bg=sbg, fg=sfg, bold=True, halign="center")
            else:
                _style(c, bg=alt_bg, halign="center" if ci == 1 else "left")
        # Fill remaining cols with empty styled cells
        for ci in range(len(row_vals) + 1, TOTAL_COLS + 1):
            c = ws.cell(row=data_row, column=ci, value="")
            _style(c, bg=alt_bg)

    user_summary_rows = len(user_agg)
    # Totals row for the summary
    tot_row = summary_start_data + user_summary_rows
    total_score_all = sum(a["score"]     for a in user_agg.values())
    total_tc_all    = sum(a["total_tcs"] for a in user_agg.values())
    full_u   = sum(1 for a in user_agg.values() if a["score"] >= (a["total_tcs"] or 1))
    part_u   = sum(1 for a in user_agg.values()
                   if 0 < a["score"] < (a["total_tcs"] or 1))
    zero_u   = sum(1 for a in user_agg.values() if a["score"] == 0)
    for ci, val in enumerate([
        "TOTALS", len(user_agg), "", total_score_all, total_tc_all,
        _pct(total_score_all, total_tc_all or 1),
        f"{part_u} partial",
        f"Full={full_u}  Partial={part_u}  Zero={zero_u}",
    ], 1):
        c = ws.cell(row=tot_row, column=ci, value=val)
        _style(c, bg=_C["hdr_light"], bold=True, fg=_C["hdr_dark"])
    for ci in range(len(SUMM_COLS) + 1, TOTAL_COLS + 1):
        ws.cell(row=tot_row, column=ci).fill = _fill(_C["hdr_light"])

    # Blank separator row then the per-submission detail title
    sep_row   = tot_row + 1
    ws.row_dimensions[sep_row].height = 8

    detail_title_row = sep_row + 1
    _write_title(ws, detail_title_row, "PER-SUBMISSION DETAIL", TOTAL_COLS, fontsize=12)

    # ── Column headers row (for per-submission table) ─────────────────────────
    hdr_row = detail_title_row + 1

    for ci, h in enumerate(STATIC, 1):
        _write_col_header(ws, hdr_row, ci, h, bg=_C["hdr_mid"], fg="FFFFFF")

    for tc_i in range(1, max_tcs + 1):
        col = N_STATIC + (tc_i - 1) * TC_COLS + 1
        _write_col_header(ws, hdr_row, col,     f"TC{tc_i}  Status",   bg=_C["hdr_light"], fg=_C["hdr_dark"])
        _write_col_header(ws, hdr_row, col + 1, f"TC{tc_i}  Got",      bg=_C["hdr_light"], fg=_C["hdr_dark"])
        _write_col_header(ws, hdr_row, col + 2, f"TC{tc_i}  Expected", bg=_C["hdr_light"], fg=_C["hdr_dark"])

    ws.row_dimensions[hdr_row].height = 18

    # ── Data rows ─────────────────────────────────────────────────────────────
    detail_data_start = hdr_row + 1
    for i, sub in enumerate(all_subs):
        data_row = detail_data_start + i
        alt_bg   = _C["alt_row"] if i % 2 else _C["white"]

        harness_status = sub.get("harness_status", "UNKNOWN")
        score          = sub.get("score", 0)
        total_tcs      = sub.get("total_tcs", 0)
        marks_pct      = (score / total_tcs * 100) if total_tcs > 0 else 0.0
        partial        = (0 < score < total_tcs)
        latency        = sub.get("latency_ms", "")
        error_note     = sub.get("error", "") or sub.get("judge0_status", "")
        source_code    = sub.get("source_code", "")

        static_vals = [
            sub.get("user_id", ""),
            sub.get("_problem_id", ""),
            sub.get("solution_id", ""),
            sub.get("solution_type", ""),
            harness_status,
            score,
            total_tcs,
            f"{marks_pct:.1f}%",
            "Yes" if partial else "No",
            latency,
            error_note,
            source_code,         # col 12 — Submitted Code
        ]

        for ci, val in enumerate(static_vals, 1):
            c = ws.cell(row=data_row, column=ci, value=val)
            # Default styling
            _style(c, bg=alt_bg)

            # Harness status cell (col 5)
            if ci == 5:
                bg_s, fg_s = _STATUS_STYLE.get(harness_status, (_C["unknown"], "000000"))
                _style(c, bg=bg_s, fg=fg_s, bold=True, halign="center")

            # Marks % cell (col 8)
            elif ci == 8:
                if marks_pct >= 100:
                    mb, mf = _C["pass"], _C["pass_fg"]
                elif marks_pct == 0:
                    mb, mf = _C["fail"], _C["fail_fg"]
                else:
                    mb, mf = _C["partial"], _C["partial_fg"]
                _style(c, bg=mb, fg=mf, bold=True, halign="center")

            # Partial? (col 9)
            elif ci == 9:
                if partial:
                    _style(c, bg=_C["partial"], fg=_C["partial_fg"], bold=True, halign="center")
                else:
                    _style(c, bg=alt_bg, halign="center")

            # Submitted Code (col 12) — fixed-height, monospaced font, top-aligned
            elif ci == 12:
                c.font      = Font(name="Courier New", size=8, color="1F1F1F")
                c.fill      = _fill(_C["light_grey"])
                c.border    = _thin_border()
                c.alignment = Alignment(horizontal="left", vertical="top",
                                        wrap_text=True)
                ws.row_dimensions[data_row].height = max(
                    ws.row_dimensions[data_row].height or 15,
                    min(120, 13 + source_code.count("\n") * 10)
                )

            # User ID / latency — centre
            elif ci in (1, 6, 7, 10):
                c.alignment = _center_align(wrap=False)

        # ── TC columns ────────────────────────────────────────────────────────
        test_cases = sub.get("test_cases", [])
        for tc_idx, tc in enumerate(test_cases):
            col    = N_STATIC + tc_idx * TC_COLS + 1
            status = tc.get("status", "")
            got    = tc.get("got", "") or tc.get("detail", "")
            exp    = tc.get("expected", "")

            tc_bg, tc_fg = _STATUS_STYLE.get(status, (_C["unknown"], "000000"))

            # Status cell — coloured
            cs = ws.cell(row=data_row, column=col, value=status)
            _style(cs, bg=tc_bg, fg=tc_fg, bold=True, halign="center")

            # Got cell
            cg = ws.cell(row=data_row, column=col + 1, value=got)
            _style(cg, bg=alt_bg, size=9)

            # Expected cell
            ce = ws.cell(row=data_row, column=col + 2, value=exp)
            _style(ce, bg=alt_bg, size=9)

        # Fill remaining TC columns with empty styled cells
        filled_tcs = len(test_cases)
        for tc_idx in range(filled_tcs, max_tcs):
            col = N_STATIC + tc_idx * TC_COLS + 1
            for offset in range(TC_COLS):
                c = ws.cell(row=data_row, column=col + offset, value="")
                _style(c, bg=alt_bg)

    # ── Summary row at the bottom ─────────────────────────────────────────────
    if all_subs:
        summary_row = detail_data_start + len(all_subs)
        ws.row_dimensions[summary_row].height = 16
        totals = [
            "TOTALS",
            f"{len(set(s['_problem_id'] for s in all_subs))} problems",
            "",
            "",
            "",
            sum(s.get("score", 0) for s in all_subs),
            sum(s.get("total_tcs", 0) for s in all_subs),
            _pct(
                sum(s.get("score", 0) for s in all_subs),
                sum(s.get("total_tcs", 0) for s in all_subs) or 1
            ),
            f"{sum(1 for s in all_subs if 0 < s.get('score',0) < s.get('total_tcs',1))} partial",
            "",
            f"{len(all_subs)} total submissions",
        ]
        for ci, val in enumerate(totals, 1):
            c = ws.cell(row=summary_row, column=ci, value=val)
            _style(c, bg=_C["hdr_light"], bold=True, fg=_C["hdr_dark"])

    # ── Column widths ─────────────────────────────────────────────────────────
    static_widths = [8, 18, 30, 22, 16, 8, 10, 10, 10, 12, 35, 55]
    _set_col_widths(ws, static_widths)
    for tc_i in range(max_tcs):
        base = N_STATIC + tc_i * TC_COLS + 1
        ws.column_dimensions[get_column_letter(base    )].width = 11  # Status
        ws.column_dimensions[get_column_letter(base + 1)].width = 18  # Got
        ws.column_dimensions[get_column_letter(base + 2)].width = 14  # Expected

    # Freeze row 1 (title) and columns A+B (User ID / Problem ID) so the sheet
    # scrolls freely in both directions regardless of how many users are in the
    # summary section above the detail table.
    ws.freeze_panes = "C2"


# ─────────────────────────────────────────────────────────────────────────────
#  Sheet 4 — Scenario Comparison
# ─────────────────────────────────────────────────────────────────────────────

def build_sheet4_comparison(wb: openpyxl.Workbook, reports: list):
    """
    reports: list of (filename, report_dict) tuples — one per JSON file.
    Primary report is index 0; comparison runs follow.
    """
    ws = wb.create_sheet("Scenario Comparison")
    ws.sheet_view.showGridLines = False

    n_reports = len(reports)
    # Metric column + one column per report
    NCOLS = 1 + n_reports

    _write_title(ws, 1, "MULTI-RUN SCENARIO COMPARISON", NCOLS, fontsize=14)

    ts = _now_utc()
    c = ws.cell(row=2, column=1, value=f"Generated: {ts} UTC  |  {n_reports} run(s) compared")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=NCOLS)
    _style(c, bg=_C["light_grey"], size=10, halign="left")

    row = 3

    # ── Report File Headers ────────────────────────────────────────────────────
    c0 = ws.cell(row=row, column=1, value="Metric")
    _style(c0, bg=_C["hdr_mid"], fg="FFFFFF", bold=True, halign="center")

    for ri, (fname, rpt) in enumerate(reports, 2):
        cfg_r = rpt.get("config", {})
        label = f"{Path(fname).name}\n({_scenario_label(cfg_r)})"
        c = ws.cell(row=row, column=ri, value=label)
        _style(c, bg=_C["hdr_mid"], fg="FFFFFF", bold=True, halign="center")
        ws.row_dimensions[row].height = 32
    row += 1

    # ── Helper to write a section divider spanning all cols ───────────────────
    def section(title: str):
        nonlocal row
        _write_section(ws, row, title, NCOLS)
        row += 1

    # ── Helper to write one metric row ────────────────────────────────────────
    def metric_row(label: str, values: list, highlight_best: bool = False,
                   lower_is_better: bool = False, fmt=None):
        nonlocal row
        alt = (row % 2 == 0)
        bg  = _C["alt_row"] if alt else _C["white"]

        c0 = ws.cell(row=row, column=1, value=label)
        _style(c0, bg=bg, bold=True)

        # Find best value for optional highlighting
        numeric_vals = [v for v in values if isinstance(v, (int, float))]
        best = None
        if highlight_best and numeric_vals:
            best = min(numeric_vals) if lower_is_better else max(numeric_vals)

        for ri, val in enumerate(values, 2):
            disp = fmt(val) if fmt and isinstance(val, (int, float)) else val
            c = ws.cell(row=row, column=ri, value=disp)
            cell_bg = bg
            if highlight_best and val == best and isinstance(val, (int, float)):
                cell_bg = _C["pass"]
            _style(c, bg=cell_bg, halign="center")
        row += 1

    # ── Section: Run Configuration ────────────────────────────────────────────
    section("RUN CONFIGURATION")

    def cfg_vals(key, default="–"):
        return [r.get("config", {}).get(key, default) for _, r in reports]

    metric_row("Target URL",           cfg_vals("target"))
    metric_row("Mode",                 cfg_vals("mode"))
    metric_row("Virtual Users",        [r.get("config",{}).get("users",0) for _,r in reports],
               highlight_best=True)
    metric_row("Ramp-up (s)",          [r.get("config",{}).get("ramp_up_sec",0) for _,r in reports])
    metric_row("Batch Size",           [r.get("config",{}).get("batch_size",0) or "flat" for _,r in reports])
    metric_row("Concurrency Mode",     [_concurrency_label(r.get("config",{})) for _,r in reports])
    metric_row("Dry Run",              [r.get("config",{}).get("dry_run",False) for _,r in reports])

    # ── Section: Infrastructure Configuration ─────────────────────────────────
    section("INFRASTRUCTURE CONFIGURATION")

    def _workers(rpt):
        return rpt.get("config", {}).get("workers", rpt.get("_workers", 3))

    def _runners(rpt):
        return rpt.get("config", {}).get("runners", rpt.get("_runners", 2))

    metric_row("Judge0 Workers (--scale workers=N)",
               [_workers(r) for _, r in reports],
               highlight_best=True)
    metric_row("MAX_RUNNERS per worker",
               [_runners(r) for _, r in reports],
               highlight_best=True)
    metric_row("Total Concurrent Sandboxes",
               [_workers(r) * _runners(r) for _, r in reports],
               highlight_best=True)
    metric_row("Puma HTTP slots (WEB_CONCURRENCY × THREADS)",
               [r.get("config", {}).get("puma_slots", "–") for _, r in reports])

    section("THROUGHPUT & DURATION")

    metric_row("Test Duration (s)",
               [round(_duration(r.get("summary",{})), 1) for _,r in reports],
               highlight_best=True, lower_is_better=True)
    metric_row("Throughput (req/s)",
               [round(r.get("summary",{}).get("throughput_per_sec",0), 2) for _,r in reports],
               highlight_best=True, lower_is_better=False)
    metric_row("Total Submissions",
               [r.get("summary",{}).get("total_submissions",0) for _,r in reports])
    metric_row("Completed",
               [r.get("summary",{}).get("completed",0) for _,r in reports])
    metric_row("Peak In-Flight Jobs",
               [r.get("summary",{}).get("peak_in_flight_jobs",0) for _,r in reports],
               highlight_best=True, lower_is_better=True)

    # ── Section: Latency ──────────────────────────────────────────────────────
    section("LATENCY  (milliseconds)")

    def lat_vals(key):
        return [r.get("summary",{}).get(key, 0) for _,r in reports]

    def lat_from_results(pct_fn):
        out = []
        for _, rpt in reports:
            lats = sorted(
                res.get("latency_ms", 0)
                for res in rpt.get("results", [])
                if "latency_ms" in res
            )
            out.append(pct_fn(lats) if lats else 0)
        return out

    def _pct_val(lats, p):
        if not lats: return 0
        idx = max(0, int(math.ceil(len(lats) * p / 100)) - 1)
        return lats[idx]

    metric_row("P50  (median)",  lat_from_results(lambda l: _pct_val(l, 50)),
               highlight_best=True, lower_is_better=True)
    metric_row("P75",            lat_from_results(lambda l: _pct_val(l, 75)),
               highlight_best=True, lower_is_better=True)
    metric_row("P90",            lat_from_results(lambda l: _pct_val(l, 90)),
               highlight_best=True, lower_is_better=True)
    metric_row("P95",            lat_vals("latency_p95_ms"),
               highlight_best=True, lower_is_better=True)
    metric_row("P99",            lat_vals("latency_p99_ms"),
               highlight_best=True, lower_is_better=True)
    metric_row("P99.9",          lat_from_results(lambda l: _pct_val(l, 99.9)),
               highlight_best=True, lower_is_better=True)
    metric_row("Average",
               [round(sum(res.get("latency_ms",0) for res in rpt.get("results",[]))
                      / max(len(rpt.get("results",[])), 1), 1)
                for _, rpt in reports],
               highlight_best=True, lower_is_better=True)
    metric_row("Max",
               [max((res.get("latency_ms",0) for res in rpt.get("results",[])), default=0)
                for _, rpt in reports],
               highlight_best=True, lower_is_better=True)

    # ── Section: Outcomes ─────────────────────────────────────────────────────
    section("SUBMISSION OUTCOMES")

    def status_vals(status):
        return [r.get("status_counts",{}).get(status, 0) for _,r in reports]

    def status_pct_vals(status):
        return [
            _pct(r.get("status_counts",{}).get(status,0),
                 r.get("summary",{}).get("completed",1))
            for _,r in reports
        ]

    for status in ["PASS","FAIL","TLE","ERROR","BLOCKED","RATE_LIMITED","SYSTEM_ERROR","JUDGE0_ERROR"]:
        any_present = any(r.get("status_counts",{}).get(status,0) > 0 for _,r in reports)
        if any_present:
            metric_row(f"{status}  (count)", status_vals(status))
            metric_row(f"{status}  (%)",     status_pct_vals(status))

    # ── Section: Admission Control / Idempotency ──────────────────────────────
    section("ADMISSION CONTROL & IDEMPOTENCY")

    metric_row("Rate Limited (HTTP 429)",
               [r.get("summary",{}).get("rate_limited_429",0) for _,r in reports],
               highlight_best=True, lower_is_better=True)
    metric_row("Rate Limited %",
               [_pct(r.get("summary",{}).get("rate_limited_429",0),
                     r.get("summary",{}).get("total_submissions",1))
                for _,r in reports])
    metric_row("Duplicate (idempotent)",
               [r.get("summary",{}).get("duplicate_idempotent",0) for _,r in reports])
    metric_row("Duplicate %",
               [_pct(r.get("summary",{}).get("duplicate_idempotent",0),
                     r.get("summary",{}).get("total_submissions",1))
                for _,r in reports])
    metric_row("System Errors (infra)",
               [r.get("summary",{}).get("system_errors",0) for _,r in reports],
               highlight_best=True, lower_is_better=True)

    # ── Section: Score Distribution ───────────────────────────────────────────
    section("SCORE DISTRIBUTION")

    def score_dist_col(key_idx):
        out = []
        for _, rpt in reports:
            all_r = rpt.get("results", [])
            f, p, z = _score_distribution(all_r)
            out.append([f, p, z][key_idx])
        return out

    completed_list = [r.get("summary",{}).get("completed",1) for _,r in reports]
    metric_row("Full Marks  (100%)",    score_dist_col(0), highlight_best=True)
    metric_row("Full Marks  % of completed",
               [_pct(score_dist_col(0)[i], completed_list[i]) for i in range(n_reports)])
    metric_row("Partial Marks  (1–99%)", score_dist_col(1))
    metric_row("Zero Marks  (0%)",      score_dist_col(2), highlight_best=True, lower_is_better=True)

    # ── Section: Infrastructure Capacity ──────────────────────────────────────
    section("INFRASTRUCTURE CAPACITY")

    metric_row("Harness Jobs (actual)",
               [r.get("summary",{}).get("harness_jobs",0) for _,r in reports])
    metric_row("Batch-Equivalent Jobs",
               [r.get("summary",{}).get("batch_equivalent",0) for _,r in reports])
    metric_row("Queue Reduction Factor",
               [r.get("summary",{}).get("queue_reduction","N/A") for _,r in reports])

    # ── Column widths ─────────────────────────────────────────────────────────
    ws.column_dimensions["A"].width = 32
    col_w = max(20, int(90 / n_reports))
    for ri in range(2, n_reports + 2):
        ws.column_dimensions[get_column_letter(ri)].width = col_w

    ws.freeze_panes = "B4"


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate EC2 load-test Excel report from load_test_report.json",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "report",
        nargs="?",
        default="load_test_report.json",
        help="Path to load_test_report.json produced by the Go load tester",
    )
    parser.add_argument(
        "--metrics",
        default=None,
        metavar="FILE",
        help="Path to JSONL metrics file from collect_ec2_metrics.py",
    )
    parser.add_argument(
        "--out",
        default=None,
        metavar="FILE",
        help="Output .xlsx path (default: same stem as report)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Judge0 worker containers (--scale workers=N). "
             "Auto-read from the JSON report if not specified.",
    )
    parser.add_argument(
        "--runners",
        type=int,
        default=None,
        help="MAX_RUNNERS per worker container. "
             "Auto-read from the JSON report if not specified.",
    )
    parser.add_argument(
        "--compare",
        nargs="*",
        metavar="FILE",
        default=None,
        help=(
            "Add Sheet 4: multi-run scenario comparison. "
            "Pass specific JSON files to compare, or omit filenames to "
            "auto-detect all load_test_report*.json in the same directory."
        ),
    )
    parser.add_argument(
        "--all",
        nargs="+",
        metavar="FILE",
        default=None,
        help=(
            "All-files mode: pass every JSON file to compare. No 'primary' "
            "designation — all files appear equally in Sheet 4. "
            "The first file is used for Sheets 1-3 as a representative sample. "
            "Overrides the positional 'report' argument and --compare."
        ),
    )
    args = parser.parse_args()

    # ── --all mode: first file becomes primary, all go into Sheet 4 ──────────
    if args.all:
        args.report = args.all[0]
        # We'll handle compare_reports directly in the --all block below

    # ── Load primary report ───────────────────────────────────────────────────
    report_path = Path(args.report)
    if not report_path.exists():
        print(f"ERROR: Report file not found: {report_path}")
        sys.exit(1)

    print(f"Reading report  : {report_path}")
    with open(report_path, encoding="utf-8") as fh:
        report = json.load(fh)

    # workers/runners: CLI flag overrides JSON config; JSON config overrides default.
    cfg_block = report.get("config", {})
    workers = args.workers if args.workers is not None else cfg_block.get("workers", 3)
    runners = args.runners if args.runners is not None else cfg_block.get("runners", 2)
    report["_workers"] = workers
    report["_runners"] = runners
    print(f"Workers         : {workers}  runners/worker: {runners}  (sandboxes: {workers*runners})")

    # ── Load optional live metrics ────────────────────────────────────────────
    metrics_data: list[dict] = []
    if args.metrics:
        mpath = Path(args.metrics)
        if mpath.exists():
            print(f"Reading metrics : {mpath}")
            with open(mpath, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            metrics_data.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            print(f"  {len(metrics_data)} metric snapshots loaded")
        else:
            print(f"WARNING: Metrics file not found: {mpath}  (continuing without)")

    # ── Resolve comparison reports ────────────────────────────────────────────
    compare_reports: list = []   # list of (filename_str, report_dict)

    if args.all:
        # --all mode: load every file; first is already loaded as primary above
        for fpath_str in args.all[1:]:
            cpath = Path(fpath_str)
            if not cpath.exists():
                print(f"WARNING: File not found: {cpath}  (skipping)")
                continue
            try:
                with open(cpath, encoding="utf-8") as fh:
                    crpt = json.load(fh)
                compare_reports.append((str(cpath), crpt))
                print(f"  File : {cpath}")
            except Exception as exc:
                print(f"WARNING: Could not read {cpath}: {exc}  (skipping)")
    elif args.compare is not None:
        # --compare with explicit files
        if args.compare:
            compare_files = [Path(f) for f in args.compare]
        else:
            # Auto-detect all load_test_report*.json in same directory
            compare_files = sorted(
                report_path.parent.glob("load_test_report*.json")
            )

        for cpath in compare_files:
            if not cpath.exists():
                print(f"WARNING: Comparison file not found: {cpath}  (skipping)")
                continue
            try:
                with open(cpath, encoding="utf-8") as fh:
                    crpt = json.load(fh)
                compare_reports.append((str(cpath), crpt))
                print(f"  Comparison file : {cpath}")
            except Exception as exc:
                print(f"WARNING: Could not read {cpath}: {exc}  (skipping)")

        if not compare_reports:
            print("WARNING: No valid comparison files found; Sheet 4 will be skipped.")

    # ── Output path ───────────────────────────────────────────────────────────
    out_path = Path(args.out) if args.out else report_path.with_suffix(".xlsx")

    # ── Build workbook ────────────────────────────────────────────────────────
    print("Building Excel workbook…")
    wb = openpyxl.Workbook()

    build_sheet1(wb, report, workers=workers, runners=runners)
    build_sheet2(wb, report, metrics_data)
    build_sheet3(wb, report)

    if compare_reports:
        # All files go into Sheet 4 equally; primary is always listed first
        all_for_compare = [(str(report_path), report)] + [
            (f, r) for f, r in compare_reports if str(f) != str(report_path)
        ]
        build_sheet4_comparison(wb, all_for_compare)
        print(f"  Sheet 4  — Scenario Comparison  ({len(all_for_compare)} runs)")

    wb.active = wb.worksheets[0]
    wb.save(out_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    n_subs = len(report.get("results", []))
    if not n_subs:
        n_subs = sum(
            len(p.get("submissions", []))
            for p in report.get("per_problem_report", [])
        )

    print(f"\n✓  Report saved : {out_path}")
    print(f"   Sheet 1  — System Info")
    print(f"   Sheet 2  — Load Test Metrics"
          f"  ({len(metrics_data)} live-metric snapshots)")
    print(f"   Sheet 3  — User Test Case Results  ({n_subs} submissions)")


if __name__ == "__main__":
    main()
