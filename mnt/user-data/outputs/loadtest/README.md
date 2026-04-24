# Judge0 Harness Load Tester

## Build

```bash
go build -o loadtester .
```

## Run — Dry Run (no Judge0 needed)

```bash
./loadtester -dryrun=true -users=500 -ramp=10
```

## Run — Against Real Judge0

```bash
./loadtester \
  -url   http://your-judge0-host:2358 \
  -key   your-api-key \
  -users 500 \
  -ramp  10 \
  -poll  500ms \
  -out   report.json
```

## All Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-url` | `http://localhost:2358` | Judge0 base URL |
| `-key` | `` | Judge0 API key (`X-Auth-Token`) |
| `-users` | `500` | Number of concurrent virtual users |
| `-ramp` | `10` | Ramp-up seconds (users spread over this window) |
| `-bank` | `question_bank.json` | Path to question bank |
| `-poll` | `500ms` | How often to poll Judge0 for result |
| `-maxpoll` | `120` | Max seconds to wait for one result before timeout |
| `-dryrun` | `false` | Build harnesses but skip Judge0 — for local validation |
| `-out` | `load_test_report.json` | JSON report output path |

## What Each Virtual User Does

```
1. Pick random problem (two_sum / fibonacci / lis)
2. Pick random solution (accepted / wrong / tle / syntax_error / runtime_error)
3. Run AST pre-check → BLOCKED if syntax_error type
4. Build Python stdio harness (1 job regardless of TC count)
5. Submit to Judge0 with global_limit = (per_tc_limit × N) + 5s overhead
6. Poll until result
7. Parse harness output with unique session delimiter
8. Record: status, per-TC breakdown, latency, score
```

## Metrics Output

- **Queue reduction**: 500 users × 4 TCs = 2000 batch jobs vs 500 harness jobs
- **Latency**: avg, p50, p95, p99, max
- **Status breakdown**: PASS / FAIL / TLE / ERROR / BLOCKED
- **Per-problem and per-solution-type breakdown**
- **Peak in-flight jobs**: max concurrent Judge0 submissions at any moment
- **JSON report**: full per-submission detail for analysis

## Question Bank Changes from Original

The original question bank used Judge0 native batch submissions.
This version adapts it for the harness approach:

| Original | Harness Version |
|----------|----------------|
| `stdin` per TC sent separately | `stdin_text` per TC baked into harness |
| `expected_output` compared by Judge0 | `expected` compared inside harness |
| `compile_error` = Judge0 status 6 | `syntax_error` = blocked by AST checker |
| N jobs per submission | 1 job per submission |
| `cpu_time_limit` = global | `per_tc_limit_s` = per-TC via signal.alarm |
| `memory_limit` = global | `memory_limit_mb` = per-TC tracked in harness |
