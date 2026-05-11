#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  run_load_tests.sh  — Full automated load testing: Phases A → E (17 runs)
#
#  Run ON the EC2 instance from the deploy directory:
#    cd /home/ubuntu/judge0
#    chmod +x run_load_tests.sh
#    ./run_load_tests.sh              # full run
#    ./run_load_tests.sh --phase A    # single phase only
#    ./run_load_tests.sh --resume     # skip scenarios whose JSON already exists
#    ./run_load_tests.sh --no-report  # skip per-run Excel generation
#
#  Phases:
#    A — Config A (workers=3, MAX_RUNNERS=2,  6 sandboxes)  6 scenarios
#    B — Config B (workers=3, MAX_RUNNERS=3,  9 sandboxes)  3 scenarios
#    C — Config C (workers=4, MAX_RUNNERS=2,  8 sandboxes)  3 scenarios
#    D — Config D (workers=4, MAX_RUNNERS=3, 12 sandboxes)  2 scenarios ⚠ RAM
#    E — Config E (workers=5, MAX_RUNNERS=2, 10 sandboxes)  3 scenarios
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✔ $*${RESET}"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠ $*${RESET}"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ✘ $*${RESET}"; exit 1; }
banner() {
  echo ""
  echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
  printf  "${BOLD}${CYAN}║  %-62s║${RESET}\n" "$*"
  echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
  echo ""
}

# ── Defaults ─────────────────────────────────────────────────────────────────
DEPLOY_DIR="/home/ubuntu/judge0"
FLASK_URL="http://localhost:5001"
BANK="question_bank_mixed.json"
METRICS_INTERVAL=5

# Cooldowns (seconds)
COOLDOWN_BETWEEN_RUNS=45       # wait between consecutive scenarios in same phase
COOLDOWN_PHASE_START=120       # wait after docker compose up — Resque workers need ~90s to register
COMPOSE_DOWN_WAIT=20           # wait after docker compose down

# Flags
ONLY_PHASE=""
RESUME=false
SKIP_EXCEL=false
LOG_FILE="run_load_tests_$(date '+%Y%m%d_%H%M%S').log"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase)      ONLY_PHASE="${2^^}"; shift 2 ;;
    --resume)     RESUME=true;         shift ;;
    --no-report)  SKIP_EXCEL=true;     shift ;;
    --flask-url)  FLASK_URL="$2";      shift 2 ;;
    --bank)       BANK="$2";           shift 2 ;;
    --help|-h)
      grep '^#  ' "$0" | sed 's/^#  //'
      exit 0
      ;;
    *) die "Unknown flag: $1" ;;
  esac
done

# ── Redirect all output to log + terminal ────────────────────────────────────
exec > >(tee -a "$LOG_FILE") 2>&1
log "Logging to $LOG_FILE"

# ── Pre-flight checks ─────────────────────────────────────────────────────────
[[ -d "$DEPLOY_DIR" ]] || die "Deploy dir $DEPLOY_DIR not found. Run deploy-ec2.sh first."
cd "$DEPLOY_DIR"

[[ -f ".env" ]]                    || die ".env file not found."
[[ -f "$BANK" ]]                   || die "Question bank '$BANK' not found."
[[ -f "docker-compose.ec2.yml" ]]  || die "docker-compose.ec2.yml not found."
command -v docker  &>/dev/null     || die "docker not found."
command -v python3 &>/dev/null     || die "python3 not found."
command -v go      &>/dev/null     || die "go not found."

mkdir -p Reports/Jsons

# ── Helper: wait for Flask health ────────────────────────────────────────────
# Usage: wait_healthy <expected_worker_count>
wait_healthy() {
  local expected_workers="${1:-1}"

  # 1. Flask API
  local url="$FLASK_URL/health"
  local deadline=$(( $(date +%s) + 120 ))
  log "Waiting for Flask health at $url ..."
  until curl -sf "$url" &>/dev/null; do
    [[ $(date +%s) -lt $deadline ]] || die "Flask did not become healthy within 120s"
    sleep 5
  done
  ok "Flask healthy"

  # 2. Judge0 /system_info — verify EXACT expected worker count is connected
  #    Resque workers take ~60-120s after container start to register.
  #    We wait up to 240s total. If workers never appear we ABORT — running
  #    a test with 0 workers produces 100% SYSTEM_ERROR and wastes the run.
  #    Judge0 server is always on port 2358 on the same host.
  local host_only="${FLASK_URL#http://}"   # strip scheme
  host_only="${host_only%%:*}"             # strip :port  →  "localhost" or IP
  local j0url="http://${host_only}:2358"
  deadline=$(( $(date +%s) + 240 ))
  log "Waiting for Judge0 to report $expected_workers worker(s) at $j0url (up to 240s) ..."
  until curl -sf "$j0url/system_info" \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception as e:
    print(f'  /system_info parse error: {e}', flush=True)
    sys.exit(1)
got = int(d.get('workers', 0))
want = $expected_workers
if got < want:
    print(f'  workers={got}/{want} — waiting...', flush=True)
    sys.exit(1)
print(f'  workers={got}/{want} — OK')
" 2>/dev/null; do
    [[ $(date +%s) -lt $deadline ]] || {
      die "Judge0 /system_info: expected $expected_workers workers but none connected after 240s. Check worker logs: docker compose -f docker-compose.ec2.yml logs --tail=50 workers"
    }
    sleep 5
  done
  ok "Judge0 workers verified ($expected_workers)"

  # 3. Verify all expected containers are actually running (not restarting/exited)
  log "Verifying container health (docker ps) ..."
  local not_running
  not_running=$(docker compose -f docker-compose.ec2.yml ps --format json 2>/dev/null \
    | python3 -c "
import sys, json
lines = sys.stdin.read().strip().splitlines()
bad = []
for line in lines:
    try:
        c = json.loads(line)
        state = c.get('State','').lower()
        name  = c.get('Name', c.get('Service','?'))
        if state not in ('running',):
            bad.append(f'{name}={state}')
    except: pass
if bad:
    print(' '.join(bad))
" 2>/dev/null || true)
  if [[ -n "$not_running" ]]; then
    warn "Some containers are not in 'running' state: $not_running"
    log "Container statuses:"
    docker compose -f docker-compose.ec2.yml ps
    die "Aborting — fix container issues before running load tests"
  fi
  ok "All containers running"
}

# ── Helper: bring up a config ─────────────────────────────────────────────────
#   Usage: bring_up  <SCALE>  [override_file]
#   Always tears down with the BASE file only (project-scoped, catches all
#   services regardless of which override was previously active).
bring_up() {
  local scale="$1"
  local override="${2:-}"

  log "Stopping ALL existing containers (base project teardown)..."
  # Use base file only for down — project name is the same regardless of override,
  # so this always stops everything cleanly without needing to know the old override.
  docker compose -f docker-compose.ec2.yml down --remove-orphans --timeout 30 2>/dev/null || true
  sleep "$COMPOSE_DOWN_WAIT"

  log "Starting containers (--scale workers=$scale) ..."
  if [[ -n "$override" ]]; then
    docker compose -f docker-compose.ec2.yml -f "$override" \
      up -d --scale workers="$scale" --force-recreate --remove-orphans
  else
    docker compose -f docker-compose.ec2.yml \
      up -d --scale workers="$scale" --force-recreate --remove-orphans
  fi

  log "Waiting ${COOLDOWN_PHASE_START}s for containers to warm up..."
  sleep "$COOLDOWN_PHASE_START"
  wait_healthy "$scale"
}

# ── Helper: pre-scenario container check (between runs in same phase) ─────────
#   Verifies no workers crashed/OOMed since the last run.
#   Usage: check_containers_still_healthy <expected_worker_count>
check_containers_still_healthy() {
  local expected_workers="$1"
  log "Pre-scenario health check (workers expected: $expected_workers) ..."

  # Count running worker containers
  local running_workers
  running_workers=$(docker compose -f docker-compose.ec2.yml ps --format json 2>/dev/null \
    | python3 -c "
import sys, json
lines = sys.stdin.read().strip().splitlines()
count = 0
for line in lines:
    try:
        c = json.loads(line)
        svc   = c.get('Service','')
        state = c.get('State','').lower()
        if svc == 'workers' and state == 'running':
            count += 1
    except: pass
print(count)
" 2>/dev/null || echo "0")

  if [[ "$running_workers" -lt "$expected_workers" ]]; then
    warn "Only $running_workers/$expected_workers worker(s) still running — possible OOM or crash"
    log "Container states:"
    docker compose -f docker-compose.ec2.yml ps
    log "Recent worker logs (last 20 lines each):"
    docker compose -f docker-compose.ec2.yml logs --tail=20 workers 2>/dev/null || true
    die "Worker count degraded. Aborting phase to avoid corrupt results."
  fi
  ok "All $running_workers/$expected_workers worker(s) still running"
}

# ── Helper: run one load test scenario ───────────────────────────────────────
#   Usage: run_scenario  <OUT_STEM>  <WORKERS>  <RUNNERS>  <PUMA_SLOTS>  \
#                        <USERS>     <RAMP>      <BATCH>
run_scenario() {
  local stem="$1"       # e.g. A_1000u_ramp20
  local workers="$2"
  local runners="$3"
  local puma_slots="$4"
  local users="$5"
  local ramp="$6"
  local batch="$7"

  local out="Reports/Jsons/${stem}.json"

  if $RESUME && [[ -f "$out" ]]; then
    warn "SKIP (--resume): $out already exists"
    return 0
  fi

  banner "Scenario: $stem  |  users=$users ramp=${ramp}s batch=$batch"
  log "workers=$workers  runners=$runners  puma_slots=$puma_slots"
  log "Output → $out"

  local auto_report_flag="--auto-report"
  $SKIP_EXCEL && auto_report_flag="-auto-report=false"

  go run main.go \
    -flask-url  "$FLASK_URL" \
    -bank       "$BANK" \
    -users      "$users" \
    -ramp       "$ramp" \
    -batch      "$batch" \
    -out        "$out" \
    -report-workers     "$workers" \
    -report-runners     "$runners" \
    -report-puma-slots  "$puma_slots" \
    -auto-metrics=true \
    -metrics-interval "$METRICS_INTERVAL" \
    $auto_report_flag

  ok "Done: $stem → $out"
}

# ── Helper: cooldown between runs ─────────────────────────────────────────────
cooldown() {
  log "Cooldown ${COOLDOWN_BETWEEN_RUNS}s (queue drain + system settle)..."
  sleep "$COOLDOWN_BETWEEN_RUNS"
}

# ─────────────────────────────────────────────────────────────────────────────
#  PHASE A — Config A: workers=3, MAX_RUNNERS=2, 6 sandboxes, puma_slots=32
# ─────────────────────────────────────────────────────────────────────────────
run_phase_A() {
  banner "PHASE A — workers=3  MAX_RUNNERS=2  (6 sandboxes)  puma_slots=32"
  bring_up 3  # no override file

  run_scenario "A_100u_flat"    3 2 32  100  0  0
  cooldown; check_containers_still_healthy 3

  run_scenario "A_500u_ramp10"  3 2 32  500  10 0
  cooldown; check_containers_still_healthy 3

  run_scenario "A_1000u_ramp20" 3 2 32  1000 20 0
  cooldown; check_containers_still_healthy 3

  run_scenario "A_1000u_spike"  3 2 32  1000 0  0
  cooldown; check_containers_still_healthy 3

  run_scenario "A_1000u_pipe50" 3 2 32  1000 20 50
  cooldown; check_containers_still_healthy 3

  run_scenario "A_1000u_pipe100" 3 2 32 1000 20 100

  ok "Phase A complete"
}

# ─────────────────────────────────────────────────────────────────────────────
#  PHASE B — Config B: workers=3, MAX_RUNNERS=3, 9 sandboxes, puma_slots=40
# ─────────────────────────────────────────────────────────────────────────────
run_phase_B() {
  banner "PHASE B — workers=3  MAX_RUNNERS=3  (9 sandboxes)  puma_slots=40"
  bring_up 3 "docker-compose.ec2.w3mr3.yml"

  run_scenario "B_1000u_ramp20" 3 3 40  1000 20 0
  cooldown; check_containers_still_healthy 3

  run_scenario "B_1000u_spike"  3 3 40  1000 0  0
  cooldown; check_containers_still_healthy 3

  run_scenario "B_1000u_pipe50" 3 3 40  1000 20 50

  ok "Phase B complete"
}

# ─────────────────────────────────────────────────────────────────────────────
#  PHASE C — Config C: workers=4, MAX_RUNNERS=2, 8 sandboxes, puma_slots=40
# ─────────────────────────────────────────────────────────────────────────────
run_phase_C() {
  banner "PHASE C — workers=4  MAX_RUNNERS=2  (8 sandboxes)  puma_slots=40"
  bring_up 4 "docker-compose.ec2.w4mr2.yml"

  run_scenario "C_1000u_ramp20" 4 2 40  1000 20 0
  cooldown; check_containers_still_healthy 4

  run_scenario "C_1000u_spike"  4 2 40  1000 0  0
  cooldown; check_containers_still_healthy 4

  run_scenario "C_1000u_pipe50" 4 2 40  1000 20 50

  ok "Phase C complete"
}

# ─────────────────────────────────────────────────────────────────────────────
#  PHASE D — Config D: workers=4, MAX_RUNNERS=3, 12 sandboxes, puma_slots=40
#  ⚠ Borderline RAM (~7.3 GB / 7.6 GB). OOM possible — script continues on fail.
# ─────────────────────────────────────────────────────────────────────────────
run_phase_D() {
  banner "PHASE D — workers=4  MAX_RUNNERS=3  (12 sandboxes)  puma_slots=40  ⚠ RAM"
  bring_up 4 "docker-compose.ec2.w4mr3.yml"

  log "⚠ Config D has only ~300 MB RAM headroom — monitoring OOM..."
  # Non-fatal: if a run fails with OOM, log a warning and continue
  run_scenario "D_500u_ramp10"  4 3 40  500  10 0 || warn "D_500u_ramp10 failed (possible OOM) — continuing"
  cooldown
  # After cooldown, check if workers survived — Phase D may OOM so downgrade
  # to a warning (not die) to still attempt the second scenario.
  local d_workers
  d_workers=$(docker compose -f docker-compose.ec2.yml ps --format json 2>/dev/null \
    | python3 -c "
import sys, json
lines = sys.stdin.read().strip().splitlines()
count = sum(1 for l in lines if json.loads(l).get('Service')=='workers' and json.loads(l).get('State','').lower()=='running')
print(count)
" 2>/dev/null || echo "0")
  if [[ "$d_workers" -lt 4 ]]; then
    warn "Phase D: only $d_workers/4 workers running after first scenario (OOM likely). Attempting restart before next run..."
    bring_up 4 "docker-compose.ec2.w4mr3.yml"
  fi

  run_scenario "D_1000u_ramp20" 4 3 40  1000 20 0 || warn "D_1000u_ramp20 failed (possible OOM) — continuing"

  ok "Phase D complete"
}

# ─────────────────────────────────────────────────────────────────────────────
#  PHASE E — Config E: workers=5, MAX_RUNNERS=2, 10 sandboxes, puma_slots=48
# ─────────────────────────────────────────────────────────────────────────────
run_phase_E() {
  banner "PHASE E — workers=5  MAX_RUNNERS=2  (10 sandboxes)  puma_slots=48"
  bring_up 5 "docker-compose.ec2.w5mr2.yml"

  run_scenario "E_1000u_ramp20" 5 2 48  1000 20 0
  cooldown; check_containers_still_healthy 5

  run_scenario "E_1000u_spike"  5 2 48  1000 0  0
  cooldown; check_containers_still_healthy 5

  run_scenario "E_1000u_pipe50" 5 2 48  1000 20 50

  ok "Phase E complete"
}

# ─────────────────────────────────────────────────────────────────────────────
#  Cumulative report
# ─────────────────────────────────────────────────────────────────────────────
generate_cumulative_report() {
  banner "Generating Cumulative Comparison Report"

  # Gather every JSON that exists in the canonical order
  local files=()
  local ordered=(
    A_100u_flat      A_500u_ramp10   A_1000u_ramp20  A_1000u_spike
    A_1000u_pipe50   A_1000u_pipe100
    B_1000u_ramp20   B_1000u_spike   B_1000u_pipe50
    C_1000u_ramp20   C_1000u_spike   C_1000u_pipe50
    D_500u_ramp10    D_1000u_ramp20
    E_1000u_ramp20   E_1000u_spike   E_1000u_pipe50
  )
  for stem in "${ordered[@]}"; do
    local f="Reports/Jsons/${stem}.json"
    [[ -f "$f" ]] && files+=("$f") || warn "Missing: $f — excluded from cumulative report"
  done

  if [[ ${#files[@]} -lt 2 ]]; then
    warn "Fewer than 2 result files found — skipping cumulative report"
    return
  fi

  log "Combining ${#files[@]} result files..."
  python3 generate_ec2_report.py \
    --all "${files[@]}" \
    --out Reports/cumulative_report.xlsx

  ok "Cumulative report → Reports/cumulative_report.xlsx"
}

# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
START_TS=$(date '+%Y-%m-%d %H:%M:%S')
banner "Judge0 Automated Load Test Suite  |  $START_TS"

log "Deploy dir  : $DEPLOY_DIR"
log "Flask URL   : $FLASK_URL"
log "Question bank: $BANK"
log "Resume mode : $RESUME"
log "Skip Excel  : $SKIP_EXCEL"
log "Only phase  : ${ONLY_PHASE:-all}"
echo ""

case "$ONLY_PHASE" in
  A)    run_phase_A ;;
  B)    run_phase_B ;;
  C)    run_phase_C ;;
  D)    run_phase_D ;;
  E)    run_phase_E ;;
  "")
    run_phase_A
    run_phase_B
    run_phase_C
    run_phase_D
    run_phase_E
    generate_cumulative_report
    ;;
  *)    die "Unknown phase: $ONLY_PHASE  (valid: A B C D E)" ;;
esac

# Final cumulative report when running a single phase is skipped to avoid
# combining partial data. Run without --phase to get it.
[[ -n "$ONLY_PHASE" ]] || true  # already called above in full run

END_TS=$(date '+%Y-%m-%d %H:%M:%S')
banner "All done  |  Started: $START_TS  |  Finished: $END_TS"
log "Full log saved to: $LOG_FILE"
