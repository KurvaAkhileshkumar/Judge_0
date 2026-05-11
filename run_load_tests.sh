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
COOLDOWN_PHASE_START=60        # wait after docker compose up before first test
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

# Build the load tester binary if missing or stale
if [[ ! -f "load_tester" ]] || [[ "main.go" -nt "load_tester" ]]; then
  log "Building load tester binary..."
  go build -o load_tester main.go || die "go build failed"
  ok "load_tester built"
fi

mkdir -p Reports/Jsons

# ── Helper: wait for Flask health ────────────────────────────────────────────
wait_healthy() {
  local url="$FLASK_URL/health"
  local deadline=$(( $(date +%s) + 120 ))
  log "Waiting for Flask health at $url ..."
  until curl -sf "$url" &>/dev/null; do
    [[ $(date +%s) -lt $deadline ]] || die "Flask did not become healthy within 120s"
    sleep 5
  done
  ok "Flask healthy"

  # Also check Judge0 /system_info to confirm workers are connected
  local j0url="${FLASK_URL%:5001}:2358"  # Judge0 is on :2358
  deadline=$(( $(date +%s) + 60 ))
  log "Waiting for Judge0 /system_info at $j0url ..."
  until curl -sf "$j0url/system_info" | python3 -c "import sys,json; d=json.load(sys.stdin); assert int(d.get('workers',0))>0" 2>/dev/null; do
    [[ $(date +%s) -lt $deadline ]] || { warn "Judge0 /system_info check timed out — proceeding anyway"; break; }
    sleep 5
  done
  ok "Judge0 reports workers online"
}

# ── Helper: bring up a config ─────────────────────────────────────────────────
#   Usage: bring_up  <SCALE>  [override_file]
bring_up() {
  local scale="$1"
  local override="${2:-}"
  log "Stopping existing containers..."
  docker compose -f docker-compose.ec2.yml ${override:+-f "$override"} down --remove-orphans 2>/dev/null || true
  sleep "$COMPOSE_DOWN_WAIT"

  log "Starting containers (--scale workers=$scale)..."
  if [[ -n "$override" ]]; then
    docker compose -f docker-compose.ec2.yml -f "$override" \
      up -d --scale workers="$scale" --force-recreate
  else
    docker compose -f docker-compose.ec2.yml \
      up -d --scale workers="$scale" --force-recreate
  fi

  log "Waiting ${COOLDOWN_PHASE_START}s for containers to warm up..."
  sleep "$COOLDOWN_PHASE_START"
  wait_healthy
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

  ./load_tester \
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
  cooldown

  run_scenario "A_500u_ramp10"  3 2 32  500  10 0
  cooldown

  run_scenario "A_1000u_ramp20" 3 2 32  1000 20 0
  cooldown

  run_scenario "A_1000u_spike"  3 2 32  1000 0  0
  cooldown

  run_scenario "A_1000u_pipe50" 3 2 32  1000 20 50
  cooldown

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
  cooldown

  run_scenario "B_1000u_spike"  3 3 40  1000 0  0
  cooldown

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
  cooldown

  run_scenario "C_1000u_spike"  4 2 40  1000 0  0
  cooldown

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
  cooldown

  run_scenario "E_1000u_spike"  5 2 48  1000 0  0
  cooldown

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
