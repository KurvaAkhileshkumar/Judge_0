#!/usr/bin/env bash
# deploy-ec2.sh — run this ON the EC2 instance after cloning the repo.
# Usage:  bash deploy-ec2.sh
set -euo pipefail

REPO_URL="https://github.com/KurvaAkhileshkumar/Judge_0.git"
DEPLOY_DIR="/home/ubuntu/judge0"
COMPOSE_FILE="docker-compose.ec2.yml"

echo "=== Judge0 EC2 Deploy Script ==="
echo "Target: $DEPLOY_DIR"
echo ""

# ── 0. Disk space check ────────────────────────────────────────────────────
DISK_FREE=$(df / --output=avail -BG | tail -1 | tr -d 'G ')
echo "[0/6] Disk free: ${DISK_FREE}GB"
if [ "$DISK_FREE" -lt 5 ]; then
  echo "ERROR: Less than 5 GB free — run 'sudo docker system prune -a' first."
  exit 1
fi

# ── 1. Install Docker + Compose plugin if missing ─────────────────────────
if ! command -v docker &>/dev/null; then
  echo "[1/6] Installing Docker..."
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker ubuntu
  echo "      Docker installed. Re-run this script in a new shell if needed."
  exit 0
else
  echo "[1/6] Docker already installed: $(docker --version)"
fi

if ! sudo docker compose version &>/dev/null; then
  echo "      Installing Docker Compose plugin..."
  sudo apt-get install -y docker-compose-plugin
fi

# ── 2. Clone / update repo ─────────────────────────────────────────────────
echo "[2/6] Cloning repo..."
if [ -d "$DEPLOY_DIR/.git" ]; then
  echo "      Repo exists — pulling latest..."
  git -C "$DEPLOY_DIR" pull
else
  git clone "$REPO_URL" "$DEPLOY_DIR"
fi
cd "$DEPLOY_DIR"

# ── 3. Create .env if it doesn't exist ────────────────────────────────────
echo "[3/6] Setting up secrets (.env)..."
if [ ! -f .env ]; then
  REDIS_PW=$(openssl rand -base64 32)
  PG_PW=$(openssl rand -base64 32)
  SECRET=$(openssl rand -hex 64)

  cat > .env <<EOF
REDIS_PASSWORD=${REDIS_PW}
POSTGRES_PASSWORD=${PG_PW}
RAILS_SECRET_KEY_BASE=${SECRET}
EOF
  echo "      .env created with fresh random secrets."
  echo ""
  echo "  !! SAVE THESE PASSWORDS — you will need them for DB access !!"
  echo "  REDIS_PASSWORD:    ${REDIS_PW}"
  echo "  POSTGRES_PASSWORD: ${PG_PW}"
  echo ""
else
  echo "      .env already exists — skipping."
fi

# ── 4. Pull Judge0 image (skip if already present) ────────────────────────
echo "[4/6] Pulling Judge0 image..."
if sudo docker image inspect judge0/judge0:1.13.1 &>/dev/null; then
  echo "      judge0/judge0:1.13.1 already present — skipping pull."
else
  sudo docker pull judge0/judge0:1.13.1
fi

# ── 5. Build Python services (skip if images already exist and up-to-date) ─
echo "[5/6] Building Flask API + grading worker images..."
sudo docker compose -f "$COMPOSE_FILE" build

# ── 6. Start the stack ─────────────────────────────────────────────────────
echo "[6/6] Starting stack (3 Judge0 worker replicas)..."
sudo docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
sudo docker compose -f "$COMPOSE_FILE" up -d --scale workers=3

# ── Verify all containers actually started ─────────────────────────────────
echo ""
echo "=== Verifying containers started... ==="
sleep 10

FAILED=0
for SVC in server db redis api grading_worker reconciler; do
  STATUS=$(sudo docker compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null | \
    python3 -c "
import sys, json
data = sys.stdin.read().strip()
lines = [l for l in data.splitlines() if l.strip()]
for line in lines:
    try:
        c = json.loads(line)
        if '${SVC}' in c.get('Name','') or '${SVC}' in c.get('Service',''):
            print(c.get('State','unknown'))
            break
    except: pass
" 2>/dev/null || echo "unknown")

  if [[ "$STATUS" == "running" ]]; then
    echo "  ✓ $SVC — running"
  else
    echo "  ✗ $SVC — $STATUS (showing last 20 log lines):"
    sudo docker compose -f "$COMPOSE_FILE" logs --tail=20 "$SVC" 2>/dev/null || true
    FAILED=1
  fi
done

if [ "$FAILED" -eq 1 ]; then
  echo ""
  echo "ERROR: One or more services failed to start. Full status:"
  sudo docker compose -f "$COMPOSE_FILE" ps
  echo ""
  echo "Check logs with:  sudo docker compose -f $COMPOSE_FILE logs -f"
  exit 1
fi

# ── Wait for Judge0 DB migrations (first boot takes ~45s) ─────────────────
echo ""
echo "=== Waiting 45s for Judge0 DB migrations... ==="
sleep 45

# ── Health checks ──────────────────────────────────────────────────────────
echo ""
echo "--- Judge0 /system_info ---"
J0_OK=0
for i in 1 2 3 4; do
  RESP=$(curl -sf http://localhost:2358/system_info 2>/dev/null) && {
    echo "$RESP" | python3 -m json.tool 2>/dev/null || echo "$RESP"
    J0_OK=1
    break
  } || { echo "  Attempt $i/4 — retrying in 15s..."; sleep 15; }
done
[ "$J0_OK" -eq 0 ] && {
  echo "ERROR: Judge0 not responding. Logs:"
  sudo docker compose -f "$COMPOSE_FILE" logs --tail=30 server
  exit 1
}

echo ""
echo "--- Flask API /health ---"
curl -sf http://localhost:5001/health | python3 -m json.tool 2>/dev/null || \
  curl -s http://localhost:5001/health

echo ""
echo "--- Container status ---"
sudo docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "=== Deploy complete ==="
echo ""
echo "Run the load test from your LOCAL machine (not EC2):"
echo "  go run main.go -flask-url http://15.206.211.150:5001 -users 1000 -accept-only -out results_1000.json"
echo "  go run main.go -url http://15.206.211.150:2358 -users 500 -accept-only -out results_direct_500.json"
