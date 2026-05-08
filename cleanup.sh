#!/bin/bash
# cleanup.sh — Judge0 EC2 post-load-test cleanup
# Run after every load test: ./cleanup.sh
# Run with --full for weekly deep clean: ./cleanup.sh --full

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.ec2.yml}"
ENV_FILE="${ENV_FILE:-.env}"
DISK_WARN_PCT=85
DISK_CRIT_PCT=95

RED='\033[0;31m'
YLW='\033[1;33m'
GRN='\033[0;32m'
NC='\033[0m'

FULL_CLEAN=false
if [[ "${1:-}" == "--full" ]]; then
  FULL_CLEAN=true
fi

echo "=== Judge0 Cleanup $(date '+%Y-%m-%d %H:%M:%S') ==="
echo ""

# ── Disk before ───────────────────────────────────────────────────────────────
DISK_PCT=$(df / --output=pcent | tail -1 | tr -d '% ')
DISK_FREE=$(df -h / --output=avail | tail -1 | tr -d ' ')
echo "Disk before: ${DISK_FREE} free (${DISK_PCT}% used)"

if [ "$DISK_PCT" -ge "$DISK_CRIT_PCT" ]; then
  echo -e "${RED}CRITICAL: Disk at ${DISK_PCT}% — cleaning urgently${NC}"
elif [ "$DISK_PCT" -ge "$DISK_WARN_PCT" ]; then
  echo -e "${YLW}WARNING: Disk at ${DISK_PCT}% — cleanup recommended${NC}"
else
  echo -e "${GRN}Disk OK${NC}"
fi
echo ""

# ── 1. PostgreSQL submissions ─────────────────────────────────────────────────
echo "[1/5] Truncating PostgreSQL submissions table..."
DB_CONTAINER=$(sudo docker ps --filter "name=db" --format "{{.Names}}" | head -1)
if [ -n "$DB_CONTAINER" ]; then
  BEFORE=$(sudo docker exec "$DB_CONTAINER" psql -U judge0 -d judge0 -tAq \
    -c "SELECT pg_size_pretty(pg_total_relation_size('submissions'));" 2>/dev/null || echo "unknown")
  sudo docker exec "$DB_CONTAINER" psql -U judge0 -d judge0 \
    -c "TRUNCATE submissions CASCADE;" 2>/dev/null
  AFTER=$(sudo docker exec "$DB_CONTAINER" psql -U judge0 -d judge0 -tAq \
    -c "SELECT pg_size_pretty(pg_total_relation_size('submissions'));" 2>/dev/null || echo "unknown")
  echo "  submissions: ${BEFORE} → ${AFTER} ✓"
else
  echo "  WARNING: db container not found — skipping"
fi
echo ""

# ── 2. PostgreSQL VACUUM ──────────────────────────────────────────────────────
echo "[2/5] Running PostgreSQL VACUUM to reclaim disk space..."
if [ -n "$DB_CONTAINER" ]; then
  sudo docker exec "$DB_CONTAINER" psql -U judge0 -d judge0 \
    -c "VACUUM ANALYZE;" 2>/dev/null
  DB_SIZE=$(sudo docker exec "$DB_CONTAINER" psql -U judge0 -d judge0 -tAq \
    -c "SELECT pg_size_pretty(pg_database_size('judge0'));" 2>/dev/null || echo "unknown")
  echo "  judge0 database size: ${DB_SIZE} ✓"
else
  echo "  WARNING: db container not found — skipping"
fi
echo ""

# ── 3. Redis AOF rewrite ──────────────────────────────────────────────────────
echo "[3/5] Compressing Redis AOF log..."
REDIS_CONTAINER=$(sudo docker ps --filter "name=redis" --format "{{.Names}}" | head -1)
if [ -n "$REDIS_CONTAINER" ]; then
  REDIS_PASSWORD=""
  if [ -f "$ENV_FILE" ]; then
    REDIS_PASSWORD=$(grep REDIS_PASSWORD "$ENV_FILE" | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d ' ')
  fi

  if [ -n "$REDIS_PASSWORD" ]; then
    sudo docker exec "$REDIS_CONTAINER" \
      redis-cli -a "$REDIS_PASSWORD" BGREWRITEAOF 2>/dev/null || true
    echo "  AOF rewrite triggered ✓"
    # Wait briefly for rewrite to start
    sleep 2
    AOF_STATUS=$(sudo docker exec "$REDIS_CONTAINER" \
      redis-cli -a "$REDIS_PASSWORD" INFO persistence 2>/dev/null | \
      grep aof_rewrite_in_progress | tr -d '\r' | cut -d: -f2 || echo "?")
    echo "  AOF rewrite in progress: ${AOF_STATUS}"
  else
    echo "  WARNING: REDIS_PASSWORD not found in $ENV_FILE — skipping AOF rewrite"
  fi
else
  echo "  WARNING: redis container not found — skipping"
fi
echo ""

# ── 4. Docker container logs ──────────────────────────────────────────────────
echo "[4/5] Truncating Docker container logs..."
LOG_BEFORE=$(sudo find /var/lib/docker/containers -name "*.log" \
  -exec du -sh {} + 2>/dev/null | awk '{sum+=$1} END {print sum "KB"}' || echo "unknown")
sudo find /var/lib/docker/containers -name "*.log" -exec truncate -s 0 {} \;
echo "  Container logs cleared ✓"
echo ""

# ── 5. Full clean (weekly) ────────────────────────────────────────────────────
if [ "$FULL_CLEAN" = true ]; then
  echo "[5/5] Full clean (--full flag): removing unused Docker objects..."
  sudo docker system prune -f
  echo "  Docker prune done ✓"

  echo "  Vacuuming systemd journal (keep 3 days)..."
  sudo journalctl --vacuum-time=3d 2>/dev/null || true
  echo "  Journal vacuumed ✓"
else
  echo "[5/5] Skipping deep clean (run with --full for weekly cleanup)"
fi
echo ""

# ── Disk after ────────────────────────────────────────────────────────────────
DISK_PCT_AFTER=$(df / --output=pcent | tail -1 | tr -d '% ')
DISK_FREE_AFTER=$(df -h / --output=avail | tail -1 | tr -d ' ')
FREED=$((DISK_PCT - DISK_PCT_AFTER))

echo "─────────────────────────────────"
echo "Disk before: ${DISK_FREE} free  (${DISK_PCT}%)"
echo "Disk after:  ${DISK_FREE_AFTER} free  (${DISK_PCT_AFTER}%)"
if [ "$FREED" -gt 0 ]; then
  echo -e "${GRN}Freed ~${FREED}% of disk ✓${NC}"
else
  echo "Disk unchanged (data was already minimal)"
fi
echo ""

# ── Disk warning ──────────────────────────────────────────────────────────────
if [ "$DISK_PCT_AFTER" -ge "$DISK_CRIT_PCT" ]; then
  echo -e "${RED}CRITICAL: Still at ${DISK_PCT_AFTER}% after cleanup!${NC}"
  echo "  → Expand EBS in AWS Console (EC2 → Volumes → Modify Volume)"
  echo "  → Then run: sudo growpart /dev/xvda 1 && sudo resize2fs /dev/xvda1"
elif [ "$DISK_PCT_AFTER" -ge "$DISK_WARN_PCT" ]; then
  echo -e "${YLW}WARNING: Still at ${DISK_PCT_AFTER}% — consider expanding EBS soon${NC}"
else
  echo -e "${GRN}Disk healthy at ${DISK_PCT_AFTER}% ✓${NC}"
fi

echo ""
echo "=== Cleanup complete ==="
echo ""
echo "Usage:"
echo "  ./cleanup.sh          ← run after every load test"
echo "  ./cleanup.sh --full   ← run weekly (deeper clean)"
