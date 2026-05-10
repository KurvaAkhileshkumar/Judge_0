#!/usr/bin/env python3
"""
collect_ec2_metrics.py
──────────────────────
Background system-metrics collector for EC2 load tests.

Run this in a separate terminal BEFORE starting the load test, then stop it
(Ctrl-C) after the test finishes.  The resulting JSONL file is consumed by
generate_ec2_report.py (--metrics flag) to populate the "Live System Metrics"
section in Sheet 2 of the Excel report.

Metrics captured every --interval seconds:
  System:
    cpu_pct            — CPU utilisation %
    mem_pct            — RAM used %
    mem_used_gb        — RAM used (GB)
    mem_avail_gb       — RAM available (GB)
    disk_read_mb_s     — Disk read throughput (MB/s, delta from prev snapshot)
    disk_write_mb_s    — Disk write throughput (MB/s)
    net_sent_mb_s      — Network TX (MB/s)
    net_recv_mb_s      — Network RX (MB/s)
    tcp_established    — ESTABLISHED TCP connections (all processes)

  Redis (if reachable):
    redis_clients      — connected_clients
    redis_mem_used_mb  — used_memory (MB)
    redis_queue_depth  — len(judge0:jobs:normal) + len(judge0:jobs:retry)

Usage:
    # Terminal 1 — start collector
    python collect_ec2_metrics.py --out metrics.jsonl --interval 5

    # Terminal 2 — run load test
    go run main.go -flask-url http://localhost:5001 -users 100 -out report.json

    # After both finish:
    python generate_ec2_report.py report.json --metrics metrics.jsonl

Environment variables (for Redis):
    REDIS_HOST        (default: localhost)
    REDIS_PORT        (default: 6379)
    REDIS_PASSWORD    (default: none)
"""

import argparse
import datetime
import json
import os
import signal
import sys
import time

try:
    import psutil
except ImportError:
    print("ERROR: psutil not installed.  Run: pip install -r requirements-report.txt")
    sys.exit(1)

try:
    import redis as redis_lib
    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False


# ── Redis connection ──────────────────────────────────────────────────────────

def _make_redis_client():
    if not _HAS_REDIS:
        return None
    try:
        rc = redis_lib.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD") or None,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        rc.ping()
        return rc
    except Exception as exc:
        print(f"WARNING: Cannot connect to Redis ({exc}).  Redis metrics skipped.")
        return None


# ── Snapshot logic ────────────────────────────────────────────────────────────

def _collect_snapshot(prev_disk, prev_net, prev_time, rc):
    """
    Capture one metrics snapshot.  Returns (snap_dict, disk_counters, net_counters, now).
    Delta-rate fields (disk/net MB/s) are only populated when a previous reading exists.
    """
    now = time.monotonic()
    elapsed = (now - prev_time) if prev_time is not None else 1.0

    snap = {
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ── CPU ───────────────────────────────────────────────────────────────────
    snap["cpu_pct"] = psutil.cpu_percent(interval=None)

    # ── Memory ────────────────────────────────────────────────────────────────
    mem = psutil.virtual_memory()
    snap["mem_pct"]      = round(mem.percent, 1)
    snap["mem_used_gb"]  = round(mem.used  / 1e9, 2)
    snap["mem_avail_gb"] = round(mem.available / 1e9, 2)

    # ── Disk I/O (rates since last snapshot) ──────────────────────────────────
    disk = psutil.disk_io_counters()
    if prev_disk is not None and disk is not None and elapsed > 0:
        snap["disk_read_mb_s"]  = round((disk.read_bytes  - prev_disk.read_bytes)  / elapsed / 1e6, 2)
        snap["disk_write_mb_s"] = round((disk.write_bytes - prev_disk.write_bytes) / elapsed / 1e6, 2)

    # ── Network I/O (rates since last snapshot) ───────────────────────────────
    net = psutil.net_io_counters()
    if prev_net is not None and net is not None and elapsed > 0:
        snap["net_sent_mb_s"] = round((net.bytes_sent - prev_net.bytes_sent) / elapsed / 1e6, 2)
        snap["net_recv_mb_s"] = round((net.bytes_recv - prev_net.bytes_recv) / elapsed / 1e6, 2)

    # ── TCP connections ───────────────────────────────────────────────────────
    try:
        conns = psutil.net_connections(kind="tcp")
        snap["tcp_established"] = sum(1 for c in conns if c.status == "ESTABLISHED")
    except (psutil.AccessDenied, OSError):
        # macOS requires root for full net_connections; skip gracefully
        pass

    # ── Redis ─────────────────────────────────────────────────────────────────
    if rc is not None:
        try:
            info = rc.info()
            snap["redis_clients"]     = info.get("connected_clients", 0)
            snap["redis_mem_used_mb"] = round(info.get("used_memory", 0) / 1e6, 1)
            # Grading queue depth
            normal_depth = rc.llen("judge0:jobs:normal") or 0
            retry_depth  = rc.llen("judge0:jobs:retry")  or 0
            snap["redis_queue_depth"] = int(normal_depth) + int(retry_depth)
        except Exception:
            pass  # Redis disconnected mid-test — skip this snapshot's Redis fields

    return snap, disk, net, now


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="EC2 load-test live metrics collector — run alongside the load test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--out",      default="metrics.jsonl",
                        help="Output JSONL file (one JSON object per line)")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="Sampling interval in seconds")
    args = parser.parse_args()

    # Warm up the CPU percent counter — first call always returns 0.0
    psutil.cpu_percent(interval=None)

    rc = _make_redis_client()
    if rc:
        host = os.getenv("REDIS_HOST", "localhost")
        port = os.getenv("REDIS_PORT", 6379)
        print(f"Connected to Redis @ {host}:{port}")

    # Graceful shutdown on Ctrl-C / SIGTERM
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # Seed previous counters
    prev_disk = psutil.disk_io_counters()
    prev_net  = psutil.net_io_counters()
    prev_time = time.monotonic()

    count = 0
    with open(args.out, "w") as fh:
        print(f"Collecting system metrics every {args.interval}s  →  {args.out}")
        print("Press Ctrl-C to stop.\n")
        print(f"{'Timestamp':<22} {'CPU%':>6} {'RAM%':>6} {'RAM Used':>10} "
              f"{'Disk R':>8} {'Disk W':>8} {'Net TX':>8} {'Net RX':>8} "
              f"{'TCP':>6} {'RQueue':>8} {'RMem':>8}")
        print("-" * 105)

        while running:
            time.sleep(args.interval)

            snap, prev_disk, prev_net, prev_time = _collect_snapshot(
                prev_disk, prev_net, prev_time, rc
            )

            fh.write(json.dumps(snap) + "\n")
            fh.flush()
            count += 1

            # Pretty console output
            print(
                f"{snap['timestamp']:<22}"
                f" {snap.get('cpu_pct', '?'):>5}%"
                f" {snap.get('mem_pct', '?'):>5}%"
                f" {snap.get('mem_used_gb', '?'):>9} GB"
                f" {snap.get('disk_read_mb_s',  '-'):>7}/s"
                f" {snap.get('disk_write_mb_s', '-'):>7}/s"
                f" {snap.get('net_sent_mb_s',   '-'):>7}/s"
                f" {snap.get('net_recv_mb_s',   '-'):>7}/s"
                f" {snap.get('tcp_established', '?'):>6}"
                f" {snap.get('redis_queue_depth', '?'):>8}"
                f" {snap.get('redis_mem_used_mb', '?'):>7} MB"
            )

    print(f"\n✓ Done.  {count} snapshots written to {args.out}")


if __name__ == "__main__":
    main()
