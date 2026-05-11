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

  Docker containers (via `docker stats --no-stream`, unless --no-docker):
    containers[]       — list per running container:
      .name            — container name
      .cpu_pct         — CPU utilisation %
      .mem_mb          — memory used (MB)
      .mem_limit_mb    — memory limit (MB)
      .mem_pct         — memory used %

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
import subprocess
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


# ── Docker stats helpers ─────────────────────────────────────────────────────

def _parse_docker_mem(mem_str: str):
    """Parse Docker MemUsage string e.g. '450MiB / 7.5GiB' → (used_mb, limit_mb)."""
    def _to_mb(s: str) -> float:
        s = s.strip()
        if s.endswith('GiB'):
            return float(s[:-3]) * 1024
        if s.endswith('GB'):
            return float(s[:-2]) * 1000
        if s.endswith('MiB') or s.endswith('MB'):
            return float(s[:-3 if s.endswith('iB') else -2])
        if s.endswith('KiB') or s.endswith('kB'):
            return float(s[:-3 if s.endswith('iB') else -2]) / 1024
        if s.endswith('B'):
            return float(s[:-1]) / (1024 * 1024)
        try:
            return float(s)
        except ValueError:
            return 0.0
    parts = mem_str.split(' / ')
    if len(parts) == 2:
        return _to_mb(parts[0]), _to_mb(parts[1])
    return 0.0, 0.0


def _collect_docker_stats() -> list:
    """Run `docker stats --no-stream` — returns cgroup-relative CPU% and cgroup mem."""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            capture_output=True, text=True, timeout=15,
        )
        containers = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                cpu = float(d.get("CPUPerc", "0%").rstrip('%'))
                used_mb, limit_mb = _parse_docker_mem(d.get("MemUsage", "0MiB / 0MiB"))
                mem_pct = float(d.get("MemPerc", "0%").rstrip('%'))
                containers.append({
                    "name":         d.get("Name", d.get("Container", "?")),
                    "cg_cpu_pct":   round(cpu, 2),      # cgroup-relative (can exceed 100% on multi-core)
                    "mem_mb":       round(used_mb, 1),
                    "mem_limit_mb": round(limit_mb, 1),
                    "mem_pct":      round(mem_pct, 2),  # cgroup mem %
                })
            except Exception:
                pass
        return containers
    except Exception:
        return []


def _collect_container_psutil_stats() -> dict:
    """
    For each running container get its root PID via docker inspect, then walk
    the full process tree with psutil to measure actual host-level utilisation:
      real_cpu_pct      — sum of cpu_percent() across all container PIDs
                          (0-400% on a 4-core host, same scale as `top`)
      real_cpu_pct_norm — real_cpu_pct / cpu_count  (0-100% normalised)
      real_rss_mb       — sum of RSS across all container PIDs (MB)
    Returns dict keyed by container name.
    """
    result = {}
    try:
        ps_result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10,
        )
        names = [n.strip() for n in ps_result.stdout.strip().splitlines() if n.strip()]
        if not names:
            return result

        inspect_result = subprocess.run(
            ["docker", "inspect", "--format", "{{.Name}} {{.State.Pid}}"] + names,
            capture_output=True, text=True, timeout=10,
        )
        n_cpus = psutil.cpu_count(logical=True) or 1

        for line in inspect_result.stdout.strip().splitlines():
            parts = line.strip().lstrip('/').split()
            if len(parts) != 2:
                continue
            cname, pid_str = parts
            try:
                root_pid = int(pid_str)
                if root_pid == 0:
                    continue
                try:
                    root_proc = psutil.Process(root_pid)
                    procs = [root_proc] + root_proc.children(recursive=True)
                except psutil.NoSuchProcess:
                    continue

                # Prime CPU counters (first call always returns 0.0)
                for p in procs:
                    try:
                        p.cpu_percent(interval=None)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                time.sleep(0.15)  # short window for delta measurement

                total_cpu = 0.0
                total_rss = 0
                for p in procs:
                    try:
                        total_cpu += p.cpu_percent(interval=None)
                        total_rss += p.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                result[cname] = {
                    "real_cpu_pct":      round(total_cpu, 2),
                    "real_cpu_pct_norm": round(total_cpu / n_cpus, 2),
                    "real_rss_mb":       round(total_rss / 1e6, 1),
                }
            except Exception:
                pass
    except Exception:
        pass
    return result


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

def _collect_snapshot(prev_disk, prev_net, prev_time, rc, docker_enabled: bool = True):
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

    # ── Docker container stats ────────────────────────────────────────────────
    if docker_enabled:
        containers = _collect_docker_stats()
        psutil_by_name = _collect_container_psutil_stats()
        for c in containers:
            ps = psutil_by_name.get(c["name"], {})
            c["real_cpu_pct"]      = ps.get("real_cpu_pct")       # actual host CPU% (summed)
            c["real_cpu_pct_norm"] = ps.get("real_cpu_pct_norm")  # normalised 0-100%
            c["real_rss_mb"]       = ps.get("real_rss_mb")        # actual RSS (MB)
        if containers:
            snap["containers"] = containers

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
    parser.add_argument("--no-docker", action="store_true",
                        help="Disable per-container docker stats collection")
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
                prev_disk, prev_net, prev_time, rc,
                docker_enabled=not args.no_docker,
            )

            fh.write(json.dumps(snap) + "\n")
            fh.flush()
            count += 1

            # Pretty console output — host metrics
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
            # Per-container stats
            for c in snap.get('containers', []):
                cg_cpu   = c.get('cg_cpu_pct', 0)
                rc_cpu   = c.get('real_cpu_pct_norm')
                rc_rss   = c.get('real_rss_mb')
                cg_flag  = ' !!!' if cg_cpu >= 90 else (' ! ' if cg_cpu >= 70 else '    ')
                rc_flag  = ''
                if rc_cpu is not None:
                    rc_flag = ' !!!' if rc_cpu >= 90 else (' ! ' if rc_cpu >= 70 else '    ')
                real_str = (
                    f"  real_cpu={rc_cpu:>5.1f}%{rc_flag}  real_rss={rc_rss:>7.0f} MB"
                    if rc_cpu is not None else ""
                )
                print(
                    f"  {c['name']:<35}"
                    f"  cg_cpu={cg_cpu:>5.1f}%{cg_flag}"
                    f"  cg_mem={c['mem_mb']:>7.0f} MB ({c['mem_pct']:.1f}%)"
                    f"{real_str}"
                )

    print(f"\n✓ Done.  {count} snapshots written to {args.out}")


if __name__ == "__main__":
    main()
