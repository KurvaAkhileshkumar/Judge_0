#!/usr/bin/env python3
"""
collect_ec2_metrics.py
──────────────────────
Background system-metrics collector for EC2 load tests.

Run this in a separate terminal BEFORE starting the load test, then stop it
(Ctrl-C) after the test finishes.  The resulting JSONL file is consumed by
generate_ec2_report.py (--metrics flag) to populate the "Live System Metrics"
section in Sheet 2 of the Excel report.

Host metrics are read directly from the Linux kernel virtual filesystem
(/proc/stat, /proc/meminfo, /proc/diskstats, /proc/net/dev, /proc/net/tcp).
No psutil or other user-space monitoring libraries are required.

Metrics captured every --interval seconds:
  System (kernel-level, from /proc):
    cpu_pct            — CPU utilisation % (all modes: user+sys+iowait+steal+irq,
                         delta between consecutive /proc/stat readings)
    mem_pct            — RAM used %  (/proc/meminfo: (MemTotal-MemAvailable)/MemTotal)
    mem_used_gb        — RAM used (GB)
    mem_avail_gb       — RAM available (GB)
    mem_total_gb       — RAM total (GB)
    disk_read_mb_s     — Disk read throughput (MB/s, delta from prev snapshot)
    disk_write_mb_s    — Disk write throughput (MB/s)
    net_sent_mb_s      — Network TX (MB/s)
    net_recv_mb_s      — Network RX (MB/s)
    tcp_established    — ESTABLISHED TCP connections (/proc/net/tcp + /proc/net/tcp6)
    n_cpus             — Logical CPU count (/proc/cpuinfo)

  Redis (if reachable):
    redis_clients      — connected_clients
    redis_mem_used_mb  — used_memory (MB)
    redis_queue_depth  — len(judge0:jobs:normal) + len(judge0:jobs:retry)

  Docker containers (via `docker stats --no-stream`, cgroup-based):
    containers[]       — list per running container:
      .name            — container name
      .cg_cpu_pct      — cgroup-relative CPU %  (can exceed 100% on multi-core)
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
    import redis as redis_lib
    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False


# ── Kernel /proc readers ──────────────────────────────────────────────────────

def _read_proc_cpu_stat() -> dict:
    """Read aggregate CPU tick counters from /proc/stat (first 'cpu' line)."""
    try:
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("cpu "):
                    parts = line.split()
                    # Fields: user nice system idle iowait irq softirq steal
                    names = ["user", "nice", "system", "idle",
                              "iowait", "irq", "softirq", "steal"]
                    return {names[i]: int(parts[i + 1])
                            for i in range(min(len(names), len(parts) - 1))}
    except Exception:
        pass
    return {}


def _cpu_pct_from_delta(prev: dict, curr: dict):
    """
    CPU utilisation % between two /proc/stat tick snapshots.
    Counts all non-idle modes: user + nice + system + iowait + irq + softirq + steal.
    iowait is NOT treated as idle — a CPU blocked on I/O is a loaded CPU.
    Returns None when either snapshot is empty (e.g. first reading has no prev).
    """
    if not prev or not curr:
        return None
    # Pure idle only — iowait counts toward utilisation, not idle
    prev_idle   = prev.get("idle", 0)
    curr_idle   = curr.get("idle", 0)
    prev_total  = sum(prev.values())
    curr_total  = sum(curr.values())
    delta_total = curr_total - prev_total
    delta_idle  = curr_idle  - prev_idle
    if delta_total <= 0:
        return 0.0
    return round(100.0 * (1.0 - delta_idle / delta_total), 1)


def _read_meminfo() -> dict:
    """
    Read /proc/meminfo and return host-level memory metrics.
    Uses MemAvailable (accounts for reclaimable page cache) rather than MemFree.
    """
    kv: dict[str, int] = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        kv[parts[0].rstrip(":")] = int(parts[1])  # value in kB
                    except ValueError:
                        pass
    except Exception:
        return {}
    total = kv.get("MemTotal", 0)
    avail = kv.get("MemAvailable", 0)
    used  = total - avail
    return {
        "mem_total_gb": round(total / (1024 ** 2), 2),
        "mem_used_gb":  round(used  / (1024 ** 2), 2),
        "mem_avail_gb": round(avail / (1024 ** 2), 2),
        "mem_pct":      round(100.0 * used / total, 1) if total else 0.0,
    }


_SECTOR_BYTES = 512  # Linux always uses 512-byte logical sectors in /proc/diskstats


def _read_diskstats() -> tuple[int, int]:
    """
    Read /proc/diskstats and return cumulative (read_bytes, write_bytes) summed
    across all whole-device block devices (partitions excluded by name heuristic).
    """
    reads = writes = 0
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 14:
                    continue
                dev = parts[2]
                # Skip partition entries: device name ends in a digit (sda1, nvme0n1p2…)
                if dev[-1].isdigit():
                    continue
                reads  += int(parts[5])   # sectors read
                writes += int(parts[9])   # sectors written
    except Exception:
        pass
    return reads * _SECTOR_BYTES, writes * _SECTOR_BYTES


def _read_netdev() -> tuple[int, int]:
    """
    Read /proc/net/dev and return cumulative (sent_bytes, recv_bytes) summed
    across all non-loopback network interfaces.
    """
    sent = recv = 0
    try:
        with open("/proc/net/dev") as f:
            for line in f:
                if ":" not in line:
                    continue
                iface, data = line.split(":", 1)
                if iface.strip() == "lo":
                    continue
                fields = data.split()
                recv += int(fields[0])   # bytes received  (column 1 after iface:)
                sent += int(fields[8])   # bytes transmitted (column 9)
    except Exception:
        pass
    return sent, recv


def _count_tcp_established() -> int:
    """
    Count ESTABLISHED TCP connections from /proc/net/tcp and /proc/net/tcp6.
    State 0x01 = ESTABLISHED in Linux kernel net/ipv4/tcp.c.
    """
    count = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path) as f:
                next(f)  # skip header line
                for line in f:
                    parts = line.split()
                    if len(parts) >= 4 and parts[3] == "01":
                        count += 1
        except FileNotFoundError:
            pass
    return count


def _read_n_cpus() -> int:
    """Count logical CPUs from /proc/cpuinfo (number of 'processor' entries)."""
    count = 0
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("processor"):
                    count += 1
    except Exception:
        pass
    return count or 1


# ── Docker stats helpers ──────────────────────────────────────────────────────

def _parse_docker_mem(mem_str: str):
    """Parse Docker MemUsage string e.g. '450MiB / 7.5GiB' → (used_mb, limit_mb)."""
    def _to_mb(s: str) -> float:
        s = s.strip()
        if s.endswith("GiB"):  return float(s[:-3]) * 1024
        if s.endswith("GB"):   return float(s[:-2]) * 1000
        if s.endswith("MiB"):  return float(s[:-3])
        if s.endswith("MB"):   return float(s[:-2])
        if s.endswith("KiB"):  return float(s[:-3]) / 1024
        if s.endswith("kB"):   return float(s[:-2]) / 1024
        if s.endswith("B"):    return float(s[:-1]) / (1024 * 1024)
        try:                   return float(s)
        except ValueError:     return 0.0
    parts = mem_str.split(" / ")
    if len(parts) == 2:
        return _to_mb(parts[0]), _to_mb(parts[1])
    return 0.0, 0.0


def _collect_docker_stats() -> list:
    """
    Run `docker stats --no-stream` and return cgroup-relative CPU % and cgroup
    memory per container.  Docker daemon samples internally for ~1 s so this
    always reflects an accurate ~1 s average regardless of collector interval.
    """
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
                cpu = float(d.get("CPUPerc", "0%").rstrip("%"))
                used_mb, limit_mb = _parse_docker_mem(d.get("MemUsage", "0MiB / 0MiB"))
                mem_pct = float(d.get("MemPerc", "0%").rstrip("%"))
                containers.append({
                    "name":         d.get("Name", d.get("Container", "?")),
                    "cg_cpu_pct":   round(cpu, 2),
                    "mem_mb":       round(used_mb, 1),
                    "mem_limit_mb": round(limit_mb, 1),
                    "mem_pct":      round(mem_pct, 2),
                })
            except Exception:
                pass
        return containers
    except Exception:
        return []


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

def _collect_snapshot(prev_cpu: dict, prev_disk: tuple, prev_net: tuple,
                      prev_time: float, rc, n_cpus: int,
                      docker_enabled: bool = True):
    """
    Capture one metrics snapshot using kernel /proc files for host-level metrics.

    prev_cpu   — previous /proc/stat tick counters dict (used for CPU delta)
    prev_disk  — previous (read_bytes, write_bytes) tuple from /proc/diskstats
    prev_net   — previous (sent_bytes, recv_bytes) tuple from /proc/net/dev
    prev_time  — time.monotonic() of previous reading (for MB/s rate calculation)
    n_cpus     — logical CPU count (read once at startup, stored in every snapshot)

    Returns (snap_dict, curr_cpu, curr_disk, curr_net, now).
    """
    now = time.monotonic()
    elapsed = (now - prev_time) if prev_time is not None else 1.0

    curr_cpu  = _read_proc_cpu_stat()
    curr_disk = _read_diskstats()
    curr_net  = _read_netdev()

    snap: dict = {
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "n_cpus":    n_cpus,
    }

    # ── CPU (kernel /proc/stat delta, covers user+sys+iowait+steal+irq) ───────
    cpu_pct = _cpu_pct_from_delta(prev_cpu, curr_cpu)
    if cpu_pct is not None:
        snap["cpu_pct"] = cpu_pct

    # ── Memory (/proc/meminfo) ────────────────────────────────────────────────
    snap.update(_read_meminfo())

    # ── Disk I/O rates ────────────────────────────────────────────────────────
    if prev_disk is not None and elapsed > 0:
        snap["disk_read_mb_s"]  = round((curr_disk[0] - prev_disk[0]) / elapsed / 1e6, 2)
        snap["disk_write_mb_s"] = round((curr_disk[1] - prev_disk[1]) / elapsed / 1e6, 2)

    # ── Network I/O rates ─────────────────────────────────────────────────────
    if prev_net is not None and elapsed > 0:
        snap["net_sent_mb_s"] = round((curr_net[0] - prev_net[0]) / elapsed / 1e6, 2)
        snap["net_recv_mb_s"] = round((curr_net[1] - prev_net[1]) / elapsed / 1e6, 2)

    # ── TCP connections (/proc/net/tcp + /proc/net/tcp6) ──────────────────────
    snap["tcp_established"] = _count_tcp_established()

    # ── Redis ─────────────────────────────────────────────────────────────────
    if rc is not None:
        try:
            info = rc.info()
            snap["redis_clients"]     = info.get("connected_clients", 0)
            snap["redis_mem_used_mb"] = round(info.get("used_memory", 0) / 1e6, 1)
            normal_depth = rc.llen("judge0:jobs:normal") or 0
            retry_depth  = rc.llen("judge0:jobs:retry")  or 0
            snap["redis_queue_depth"] = int(normal_depth) + int(retry_depth)
        except Exception:
            pass  # Redis disconnected mid-test — skip this snapshot's Redis fields

    # ── Docker container stats (cgroup-based, no psutil) ─────────────────────
    if docker_enabled:
        containers = _collect_docker_stats()
        if containers:
            snap["containers"] = containers

    return snap, curr_cpu, curr_disk, curr_net, now


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

    n_cpus = _read_n_cpus()

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

    # Seed counters for delta calculations
    prev_cpu  = _read_proc_cpu_stat()
    prev_disk = _read_diskstats()
    prev_net  = _read_netdev()
    prev_time = time.monotonic()

    count = 0
    with open(args.out, "w") as fh:
        print(f"Collecting system metrics every {args.interval}s  →  {args.out}")
        print(f"Host: {n_cpus} logical CPUs")
        print("Press Ctrl-C to stop.\n")
        print(f"{'Timestamp':<22} {'CPU%':>6} {'RAM%':>6} {'RAM Used':>10} "
              f"{'Disk R':>8} {'Disk W':>8} {'Net TX':>8} {'Net RX':>8} "
              f"{'TCP':>6} {'RQueue':>8} {'RMem':>8}")
        print("-" * 105)

        # Write one snapshot immediately so short tests (< interval) still
        # get at least one data point during the run.
        snap, prev_cpu, prev_disk, prev_net, prev_time = _collect_snapshot(
            prev_cpu, prev_disk, prev_net, prev_time, rc, n_cpus,
            docker_enabled=not args.no_docker,
        )
        # Tag: CPU delta covers only the seed→first-read window (milliseconds).
        # Report generator skips this snapshot for avg CPU to avoid warm-up bias.
        snap["_first_snapshot"] = True
        fh.write(json.dumps(snap) + "\n")
        fh.flush()
        count += 1

        while running:
            time.sleep(args.interval)

            snap, prev_cpu, prev_disk, prev_net, prev_time = _collect_snapshot(
                prev_cpu, prev_disk, prev_net, prev_time, rc, n_cpus,
                docker_enabled=not args.no_docker,
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
            for c in snap.get("containers", []):
                cg_cpu  = c.get("cg_cpu_pct", 0)
                cg_flag = " !!!" if cg_cpu >= 90 else (" !  " if cg_cpu >= 70 else "    ")
                print(
                    f"  {c['name']:<35}"
                    f"  cg_cpu={cg_cpu:>5.1f}%{cg_flag}"
                    f"  cg_mem={c['mem_mb']:>7.0f} MB ({c['mem_pct']:.1f}%)"
                )

    print(f"\n✓ Done.  {count} snapshots written to {args.out}")


if __name__ == "__main__":
    main()
