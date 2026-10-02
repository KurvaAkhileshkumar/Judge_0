#!/usr/bin/env python3
"""
Dump Docker container logs to S3 and truncate local log files, then purge old
judge0 submissions from Postgres.
Run once daily via cron (11:59 PM IST / 18:29 UTC).

S3 structure — date first, then per-container folder:
  edwisely-logs/
    judge0/docker-logs/
      YYYY-MM-DD/
        api/            HH-MM-SS.log.gz
        grading-worker/ HH-MM-SS.log.gz
        workers/        HH-MM-SS-<name>.log.gz

server and reconciler are NOT archived (local Docker log rotation caps).

Submissions purge: grading results are persisted to the application DB via
webhook at submission time, so judge0's internal `submissions` table is only a
disposable working buffer. We keep ~1 day and delete the rest so the table (and
disk) can't grow without bound. Plain VACUUM reuses freed space, keeping the
table flat in steady state.
"""

import gzip
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

BUCKET  = "edwisely-logs"
PREFIX  = "judge0/docker-logs"
DOCKER_CONTAINERS_PATH = "/var/lib/docker/containers"

# How many days of judge0 submissions to keep before purging.
SUBMISSIONS_RETENTION_DAYS = 1
DB_CONTAINER = "judge0-db-1"

# container_name -> s3_folder
# Only these four are archived to S3. judge0-server-1 and judge0-reconciler-1
# are NOT archived — they use a local Docker log rotation cap instead (their
# logs have no archival value: server is DEBUG-level duplicate source, and
# reconciler is a tiny event trail).
CONTAINERS = {
    "judge0-workers-2":       "workers",
    "judge0-workers-3":       "workers",
    "judge0-workers-4":       "workers",
    "judge0-grading_worker-1": "grading-worker",
    "judge0-api-1":           "api",
}


def get_container_log_path(name: str) -> str | None:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format={{.Id}}", name],
            capture_output=True, text=True, timeout=5
        )
        cid = result.stdout.strip()
        if not cid:
            return None
        log_path = f"{DOCKER_CONTAINERS_PATH}/{cid}/{cid}-json.log"
        return log_path if os.path.exists(log_path) else None
    except Exception:
        return None


def upload_and_truncate(s3, name: str, folder: str, date_part: str, time_part: str) -> None:
    log_path = get_container_log_path(name)
    if not log_path:
        print(f"[SKIP] {name}: log file not found")
        return

    size = os.path.getsize(log_path)
    if size == 0:
        print(f"[SKIP] {name}: log file is empty")
        return

    # workers folder uses container name in filename to differentiate
    if folder == "workers":
        filename = f"{time_part}-{name}.log.gz"
    else:
        filename = f"{time_part}.log.gz"

    # S3 layout:  <PREFIX>/YYYY-MM-DD/<folder>/HH-MM-SS[-<name>].log.gz
    s3_key = f"{PREFIX}/{date_part}/{folder}/{filename}"

    try:
        # Read, compress, upload
        with open(log_path, "rb") as f:
            raw = f.read()

        compressed = gzip.compress(raw)
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=compressed,
            ContentEncoding="gzip",
            ContentType="text/plain",
        )

        # Truncate only after successful upload
        with open(log_path, "w") as f:
            f.truncate(0)

        print(f"[OK] {name}: {size/1024/1024:.1f} MB → s3://{BUCKET}/{s3_key} → truncated")

    except ClientError as e:
        print(f"[ERROR] {name}: S3 upload failed — {e} — log NOT truncated")
    except Exception as e:
        print(f"[ERROR] {name}: unexpected error — {e} — log NOT truncated")


def purge_old_submissions(retention_days: int = SUBMISSIONS_RETENTION_DAYS) -> None:
    """Delete judge0 submissions older than retention_days and reclaim space.

    Results already live in the application DB (written via webhook at submission
    time), so judge0's `submissions` table is a disposable buffer. Plain VACUUM
    (ANALYZE) is non-blocking and lets new rows reuse the freed space, so the
    table stays flat instead of growing forever.

    SQL is fed via stdin (simple query protocol) so each statement auto-commits
    individually — VACUUM cannot run inside a transaction block, which is what
    `psql -c "a; b"` would do.
    """
    sql = (
        f"DELETE FROM submissions WHERE created_at < now() - interval '{retention_days} day';\n"
        "VACUUM (ANALYZE) submissions;\n"
        "SELECT count(*) AS rows_remaining, "
        "pg_size_pretty(pg_total_relation_size('submissions')) AS size FROM submissions;\n"
    )
    print(f"--- purge submissions older than {retention_days}d ---")
    try:
        result = subprocess.run(
            ["docker", "exec", "-i", DB_CONTAINER,
             "psql", "-U", "judge0", "-d", "judge0", "-v", "ON_ERROR_STOP=1"],
            input=sql, capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0:
            print(f"[OK] submissions purge:\n{result.stdout.strip()}")
        else:
            print(f"[ERROR] submissions purge failed (rc={result.returncode}): "
                  f"{result.stderr.strip()}")
    except Exception as e:
        print(f"[ERROR] submissions purge — unexpected error — {e}")


def main():
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")   # YYYY-MM-DD
    time_part = now.strftime("%H-%M-%S")   # HH-MM-SS
    print(f"=== dump_docker_logs {date_part} {time_part} UTC ===")

    s3 = boto3.client("s3")

    for name, folder in CONTAINERS.items():
        upload_and_truncate(s3, name, folder, date_part, time_part)

    # Purge old submissions on the same daily run (reuses this cron).
    purge_old_submissions()

    print("=== done ===")


if __name__ == "__main__":
    main()
