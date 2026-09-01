#!/usr/bin/env python3
"""
Dump Docker container logs to S3 and truncate local log files.
Run every 15 minutes via cron.

S3 structure:
  edwisely-logs/
    judge0/docker-logs/
      server/        YYYY-MM-DD/HH-MM-SS.log.gz
      workers/       YYYY-MM-DD/HH-MM-SS-<name>.log.gz
      grading-worker/ YYYY-MM-DD/HH-MM-SS.log.gz
      api/           YYYY-MM-DD/HH-MM-SS.log.gz
      reconciler/    YYYY-MM-DD/HH-MM-SS.log.gz
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

    # S3 layout:  <PREFIX>/<folder>/YYYY-MM-DD/HH-MM-SS[-<name>].log.gz
    s3_key = f"{PREFIX}/{folder}/{date_part}/{filename}"

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


def main():
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y-%m-%d")   # YYYY-MM-DD
    time_part = now.strftime("%H-%M-%S")   # HH-MM-SS
    print(f"=== dump_docker_logs {date_part} {time_part} UTC ===")

    s3 = boto3.client("s3")

    for name, folder in CONTAINERS.items():
        upload_and_truncate(s3, name, folder, date_part, time_part)

    print("=== done ===")


if __name__ == "__main__":
    main()
