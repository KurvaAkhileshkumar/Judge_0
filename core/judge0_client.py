"""
judge0_client.py — v3
──────────────────────
Key change from v2:

  v2 (sequential harness):
      global_limit = per_tc_limit_s × tc_count + overhead
      Example: 2s × 10 TCs + 5s = 25s per job

  v3 (parallel harness):
      global_limit = per_tc_limit_s + overhead
      Example: 2s + 5s = 7s per job

  This is a 10× reduction in Judge0 job duration for 10-TC problems.
  Workers are freed 10× faster, enabling higher throughput per worker.

  The global limit is now a safety net only — the harness enforces per-TC
  limits internally (via SIGALRM per child / Thread.join with deadline).
  If the global limit fires, it means student code bypassed all internal
  enforcement, which is an extreme edge case.
"""

import time
import base64
import requests
from dataclasses import dataclass
from typing import Optional

LANGUAGE_IDS = {
    "python": 71,
    "c":      50,
    "cpp":    54,
    "java":   62,
}

JUDGE0_STATUS = {
    1:  "In Queue",
    2:  "Processing",
    3:  "Accepted",
    4:  "Wrong Answer",
    5:  "Time Limit Exceeded",
    6:  "Compilation Error",
    7:  "Runtime Error (SIGSEGV)",
    8:  "Runtime Error (SIGFPE)",
    9:  "Runtime Error (SIGABRT)",
    10: "Runtime Error (NZEC)",
    11: "Runtime Error (Other)",
    12: "Internal Error",
    13: "Exec Format Error",
}


@dataclass
class Judge0Config:
    base_url:        str
    api_key:         Optional[str] = None
    poll_interval_s: float = 0.5
    max_polls:       int   = 60


@dataclass
class Judge0Result:
    stdout:         str
    stderr:         str
    status_str:     str
    status_id:      int
    compile_output: str
    time_taken_s:   Optional[float]
    memory_kb:      Optional[int]


class Judge0Client:

    def __init__(self, config: Judge0Config):
        self.cfg     = config
        self.headers = {"Content-Type": "application/json"}
        if config.api_key:
            self.headers["X-Auth-Token"] = config.api_key

    def submit_and_wait(
        self,
        source_code:     str,
        language:        str,
        per_tc_limit_s:  int,
        tc_count:        int,       # kept for API compatibility — no longer multiplied
        memory_limit_mb: int = 256,
        overhead_s:      int = 5,   # harness startup: imports, fork overhead, JVM warmup
    ) -> Judge0Result:

        lang_id = LANGUAGE_IDS.get(language.lower())
        if not lang_id:
            raise ValueError(f"Unsupported language: {language}")

        # ── v3: parallel execution — global limit = per_tc + overhead ──
        # All TCs run simultaneously inside the harness.
        # Worst case: one TC takes the full per_tc_limit_s.
        # We add overhead_s for harness setup and process/thread launch.
        #
        # Comparison:
        #   v2 sequential: 2s × 10 TCs + 5s overhead = 25s
        #   v3 parallel:   2s           + 5s overhead =  7s  ← 3.6× faster job
        global_limit_s = per_tc_limit_s + overhead_s

        payload = {
            "source_code":     self._b64(source_code),
            "language_id":     lang_id,
            "cpu_time_limit":  global_limit_s,
            "wall_time_limit": global_limit_s + 2,
            "memory_limit":    memory_limit_mb * 1024,
            "stdin":           "",
            "base64_encoded":  True,
            # Setting both to true makes isolate_job.rb omit --cg from the
            # isolate command, so cgroup v1 directories are never needed.
            # This is required for Docker Desktop on Mac (cgroup v2 only).
            # Resource limits fall back to setrlimit(-m) and per-process SIGALRM,
            # which is perfectly adequate — the harness enforces its own limits anyway.
            "enable_per_process_and_thread_time_limit":   True,
            "enable_per_process_and_thread_memory_limit": True,
        }

        resp = requests.post(
            f"{self.cfg.base_url}/submissions?base64_encoded=true&wait=false",
            json=payload,
            headers=self.headers,
            timeout=10,
        )
        resp.raise_for_status()
        token = resp.json()["token"]

        return self._poll(token)

    def _poll(self, token: str) -> Judge0Result:
        for _ in range(self.cfg.max_polls):
            time.sleep(self.cfg.poll_interval_s)

            resp = requests.get(
                f"{self.cfg.base_url}/submissions/{token}?base64_encoded=true",
                headers=self.headers,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            status_id = data["status"]["id"]
            if status_id <= 2:
                continue

            return Judge0Result(
                stdout         = self._decode(data.get("stdout")),
                stderr         = self._decode(data.get("stderr")),
                status_str     = JUDGE0_STATUS.get(status_id, "Unknown"),
                status_id      = status_id,
                compile_output = self._decode(data.get("compile_output")),
                time_taken_s   = data.get("time"),
                memory_kb      = data.get("memory"),
            )

        raise TimeoutError(f"Judge0 did not respond after {self.cfg.max_polls} polls")

    @staticmethod
    def _b64(s: str) -> str:
        return base64.b64encode(s.encode()).decode()

    @staticmethod
    def _decode(s: Optional[str]) -> str:
        if not s:
            return ""
        try:
            return base64.b64decode(s).decode(errors="replace")
        except Exception:
            return s
