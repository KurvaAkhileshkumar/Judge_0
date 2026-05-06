"""
judge0_client.py — v4
──────────────────────
Adds callback (webhook) mode alongside polling.

  Polling  (default): client polls GET /submissions/:token every N ms.
                       Each in-flight submission holds one RAILS_MAX_THREADS slot
                       for the duration of the job.

  Callback (opt-in):  client embeds a tiny HTTP server, passes its URL in the
                       submission payload as `callback_url`.  Judge0 fires a PUT
                       to that URL the instant the job finishes.  The client
                       blocks on a threading.Event instead of polling — zero
                       HTTP traffic between submit and result.

                       RAILS_MAX_THREADS pressure drops to 2 calls per submission
                       (submit + the single incoming webhook) vs. 1 + N polls.

Usage — polling (no change from v3):
    client = Judge0Client(config)
    result = client.submit_and_wait(...)

Usage — callback:
    cb = CallbackServer()
    cb.start(port=8080, host="0.0.0.0")
    # Judge0 must reach this machine; set callback_host accordingly.
    client = Judge0Client(config, callback_server=cb,
                          callback_host="host.docker.internal", callback_port=8080)
    result = client.submit_and_wait(...)
    cb.stop()    # when done

Race condition safety
─────────────────────
Judge0 can fire the webhook before Python registers the token (e.g. a
compile error resolves in ~100 ms, faster than the Python GIL round-trip
after requests.post() returns).  CallbackServer buffers all early arrivals.
register() checks the buffer and immediately signals if the result is
already there.
"""

import json
import math
import time
import base64
import threading
import concurrent.futures
import requests
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional


# ── Fix 2.5: Circuit breaker ────────────────────────────────────────────
# Prevents retry storms when Judge0 is overloaded.  After
# _failure_threshold consecutive HTTP errors, the breaker opens for
# _recovery_s seconds.  All requests during the open window raise
# RuntimeError immediately instead of hammering Judge0 further.
class _CircuitBreaker:
    """Thread-safe circuit breaker for Judge0 HTTP calls."""

    def __init__(self, failure_threshold: int = 10, recovery_s: float = 30.0):
        self._lock      = threading.Lock()
        self._failures  = 0
        self._threshold = failure_threshold
        self._recovery  = recovery_s
        self._open_until = 0.0

    def is_open(self) -> bool:
        return time.monotonic() < self._open_until

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                self._open_until = time.monotonic() + self._recovery
                self._failures   = 0   # reset counter for next window

    def reset(self) -> None:
        """Manually close the breaker (e.g. after a health-check confirms Judge0 is up)."""
        with self._lock:
            self._failures   = 0
            self._open_until = 0.0


# Module-level singleton so all Judge0Client instances share one breaker.
_judge0_breaker = _CircuitBreaker(failure_threshold=10, recovery_s=30.0)


# Maximum test cases run in parallel inside one harness process.
# With batching, peak RSS = MAX_PARALLEL_TCS × memory_limit_mb (worst).
# Wall time = ceil(N / MAX_PARALLEL_TCS) × per_tc_limit_s + overhead.
MAX_PARALLEL_TCS = 200

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


# ── Callback server ───────────────────────────────────────────────────────

class CallbackServer:
    """
    Embedded HTTP server that receives Judge0 webhook callbacks.

    Judge0 sends PUT /result (or POST, depending on version) with the full
    submission result as JSON when a job finishes.  This server:
      1. Parses the JSON body.
      2. Stores the payload keyed by token.
      3. Signals the threading.Event that submit_and_wait() is blocking on.

    Thread safety: a single Lock guards both _events and _results so that
    deliver() and register() are atomic with respect to each other.
    """

    def __init__(self):
        self._lock    = threading.Lock()
        self._events  = {}   # token → threading.Event
        self._results = {}   # token → raw payload dict (buffer for early arrivals)
        self._httpd   = None
        self._thread  = None

    def start(self, port: int, host: str = "0.0.0.0") -> None:
        server_ref = self

        class _Handler(BaseHTTPRequestHandler):
            def do_PUT(self):  self._handle()
            def do_POST(self): self._handle()

            def _handle(self):
                length = int(self.headers.get("Content-Length", 0))
                body   = self.rfile.read(length)
                # Respond 200 immediately — Judge0 does not retry on failure.
                self.send_response(200)
                self.end_headers()
                try:
                    payload = json.loads(body)
                    token   = payload.get("token", "")
                    if token:
                        server_ref._deliver(token, payload)
                except Exception:
                    pass

            def log_message(self, *_):
                pass  # silence HTTP server logs

        # F-09/F-10 fix: replace ThreadingMixIn (1 OS thread per connection) with
        # a bounded ThreadPoolExecutor (32 threads max).
        # Under 1000 concurrent webhooks, ThreadingMixIn spawned 1000 threads
        # (~8 GB stack on Linux); 32 threads handle the same load since each
        # webhook is a tiny JSON payload resolved in < 1 ms.
        class _PooledHTTPServer(HTTPServer):
            def __init__(self, server_address, RequestHandlerClass):
                self._pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=32, thread_name_prefix="judge0-cb"
                )
                super().__init__(server_address, RequestHandlerClass)

            def process_request(self, request, client_address):
                self._pool.submit(self._handle, request, client_address)

            def _handle(self, request, client_address):
                try:
                    self.finish_request(request, client_address)
                except Exception:
                    self.handle_error(request, client_address)
                finally:
                    self.shutdown_request(request)

            def server_close(self):
                self._pool.shutdown(wait=False)
                super().server_close()

        self._httpd       = _PooledHTTPServer((host, port), _Handler)
        # F-10 fix: store the OS-assigned port so url() and actual_port are correct
        # when start(port=0) is used for automatic port selection.
        self._actual_port = self._httpd.server_address[1]
        self._thread      = threading.Thread(
            target=self._httpd.serve_forever,
            daemon=True,
            name="judge0-callback-server",
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()

    @property
    def actual_port(self) -> int:
        """Actual bound port. Correct when start(port=0) lets the OS pick."""
        return getattr(self, "_actual_port", 0)

    def url(self, host: str) -> str:
        """Callback URL using the actual OS-assigned port."""
        return f"http://{host}:{self._actual_port}/result"

    def register(self, token: str) -> threading.Event:
        """
        Register interest in a token and return an Event to block on.
        If the result already arrived (early webhook), the Event is
        pre-set so wait() returns immediately.
        """
        evt = threading.Event()
        with self._lock:
            self._events[token] = evt
            if token in self._results:
                # Result arrived before register() was called — signal now.
                evt.set()
        return evt

    def pop_result(self, token: str) -> Optional[dict]:
        """Consume and return the buffered payload for a token."""
        with self._lock:
            self._events.pop(token, None)
            return self._results.pop(token, None)

    def _deliver(self, token: str, payload: dict) -> None:
        """Called from the HTTP handler thread when a webhook arrives."""
        with self._lock:
            self._results[token] = payload
            evt = self._events.get(token)
        if evt:
            evt.set()


# ── Config / Result dataclasses ───────────────────────────────────────────

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


# ── Client ────────────────────────────────────────────────────────────────

class Judge0Client:

    def __init__(
        self,
        config:          Judge0Config,
        callback_server: Optional[CallbackServer] = None,
        callback_host:   str = "host.docker.internal",
        callback_port:   int = 0,
    ):
        self.cfg             = config
        self.callback_server = callback_server
        self.callback_host   = callback_host
        self.callback_port   = callback_port
        self.headers         = {"Content-Type": "application/json"}
        if config.api_key:
            self.headers["X-Auth-Token"] = config.api_key

    def _build_payload(
        self,
        source_code:     str,
        language:        str,
        per_tc_limit_s:  int,
        tc_count:        int,
        memory_limit_mb: int = 256,
        overhead_s:      int = 5,
    ) -> tuple:
        """Build the Judge0 submission payload. Returns (payload_dict, global_limit_s)."""
        lang_id = LANGUAGE_IDS.get(language.lower())
        if not lang_id:
            raise ValueError(f"Unsupported language: {language}")

        # Batched execution: at most MAX_PARALLEL_TCS TCs run simultaneously.
        # global limit = ceil(N / MAX_PARALLEL_TCS) × per_tc + overhead
        # Examples (per_tc=2s, overhead=5s):
        #   N≤200 → 1×2+5=7s  |  N=500 → 3×2+5=11s  |  N=1000 → 5×2+5=15s
        global_limit_s = math.ceil(max(tc_count, 1) / MAX_PARALLEL_TCS) * per_tc_limit_s + overhead_s

        # Fix 1.5: scale sandbox memory for parallel harness children.
        # In cgroup mode the memory_limit applies to the entire sandbox cgroup.
        # With MAX_PARALLEL_TCS children each needing up to memory_limit_mb,
        # the cgroup budget must cover all children + the harness process itself.
        # Cap at 3,500 MB to stay within the 4 GB container mem_limit.
        # In --no-cg mode (Mac/dev) this value is set per-child via RLIMIT_AS
        # inside the harness, so oversetting here is harmless.
        _HARNESS_OVERHEAD_MB = 256  # Python/JVM interpreter overhead
        _batch_size   = min(tc_count, MAX_PARALLEL_TCS)
        sandbox_mem   = min(
            _batch_size * memory_limit_mb + _HARNESS_OVERHEAD_MB,
            3500,   # cap: stay within 4 GB container mem_limit
        )

        payload = {
            "source_code":     self._b64(source_code),
            "language_id":     lang_id,
            "cpu_time_limit":  global_limit_s,
            "wall_time_limit": global_limit_s + 2,
            "memory_limit":    sandbox_mem * 1024,  # Judge0 expects KB
            "stdin":           "",
            "base64_encoded":  True,
            # Omits --cg from isolate so cgroup v1 dirs are not required.
            # Required for Docker Desktop on Mac (cgroup v2 only).
            "enable_per_process_and_thread_time_limit":   True,
            "enable_per_process_and_thread_memory_limit": True,
        }
        return payload, global_limit_s

    def _post_with_retry(
        self,
        path:        str,
        payload:     dict,
        timeout:     int = 120,
        max_retries: int = 3,
    ) -> dict:
        """
        POST to Judge0 with exponential back-off on transient 5xx / network
        errors (0.5 s, 1 s, 2 s).  4xx are not retried (client error).

        Fix 2.5: honours the circuit breaker.  When the breaker is open, all
        calls fail fast with RuntimeError instead of hammering Judge0 further.
        """
        if _judge0_breaker.is_open():
            raise RuntimeError(
                "Judge0 circuit breaker open: too many recent errors. "
                "Retrying in ~30s."
            )
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    f"{self.cfg.base_url}{path}",
                    json=payload,
                    headers=self.headers,
                    timeout=timeout,
                )
                if resp.status_code < 500:
                    resp.raise_for_status()
                    _judge0_breaker.record_success()
                    return resp.json()
                last_exc = requests.HTTPError(
                    f"HTTP {resp.status_code}", response=resp
                )
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
            if attempt < max_retries - 1:
                time.sleep(0.5 * (2 ** attempt))   # 0.5 s, 1 s, 2 s
        _judge0_breaker.record_failure()
        raise last_exc  # type: ignore[misc]

    def submit_only(
        self,
        source_code:     str,
        language:        str,
        per_tc_limit_s:  int,
        tc_count:        int,
        memory_limit_mb: int = 256,
        overhead_s:      int = 5,
    ) -> str:
        """
        Submit to Judge0 without waiting for the result.
        Returns the Judge0 token string for later retrieval via wait_for_token().

        Dead-letter-queue pattern:
            token = client.submit_only(...)  # returns immediately
            db.save(ticket_id, token)        # persist BEFORE waiting
            result = client.wait_for_token(token, timeout=300)

        If wait_for_token() times out or the process crashes, the token is
        already in the DB.  A background reconciler can call wait_for_token()
        again later to retrieve the result once Judge0 finishes.
        No callback_url is attached — polling is always used so that retrieval
        is safe across process restarts.
        """
        payload, _ = self._build_payload(
            source_code, language, per_tc_limit_s, tc_count, memory_limit_mb, overhead_s
        )
        data = self._post_with_retry(
            "/submissions?base64_encoded=true&wait=false", payload, timeout=120
        )
        return data["token"]

    def wait_for_token(self, token: str, timeout: int = 300) -> Judge0Result:
        """
        Poll Judge0 for a previously submitted token until a terminal status
        is returned or *timeout* seconds elapse.

        Always uses polling (not the callback server) so it is safe to call
        after a process restart — the callback server's in-memory event map
        would not survive a restart.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            resp = self._get_with_retry(
                f"{self.cfg.base_url}/submissions/{token}?base64_encoded=true"
            )
            data      = resp.json()
            status_id = data["status"]["id"]
            if status_id > 2:
                return Judge0Result(
                    stdout         = self._decode(data.get("stdout")),
                    stderr         = self._decode(data.get("stderr")),
                    status_str     = JUDGE0_STATUS.get(status_id, "Unknown"),
                    status_id      = status_id,
                    compile_output = self._decode(data.get("compile_output")),
                    time_taken_s   = _parse_time(data.get("time")),
                    memory_kb      = data.get("memory"),
                )
            time.sleep(self.cfg.poll_interval_s)
        raise TimeoutError(
            f"Judge0 token {token!r} did not reach a terminal status within {timeout}s"
        )

    def submit_and_wait(
        self,
        source_code:     str,
        language:        str,
        per_tc_limit_s:  int,
        tc_count:        int,       # kept for API compatibility — no longer multiplied
        memory_limit_mb: int = 256,
        overhead_s:      int = 5,   # harness startup: imports, fork overhead, JVM warmup
    ) -> Judge0Result:

        payload, global_limit_s = self._build_payload(
            source_code, language, per_tc_limit_s, tc_count, memory_limit_mb, overhead_s
        )

        # Attach callback URL if a started server is provided.
        # Uses actual_port (OS-assigned) not the requested port — fixes the
        # silent polling fallback when start(port=0) was used.
        use_callback = bool(self.callback_server and self.callback_server.actual_port)
        if use_callback:
            payload["callback_url"] = self.callback_server.url(self.callback_host)

        # FIX (submit timeout): with Puma's 25-thread pool and 1000 concurrent
        # users the submission POST can queue for >10 s before being handled.
        # The project memory note ("HTTP submit timeout: 120 s minimum") is
        # what this 120 s value comes from.
        data  = self._post_with_retry(
            "/submissions?base64_encoded=true&wait=false", payload, timeout=120
        )
        token = data["token"]

        if use_callback:
            return self._wait_callback(token, global_limit_s)

        # Fix 1.1: compute a safe poll budget from the actual global_limit_s
        # rather than the static max_polls=60 (30 s) default.
        # Budget = execution time + 60 s burst-queue margin + 5 s network buffer.
        # At poll_interval_s=0.5 this gives ceil((15+65)/0.5) = 160 polls for
        # a 1000-TC job, safely above the 30-s static cap that timed out under
        # burst load.
        safe_polls = math.ceil(
            (global_limit_s + 65) / max(self.cfg.poll_interval_s, 0.1)
        )
        return self._poll(token, max_polls=safe_polls)

    # ── Callback path ─────────────────────────────────────────────────────

    def _wait_callback(self, token: str, global_limit_s: int) -> Judge0Result:
        """
        Register the token, then block on the Event until Judge0 fires the
        webhook.  Timeout = global_limit_s + a generous 30s buffer for
        network latency and Judge0 queue time.
        """
        timeout_s = global_limit_s + 30
        evt       = self.callback_server.register(token)
        fired     = evt.wait(timeout=timeout_s)

        if not fired:
            raise TimeoutError(
                f"Callback not received for token {token} after {timeout_s}s"
            )

        raw = self.callback_server.pop_result(token)
        if not raw:
            raise RuntimeError(f"Callback event fired but no payload found for {token}")

        return self._parse_webhook_payload(raw)

    def _parse_webhook_payload(self, data: dict) -> Judge0Result:
        """
        Judge0 sends the webhook payload with the same shape as the polling
        response — stdout/stderr/compile_output are base64 encoded.
        """
        status_id = data.get("status", {}).get("id", 11)
        return Judge0Result(
            stdout         = self._decode(data.get("stdout")),
            stderr         = self._decode(data.get("stderr")),
            status_str     = JUDGE0_STATUS.get(status_id, "Unknown"),
            status_id      = status_id,
            compile_output = self._decode(data.get("compile_output")),
            time_taken_s   = _parse_time(data.get("time")),
            memory_kb      = data.get("memory"),
        )

    # ── Polling path ──────────────────────────────────────────────────────

    def _get_with_retry(self, url: str, max_retries: int = 3) -> requests.Response:
        """
        FIX-22: the old _poll() used requests.get(..., timeout=10) with no
        retry.  A single transient 5xx or network hiccup raised an exception
        and lost the student's result permanently.

        Strategy: exponential back-off (0.5 s, 1 s, 2 s) on Timeout,
        ConnectionError, or 5xx responses.  4xx are not retried (client error).
        Poll timeout raised to 30 s to survive a loaded Judge0 under callback
        traffic (individual polls are rare but should not time out spuriously).
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, headers=self.headers, timeout=30)
                if resp.status_code < 500:
                    resp.raise_for_status()
                    return resp
                # 5xx — Judge0 overloaded; retry after back-off
                last_exc = requests.HTTPError(
                    f"HTTP {resp.status_code}", response=resp
                )
            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
            if attempt < max_retries - 1:
                time.sleep(0.5 * (2 ** attempt))   # 0.5 s, 1 s, 2 s
        raise last_exc  # type: ignore[misc]

    def _poll(self, token: str, max_polls: int = None) -> Judge0Result:
        # Fix 1.1: accept dynamic max_polls; fall back to config default.
        polls_remaining = max_polls if max_polls is not None else self.cfg.max_polls
        # FIX-7: check BEFORE sleeping so a sub-500ms job is not forced to
        # wait poll_interval_s.  The old code always slept first.
        for _ in range(polls_remaining):
            resp = self._get_with_retry(
                f"{self.cfg.base_url}/submissions/{token}?base64_encoded=true"
            )
            data      = resp.json()
            status_id = data["status"]["id"]
            if status_id > 2:
                _judge0_breaker.record_success()
                return Judge0Result(
                    stdout         = self._decode(data.get("stdout")),
                    stderr         = self._decode(data.get("stderr")),
                    status_str     = JUDGE0_STATUS.get(status_id, "Unknown"),
                    status_id      = status_id,
                    compile_output = self._decode(data.get("compile_output")),
                    time_taken_s   = _parse_time(data.get("time")),
                    memory_kb      = data.get("memory"),
                )
            time.sleep(self.cfg.poll_interval_s)

        raise TimeoutError(f"Judge0 did not respond after {self.cfg.max_polls} polls")

    # ── Helpers ───────────────────────────────────────────────────────────

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


def _parse_time(value) -> Optional[float]:
    """Judge0 returns time as a string like '0.012' or None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
