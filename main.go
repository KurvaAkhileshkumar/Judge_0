package main

/*
 * Judge0 Harness Load Tester
 * ──────────────────────────
 * Two modes:
 *
 *   Direct mode (default):
 *     Submits harnesses straight to Judge0 /submissions — tests sandbox capacity.
 *     Use: go run main.go -url http://localhost:2358 -users 500
 *
 *   Flask stack mode (-flask-url):
 *     Submits raw student code to the Flask API /submit — tests the full pipeline
 *     (admission control → Redis queue → Python worker → Judge0 → SSE/polling).
 *     Use: go run main.go -flask-url http://localhost:5000 -users 500
 *
 * Each virtual user:
 *   1. Picks a random problem from question_bank.json
 *   2. Picks a random solution for that problem
 *   3. Submits (either directly to Judge0 or via Flask API)
 *   4. Waits for the result and records latency + outcome
 *
 * New metrics vs. previous version:
 *   - RateLimited:   429 responses from /submit (admission control triggered)
 *   - DuplicateCount: idempotent 200 responses (same code re-submitted within 24 h)
 *   - SystemErrors:  system_error results (infra failure, not student code)
 *   - MaxQueueDepth: no longer populated (/status removed; results via SSE only)
 *   - globalLimitS:  now ceil(TCs/200)*per_tc+5 (correct for batched harness)
 */

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"
)

// ── Config ────────────────────────────────────────────────────────────────

type Config struct {
	Judge0URL    string
	FlaskAPIURL  string // when set, use Flask stack mode instead of direct Judge0
	APIKey       string
	Users        int
	RampUpSec    int
	BatchSize    int
	AcceptOnly   bool
	CallbackPort int
	CallbackHost string
	QuestionBank string
	PollInterval time.Duration
	MaxPollSecs  int
	DryRun       bool
	OutputFile   string
	RunID        string // unique suffix per run to bypass idempotency cache
}

// ── Question Bank Types ───────────────────────────────────────────────────

type QuestionBank struct {
	Problems []Problem `json:"problems"`
}

type Problem struct {
	ID            string     `json:"id"`
	Title         string     `json:"title"`
	Difficulty    string     `json:"difficulty"`
	Language      string     `json:"language"`
	PerTCLimitS   int        `json:"per_tc_limit_s"`
	MemoryLimitMB int        `json:"memory_limit_mb"`
	TestCases     []TestCase `json:"test_cases"`
	Solutions     []Solution `json:"solutions"`
}

type TestCase struct {
	ID          string `json:"id"`
	StdinText   string `json:"stdin_text"`
	Expected    string `json:"expected"`
	Description string `json:"description"`
}

type Solution struct {
	ID                      string   `json:"id"`
	Type                    string   `json:"type"`
	Description             string   `json:"description"`
	ExpectedHarnessStatuses []string `json:"expected_harness_statuses"`
	SourceCode              string   `json:"source_code"`
}

// ── Per-TC Result ─────────────────────────────────────────────────────────

type TCResult struct {
	TCID        string `json:"tc_id"`
	Description string `json:"description"`
	Stdin       string `json:"stdin"`
	Expected    string `json:"expected"`
	Got         string `json:"got,omitempty"`
	Detail      string `json:"detail,omitempty"`
	Status      string `json:"status"`
}

// ── Judge0 Types ──────────────────────────────────────────────────────────

type Judge0SubmitRequest struct {
	SourceCode    string `json:"source_code"`
	LanguageID    int    `json:"language_id"`
	Stdin         string `json:"stdin"`
	CPUTimeLimit  int    `json:"cpu_time_limit"`
	WallTimeLimit int    `json:"wall_time_limit"`
	MemoryLimit   int    `json:"memory_limit"`
	Base64Encoded bool   `json:"base64_encoded"`
	// Both true → isolate_job.rb omits --cg. Required for Docker Desktop Mac.
	EnablePerProcessThreadTimeLimit   bool   `json:"enable_per_process_and_thread_time_limit"`
	EnablePerProcessThreadMemoryLimit bool   `json:"enable_per_process_and_thread_memory_limit"`
	CallbackURL                       string `json:"callback_url,omitempty"`
}

type Judge0SubmitResponse struct {
	Token string `json:"token"`
}

type Judge0StatusResponse struct {
	Token  string `json:"token"`
	Status struct {
		ID          int    `json:"id"`
		Description string `json:"description"`
	} `json:"status"`
	Stdout        string `json:"stdout"`
	Stderr        string `json:"stderr"`
	CompileOutput string `json:"compile_output"`
	Time          string `json:"time"`
	Memory        int    `json:"memory"`
}

// ── Flask API Types ───────────────────────────────────────────────────────

type FlaskSubmitRequest struct {
	StudentID     string              `json:"student_id"`
	AssessmentID  string              `json:"assessment_id"`
	Language      string              `json:"language"`
	StudentCode   string              `json:"student_code"`
	TestCases     []map[string]string `json:"test_cases"`
	Mode          string              `json:"mode"`
	PerTCLimitS   int                 `json:"per_tc_limit_s"`
	MemoryLimitMB int                 `json:"memory_limit_mb"`
}

type FlaskSubmitResponse struct {
	TicketID string `json:"ticket_id"`
	Status   string `json:"status"` // "queued" | "duplicate"
	Error    string `json:"error,omitempty"`
}

type FlaskStatusResponse struct {
	Status         string      `json:"status"` // "pending" | "done"
	QueueDepth     int64       `json:"queue_depth"`
	EstimatedWaitS float64     `json:"estimated_wait_s"`
	Result         *FlaskResult `json:"result,omitempty"`
}

type FlaskResult struct {
	Score         int             `json:"score"`
	Total         int             `json:"total"`
	GlobalTLE     bool            `json:"global_tle"`
	TCResults     []FlaskTCResult `json:"tc_results"`
	SystemError   string          `json:"system_error"`
	SecurityError string          `json:"security_error"`
}

type FlaskTCResult struct {
	TCNum    int    `json:"tc_num"`
	Status   string `json:"status"`
	Got      string `json:"got"`
	Expected string `json:"expected"`
	Detail   string `json:"detail"`
}

// ── Result Types ──────────────────────────────────────────────────────────

type SubmissionResult struct {
	UserID        int
	ProblemID     string
	SolutionID    string
	SolutionType  string
	HarnessStatus string // PASS/FAIL/TLE/ERROR/BLOCKED/JUDGE0_ERROR/SYSTEM_ERROR/RATE_LIMITED
	TCDetails     []TCResult
	LatencyMs     int64
	Judge0Status  string
	Error         string
	GlobalTLE     bool
	Score         int
	TotalTCs      int
}

// ── Metrics ───────────────────────────────────────────────────────────────

type Metrics struct {
	mu sync.Mutex

	TotalSubmissions int64
	Completed        int64
	Errors           int64
	Blocked          int64
	RateLimited      int64 // 429 from /submit — admission control triggered
	DuplicateCount   int64 // 200 idempotent — same code resubmitted within 24 h
	SystemErrors     int64 // system_error in result — infra failure, not student code

	StatusCounts  map[string]int64
	ProblemCounts map[string]int64
	TypeCounts    map[string]int64

	Latencies []int64

	InFlight      int64 // atomic
	MaxInFlight   int64 // atomic
	MaxQueueDepth int64 // max queue_depth seen (no longer populated — kept for JSON compat)

	StartTime time.Time
	EndTime   time.Time

	Results []SubmissionResult
}

func NewMetrics() *Metrics {
	return &Metrics{
		StatusCounts:  make(map[string]int64),
		ProblemCounts: make(map[string]int64),
		TypeCounts:    make(map[string]int64),
		StartTime:     time.Now(),
	}
}

func (m *Metrics) Record(r SubmissionResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Completed++
	m.Latencies = append(m.Latencies, r.LatencyMs)

	switch r.HarnessStatus {
	case "JUDGE0_ERROR":
		m.Errors++
	case "BLOCKED":
		m.Blocked++
	case "RATE_LIMITED":
		m.RateLimited++
	case "SYSTEM_ERROR":
		m.SystemErrors++
	}

	status := r.HarnessStatus
	if status == "" {
		status = "UNKNOWN"
	}
	m.StatusCounts[status]++
	m.ProblemCounts[r.ProblemID]++
	m.TypeCounts[r.SolutionType]++
	m.Results = append(m.Results, r)
}

func (m *Metrics) UpdateMaxQueueDepth(d int64) {
	for {
		cur := atomic.LoadInt64(&m.MaxQueueDepth)
		if d <= cur {
			return
		}
		if atomic.CompareAndSwapInt64(&m.MaxQueueDepth, cur, d) {
			return
		}
	}
}

func (m *Metrics) Print(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()

	duration := m.EndTime.Sub(m.StartTime).Seconds()
	if duration == 0 {
		duration = 1
	}
	throughput := float64(m.Completed) / duration

	var totalLatency int64
	for _, l := range m.Latencies {
		totalLatency += l
	}
	var avgLatency float64
	if len(m.Latencies) > 0 {
		avgLatency = float64(totalLatency) / float64(len(m.Latencies))
	}

	sorted := make([]int64, len(m.Latencies))
	copy(sorted, m.Latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var maxLatency int64
	if len(sorted) > 0 {
		maxLatency = sorted[len(sorted)-1]
	}

	batchEquivalent := m.TotalSubmissions * 4

	mode := "Direct Judge0"
	if cfg.FlaskAPIURL != "" {
		mode = "Flask Stack (full pipeline)"
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              JUDGE0 HARNESS LOAD TEST RESULTS               ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Printf("  %-32s %s\n", "Test Mode:", mode)
	fmt.Printf("  %-32s %d\n", "Total Users:", cfg.Users)
	fmt.Printf("  %-32s %.1fs\n", "Test Duration:", duration)
	fmt.Printf("  %-32s %d\n", "Total Submissions:", m.TotalSubmissions)
	fmt.Printf("  %-32s %d\n", "Completed:", m.Completed)
	fmt.Println()

	fmt.Println("  ── Submission Outcomes ────────────────────────────────────")
	fmt.Printf("  %-32s %d\n", "Errors (network/Judge0):", m.Errors)
	fmt.Printf("  %-32s %d\n", "Blocked by security checker:", m.Blocked)
	if cfg.FlaskAPIURL != "" {
		fmt.Printf("  %-32s %d\n", "Rate Limited (429):", m.RateLimited)
		fmt.Printf("  %-32s %d\n", "Duplicate (idempotent):", m.DuplicateCount)
		fmt.Printf("  %-32s %d\n", "System Errors (infra):", m.SystemErrors)
	}
	fmt.Println()

	fmt.Println("  ── Queue Depth Comparison ─────────────────────────────────")
	fmt.Printf("  %-32s %d\n", "Harness jobs (actual):", m.TotalSubmissions)
	fmt.Printf("  %-32s %d\n", "Batch equivalent jobs:", batchEquivalent)
	fmt.Printf("  %-32s %.1fx\n", "Queue reduction factor:", float64(batchEquivalent)/float64(max64(m.TotalSubmissions, 1)))
	fmt.Printf("  %-32s %d\n", "Peak in-flight jobs:", m.MaxInFlight)
	if cfg.FlaskAPIURL != "" {
		fmt.Printf("  %-32s %s\n", "Result delivery:", "SSE (/results/stream)")
	}
	fmt.Println()

	fmt.Println("  ── Latency ────────────────────────────────────────────────")
	fmt.Printf("  %-32s %.0fms\n", "Average:", avgLatency)
	fmt.Printf("  %-32s %dms\n", "p50:", percentile(sorted, 50))
	fmt.Printf("  %-32s %dms\n", "p95:", percentile(sorted, 95))
	fmt.Printf("  %-32s %dms\n", "p99:", percentile(sorted, 99))
	fmt.Printf("  %-32s %dms\n", "Max:", maxLatency)
	fmt.Println()

	fmt.Println("  ── Throughput ─────────────────────────────────────────────")
	fmt.Printf("  %-32s %.2f/sec\n", "Submissions/sec:", throughput)
	fmt.Println()

	fmt.Println("  ── Result Status Breakdown ────────────────────────────────")
	statuses := []string{"PASS", "FAIL", "TLE", "ERROR", "BLOCKED", "SYSTEM_ERROR", "RATE_LIMITED", "JUDGE0_ERROR", "UNKNOWN"}
	for _, s := range statuses {
		count := m.StatusCounts[s]
		if count > 0 {
			pct := float64(count) / float64(m.Completed) * 100
			fmt.Printf("  %-32s %d (%.1f%%)\n", s+":", count, pct)
		}
	}
	fmt.Println()

	fmt.Println("  ── Solution Type Breakdown ────────────────────────────────")
	for stype, count := range m.TypeCounts {
		fmt.Printf("  %-32s %d\n", stype+":", count)
	}
	fmt.Println()

	fmt.Println("  ── Per-Problem Breakdown ──────────────────────────────────")
	for pid, count := range m.ProblemCounts {
		fmt.Printf("  %-32s %d submissions\n", pid+":", count)
	}
	fmt.Println()
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ── Batch Record ─────────────────────────────────────────────────────────

type BatchRecord struct {
	BatchID   int
	StartTime time.Time
	EndTime   time.Time
	Results   []SubmissionResult
}

func (b *BatchRecord) Duration() time.Duration { return b.EndTime.Sub(b.StartTime) }

func (b *BatchRecord) Passed() int {
	n := 0
	for _, r := range b.Results {
		if r.HarnessStatus == "PASS" {
			n++
		}
	}
	return n
}

func (b *BatchRecord) AvgLatencyMs() float64 {
	if len(b.Results) == 0 {
		return 0
	}
	var sum int64
	for _, r := range b.Results {
		sum += r.LatencyMs
	}
	return float64(sum) / float64(len(b.Results))
}

func (b *BatchRecord) MaxLatencyMs() int64 {
	var mx int64
	for _, r := range b.Results {
		if r.LatencyMs > mx {
			mx = r.LatencyMs
		}
	}
	return mx
}

func runBatch(
	batchID int,
	userIDs []int,
	bank QuestionBank,
	client *Judge0Client,
	flaskClient *FlaskClient,
	metrics *Metrics,
	cfg Config,
	inFlight *int64,
	maxInFlight *int64,
	cs *CallbackServer,
) BatchRecord {
	br := BatchRecord{BatchID: batchID, StartTime: time.Now()}

	resCh := make(chan SubmissionResult, len(userIDs))
	var wg sync.WaitGroup

	for _, uid := range userIDs {
		wg.Add(1)
		go func(uid int) {
			defer wg.Done()
			var innerWG sync.WaitGroup
			innerWG.Add(1)
			metrics.mu.Lock()
			snapshotLen := len(metrics.Results)
			metrics.mu.Unlock()

			runUser(uid, bank, client, flaskClient, metrics, cfg, &innerWG, inFlight, maxInFlight, cs)
			innerWG.Wait()

			metrics.mu.Lock()
			if len(metrics.Results) > snapshotLen {
				resCh <- metrics.Results[len(metrics.Results)-1]
			}
			metrics.mu.Unlock()
		}(uid)
	}

	wg.Wait()
	close(resCh)
	for r := range resCh {
		br.Results = append(br.Results, r)
	}
	br.EndTime = time.Now()
	return br
}

func runPipelinedBatches(
	bank QuestionBank,
	client *Judge0Client,
	flaskClient *FlaskClient,
	metrics *Metrics,
	cfg Config,
	inFlight *int64,
	maxInFlight *int64,
	cs *CallbackServer,
) []BatchRecord {
	numBatches := (cfg.Users + cfg.BatchSize - 1) / cfg.BatchSize

	resultMode := "Polling"
	if cs != nil {
		resultMode = fmt.Sprintf("Callback → %s", cs.URL(cfg.CallbackHost, cfg.CallbackPort))
	}
	if cfg.FlaskAPIURL != "" {
		resultMode = "Flask /results/stream (SSE)"
	}

	fmt.Printf("\n  ═══════════════════════════════════════════════════════════\n")
	fmt.Printf("  JUDGE0 PIPELINED BATCH LOAD TEST\n")
	fmt.Printf("  ═══════════════════════════════════════════════════════════\n")
	fmt.Printf("  Batches:       %d\n", numBatches)
	fmt.Printf("  Users/batch:   %d\n", cfg.BatchSize)
	fmt.Printf("  Total users:   %d\n", cfg.Users)
	fmt.Printf("  Pipeline mode: all %d batches fired simultaneously\n", numBatches)
	fmt.Printf("  Result mode:   %s\n", resultMode)
	fmt.Printf("  ═══════════════════════════════════════════════════════════\n\n")

	batchUserIDs := make([][]int, numBatches)
	uid := 1
	for b := 0; b < numBatches; b++ {
		end := uid + cfg.BatchSize
		if end > cfg.Users+1 {
			end = cfg.Users + 1
		}
		for u := uid; u < end; u++ {
			batchUserIDs[b] = append(batchUserIDs[b], u)
		}
		uid = end
	}

	batchResults := make([]BatchRecord, numBatches)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for b := 0; b < numBatches; b++ {
		wg.Add(1)
		go func(bIdx int) {
			defer wg.Done()
			fmt.Printf("  [Batch %02d] FIRED  @ %s  (%d users)\n",
				bIdx+1, time.Now().Format("15:04:05.000"), len(batchUserIDs[bIdx]))

			br := runBatch(bIdx+1, batchUserIDs[bIdx], bank, client, flaskClient, metrics, cfg, inFlight, maxInFlight, cs)

			mu.Lock()
			batchResults[bIdx] = br
			mu.Unlock()

			passed := br.Passed()
			total := len(br.Results)
			pct := 0.0
			if total > 0 {
				pct = float64(passed) / float64(total) * 100
			}
			fmt.Printf("  [Batch %02d] DONE   @ %s  duration=%.2fs  passed=%d/%d (%.0f%%)  avg_lat=%.0fms\n",
				bIdx+1, time.Now().Format("15:04:05.000"),
				br.Duration().Seconds(), passed, total, pct, br.AvgLatencyMs())
		}(b)
	}
	wg.Wait()
	return batchResults
}

// ── Python Harness Builder (direct mode only) ─────────────────────────────

const pythonHarnessTemplate = `import signal
import sys
import io
import os
import select
import time
import traceback
import json
import builtins

MODE  = "stdio"
DELIM = "@@TC_RESULT__{{.SessionID}}__"

_real_open   = open
_real_signal = signal.signal
_HARNESS_FILE = __file__

def _safe_open(file, mode="r", *a, **kw):
    try:
        if os.path.abspath(str(file)) == os.path.abspath(_HARNESS_FILE):
            raise PermissionError("Access denied")
    except PermissionError:
        raise
    except Exception:
        raise PermissionError("Access denied")
    return _real_open(file, mode, *a, **kw)

builtins.open = _safe_open

_sig_override = [False]
def _safe_signal(signum, handler):
    if signum == signal.SIGALRM:
        _sig_override[0] = True
        return
    return _real_signal(signum, handler)
signal.signal = _safe_signal

def _safe_exit(*a): raise SystemExit("__HARNESS_BLOCKED__")
sys.exit = builtins.exit = builtins.quit = _safe_exit

_STUDENT_SOURCE = {{.StudentCodeRepr}}

def _run_tc_child(tc, per_tc_limit_s, write_fd):
    class _TLE(Exception): pass
    def _tle(s, f): raise _TLE()

    fake_in  = io.StringIO(tc["stdin_text"])
    fake_out = io.StringIO()
    sys.stdin  = fake_in
    sys.stdout = fake_out
    _real_signal(signal.SIGALRM, _tle)
    signal.alarm(per_tc_limit_s)
    try:
        ns = {"__name__": "__main__", "open": _safe_open, "exit": _safe_exit, "quit": _safe_exit}
        exec(compile(_STUDENT_SOURCE, "<student>", "exec"), ns)
        signal.alarm(0)
        got      = fake_out.getvalue().strip()
        expected = str(tc["expected"]).strip()
        result   = {"status": "PASS" if got == expected else "FAIL",
                    "got": got, "expected": expected}
        if _sig_override[0]:
            result["warning"] = "signal_override_attempted"
    except _TLE:
        result = {"status": "TLE", "detail": f"Exceeded {per_tc_limit_s}s"}
    except SystemExit as e:
        signal.alarm(0)
        msg = "Called sys.exit()" if "__HARNESS_BLOCKED__" in str(e) else "SystemExit"
        result = {"status": "ERROR", "detail": msg}
    except Exception:
        signal.alarm(0)
        lines = traceback.format_exc().strip().splitlines()
        result = {"status": "ERROR", "detail": " | ".join(lines[-2:])}
    try:
        os.write(write_fd, json.dumps(result).encode())
        os.close(write_fd)
    except OSError:
        pass
    os._exit(0)

def _run_all(test_cases, per_tc_limit_s):
    _real_out = sys.stdout
    pids, fds = [], []

    for i, tc in enumerate(test_cases):
        try:
            r, w = os.pipe()
        except OSError:
            pids.append(None); fds.append((i, None)); continue
        try:
            pid = os.fork()
        except OSError:
            os.close(r); os.close(w)
            pids.append(None); fds.append((i, None)); continue
        if pid == 0:
            os.close(r)
            _run_tc_child(tc, per_tc_limit_s, w)
        os.close(w)
        pids.append(pid)
        fds.append((i, r))

    results  = [None] * len(test_cases)
    open_fds = [r for _, r in fds if r is not None]
    fd_idx   = {r: i for i, r in fds if r is not None}
    bufs     = {r: b"" for r in open_fds}
    t0       = time.monotonic()

    while open_fds:
        remaining = per_tc_limit_s + 1.5 - (time.monotonic() - t0)
        if remaining <= 0:
            break
        try:
            ready, _, _ = select.select(open_fds, [], [], remaining)
        except (ValueError, OSError):
            break
        if not ready:
            break
        for fd in list(ready):
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                chunk = b""
            if chunk:
                bufs[fd] += chunk
            else:
                idx = fd_idx[fd]
                try:
                    results[idx] = json.loads(bufs[fd].decode())
                except Exception:
                    results[idx] = {"status": "ERROR", "detail": "result parse error"}
                try: os.close(fd)
                except OSError: pass
                open_fds.remove(fd)

    for pid in pids:
        if pid is not None:
            try: os.waitpid(pid, os.WNOHANG)
            except OSError: pass

    for i in range(len(results)):
        if results[i] is None:
            results[i] = {"status": "TLE", "detail": f"Exceeded {per_tc_limit_s}s"}

    for i, r in enumerate(results):
        _real_out.write(f"{DELIM}START_{i+1}\n")
        _real_out.write(json.dumps(r) + "\n")
        _real_out.write(f"{DELIM}END_{i+1}\n")
        _real_out.flush()
    _real_out.write(f"{DELIM}DONE\n")
    _real_out.flush()

_TEST_CASES     = {{.TestCasesJSON}}
_PER_TC_LIMIT_S = {{.PerTCLimitS}}

_run_all(_TEST_CASES, _PER_TC_LIMIT_S)
`

type HarnessParams struct {
	SessionID       string
	StudentCodeRepr string
	TestCasesJSON   string
	PerTCLimitS     int
}

func buildHarness(p Problem, sol Solution, sessionID string) (string, error) {
	tcList := make([]map[string]string, len(p.TestCases))
	for i, tc := range p.TestCases {
		tcList[i] = map[string]string{
			"stdin_text": tc.StdinText,
			"expected":   tc.Expected,
		}
	}
	tcJSON, err := json.Marshal(tcList)
	if err != nil {
		return "", err
	}

	studentCodeRepr := pythonRepr(sol.SourceCode)

	tmpl, err := template.New("harness").Parse(pythonHarnessTemplate)
	if err != nil {
		return "", err
	}

	params := HarnessParams{
		SessionID:       sessionID,
		StudentCodeRepr: studentCodeRepr,
		TestCasesJSON:   string(tcJSON),
		PerTCLimitS:     p.PerTCLimitS,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func pythonRepr(code string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(code))
	return fmt.Sprintf(`__import__('base64').b64decode('%s').decode()`, encoded)
}

// globalLimitS computes the Judge0 cpu_time_limit for a harness job.
// The Python harness batches TCs in groups of MAX_PARALLEL_TCS=200, running
// each batch in parallel.  Total wall time = ceil(N/200) * per_tc + overhead.
// For the question_bank (4 TCs): ceil(4/200)*2+5 = 7s.
// For a 1000-TC problem:         ceil(1000/200)*2+5 = 15s.
func globalLimitS(tcCount, perTCLimitS int) int {
	batches := int(math.Ceil(float64(tcCount) / 200.0))
	if batches < 1 {
		batches = 1
	}
	return batches*perTCLimitS + 5
}

// ── AST Pre-Check (Go-side, direct mode only) ─────────────────────────────

func quickSyntaxCheck(code string) string {
	lines := strings.Split(code, "\n")
	for _, line := range lines {
		stripped := strings.TrimSpace(line)
		for _, kw := range []string{"def ", "if ", "for ", "while ", "class ", "elif ", "else", "try", "except", "with "} {
			if strings.HasPrefix(stripped, kw) && !strings.Contains(stripped, ":") &&
				!strings.HasSuffix(stripped, "\\") && stripped != "" {
				return fmt.Sprintf("Possible SyntaxError: missing colon in '%s'", stripped)
			}
		}
	}
	return ""
}

// ── Judge0 Client (direct mode) ───────────────────────────────────────────

type Judge0Client struct {
	BaseURL      string
	APIKey       string
	HTTPClient   *http.Client
	PollInterval time.Duration
	MaxPollSecs  int
	submitSem    chan struct{}
	pollSem      chan struct{}
}

func NewJudge0Client(cfg Config) *Judge0Client {
	return &Judge0Client{
		BaseURL: cfg.Judge0URL,
		APIKey:  cfg.APIKey,
		HTTPClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		PollInterval: cfg.PollInterval,
		MaxPollSecs:  cfg.MaxPollSecs,
		submitSem:    make(chan struct{}, 15),
		pollSem:      make(chan struct{}, 15),
	}
}

func (c *Judge0Client) Submit(harness string, cpuLimitS int, memLimitMB int, callbackURL string) (string, error) {
	encoded := base64.StdEncoding.EncodeToString([]byte(harness))

	req := Judge0SubmitRequest{
		SourceCode:    encoded,
		LanguageID:    71,
		Stdin:         "",
		CPUTimeLimit:  cpuLimitS,
		WallTimeLimit: cpuLimitS + 2,
		MemoryLimit:   memLimitMB * 1024,
		Base64Encoded: true,
		EnablePerProcessThreadTimeLimit:   true,
		EnablePerProcessThreadMemoryLimit: true,
		CallbackURL:                       callbackURL,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	httpReq, err := http.NewRequest("POST",
		c.BaseURL+"/submissions?base64_encoded=true&wait=false",
		bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		httpReq.Header.Set("X-Auth-Token", c.APIKey)
	}

	c.submitSem <- struct{}{}
	resp, err := c.HTTPClient.Do(httpReq)
	<-c.submitSem
	if err != nil {
		return "", fmt.Errorf("submit HTTP error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("submit failed %d: %s", resp.StatusCode, string(b))
	}

	var result Judge0SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.Token, nil
}

func (c *Judge0Client) Poll(token string) (*Judge0StatusResponse, error) {
	deadline := time.Now().Add(time.Duration(c.MaxPollSecs) * time.Second)
	time.Sleep(2 * time.Second)

	for time.Now().Before(deadline) {
		c.pollSem <- struct{}{}
		url := fmt.Sprintf("%s/submissions/%s?base64_encoded=true", c.BaseURL, token)
		httpReq, err := http.NewRequest("GET", url, nil)
		if err != nil {
			<-c.pollSem
			return nil, err
		}
		if c.APIKey != "" {
			httpReq.Header.Set("X-Auth-Token", c.APIKey)
		}

		resp, err := c.HTTPClient.Do(httpReq)
		<-c.pollSem
		if err != nil {
			return nil, fmt.Errorf("poll HTTP error: %w", err)
		}

		var result Judge0StatusResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		statusID := result.Status.ID
		if statusID == 1 || statusID == 2 {
			time.Sleep(c.PollInterval)
			continue
		}
		return &result, nil
	}
	return nil, fmt.Errorf("poll timeout after %ds", c.MaxPollSecs)
}

// ── Callback Server (direct mode) ─────────────────────────────────────────

type CallbackServer struct {
	pending sync.Map
	server  *http.Server
}

func NewCallbackServer() *CallbackServer { return &CallbackServer{} }

func (cs *CallbackServer) Register(token string) <-chan Judge0StatusResponse {
	ch := make(chan Judge0StatusResponse, 1)
	cs.pending.Store(token, ch)
	return ch
}

func (cs *CallbackServer) handleResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var payload Judge0StatusResponse
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Token == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	if v, ok := cs.pending.LoadAndDelete(payload.Token); ok {
		v.(chan Judge0StatusResponse) <- payload
	}
}

func (cs *CallbackServer) Start(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/result", cs.handleResult)
	cs.server = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		if err := cs.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		return fmt.Errorf("callback server: %w", err)
	case <-time.After(150 * time.Millisecond):
		return nil
	}
}

func (cs *CallbackServer) Stop() {
	if cs.server != nil {
		cs.server.Close()
	}
}

func (cs *CallbackServer) URL(host string, port int) string {
	return fmt.Sprintf("http://%s:%d/result", host, port)
}

// ── Flask API Client (Flask stack mode) ──────────────────────────────────

type FlaskClient struct {
	BaseURL      string
	HTTPClient   *http.Client
	PollInterval time.Duration
	MaxPollSecs  int
}

func NewFlaskClient(cfg Config) *FlaskClient {
	return &FlaskClient{
		BaseURL:      cfg.FlaskAPIURL,
		HTTPClient:   &http.Client{Timeout: 30 * time.Second},
		PollInterval: cfg.PollInterval,
		MaxPollSecs:  cfg.MaxPollSecs,
	}
}

// Submit posts to /submit. Returns (ticketID, submitStatus, error).
// submitStatus is one of: "queued", "duplicate", "rate_limited".
func (f *FlaskClient) Submit(req FlaskSubmitRequest) (string, string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", "", err
	}
	resp, err := f.HTTPClient.Post(f.BaseURL+"/submit", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", "", fmt.Errorf("submit: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return "", "rate_limited", nil
	}

	var result FlaskSubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", err
	}
	if result.Error != "" {
		return "", "", fmt.Errorf("api error: %s", result.Error)
	}
	return result.TicketID, result.Status, nil
}

// StreamResult connects to GET /results/stream/{ticketID} (SSE) and waits
// for the single "result" event.  Returns the parsed FlaskStatusResponse
// with Status="done" and the result populated.
func (f *FlaskClient) StreamResult(ticketID string) (*FlaskStatusResponse, error) {
	timeout := time.Duration(f.MaxPollSecs) * time.Second
	req, err := http.NewRequest("GET", f.BaseURL+"/results/stream/"+ticketID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimPrefix(line, "data:")
		data = strings.TrimSpace(data)

		var result FlaskResult
		if err := json.Unmarshal([]byte(data), &result); err != nil {
			return nil, fmt.Errorf("stream decode: %w", err)
		}
		return &FlaskStatusResponse{
			Status: "done",
			Result: &result,
		}, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("stream read: %w", err)
	}
	return nil, fmt.Errorf("stream closed without result event after %s", timeout)
}

// HealthCheck calls /health and returns an error if the system is not "ok".
func (f *FlaskClient) HealthCheck() (string, error) {
	resp, err := f.HTTPClient.Get(f.BaseURL + "/health")
	if err != nil {
		return "", fmt.Errorf("health check: %w", err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	status, _ := result["status"].(string)
	if resp.StatusCode == 503 {
		return status, fmt.Errorf("system unavailable: status=%s", status)
	}
	return status, nil
}

// ── Output Parser (direct mode) ───────────────────────────────────────────

type ParsedResult struct {
	TCDetails []TCResult
	Score     int
	Total     int
	GlobalTLE bool
}

func parseHarnessOutput(stdout, sessionID string, testCases []TestCase) ParsedResult {
	totalTCs := len(testCases)
	delim := fmt.Sprintf("@@TC_RESULT__%s__", sessionID)
	doneMarker := delim + "DONE"
	globalTLE := !strings.Contains(stdout, doneMarker)

	result := ParsedResult{
		TCDetails: make([]TCResult, totalTCs),
		Total:     totalTCs,
		GlobalTLE: globalTLE,
	}

	for i, tc := range testCases {
		defaultStatus := "MISSING"
		if globalTLE {
			defaultStatus = "TLE"
		}
		result.TCDetails[i] = TCResult{
			TCID:        tc.ID,
			Description: tc.Description,
			Stdin:       tc.StdinText,
			Expected:    tc.Expected,
			Status:      defaultStatus,
		}
	}

	for i := 1; i <= totalTCs; i++ {
		startMarker := fmt.Sprintf("%sSTART_%d\n", delim, i)
		endMarker := fmt.Sprintf("%sEND_%d\n", delim, i)

		startIdx := strings.Index(stdout, startMarker)
		if startIdx == -1 {
			continue
		}
		startIdx += len(startMarker)

		endIdx := strings.Index(stdout[startIdx:], endMarker)
		if endIdx == -1 {
			continue
		}
		content := strings.TrimSpace(stdout[startIdx : startIdx+endIdx])

		var tcData map[string]interface{}
		if err := json.Unmarshal([]byte(content), &tcData); err != nil {
			result.TCDetails[i-1].Status = "PARSE_ERROR"
			result.TCDetails[i-1].Detail = "JSON parse error: " + err.Error()
			continue
		}

		status, _ := tcData["status"].(string)
		if status == "" {
			status = "UNKNOWN"
		}
		result.TCDetails[i-1].Status = status
		if got, ok := tcData["got"].(string); ok {
			result.TCDetails[i-1].Got = got
		}
		if detail, ok := tcData["detail"].(string); ok {
			result.TCDetails[i-1].Detail = detail
		}
		if exp, ok := tcData["expected"].(string); ok && exp != "" {
			result.TCDetails[i-1].Expected = exp
		}
		if status == "PASS" {
			result.Score++
		}
	}

	return result
}

// ── Virtual User ──────────────────────────────────────────────────────────

func runUser(
	userID int,
	bank QuestionBank,
	client *Judge0Client,
	flaskClient *FlaskClient, // non-nil → Flask stack mode
	metrics *Metrics,
	cfg Config,
	wg *sync.WaitGroup,
	inFlight *int64,
	maxInFlight *int64,
	cs *CallbackServer,
) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(userID)))

	prob := bank.Problems[rng.Intn(len(bank.Problems))]

	var candidates []Solution
	if cfg.AcceptOnly {
		for _, s := range prob.Solutions {
			if s.Type == "accepted" {
				candidates = append(candidates, s)
			}
		}
	}
	if len(candidates) == 0 {
		candidates = prob.Solutions
	}
	sol := candidates[rng.Intn(len(candidates))]

	atomic.AddInt64(&metrics.TotalSubmissions, 1)
	start := time.Now()

	result := SubmissionResult{
		UserID:       userID,
		ProblemID:    prob.ID,
		SolutionID:   sol.ID,
		SolutionType: sol.Type,
		TotalTCs:     len(prob.TestCases),
	}

	// ── Flask stack mode ──────────────────────────────────────────────────
	if flaskClient != nil {
		runUserFlask(userID, prob, sol, flaskClient, metrics, cfg, &result, start, inFlight, maxInFlight)
		return
	}

	// ── Direct Judge0 mode ────────────────────────────────────────────────

	sessionID := fmt.Sprintf("%d_%d_%d", userID, time.Now().UnixNano(), rng.Int63())

	if syntaxErr := quickSyntaxCheck(sol.SourceCode); syntaxErr != "" || sol.Type == "syntax_error" {
		result.HarnessStatus = "BLOCKED"
		result.Error = "AST check: " + syntaxErr
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	harness, err := buildHarness(prob, sol, sessionID)
	if err != nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "harness build: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	if cfg.DryRun {
		result.HarnessStatus = map[string]string{
			"accepted":            "PASS",
			"wrong_answer":        "FAIL",
			"time_limit_exceeded": "TLE",
			"runtime_error":       "ERROR",
			"syntax_error":        "BLOCKED",
		}[sol.Type]
		if result.HarnessStatus == "" {
			result.HarnessStatus = "UNKNOWN"
		}
		time.Sleep(time.Duration(500+rng.Intn(2000)) * time.Millisecond)
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	current := atomic.AddInt64(inFlight, 1)
	for {
		maxV := atomic.LoadInt64(maxInFlight)
		if current <= maxV {
			break
		}
		if atomic.CompareAndSwapInt64(maxInFlight, maxV, current) {
			break
		}
	}

	// Correct global limit: ceil(TCs/200) * per_tc + overhead
	limit := globalLimitS(len(prob.TestCases), prob.PerTCLimitS)

	var callbackURL string
	if cs != nil {
		callbackURL = cs.URL(cfg.CallbackHost, cfg.CallbackPort)
	}

	token, err := client.Submit(harness, limit, prob.MemoryLimitMB, callbackURL)
	if err != nil {
		atomic.AddInt64(inFlight, -1)
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "submit: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	var j0result *Judge0StatusResponse
	if cs != nil {
		resultCh := cs.Register(token)
		select {
		case r := <-resultCh:
			j0result = &r
		case <-time.After(time.Duration(cfg.MaxPollSecs) * time.Second):
			atomic.AddInt64(inFlight, -1)
			result.HarnessStatus = "JUDGE0_ERROR"
			result.Error = fmt.Sprintf("callback timeout after %ds", cfg.MaxPollSecs)
			result.LatencyMs = time.Since(start).Milliseconds()
			metrics.Record(result)
			return
		}
	} else {
		var pollErr error
		j0result, pollErr = client.Poll(token)
		if pollErr != nil {
			atomic.AddInt64(inFlight, -1)
			result.HarnessStatus = "JUDGE0_ERROR"
			result.Error = "poll: " + pollErr.Error()
			result.LatencyMs = time.Since(start).Milliseconds()
			metrics.Record(result)
			return
		}
	}
	atomic.AddInt64(inFlight, -1)

	result.Judge0Status = j0result.Status.Description
	result.LatencyMs = time.Since(start).Milliseconds()

	var stdout string
	if j0result.Stdout != "" {
		decoded, err := base64.StdEncoding.DecodeString(j0result.Stdout)
		if err == nil {
			stdout = string(decoded)
		} else {
			stdout = j0result.Stdout
		}
	}

	if j0result.Status.ID == 5 {
		result.HarnessStatus = "TLE"
		result.GlobalTLE = true
		result.TCDetails = make([]TCResult, len(prob.TestCases))
		for i, tc := range prob.TestCases {
			result.TCDetails[i] = TCResult{
				TCID: tc.ID, Description: tc.Description,
				Stdin: tc.StdinText, Expected: tc.Expected,
				Status: "TLE", Detail: "Judge0 global TLE",
			}
		}
	} else if j0result.Status.ID == 6 {
		result.HarnessStatus = "ERROR"
		result.Error = "Compilation error"
	} else if j0result.Status.ID == 12 || j0result.Status.ID == 13 {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = fmt.Sprintf("Judge0 infrastructure error (status %d): %s", j0result.Status.ID, j0result.Status.Description)
	} else {
		parsed := parseHarnessOutput(stdout, sessionID, prob.TestCases)
		result.TCDetails = parsed.TCDetails
		result.Score = parsed.Score
		result.GlobalTLE = parsed.GlobalTLE

		if parsed.Score == parsed.Total {
			result.HarnessStatus = "PASS"
		} else {
			statusCount := make(map[string]int)
			for _, tc := range parsed.TCDetails {
				statusCount[tc.Status]++
			}
			dominant := "FAIL"
			for _, s := range []string{"TLE", "ERROR", "FAIL"} {
				if statusCount[s] > 0 {
					dominant = s
					break
				}
			}
			result.HarnessStatus = dominant
		}
	}

	metrics.Record(result)
}

// runUserFlask handles a single virtual user in Flask stack mode.
func runUserFlask(
	userID int,
	prob Problem,
	sol Solution,
	fc *FlaskClient,
	metrics *Metrics,
	cfg Config,
	result *SubmissionResult,
	start time.Time,
	inFlight *int64,
	maxInFlight *int64,
) {
	// Build test cases list for the Flask API.
	tcs := make([]map[string]string, len(prob.TestCases))
	for i, tc := range prob.TestCases {
		tcs[i] = map[string]string{
			"stdin_text": tc.StdinText,
			"expected":   tc.Expected,
		}
	}

	req := FlaskSubmitRequest{
		StudentID:     fmt.Sprintf("user_%d_%s", userID, cfg.RunID),
		AssessmentID:  "load_test_session_1",
		Language:      prob.Language,
		StudentCode:   sol.SourceCode,
		TestCases:     tcs,
		Mode:          "stdio",
		PerTCLimitS:   prob.PerTCLimitS,
		MemoryLimitMB: prob.MemoryLimitMB,
	}

	// Track in-flight
	current := atomic.AddInt64(inFlight, 1)
	for {
		maxV := atomic.LoadInt64(maxInFlight)
		if current <= maxV {
			break
		}
		if atomic.CompareAndSwapInt64(maxInFlight, maxV, current) {
			break
		}
	}
	defer atomic.AddInt64(inFlight, -1)

	ticketID, submitStatus, err := fc.Submit(req)
	if err != nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "submit: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(*result)
		return
	}

	// Admission control: queue was full.
	if submitStatus == "rate_limited" {
		metrics.mu.Lock()
		metrics.RateLimited++
		metrics.mu.Unlock()
		result.HarnessStatus = "RATE_LIMITED"
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(*result)
		return
	}

	// Idempotent re-submit: existing result returned immediately.
	if submitStatus == "duplicate" {
		metrics.mu.Lock()
		metrics.DuplicateCount++
		metrics.mu.Unlock()
		// Still poll for the result — it already exists.
	}

	// Stream /results/stream until done.
	statusResp, err := fc.StreamResult(ticketID)
	if err != nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "poll: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(*result)
		return
	}

	result.LatencyMs = time.Since(start).Milliseconds()

	if statusResp.Result == nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "nil result in done response"
		metrics.Record(*result)
		return
	}

	fr := statusResp.Result

	// Map Flask result to SubmissionResult.
	switch {
	case fr.SystemError != "":
		metrics.mu.Lock()
		metrics.SystemErrors++
		metrics.mu.Unlock()
		result.HarnessStatus = "SYSTEM_ERROR"
		result.Error = fr.SystemError

	case fr.SecurityError != "":
		result.HarnessStatus = "BLOCKED"
		result.Error = fr.SecurityError

	case fr.Score == fr.Total:
		result.HarnessStatus = "PASS"
		result.Score = fr.Score

	default:
		result.Score = fr.Score
		// Determine dominant failure status from TC results.
		statusCount := make(map[string]int)
		for _, tc := range fr.TCResults {
			statusCount[tc.Status]++
		}
		dominant := "FAIL"
		for _, s := range []string{"TLE", "ERROR", "FAIL"} {
			if statusCount[s] > 0 {
				dominant = s
				break
			}
		}
		if fr.GlobalTLE {
			dominant = "TLE"
		}
		result.HarnessStatus = dominant
	}

	// Convert Flask TC results to the shared TCResult type.
	result.TCDetails = make([]TCResult, len(fr.TCResults))
	for i, tc := range fr.TCResults {
		result.TCDetails[i] = TCResult{
			TCID:     fmt.Sprintf("tc%d", tc.TCNum),
			Status:   tc.Status,
			Got:      tc.Got,
			Expected: tc.Expected,
			Detail:   tc.Detail,
		}
	}

	metrics.Record(*result)
}

// ── Progress Printer ──────────────────────────────────────────────────────

func printProgress(metrics *Metrics, cfg Config, done chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			completed := atomic.LoadInt64(&metrics.Completed)
			total := int64(cfg.Users)
			inFlight := atomic.LoadInt64(&metrics.InFlight)
			pct := float64(completed) / float64(total) * 100
			if cfg.FlaskAPIURL != "" {
				fmt.Printf("\r  Progress: %d/%d (%.0f%%) | In-flight: %d | Elapsed: %.0fs (SSE)",
					completed, total, pct, inFlight,
					time.Since(metrics.StartTime).Seconds())
			} else {
				fmt.Printf("\r  Progress: %d/%d (%.0f%%) | In-flight: %d | Elapsed: %.0fs",
					completed, total, pct, inFlight,
					time.Since(metrics.StartTime).Seconds())
			}
		}
	}
}

// ── Report Writer ─────────────────────────────────────────────────────────

type PerProblemSubmission struct {
	UserID        int        `json:"user_id"`
	SolutionID    string     `json:"solution_id"`
	SolutionType  string     `json:"solution_type"`
	HarnessStatus string     `json:"harness_status"`
	Score         int        `json:"score"`
	TotalTCs      int        `json:"total_tcs"`
	LatencyMs     int64      `json:"latency_ms"`
	Judge0Status  string     `json:"judge0_status,omitempty"`
	Error         string     `json:"error,omitempty"`
	TestCases     []TCResult `json:"test_cases"`
}

type PerProblemReport struct {
	ProblemID   string                 `json:"problem_id"`
	Submissions []PerProblemSubmission `json:"submissions"`
}

func buildPerProblemReport(results []SubmissionResult) []PerProblemReport {
	order := []string{}
	byProblem := map[string]*PerProblemReport{}

	for _, r := range results {
		if _, exists := byProblem[r.ProblemID]; !exists {
			order = append(order, r.ProblemID)
			byProblem[r.ProblemID] = &PerProblemReport{ProblemID: r.ProblemID}
		}
		byProblem[r.ProblemID].Submissions = append(byProblem[r.ProblemID].Submissions, PerProblemSubmission{
			UserID:        r.UserID,
			SolutionID:    r.SolutionID,
			SolutionType:  r.SolutionType,
			HarnessStatus: r.HarnessStatus,
			Score:         r.Score,
			TotalTCs:      r.TotalTCs,
			LatencyMs:     r.LatencyMs,
			Judge0Status:  r.Judge0Status,
			Error:         r.Error,
			TestCases:     r.TCDetails,
		})
	}

	report := make([]PerProblemReport, 0, len(order))
	for _, pid := range order {
		report = append(report, *byProblem[pid])
	}
	return report
}

func writeJSONReport(metrics *Metrics, cfg Config, outputFile string, batchResults []BatchRecord) error {
	sortedLats := func() []int64 {
		s := make([]int64, len(metrics.Latencies))
		copy(s, metrics.Latencies)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s
	}()

	mode := "direct_judge0"
	target := cfg.Judge0URL
	if cfg.FlaskAPIURL != "" {
		mode = "flask_stack"
		target = cfg.FlaskAPIURL
	}

	report := map[string]interface{}{
		"config": map[string]interface{}{
			"mode":        mode,
			"target":      target,
			"users":       cfg.Users,
			"ramp_up_sec": cfg.RampUpSec,
			"batch_size":  cfg.BatchSize,
			"dry_run":     cfg.DryRun,
		},
		"summary": map[string]interface{}{
			"total_submissions":   metrics.TotalSubmissions,
			"completed":           metrics.Completed,
			"errors":              metrics.Errors,
			"blocked_by_security": metrics.Blocked,
			"rate_limited_429":    metrics.RateLimited,
			"duplicate_idempotent": metrics.DuplicateCount,
			"system_errors":       metrics.SystemErrors,
			"harness_jobs":        metrics.TotalSubmissions,
			"batch_equivalent":    metrics.TotalSubmissions * 4,
			"queue_reduction":     "4x",
			"throughput_per_sec":  float64(metrics.Completed) / metrics.EndTime.Sub(metrics.StartTime).Seconds(),
			"latency_p50_ms":      percentile(sortedLats, 50),
			"latency_p95_ms":      percentile(sortedLats, 95),
			"latency_p99_ms":      percentile(sortedLats, 99),
			"peak_in_flight_jobs": metrics.MaxInFlight,
		},
		"status_counts":      metrics.StatusCounts,
		"problem_counts":     metrics.ProblemCounts,
		"type_counts":        metrics.TypeCounts,
		"results":            metrics.Results,
		"per_problem_report": buildPerProblemReport(metrics.Results),
	}

	if len(batchResults) > 0 {
		type batchJSON struct {
			BatchID       int     `json:"batch_id"`
			DurationS     float64 `json:"duration_s"`
			TotalUsers    int     `json:"total_users"`
			Passed        int     `json:"passed"`
			AcceptancePct float64 `json:"acceptance_pct"`
			AvgLatencyMs  float64 `json:"avg_latency_ms"`
			MaxLatencyMs  int64   `json:"max_latency_ms"`
		}
		batches := make([]batchJSON, 0, len(batchResults))
		for _, br := range batchResults {
			passed := br.Passed()
			total := len(br.Results)
			pct := 0.0
			if total > 0 {
				pct = float64(passed) / float64(total) * 100
			}
			batches = append(batches, batchJSON{
				BatchID:       br.BatchID,
				DurationS:     br.Duration().Seconds(),
				TotalUsers:    total,
				Passed:        passed,
				AcceptancePct: pct,
				AvgLatencyMs:  br.AvgLatencyMs(),
				MaxLatencyMs:  br.MaxLatencyMs(),
			})
		}
		report["batches"] = batches
	}

	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

// ── Main ──────────────────────────────────────────────────────────────────

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Judge0URL, "url", "http://localhost:2358", "Judge0 base URL (direct mode)")
	flag.StringVar(&cfg.FlaskAPIURL, "flask-url", "", "Flask API base URL — enables full-stack mode (e.g. http://localhost:5000)")
	flag.StringVar(&cfg.APIKey, "key", "", "Judge0 API key (X-Auth-Token, direct mode only)")
	flag.IntVar(&cfg.Users, "users", 500, "Number of virtual users")
	flag.IntVar(&cfg.RampUpSec, "ramp", 10, "Ramp-up duration in seconds (flat mode only)")
	flag.IntVar(&cfg.BatchSize, "batch", 0, "Batch size for pipelined mode (0 = flat concurrent)")
	flag.BoolVar(&cfg.AcceptOnly, "accept-only", false, "Only submit accepted solutions (100% pass target)")
	flag.IntVar(&cfg.CallbackPort, "callback-port", 0, "Callback server port, direct mode only (0 = use polling)")
	flag.StringVar(&cfg.CallbackHost, "callback-host", "host.docker.internal", "Hostname Judge0 uses to reach this machine")
	flag.StringVar(&cfg.QuestionBank, "bank", "question_bank.json", "Path to question_bank.json")
	flag.DurationVar(&cfg.PollInterval, "poll", 400*time.Millisecond, "Poll interval for results")
	flag.IntVar(&cfg.MaxPollSecs, "maxpoll", 300, "Max seconds to wait per submission result")
	flag.BoolVar(&cfg.DryRun, "dryrun", false, "Dry run: build harnesses but don't submit")
	flag.StringVar(&cfg.OutputFile, "out", "load_test_report.json", "Output JSON report file")
	flag.StringVar(&cfg.RunID, "run-id", "", "Unique run ID suffix appended to student_id to bypass idempotency cache (auto-generated if empty)")
	flag.Parse()

	// Auto-generate RunID if not provided so every run is cache-free by default
	if cfg.RunID == "" {
		cfg.RunID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	// Load question bank
	bankData, err := os.ReadFile(cfg.QuestionBank)
	if err != nil {
		log.Fatalf("Failed to read question bank: %v", err)
	}
	var bank QuestionBank
	if err := json.Unmarshal(bankData, &bank); err != nil {
		log.Fatalf("Failed to parse question bank: %v", err)
	}

	// Determine mode
	flaskMode := cfg.FlaskAPIURL != ""

	fmt.Printf("\n  Judge0 Harness Load Tester\n")
	fmt.Printf("  ───────────────────────────────────────────────\n")
	if flaskMode {
		fmt.Printf("  Mode:         Flask Stack (full pipeline)\n")
		fmt.Printf("  Target:       %s\n", cfg.FlaskAPIURL)
	} else {
		fmt.Printf("  Mode:         Direct Judge0\n")
		fmt.Printf("  Target:       %s\n", cfg.Judge0URL)
	}
	fmt.Printf("  Users:        %d\n", cfg.Users)
	if cfg.BatchSize > 0 {
		numBatches := (cfg.Users + cfg.BatchSize - 1) / cfg.BatchSize
		fmt.Printf("  Submit mode:  Pipelined batches (%d batches × %d users, all batches fire simultaneously)\n", numBatches, cfg.BatchSize)
	} else {
		fmt.Printf("  Submit mode:  Flat concurrent (ramp-up %ds)\n", cfg.RampUpSec)
	}
	fmt.Printf("  Problems:     %d\n", len(bank.Problems))
	fmt.Printf("  Solutions:    %s\n", func() string {
		if cfg.AcceptOnly {
			return "accepted only (100% pass target)"
		}
		return "random (all types)"
	}())
	if !flaskMode {
		fmt.Printf("  Result mode:  %s\n", func() string {
			if cfg.CallbackPort > 0 {
				return fmt.Sprintf("Callback (http://%s:%d/result)", cfg.CallbackHost, cfg.CallbackPort)
			}
			return fmt.Sprintf("Polling (interval=%s)", cfg.PollInterval)
		}())
	}
	fmt.Printf("  Max wait:     %ds per submission\n", cfg.MaxPollSecs)
	fmt.Printf("  Dry run:      %v\n", cfg.DryRun)
	fmt.Printf("  globalLimitS: %ds (for 4-TC problem with per_tc=2s)\n",
		globalLimitS(4, 2))
	fmt.Printf("  Batch equiv:  ~%d jobs if using per-TC batch approach\n", cfg.Users*4)
	fmt.Printf("  ───────────────────────────────────────────────\n\n")

	// ── Flask mode: pre-flight health check ──────────────────────────────
	var flaskClient *FlaskClient
	if flaskMode {
		flaskClient = NewFlaskClient(cfg)
		fmt.Printf("  Checking Flask API health...")
		healthStatus, err := flaskClient.HealthCheck()
		if err != nil {
			fmt.Printf(" FAIL\n")
			log.Fatalf("Flask API health check failed: %v\n  Run: docker compose up && gunicorn api:app ...", err)
		}
		fmt.Printf(" %s\n\n", strings.ToUpper(healthStatus))
		if healthStatus == "degraded" {
			fmt.Printf("  WARNING: system is degraded (Judge0 or circuit breaker issue). Proceeding anyway.\n\n")
		}
	}

	metrics := NewMetrics()
	client := NewJudge0Client(cfg)

	// ── Callback server (direct mode only) ───────────────────────────────
	var cs *CallbackServer
	if !flaskMode && cfg.CallbackPort > 0 {
		cs = NewCallbackServer()
		if err := cs.Start(cfg.CallbackPort); err != nil {
			log.Fatalf("Failed to start callback server on port %d: %v", cfg.CallbackPort, err)
		}
		defer cs.Stop()
		fmt.Printf("  Callback server listening on :%d\n\n", cfg.CallbackPort)
	}

	var inFlight int64
	var maxInFlight int64

	var batchResults []BatchRecord

	if cfg.BatchSize > 0 {
		batchResults = runPipelinedBatches(bank, client, flaskClient, metrics, cfg, &inFlight, &maxInFlight, cs)
		metrics.mu.Lock()
		metrics.EndTime = time.Now()
		metrics.InFlight = inFlight
		metrics.MaxInFlight = maxInFlight
		metrics.mu.Unlock()
	} else {
		var wg sync.WaitGroup
		progressDone := make(chan struct{})
		go printProgress(metrics, cfg, progressDone)

		rampDelay := time.Duration(0)
		if cfg.RampUpSec > 0 && cfg.Users > 1 {
			rampDelay = time.Duration(cfg.RampUpSec) * time.Second / time.Duration(cfg.Users)
		}

		for i := 0; i < cfg.Users; i++ {
			wg.Add(1)
			go func(uid int) {
				runUser(uid, bank, client, flaskClient, metrics, cfg, &wg, &inFlight, &maxInFlight, cs)
			}(i)
			if rampDelay > 0 && i < cfg.Users-1 {
				time.Sleep(rampDelay)
			}
		}

		wg.Wait()
		close(progressDone)

		metrics.mu.Lock()
		metrics.EndTime = time.Now()
		metrics.InFlight = inFlight
		metrics.MaxInFlight = maxInFlight
		metrics.mu.Unlock()

		fmt.Println()
	}

	metrics.Print(cfg)

	if cfg.OutputFile != "" {
		if err := writeJSONReport(metrics, cfg, cfg.OutputFile, batchResults); err != nil {
			log.Printf("Failed to write report: %v", err)
		} else {
			fmt.Printf("  JSON report written to: %s\n\n", cfg.OutputFile)
		}
	}
}
