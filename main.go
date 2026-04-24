package main

/*
 * Judge0 Harness Load Tester
 * ──────────────────────────
 * Simulates 500 concurrent students submitting solutions from
 * question_bank.json using the harness approach (1 job per submission).
 *
 * Each virtual user:
 *   1. Picks a random problem from the bank
 *   2. Picks a random solution for that problem
 *   3. Builds a Python harness (stdio mode) wrapping the student code
 *   4. Submits to Judge0 as a single job
 *   5. Polls until result, records latency + outcome
 *
 * Metrics reported:
 *   - Total submissions / success / failure
 *   - Queue depth equivalent (jobs in flight)
 *   - Latency: p50, p95, p99, max
 *   - Throughput: submissions/sec
 *   - Per-status breakdown (PASS/FAIL/TLE/ERROR/BLOCKED)
 *   - Harness vs batch job count comparison
 *   - Per-problem breakdown
 */

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
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
	APIKey       string
	Users        int
	RampUpSec    int
	QuestionBank string
	PollInterval time.Duration
	MaxPollSecs  int
	DryRun       bool   // build harness but don't submit — for local testing
	OutputFile   string // write JSON report here
}

// ── Question Bank Types ───────────────────────────────────────────────────

type QuestionBank struct {
	Problems []Problem `json:"problems"`
}

type Problem struct {
	ID             string     `json:"id"`
	Title          string     `json:"title"`
	Difficulty     string     `json:"difficulty"`
	Language       string     `json:"language"`
	PerTCLimitS    int        `json:"per_tc_limit_s"`
	MemoryLimitMB  int        `json:"memory_limit_mb"`
	TestCases      []TestCase `json:"test_cases"`
	Solutions      []Solution `json:"solutions"`
}

type TestCase struct {
	ID          string `json:"id"`
	StdinText   string `json:"stdin_text"`
	Expected    string `json:"expected"`
	Description string `json:"description"`
}

type Solution struct {
	ID                     string   `json:"id"`
	Type                   string   `json:"type"`
	Description            string   `json:"description"`
	ExpectedHarnessStatuses []string `json:"expected_harness_statuses"`
	SourceCode             string   `json:"source_code"`
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
	// Both true → isolate_job.rb omits --cg from isolate command.
	// Required for Docker Desktop on Mac (cgroup v2 only, no cgroup v1).
	EnablePerProcessThreadTimeLimit   bool `json:"enable_per_process_and_thread_time_limit"`
	EnablePerProcessThreadMemoryLimit bool `json:"enable_per_process_and_thread_memory_limit"`
}

type Judge0SubmitResponse struct {
	Token string `json:"token"`
}

type Judge0StatusResponse struct {
	Status struct {
		ID          int    `json:"id"`
		Description string `json:"description"`
	} `json:"status"`
	Stdout        string  `json:"stdout"`
	Stderr        string  `json:"stderr"`
	CompileOutput string  `json:"compile_output"`
	Time          string  `json:"time"`
	Memory        int     `json:"memory"`
}

// ── Result Types ──────────────────────────────────────────────────────────

type SubmissionResult struct {
	UserID        int
	ProblemID     string
	SolutionID    string
	SolutionType  string
	HarnessStatus string     // PASS/FAIL/TLE/ERROR/BLOCKED/JUDGE0_ERROR
	TCDetails     []TCResult // per-TC full breakdown
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

	TotalSubmissions  int64
	Completed         int64
	Errors            int64
	Blocked           int64 // stopped by AST checker

	StatusCounts map[string]int64  // PASS/FAIL/TLE/ERROR/BLOCKED
	ProblemCounts map[string]int64
	TypeCounts   map[string]int64  // solution type breakdown

	Latencies []int64 // ms

	// Queue depth tracking
	InFlight     int64  // atomic
	MaxInFlight  int64  // atomic

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

	if r.HarnessStatus == "JUDGE0_ERROR" {
		m.Errors++
	}

	status := r.HarnessStatus
	if status == "" {
		status = "UNKNOWN"
	}
	m.StatusCounts[status]++
	m.ProblemCounts[r.ProblemID]++
	m.TypeCounts[r.SolutionType]++

	if r.HarnessStatus == "BLOCKED" {
		m.Blocked++
	}

	m.Results = append(m.Results, r)
}

func (m *Metrics) Percentile(p float64) int64 {
	if len(m.Latencies) == 0 {
		return 0
	}
	sorted := make([]int64, len(m.Latencies))
	copy(sorted, m.Latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx]
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

	batchEquivalent := m.TotalSubmissions * 4 // avg 4 TCs per problem

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║              JUDGE0 HARNESS LOAD TEST RESULTS               ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Printf("  %-30s %d\n", "Total Users:", cfg.Users)
	fmt.Printf("  %-30s %.1fs\n", "Test Duration:", duration)
	fmt.Printf("  %-30s %d\n", "Total Submissions:", m.TotalSubmissions)
	fmt.Printf("  %-30s %d\n", "Completed:", m.Completed)
	fmt.Printf("  %-30s %d\n", "Errors (network/poll):", m.Errors)
	fmt.Printf("  %-30s %d\n", "Blocked by AST checker:", m.Blocked)
	fmt.Println()

	fmt.Println("  ── Queue Depth Comparison ─────────────────────────────────")
	fmt.Printf("  %-30s %d\n", "Harness jobs (actual):", m.TotalSubmissions)
	fmt.Printf("  %-30s %d\n", "Batch equivalent jobs:", batchEquivalent)
	fmt.Printf("  %-30s %.1fx\n", "Queue reduction factor:", float64(batchEquivalent)/float64(max64(m.TotalSubmissions, 1)))
	fmt.Printf("  %-30s %d\n", "Peak in-flight jobs:", m.MaxInFlight)
	fmt.Println()

	fmt.Println("  ── Latency ────────────────────────────────────────────────")
	fmt.Printf("  %-30s %.0fms\n", "Average:", avgLatency)
	fmt.Printf("  %-30s %dms\n", "p50:", percentile(sorted, 50))
	fmt.Printf("  %-30s %dms\n", "p95:", percentile(sorted, 95))
	fmt.Printf("  %-30s %dms\n", "p99:", percentile(sorted, 99))
	fmt.Printf("  %-30s %dms\n", "Max:", maxLatency)
	fmt.Println()

	fmt.Println("  ── Throughput ─────────────────────────────────────────────")
	fmt.Printf("  %-30s %.2f/sec\n", "Submissions/sec:", throughput)
	fmt.Println()

	fmt.Println("  ── Result Status Breakdown ────────────────────────────────")
	statuses := []string{"PASS", "FAIL", "TLE", "ERROR", "BLOCKED", "JUDGE0_ERROR", "UNKNOWN"}
	for _, s := range statuses {
		count := m.StatusCounts[s]
		if count > 0 {
			pct := float64(count) / float64(m.Completed) * 100
			fmt.Printf("  %-30s %d (%.1f%%)\n", s+":", count, pct)
		}
	}
	fmt.Println()

	fmt.Println("  ── Solution Type Breakdown ────────────────────────────────")
	for stype, count := range m.TypeCounts {
		fmt.Printf("  %-30s %d\n", stype+":", count)
	}
	fmt.Println()

	fmt.Println("  ── Per-Problem Breakdown ──────────────────────────────────")
	for pid, count := range m.ProblemCounts {
		fmt.Printf("  %-30s %d submissions\n", pid+":", count)
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

// ── Python Harness Builder ────────────────────────────────────────────────

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

# stdio mode: student code runs only in child processes via exec(_STUDENT_SOURCE)

_STUDENT_SOURCE = {{.StudentCodeRepr}}

def _run_tc_child(tc, per_tc_limit_s, write_fd):
    """Run one TC in a forked child, write JSON result to write_fd, then _exit."""
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
    """Fork one child per TC so all TCs run in parallel (v3 harness)."""
    _real_out = sys.stdout
    pids, fds = [], []  # fds: list of (tc_index, read_fd | None)

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
        if pid == 0:            # child — run one TC then exit
            os.close(r)
            _run_tc_child(tc, per_tc_limit_s, w)
        os.close(w)             # parent — track read end
        pids.append(pid)
        fds.append((i, r))

    # Collect results from all children simultaneously via select()
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
            else:                           # EOF — child exited
                idx = fd_idx[fd]
                try:
                    results[idx] = json.loads(bufs[fd].decode())
                except Exception:
                    results[idx] = {"status": "ERROR", "detail": "result parse error"}
                try: os.close(fd)
                except OSError: pass
                open_fds.remove(fd)

    for pid in pids:                        # reap children
        if pid is not None:
            try: os.waitpid(pid, os.WNOHANG)
            except OSError: pass

    for i in range(len(results)):           # fill any un-collected slots (crash / hard TLE)
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
	StudentCodeRepr string // base64-decoded at runtime — avoids input() at module level
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

	// Python repr of student code: wrap in triple-quotes with escaping
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

// pythonRepr wraps a string safely for embedding as a Python string literal.
// Uses base64 decode trick to avoid any escaping issues.
func pythonRepr(code string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(code))
	return fmt.Sprintf(`__import__('base64').b64decode('%s').decode()`, encoded)
}

// ── AST Pre-Check (Go-side) ───────────────────────────────────────────────
// Quick syntactic checks before sending to Judge0.
// Full AST check happens in Python security.py in production;
// here we just check for obvious SyntaxError indicators.

func quickSyntaxCheck(code string) string {
	// Check for common Python syntax errors that would cause SyntaxError
	// This is a simplified check — production uses full AST via security.py
	lines := strings.Split(code, "\n")
	for _, line := range lines {
		stripped := strings.TrimSpace(line)
		// Detect def/if/for/while/class without colon
		for _, kw := range []string{"def ", "if ", "for ", "while ", "class ", "elif ", "else", "try", "except", "with "} {
			if strings.HasPrefix(stripped, kw) && !strings.Contains(stripped, ":") &&
				!strings.HasSuffix(stripped, "\\") && stripped != "" {
				return fmt.Sprintf("Possible SyntaxError: missing colon in '%s'", stripped)
			}
		}
	}
	return ""
}

// ── Judge0 Client ─────────────────────────────────────────────────────────

type Judge0Client struct {
	BaseURL      string
	APIKey       string
	HTTPClient   *http.Client
	PollInterval time.Duration
	MaxPollSecs  int
	submitSem    chan struct{} // limits concurrent submit requests
	pollSem      chan struct{} // limits concurrent poll requests
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
		// submitSem(12) + pollSem(8) = 20 concurrent requests, matching RAILS_MAX_THREADS=20.
		// Without this, 1000 goroutines all submit simultaneously → EOF errors.
		submitSem: make(chan struct{}, 12),
		pollSem:   make(chan struct{}, 8),
	}
}

func (c *Judge0Client) Submit(harness string, cpuLimitS int, memLimitMB int) (string, error) {
	encoded := base64.StdEncoding.EncodeToString([]byte(harness))

	req := Judge0SubmitRequest{
		SourceCode:    encoded,
		LanguageID:    71, // Python 3
		Stdin:         "",
		CPUTimeLimit:  cpuLimitS,
		WallTimeLimit: cpuLimitS + 2,
		MemoryLimit:   memLimitMB * 1024, // KB
		Base64Encoded: true,
		EnablePerProcessThreadTimeLimit:   true,
		EnablePerProcessThreadMemoryLimit: true,
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

	// Wait a few seconds before the first poll — submissions take time to queue
	// and process, so immediate polls waste server capacity.
	time.Sleep(3 * time.Second)

	for time.Now().Before(deadline) {
		// Acquire semaphore before making the HTTP request.
		// This caps concurrent poll requests at pollSem capacity (20),
		// preventing 1000 goroutines from OOM-killing the Puma process.
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
		<-c.pollSem // release immediately after response
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
			continue // queued or processing
		}
		return &result, nil
	}
	return nil, fmt.Errorf("poll timeout after %ds", c.MaxPollSecs)
}

// ── Output Parser ─────────────────────────────────────────────────────────

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

	// Fill defaults
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

	// Extract TC blocks
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
		// Overwrite expected from harness output if present (authoritative)
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
	metrics *Metrics,
	cfg Config,
	wg *sync.WaitGroup,
	inFlight *int64,
	maxInFlight *int64,
) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(userID)))

	// Pick random problem
	prob := bank.Problems[rng.Intn(len(bank.Problems))]

	// Pick random solution
	sol := prob.Solutions[rng.Intn(len(prob.Solutions))]

	sessionID := fmt.Sprintf("%d_%d_%d", userID, time.Now().UnixNano(), rng.Int63())

	atomic.AddInt64(&metrics.TotalSubmissions, 1)
	start := time.Now()

	result := SubmissionResult{
		UserID:       userID,
		ProblemID:    prob.ID,
		SolutionID:   sol.ID,
		SolutionType: sol.Type,
		TotalTCs:     len(prob.TestCases),
	}

	// ── Quick syntax check (simulates AST checker) ───────────────────
	if syntaxErr := quickSyntaxCheck(sol.SourceCode); syntaxErr != "" ||
		sol.Type == "syntax_error" {
		result.HarnessStatus = "BLOCKED"
		result.Error = "AST check: " + syntaxErr
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	// ── Build harness ────────────────────────────────────────────────
	harness, err := buildHarness(prob, sol, sessionID)
	if err != nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "harness build: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	if cfg.DryRun {
		// Dry run: simulate result based on solution type
		result.HarnessStatus = map[string]string{
			"accepted":      "PASS",
			"wrong_answer":  "FAIL",
			"time_limit_exceeded": "TLE",
			"runtime_error": "ERROR",
			"syntax_error":  "BLOCKED",
		}[sol.Type]
		if result.HarnessStatus == "" {
			result.HarnessStatus = "UNKNOWN"
		}
		// Simulate realistic latency
		time.Sleep(time.Duration(500+rng.Intn(2000)) * time.Millisecond)
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	// ── Submit to Judge0 ─────────────────────────────────────────────
	current := atomic.AddInt64(inFlight, 1)
	// Update max in-flight
	for {
		max := atomic.LoadInt64(maxInFlight)
		if current <= max {
			break
		}
		if atomic.CompareAndSwapInt64(maxInFlight, max, current) {
			break
		}
	}

	// v3 parallel harness: all TCs run at t=0, worst case = 1 TC limit + overhead
	globalLimitS := prob.PerTCLimitS + 5

	token, err := client.Submit(harness, globalLimitS, prob.MemoryLimitMB)
	if err != nil {
		atomic.AddInt64(inFlight, -1)
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "submit: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	// ── Poll for result ───────────────────────────────────────────────
	j0result, err := client.Poll(token)
	atomic.AddInt64(inFlight, -1)

	if err != nil {
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = "poll: " + err.Error()
		result.LatencyMs = time.Since(start).Milliseconds()
		metrics.Record(result)
		return
	}

	result.Judge0Status = j0result.Status.Description
	result.LatencyMs = time.Since(start).Milliseconds()

	// Decode stdout
	var stdout string
	if j0result.Stdout != "" {
		decoded, err := base64.StdEncoding.DecodeString(j0result.Stdout)
		if err == nil {
			stdout = string(decoded)
		} else {
			stdout = j0result.Stdout
		}
	}

	// ── Parse harness output ──────────────────────────────────────────
	if j0result.Status.ID == 5 {
		// Judge0 global TLE — all TCs marked TLE
		result.HarnessStatus = "TLE"
		result.GlobalTLE = true
		result.TCDetails = make([]TCResult, len(prob.TestCases))
		for i, tc := range prob.TestCases {
			result.TCDetails[i] = TCResult{
				TCID:        tc.ID,
				Description: tc.Description,
				Stdin:       tc.StdinText,
				Expected:    tc.Expected,
				Status:      "TLE",
				Detail:      "Judge0 global TLE",
			}
		}
	} else if j0result.Status.ID == 6 {
		// Compilation error (shouldn't happen for Python)
		result.HarnessStatus = "ERROR"
		result.Error = "Compilation error"
	} else if j0result.Status.ID == 12 || j0result.Status.ID == 13 {
		// Internal Error or Exec Format Error — sandbox/infrastructure failure
		// (e.g. cgroup v1 missing on Docker Desktop Mac, isolate binary incompatible)
		result.HarnessStatus = "JUDGE0_ERROR"
		result.Error = fmt.Sprintf("Judge0 infrastructure error (status %d): %s", j0result.Status.ID, j0result.Status.Description)
	} else {
		parsed := parseHarnessOutput(stdout, sessionID, prob.TestCases)
		result.TCDetails = parsed.TCDetails
		result.Score = parsed.Score
		result.GlobalTLE = parsed.GlobalTLE

		// Determine overall harness status
		if parsed.Score == parsed.Total {
			result.HarnessStatus = "PASS"
		} else {
			// Find the dominant non-PASS status
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
			fmt.Printf("\r  Progress: %d/%d (%.0f%%) | In-flight: %d | Elapsed: %.0fs",
				completed, total, pct, inFlight,
				time.Since(metrics.StartTime).Seconds())
		}
	}
}

// ── Report Writer ─────────────────────────────────────────────────────────

// PerProblemSubmission is a single submission entry in the per-problem report.
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

// PerProblemReport groups all submissions for one problem.
type PerProblemReport struct {
	ProblemID   string                 `json:"problem_id"`
	Submissions []PerProblemSubmission `json:"submissions"`
}

func buildPerProblemReport(results []SubmissionResult) []PerProblemReport {
	// Group by problem ID preserving insertion order of first occurrence.
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

func writeJSONReport(metrics *Metrics, cfg Config, outputFile string) error {
	report := map[string]interface{}{
		"config": map[string]interface{}{
			"users":         cfg.Users,
			"ramp_up_sec":   cfg.RampUpSec,
			"judge0_url":    cfg.Judge0URL,
			"dry_run":       cfg.DryRun,
		},
		"summary": map[string]interface{}{
			"total_submissions":   metrics.TotalSubmissions,
			"completed":           metrics.Completed,
			"errors":              metrics.Errors,
			"blocked_by_ast":      metrics.Blocked,
			"harness_jobs":        metrics.TotalSubmissions,
			"batch_equivalent":    metrics.TotalSubmissions * 4,
			"queue_reduction":     "4x",
			"throughput_per_sec":  float64(metrics.Completed) / metrics.EndTime.Sub(metrics.StartTime).Seconds(),
			"latency_p50_ms":      percentile(func() []int64 { s := make([]int64, len(metrics.Latencies)); copy(s, metrics.Latencies); sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }); return s }(), 50),
			"latency_p95_ms":      percentile(func() []int64 { s := make([]int64, len(metrics.Latencies)); copy(s, metrics.Latencies); sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }); return s }(), 95),
			"latency_p99_ms":      percentile(func() []int64 { s := make([]int64, len(metrics.Latencies)); copy(s, metrics.Latencies); sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }); return s }(), 99),
			"peak_in_flight_jobs": metrics.MaxInFlight,
		},
		"status_counts":        metrics.StatusCounts,
		"problem_counts":       metrics.ProblemCounts,
		"type_counts":          metrics.TypeCounts,
		"results":              metrics.Results,
		"per_problem_report":   buildPerProblemReport(metrics.Results),
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

	flag.StringVar(&cfg.Judge0URL, "url", "http://localhost:2358", "Judge0 base URL")
	flag.StringVar(&cfg.APIKey, "key", "", "Judge0 API key (X-Auth-Token)")
	flag.IntVar(&cfg.Users, "users", 500, "Number of concurrent virtual users")
	flag.IntVar(&cfg.RampUpSec, "ramp", 10, "Ramp-up duration in seconds (spread user starts)")
	flag.StringVar(&cfg.QuestionBank, "bank", "question_bank.json", "Path to question_bank.json")
	flag.DurationVar(&cfg.PollInterval, "poll", 500*time.Millisecond, "Poll interval for Judge0 results")
	flag.IntVar(&cfg.MaxPollSecs, "maxpoll", 120, "Max seconds to wait for a single submission result")
	flag.BoolVar(&cfg.DryRun, "dryrun", false, "Dry run: build harnesses but don't submit to Judge0")
	flag.StringVar(&cfg.OutputFile, "out", "load_test_report.json", "Output JSON report file")
	flag.Parse()

	// Load question bank
	bankData, err := os.ReadFile(cfg.QuestionBank)
	if err != nil {
		log.Fatalf("Failed to read question bank: %v", err)
	}
	var bank QuestionBank
	if err := json.Unmarshal(bankData, &bank); err != nil {
		log.Fatalf("Failed to parse question bank: %v", err)
	}

	fmt.Printf("\n  Judge0 Harness Load Tester\n")
	fmt.Printf("  ───────────────────────────────────────────────\n")
	fmt.Printf("  Target:       %s\n", cfg.Judge0URL)
	fmt.Printf("  Users:        %d\n", cfg.Users)
	fmt.Printf("  Ramp-up:      %ds\n", cfg.RampUpSec)
	fmt.Printf("  Problems:     %d\n", len(bank.Problems))
	fmt.Printf("  Dry run:      %v\n", cfg.DryRun)
	fmt.Printf("  Mode:         Harness (1 job per submission)\n")
	fmt.Printf("  Batch equiv:  ~%d jobs if using batch approach\n", cfg.Users*4)
	fmt.Printf("  ───────────────────────────────────────────────\n\n")

	metrics := NewMetrics()
	client := NewJudge0Client(cfg)

	var inFlight int64
	var maxInFlight int64

	var wg sync.WaitGroup
	progressDone := make(chan struct{})

	go printProgress(metrics, cfg, progressDone)

	// Ramp-up: spread user goroutine starts evenly over ramp-up period
	rampDelay := time.Duration(0)
	if cfg.RampUpSec > 0 && cfg.Users > 1 {
		rampDelay = time.Duration(cfg.RampUpSec) * time.Second / time.Duration(cfg.Users)
	}

	for i := 0; i < cfg.Users; i++ {
		wg.Add(1)
		go func(uid int) {
			runUser(uid, bank, client, metrics, cfg, &wg, &inFlight, &maxInFlight)
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

	fmt.Println() // newline after progress
	metrics.Print(cfg)

	if cfg.OutputFile != "" {
		if err := writeJSONReport(metrics, cfg, cfg.OutputFile); err != nil {
			log.Printf("Failed to write report: %v", err)
		} else {
			fmt.Printf("  JSON report written to: %s\n\n", cfg.OutputFile)
		}
	}
}
