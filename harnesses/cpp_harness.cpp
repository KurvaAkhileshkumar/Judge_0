/*
 * C++ Harness — v3  (Parallel Execution)
 * =========================================
 *
 * Identical parallel strategy to the C harness (v3):
 *   - Each child sets its own alarm(per_tc_limit_s)
 *   - All children fork simultaneously
 *   - Parent collects via poll() — fastest child first, no FD_SETSIZE limit
 *   - Total wall time = max(TC_times) not sum(TC_times)
 *
 * C++ specific additions over C harness:
 *   - Child catches std::bad_alloc as MLE before writing to pipe
 *   - Child catches std::exception and writes ERROR with message
 *   - stdout is redirected inside child using std::ostringstream
 *   - std::cout redirect is safe — each child is a separate process
 */

#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <stdexcept>
#include <new>
#include <cmath>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>

#define DELIM "{delim}"

/* ── STUDENT CODE ──────────────────────────────────────────────────── */

{student_code}

/* ── END STUDENT CODE ─────────────────────────────────────────────── */

/* FIX-6: Increased buffers from 1024→4096 (got/expected) and 512→1024 (detail).
 * 1024 bytes silently truncated larger outputs, producing wrong FAIL verdicts. */
struct TCResult {{
    char status[16];
    char got[4096];
    char expected[4096];
    char detail[1024];
}};

/* ── Per-child TLE globals (set inside child before alarm()) ────────── */
static int  _child_pipe_fd = -1;
/* FIX-6: match enlarged expected buffer */
static char _child_expected[4096];

static void _child_tle_handler(int sig) {{
    (void)sig;
    TCResult r;
    memset(&r, 0, sizeof(r));
    strcpy(r.status, "TLE");
    strncpy(r.expected, _child_expected, sizeof(r.expected) - 1);
    write(_child_pipe_fd, &r, sizeof(r));
    _exit(0);
}}

/* ── Parent safety alarm ─────────────────────────────────────────────── */
static volatile sig_atomic_t _global_tle = 0;
static void _parent_alarm_handler(int sig) {{
    (void)sig;
    _global_tle = 1;
}}

/* ── FIX-4: JSON string escaper ──────────────────────────────────────
 * Printing result.got raw inside JSON string literals caused malformed JSON
 * whenever the output contained '"', '\n', etc.  The parser then fell back to
 * keyword scanning and mis-reported correct answers as ERROR. */
static void json_escape(const char *src, char *dst, size_t dstlen) {{
    size_t di = 0;
    for (size_t i = 0; src[i] && di + 2 < dstlen; i++) {{
        unsigned char c = (unsigned char)src[i];
        if      (c == '"')  {{ dst[di++] = '\\'; dst[di++] = '"';  }}
        else if (c == '\\') {{ dst[di++] = '\\'; dst[di++] = '\\'; }}
        else if (c == '\n') {{ dst[di++] = '\\'; dst[di++] = 'n';  }}
        else if (c == '\r') {{ dst[di++] = '\\'; dst[di++] = 'r';  }}
        else if (c == '\t') {{ dst[di++] = '\\'; dst[di++] = 't';  }}
        else                {{ dst[di++] = (char)c; }}
    }}
    dst[di] = '\0';
}}

/* ── FIX-10: float-tolerant result comparator ────────────────────────
 * strcmp fails for 0.1+0.2 → "0.30000000000000004" ≠ "0.3".
 * Numeric comparison with relative+absolute epsilon when both strings
 * parse as finite doubles; falls back to strcmp otherwise. */
static bool compare_result(const std::string &got, const char *expected) {{
    if (got == expected) return true;
    try {{
        size_t pg = 0, pe = 0;
        double dg = std::stod(got, &pg);
        std::string exp_s(expected);
        double de = std::stod(exp_s, &pe);
        if (pg == got.size() && pe == exp_s.size() &&
            !std::isnan(dg) && !std::isnan(de)) {{
            double diff = std::abs(dg - de);
            double tol  = std::max(1e-9, std::abs(de) * 1e-6);
            return diff <= tol;
        }}
    }} catch (...) {{}}
    return false;
}}

/* ── Child entry point ────────────────────────────────────────────────────
 * FIX-2: signature uses {tc_params_comma} (trailing comma present only when
 * params exist), so zero-arg functions don't produce "int pipe_fd, , const"
 * which is a C++ syntax error.
 */
static void run_tc_child(int pipe_fd, {tc_params_comma}const char* expected,
                          int per_tc_limit_s, int memory_limit_mb) {{

    /* Memory limit */
    if (memory_limit_mb > 0) {{
        struct rlimit rl;
        rl.rlim_cur = (rlim_t)memory_limit_mb * 1024 * 1024;
        rl.rlim_max = rl.rlim_cur;
        setrlimit(RLIMIT_AS, &rl);
    }}

    /* Block fork() in student code */
    struct rlimit nproc_rl {{ 1, 1 }};
    setrlimit(RLIMIT_NPROC, &nproc_rl);

    /* Per-child TLE alarm */
    _child_pipe_fd = pipe_fd;
    strncpy(_child_expected, expected, sizeof(_child_expected) - 1);
    signal(SIGALRM, _child_tle_handler);
    alarm((unsigned int)per_tc_limit_s);

    TCResult result;
    memset(&result, 0, sizeof(result));
    strncpy(result.expected, expected, sizeof(result.expected) - 1);

    try {{
        /* Redirect cout to capture solve() output */
        std::ostringstream oss;
        std::streambuf* old_buf = std::cout.rdbuf(oss.rdbuf());

        {call_solve_and_capture}

        std::cout.rdbuf(old_buf);
        alarm(0);

        std::string got_str = oss.str();
        /* FIX-14: strip ALL trailing whitespace/newlines, not just one '\n'.
         * Python uses .strip(); the old C++ code stripped only the last char,
         * causing cross-language inconsistency on multi-newline output. */
        while (!got_str.empty() &&
               (got_str.back() == '\n' || got_str.back() == '\r' ||
                got_str.back() == ' '  || got_str.back() == '\t'))
            got_str.pop_back();

        strncpy(result.got, got_str.c_str(), sizeof(result.got) - 1);

        /* FIX-8/10: float-tolerant comparison */
        if (compare_result(got_str, expected)) {{
            strncpy(result.status, "PASS", sizeof(result.status) - 1);
        }} else {{
            strncpy(result.status, "FAIL", sizeof(result.status) - 1);
        }}

    }} catch (const std::bad_alloc&) {{
        alarm(0);
        strncpy(result.status, "MLE",    sizeof(result.status) - 1);
        strncpy(result.detail, "bad_alloc: out of memory", sizeof(result.detail) - 1);
    }} catch (const std::exception& e) {{
        alarm(0);
        strncpy(result.status, "ERROR",  sizeof(result.status) - 1);
        strncpy(result.detail, e.what(), sizeof(result.detail) - 1);
    }} catch (...) {{
        alarm(0);
        strncpy(result.status, "ERROR",  sizeof(result.status) - 1);
        strncpy(result.detail, "Unknown exception", sizeof(result.detail) - 1);
    }}

    write(pipe_fd, &result, sizeof(TCResult));
    close(pipe_fd);
    _exit(0);
}}

int main() {{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, _parent_alarm_handler);

    /* ── PARALLEL TC EXECUTION ──────────────────────────────────────── */
    {tc_runner_body}

    /* Use ORIGINAL stdout (not captured oss) for the DONE marker */
    std::cout << DELIM << "DONE" << std::endl;
    return 0;
}}
