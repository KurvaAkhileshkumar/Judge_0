/*
 * C++ Harness — v3  (Parallel Execution)
 * =========================================
 *
 * Identical parallel strategy to the C harness (v3):
 *   - Each child sets its own alarm(per_tc_limit_s)
 *   - All children fork simultaneously
 *   - Parent collects via select() — fastest child first
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
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>

#define DELIM "{delim}"

/* ── STUDENT CODE ──────────────────────────────────────────────────── */

{student_code}

/* ── END STUDENT CODE ─────────────────────────────────────────────── */

struct TCResult {{
    char status[16];
    char got[1024];
    char expected[1024];
    char detail[512];
}};

/* ── Per-child TLE globals (set inside child before alarm()) ────────── */
static int  _child_pipe_fd = -1;
static char _child_expected[1024];

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

/* ── Child entry point ────────────────────────────────────────────────── */
static void run_tc_child(int pipe_fd, {tc_params}, const char* expected,
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
        if (!got_str.empty() && got_str.back() == '\n')
            got_str.pop_back();

        strncpy(result.got, got_str.c_str(), sizeof(result.got) - 1);

        if (got_str == std::string(expected)) {{
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

    std::cout << DELIM << "DONE" << std::endl;
    return 0;
}}
