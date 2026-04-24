/*
 * C Harness — v3  (Parallel Execution)
 * ======================================
 *
 * Architecture change from v2:
 *   v2:  fork child → wait → next child   (sequential, time = sum)
 *   v3:  fork ALL children → select() all → collect  (parallel, time = max)
 *
 * Key design decisions:
 *
 *  1. ALARM MOVED INTO CHILD
 *     In v2 the parent set setitimer() after forking each child and
 *     waited one at a time. In v3 each child sets its own alarm(N)
 *     immediately after forking. When it fires, _child_tle_handler
 *     writes a TLE result to the pipe and calls _exit(). Children
 *     are guaranteed to terminate within per_tc_limit_s + epsilon.
 *
 *  2. SELECT() FOR PARALLEL COLLECTION
 *     The parent tracks all (pid, read_fd) pairs and uses select()
 *     to wait on all pipes simultaneously. Results arrive as children
 *     finish — fastest child first. Total wall time = max(TC_times).
 *
 *  3. GLOBAL SAFETY ALARM
 *     A single alarm(per_tc_limit_s + 2) is set in the parent before
 *     select(). If any child's internal alarm fails (e.g., student code
 *     overrode SIGALRM in C via signal()), the parent kills all remaining
 *     children and marks them TLE. This is the backstop, not the primary
 *     enforcement.
 *
 *  4. CRASH DETECTION VIA PIPE EOF
 *     If a child crashes (SIGSEGV, SIGFPE) before writing to the pipe,
 *     the write end of the pipe is automatically closed by the OS when
 *     the process exits. select() returns the fd as readable, read()
 *     returns 0 (EOF), and we inspect WIFSIGNALED() to determine the
 *     crash type.
 *
 *  5. JUDGE0 GLOBAL TIME LIMIT (handled by judge0_client.py)
 *     Old: per_tc_limit_s * N + overhead  (e.g. 2s*10+5 = 25s)
 *     New: per_tc_limit_s     + overhead  (e.g. 2s   +5 =  7s)
 *     This is a 10x reduction in the Judge0 job time limit.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>

#define DELIM      "{delim}"
#define MAX_OUTPUT 4096

/* ── STUDENT CODE ──────────────────────────────────────────────────── */

{student_code}

/* ── END STUDENT CODE ─────────────────────────────────────────────── */

typedef struct {{
    char status[16];    /* PASS | FAIL | TLE | MLE | SEGV | FPE | ERROR */
    char got[512];
    char expected[512];
    char detail[256];
}} TCResult;

/* ── Per-child TLE: globals set inside child before alarm() ────────── */
static int   _child_pipe_fd   = -1;
static char  _child_expected[512];

static void _child_tle_handler(int sig) {{
    (void)sig;
    TCResult r;
    memset(&r, 0, sizeof(r));
    strcpy(r.status, "TLE");
    strncpy(r.expected, _child_expected, sizeof(r.expected) - 1);
    write(_child_pipe_fd, &r, sizeof(r));
    _exit(0);
}}

/* ── Parent safety alarm — kills all children if they stall ─────────── */
static volatile sig_atomic_t _global_tle = 0;
static void _parent_alarm_handler(int sig) {{
    (void)sig;
    _global_tle = 1;
}}

/* ── Child entry point ────────────────────────────────────────────────
 * Runs inside the forked child. Sets its own alarm, calls solve(),
 * writes exactly one TCResult to pipe_fd, then exits.
 * Any crash/segfault terminates the child; the parent detects pipe EOF.
 */
static void run_tc_child(int pipe_fd, {tc_params}, const char* expected,
                          int per_tc_limit_s, int memory_limit_mb) {{

    /* Apply per-child memory limit */
    if (memory_limit_mb > 0) {{
        struct rlimit rl;
        rl.rlim_cur = (rlim_t)memory_limit_mb * 1024 * 1024;
        rl.rlim_max = rl.rlim_cur;
        setrlimit(RLIMIT_AS, &rl);
    }}

    /* Block further fork() calls from student code */
    struct rlimit nproc_rl;
    nproc_rl.rlim_cur = 1;
    nproc_rl.rlim_max = 1;
    setrlimit(RLIMIT_NPROC, &nproc_rl);

    /* Set up per-child TLE alarm */
    _child_pipe_fd = pipe_fd;
    strncpy(_child_expected, expected, sizeof(_child_expected) - 1);
    signal(SIGALRM, _child_tle_handler);
    alarm((unsigned int)per_tc_limit_s);

    TCResult result;
    memset(&result, 0, sizeof(result));
    strncpy(result.expected, expected, sizeof(result.expected) - 1);

    /* Call student function and capture output */
    {call_solve_and_capture}

    /* Cancel alarm — we finished in time */
    alarm(0);

    /* Compare output */
    if (strcmp(result.got, expected) == 0) {{
        strncpy(result.status, "PASS", sizeof(result.status) - 1);
    }} else {{
        strncpy(result.status, "FAIL", sizeof(result.status) - 1);
    }}

    write(pipe_fd, &result, sizeof(TCResult));
    close(pipe_fd);
    _exit(0);
}}

int main(void) {{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, _parent_alarm_handler);

    /* ── PARALLEL TC EXECUTION ──────────────────────────────────────── */
    {tc_runner_body}

    printf("%sDONE\n", DELIM);
    fflush(stdout);
    return 0;
}}
