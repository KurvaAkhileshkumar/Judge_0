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

/* FIX-6: Increased buffers from 512→4096 (got/expected) and 256→1024 (detail).
 * 512 bytes silently truncated string-sorting / graph outputs, producing wrong
 * FAIL verdicts with no indication the value was cut. */
typedef struct {{
    char status[16];
    char got[4096];
    char expected[4096];
    char detail[1024];
}} TCResult;

/* ── Per-child TLE globals (set inside child before alarm()) ────────── */
static int   _child_pipe_fd   = -1;
/* FIX-6: match enlarged expected buffer */
static char  _child_expected[4096];

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

/* ── FIX-4: JSON string escaper ──────────────────────────────────────
 * The previous code printed result.got/expected raw inside JSON strings.
 * Any '"' or '\n' in output produced malformed JSON → parser fell back to
 * keyword scanning and marked the verdict ERROR instead of FAIL/PASS.
 * This function escapes the six JSON special sequences in-place. */
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

/* ── FIX-8 + FIX-10: float-tolerant result comparator ───────────────
 * Strict strcmp fails for 0.1+0.2 → "0.30000000000000004" ≠ "0.3".
 * When both strings parse completely as doubles, use relative + absolute
 * epsilon (1e-6 relative, 1e-9 absolute). Falls back to strcmp otherwise. */
static int compare_result(const char *got, const char *expected) {{
    if (strcmp(got, expected) == 0) return 1;
    char *eg, *ee;
    double dg = strtod(got, &eg);
    double de = strtod(expected, &ee);
    /* Only use numeric comparison if both strings were fully consumed */
    if (eg != got && ee != expected && *eg == '\0' && *ee == '\0') {{
        double diff = dg - de;
        if (diff < 0) diff = -diff;
        double ref  = de < 0 ? -de : de;
        double tol  = ref > 1e-9 ? ref * 1e-6 : 1e-9;
        return diff <= tol;
    }}
    return 0;
}}

/* ── Child entry point ────────────────────────────────────────────────
 * Runs inside the forked child. Sets its own alarm, calls solve(),
 * writes exactly one TCResult to pipe_fd, then exits.
 * Any crash/segfault terminates the child; the parent detects pipe EOF.
 *
 * FIX-2: signature uses {tc_params_comma} (trailing comma present only
 * when params exist) so zero-arg functions don't produce "int pipe_fd, ,"
 * which is a C syntax error.
 */
static void run_tc_child(int pipe_fd, {tc_params_comma}const char* expected,
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

    /* FIX-8/10: use float-tolerant comparator instead of raw strcmp */
    if (compare_result(result.got, expected)) {{
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
