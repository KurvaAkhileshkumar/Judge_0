/*
 * Java Harness — v3  (Parallel Execution)
 * ==========================================
 *
 * Architecture change from v2:
 *   v2: start thread → join(limit) → next thread   (sequential, time = sum)
 *   v3: start ALL threads → join all with deadline  (parallel, time = max)
 *
 * Stdout/stdin contention solved with ThreadLocal dispatch
 * ─────────────────────────────────────────────────────────
 *   The fundamental problem: System.setOut() / System.setIn() are global.
 *   If Thread-A and Thread-B both redirect System.out, they overwrite
 *   each other's capture stream.
 *
 *   Solution: Install a DISPATCH_STREAM once as System.out.
 *   Each worker thread sets its own ThreadLocal<PrintStream>.
 *   The dispatch stream routes write() calls to the current thread's stream.
 *   The main thread keeps a reference to ORIGINAL_OUT for result printing.
 *
 *   Same pattern for System.in via DISPATCH_STDIN + ThreadLocal<InputStream>.
 *
 * Thread kill strategy (inherited from v2)
 * ─────────────────────────────────────────
 *   Phase 1: thread.interrupt()  — cooperative (works if student blocks/checks)
 *   Phase 2: thread.stop()       — forceful (deprecated, safe in throwaway JVM)
 *
 * Memory tracking (inherited from v2)
 * ─────────────────────────────────────
 *   MemoryMXBean.getHeapMemoryUsage().getUsed() after forceGC() per thread.
 *   More stable than Runtime.totalMemory() - freeMemory() delta.
 */

import java.io.*;
import java.lang.management.*;
import java.lang.reflect.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Harness {

    static final String DELIM = "{delim}";
    static final String MODE  = "{mode}";

    /* ── STUDENT CODE ───────────────────────────────────────────────── */
    {student_code_as_inner_class}
    /* ── END STUDENT CODE ────────────────────────────────────────────── */

    /* ═══════════════════════════════════════════════════════════════════
     * THREAD-LOCAL STDOUT DISPATCH
     * Each worker sets TL_OUT before running student code.
     * DISPATCH_STREAM routes all write() calls to current thread's stream.
     * Main thread keeps ORIGINAL_OUT for printing results.
     * ═══════════════════════════════════════════════════════════════════ */
    static PrintStream ORIGINAL_OUT;
    static final ThreadLocal<PrintStream> TL_OUT = new ThreadLocal<>();

    static final PrintStream DISPATCH_STREAM = new PrintStream(new OutputStream() {
        @Override public void write(int b) {
            PrintStream s = TL_OUT.get();
            if (s != null) s.write(b);
        }
        @Override public void write(byte[] b, int off, int len) {
            PrintStream s = TL_OUT.get();
            if (s != null) s.write(b, off, len);
        }
        @Override public void flush() {
            PrintStream s = TL_OUT.get();
            if (s != null) s.flush();
        }
    });

    /* ═══════════════════════════════════════════════════════════════════
     * THREAD-LOCAL STDIN DISPATCH
     * Each worker sets TL_IN before running student code.
     * DISPATCH_STDIN routes all read() calls to current thread's stream.
     * ═══════════════════════════════════════════════════════════════════ */
    static final ThreadLocal<InputStream> TL_IN = new ThreadLocal<>();

    static final InputStream DISPATCH_STDIN = new InputStream() {
        @Override public int read() throws IOException {
            InputStream s = TL_IN.get();
            return (s != null) ? s.read() : -1;
        }
        @Override public int read(byte[] b, int off, int len) throws IOException {
            InputStream s = TL_IN.get();
            return (s != null) ? s.read(b, off, len) : -1;
        }
        @Override public int available() throws IOException {
            InputStream s = TL_IN.get();
            return (s != null) ? s.available() : 0;
        }
    };

    /* ── Result holder ────────────────────────────────────────────────── */
    static class TCResult {
        String status   = "ERROR";
        String got      = "";
        String expected = "";
        String detail   = "";
    }

    /* ── Memory tracking ─────────────────────────────────────────────── */
    static final MemoryMXBean MX = ManagementFactory.getMemoryMXBean();

    static long usedHeapBytes() {
        return MX.getHeapMemoryUsage().getUsed();
    }

    static void forceGC() {
        System.gc();
        System.runFinalization();
        try { Thread.sleep(20); } catch (InterruptedException ignored) {}
    }

    /* ── Thread kill (escalating strategy, Java 8–21+) ─────────────────
     * Phase 1: interrupt()  — cooperative (works if student checks isInterrupted)
     * Phase 2: stop()       — deprecated in Java 1.2, throws UnsupportedOperationException
     *                          in Java 21+.  Caught below; the thread is a daemon
     *                          (setDaemon(true)) so the JVM abandons it when main()
     *                          returns — the TLE verdict was already written above.
     */
    @SuppressWarnings({"deprecation", "removal"})
    static boolean killThread(Thread t) {
        if (!t.isAlive()) return true;
        t.interrupt();
        try { t.join(300); } catch (InterruptedException ignored) {}
        if (!t.isAlive()) return true;
        try {
            t.stop();
        } catch (UnsupportedOperationException ignored) {
            // Java 21+: Thread.stop() removed.  The thread is a daemon so the
            // JVM will not wait for it when main() returns.
        }
        try { t.join(200); } catch (InterruptedException ignored) {}
        return !t.isAlive();
    }

    /* ═══════════════════════════════════════════════════════════════════
     * FUNCTION MODE — runs solve() via reflection in a worker thread.
     * ThreadLocal stream captures output for this thread only.
     * Returns a TCResult immediately (non-blocking creation).
     * Caller starts the thread and joins with deadline.
     * ═══════════════════════════════════════════════════════════════════ */
    static Thread launchFunctionTC(
            final Object[]               inputs,
            final String[]               paramTypes,
            final String                 functionName,
            final int                    memoryLimitMb,
            final AtomicReference<TCResult> resultRef) {

        Thread t = new Thread(() -> {
            TCResult result = new TCResult();
            // Fix 4.1: result.expected not set; OutputParser fetches from Redis

            // Set up this thread's capture streams
            ByteArrayOutputStream baos    = new ByteArrayOutputStream();
            PrintStream           capture = new PrintStream(baos);
            TL_OUT.set(capture);
            TL_IN.set(null);  // function mode has no stdin

            try {
                forceGC();
                long memBefore = usedHeapBytes();

                Class<?>[] paramClasses = resolveParamClasses(paramTypes);
                Method m = Student.class.getMethod(functionName, paramClasses);
                Object retVal = m.invoke(new Student(), inputs);

                forceGC();
                long memUsedMb = Math.max(0, usedHeapBytes() - memBefore) / (1024 * 1024);

                if (memoryLimitMb > 0 && memUsedMb > memoryLimitMb) {
                    result.status = "MLE";
                    result.detail = "Used ~" + memUsedMb + "MB, limit " + memoryLimitMb + "MB";
                    resultRef.set(result);
                    return;
                }

                String printed  = baos.toString().trim();
                String returned = (retVal != null) ? retVal.toString().trim() : "null";
                result.got      = printed.isEmpty() ? returned : printed;
                /* Fix 4.1: OUTPUT status; OutputParser does comparison */
                result.status   = "OUTPUT";

            } catch (OutOfMemoryError e) {
                result.status = "MLE";
                result.detail = "OutOfMemoryError";
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof OutOfMemoryError) {
                    result.status = "MLE";
                    result.detail = "OutOfMemoryError";
                } else {
                    result.status = "ERROR";
                    result.detail = lastLine(cause != null ? cause.toString() : e.toString());
                }
            } catch (Exception e) {
                result.status = "ERROR";
                result.detail = lastLine(e.toString());
            } finally {
                TL_OUT.remove();
                TL_IN.remove();
            }
            resultRef.set(result);
        });
        t.setDaemon(true);
        return t;
    }

    /* ═══════════════════════════════════════════════════════════════════
     * STDIO MODE — calls student's main() with per-thread stdin/stdout.
     * ThreadLocal streams prevent cross-thread output contamination.
     * ═══════════════════════════════════════════════════════════════════ */
    static Thread launchStdioTC(
            final String                 stdinInput,
            final AtomicReference<TCResult> resultRef) {

        Thread t = new Thread(() -> {
            TCResult result = new TCResult();
            // Fix 4.1: result.expected not set; OutputParser fetches from Redis

            ByteArrayOutputStream baos    = new ByteArrayOutputStream();
            PrintStream           capture = new PrintStream(baos);
            InputStream           fakeIn  = new ByteArrayInputStream(stdinInput.getBytes());
            TL_OUT.set(capture);
            TL_IN.set(fakeIn);

            try {
                Method m = Student.class.getMethod("main", String[].class);
                m.invoke(null, (Object) new String[]{});

                result.got    = baos.toString().trim();
                /* Fix 4.1: OUTPUT status; OutputParser does comparison */
                result.status = "OUTPUT";

            } catch (OutOfMemoryError e) {
                result.status = "MLE";
                result.detail = "OutOfMemoryError";
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof OutOfMemoryError) {
                    result.status = "MLE";
                    result.detail = "OutOfMemoryError";
                } else {
                    result.status = "ERROR";
                    result.detail = lastLine(cause != null ? cause.toString() : e.toString());
                }
            } catch (Exception e) {
                result.status = "ERROR";
                result.detail = lastLine(e.toString());
            } finally {
                TL_OUT.remove();
                TL_IN.remove();
            }
            resultRef.set(result);
        });
        t.setDaemon(true);
        return t;
    }

    /* ── Helpers ─────────────────────────────────────────────────────── */
    static Class<?>[] resolveParamClasses(String[] types) throws ClassNotFoundException {
        if (types == null || types.length == 0) return new Class<?>[0];
        Class<?>[] out = new Class<?>[types.length];
        for (int i = 0; i < types.length; i++) {
            switch (types[i]) {
                case "int":     out[i] = int.class;     break;
                case "long":    out[i] = long.class;    break;
                case "double":  out[i] = double.class;  break;
                case "float":   out[i] = float.class;   break;
                case "boolean": out[i] = boolean.class; break;
                case "char":    out[i] = char.class;    break;
                default:        out[i] = Class.forName(types[i]);
            }
        }
        return out;
    }

    static String lastLine(String s) {
        if (s == null || s.isEmpty()) return "unknown error";
        String[] lines = s.split("\\n");
        return lines[lines.length - 1].trim();
    }

    static String escape(String s) {
        // FIX-4: also escape \n, \r, \t so JSON output stays valid when
        // the student's result contains embedded newlines or tabs.
        // The old version only escaped backslash and double-quote, leaving
        // newline/tab literals in the JSON string which breaks the parser.
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    // FIX-10: float-tolerant comparison — identical semantics to Python's
    // _num_equal().  Strict .equals() fails for 0.1+0.2 → "0.30000000000000004".
    // Only activates when both strings parse fully as finite doubles.
    static boolean compareResults(String got, String expected) {
        if (got.equals(expected)) return true;
        try {
            double dg = Double.parseDouble(got);
            double de = Double.parseDouble(expected);
            if (Double.isNaN(dg) || Double.isNaN(de)) return false;
            double diff = Math.abs(dg - de);
            double tol  = Math.max(1e-9, Math.abs(de) * 1e-6);
            return diff <= tol;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /* printResult always uses ORIGINAL_OUT — never goes through dispatch */
    static void printResult(int tcNum, TCResult r) {
        ORIGINAL_OUT.println(DELIM + "START_" + tcNum);
        ORIGINAL_OUT.println("{");
        ORIGINAL_OUT.println("  \"status\": \""   + r.status          + "\",");
        ORIGINAL_OUT.println("  \"got\": \""      + escape(r.got)     + "\",");
        ORIGINAL_OUT.println("  \"expected\": \"" + escape(r.expected)+ "\",");
        ORIGINAL_OUT.println("  \"detail\": \""   + escape(r.detail)  + "\"");
        ORIGINAL_OUT.println("}");
        ORIGINAL_OUT.println(DELIM + "END_" + tcNum);
        ORIGINAL_OUT.flush();
    }

    /* ═══════════════════════════════════════════════════════════════════
     * MAIN — orchestrates parallel TC execution
     * ═══════════════════════════════════════════════════════════════════ */
    public static void main(String[] args) throws Exception {

        /* Block System.exit() from student code (Java 8–17).
         * SecurityManager was deprecated in Java 17 and removed in Java 21;
         * the UnsupportedOperationException catch handles Java 21+.
         * On Java 21+ the isolate wall-time limit is the backstop. */
        try {
            System.setSecurityManager(new SecurityManager() {
                @Override public void checkExit(int status) {
                    throw new SecurityException("System.exit() blocked");
                }
                @Override public void checkPermission(java.security.Permission p) {}
            });
        } catch (UnsupportedOperationException ignored) {
            // Java 21+: SecurityManager removed.
        }

        ORIGINAL_OUT = System.out;

        /* Install dispatch streams — covers all threads from this point */
        System.setOut(DISPATCH_STREAM);
        System.setIn(DISPATCH_STDIN);

        int    perTcLimitMs  = {per_tc_limit_ms};
        int    memoryLimitMb = {memory_limit_mb};
        String functionName  = "{function_name}";
        String[] paramTypes  = {param_types_array};

        /* ── PARALLEL TC RUNNER ────────────────────────────────────── */
        {tc_runner_body}

        ORIGINAL_OUT.println(DELIM + "DONE");
        ORIGINAL_OUT.flush();
    }
}
