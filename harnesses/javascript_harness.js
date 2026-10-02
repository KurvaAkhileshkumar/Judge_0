/*
 * JavaScript Harness — v1  (Parallel Execution via worker_threads)
 * ================================================================
 * Mirrors the Python/Java harnesses, adapted for Node.js.
 *
 * WHY worker_threads (not fork like Python/C)
 * -------------------------------------------
 * A `node` child process is heavy: ~7-11 OS threads (libuv pool + V8) and
 * ~40 MB RSS each, with no copy-on-write sharing.  Forking 200 of them would
 * blow both the MAX_PROCESSES_AND_OR_THREADS=220 isolate cap and the worker
 * container's memory limit.
 *
 * Instead we run ONE node process and give each test case its own Worker
 * thread.  Each Worker has its own V8 isolate (own heap, own event loop), so
 * test cases are isolated from one another — the direct analog of the Java
 * threads harness.  A Worker costs ~1 thread + a few MB, so the whole batch
 * stays well under the limits.
 *
 * Architecture
 * ------------
 *   Parent (isMainThread === true):
 *     1. Syntax-precheck the student source once (vm.Script).
 *     2. For each batch of up to MAX_PARALLEL_TCS test cases:
 *          - spawn one Worker per TC (all start together),
 *          - feed stdin (stdio mode), collect the posted result,
 *          - a per-TC timer calls worker.terminate() on TLE.
 *     3. Print all results in TC order using the DELIM protocol.
 *
 *   Worker (isMainThread === false):
 *     - function mode: compile student source, grab the target function,
 *       call it with the TC inputs, await if it returns a Promise, post the
 *       stringified return value as "got".
 *     - stdio mode: pipe the TC's stdin in, capture everything the student
 *       writes to stdout, post it as "got" when the program finishes.
 *
 * Per-TC memory note (Node 12.14.0)
 * ---------------------------------
 * Node 12.14.0 predates per-Worker resourceLimits (added in 12.16.0), so a
 * runaway allocation is bounded only by the whole-process RLIMIT_AS enforced
 * by isolate.  Batching caps the blast radius: a fatal OOM only loses the
 * current batch, and the parent reports the uncollected TCs.  resourceLimits
 * is still passed (guarded by a version check) so a future Node upgrade gets
 * true per-TC memory limits for free.
 */

'use strict';

var worker_threads = require('worker_threads');
var vm = require('vm');

// ── Config injected by HarnessBuilder (replace(), not format()) ────────────
var MODE             = "{mode}";          // "function" | "stdio"
var DELIM            = "@@TC_RESULT__{session_id}__";
var FUNCTION_NAME    = "{function_name}";
var PER_TC_LIMIT_S   = {per_tc_limit_s};
var MEMORY_LIMIT_MB  = {memory_limit_mb};
var MAX_PARALLEL_TCS = {max_parallel_tcs};

// Test cases:
//   function mode → [{ "input": [..] }, ...]
//   stdio mode    → [{ "stdin_text": ".." }, ...]
var TEST_CASES = {test_cases_json};

// Exact original student source (JSON string literal — safe for any content).
var STUDENT_SOURCE = {student_source_json};

// ── Shared helpers ─────────────────────────────────────────────────────────

// CommonJS module wrapper so student code gets require/module/exports and so
// runtime errors report `<student>:N` line numbers that map 1:1 to the
// student's own lines (the wrapper prefix has no trailing newline, exactly
// like Node's internal Module.wrap).
function wrapSource(includeReturn) {
    var header = "(function (exports, require, module, __filename, __dirname) { ";
    var footer = includeReturn
        ? "\n;return (typeof " + FUNCTION_NAME + " === 'function') ? " + FUNCTION_NAME + " : undefined;\n})"
        : "\n})";
    return header + STUDENT_SOURCE + footer;
}

// Format a runtime/compile error into a short, student-relative message.
function formatError(err) {
    var msg;
    if (err && err.name && err.message) {
        msg = err.name + ": " + err.message;
    } else {
        msg = String(err);
    }
    if (err && err.stack) {
        var m = err.stack.match(/<student>:(\d+)/);
        if (m) {
            return "line " + m[1] + ": " + msg;
        }
    }
    return msg;
}

// Stringify a function-mode return value the way the OutputParser expects:
// primitives via String(), arrays/objects via compact JSON.
function formatReturn(v) {
    if (v === null) return "null";
    if (v === undefined) return "undefined";
    if (typeof v === "object") {
        try { return JSON.stringify(v); } catch (e) { return String(v); }
    }
    return String(v);
}

function rstrip(s) {
    return String(s).replace(/\s+$/, "");
}

// ════════════════════════════════════════════════════════════════════════
// WORKER ROLE — runs ONE test case in its own V8 isolate
// ════════════════════════════════════════════════════════════════════════
function runWorker() {
    var parentPort = worker_threads.parentPort;
    var workerData = worker_threads.workerData;
    var tc = TEST_CASES[workerData.tcIndex] || {};

    var posted = false;
    function post(result) {
        if (posted) return;
        posted = true;
        try { parentPort.postMessage(result); } catch (e) { /* parent gone */ }
    }

    // Any async error that escapes student callbacks lands here.
    process.on("uncaughtException", function (e) {
        post({ status: "ERROR", detail: formatError(e) });
    });
    process.on("unhandledRejection", function (e) {
        post({ status: "ERROR", detail: formatError(e) });
    });

    try {
        if (MODE === "stdio") {
            runStdioWorker(post);
        } else {
            runFunctionWorker(tc, post);
        }
    } catch (e) {
        // Synchronous throw (incl. SyntaxError from compiling student source).
        post({ status: "ERROR", detail: formatError(e) });
    }
}

function runFunctionWorker(tc, post) {
    var factory = vm.runInThisContext(wrapSource(true), { filename: "<student>" });
    var fn = factory(module.exports, require, module, "<student>", process.cwd());
    if (typeof fn !== "function") {
        post({ status: "ERROR", detail: "Function '" + FUNCTION_NAME + "' is not defined" });
        return;
    }
    var inputs = tc.input || [];
    var ret = fn.apply(null, inputs);
    // Support async solutions: await the Promise, then post.
    Promise.resolve(ret).then(
        function (value) { post({ status: "OUTPUT", got: rstrip(formatReturn(value)) }); },
        function (err)   { post({ status: "ERROR", detail: formatError(err) }); }
    );
}

function runStdioWorker(post) {
    // Capture everything the student prints to stdout.  We replace
    // process.stdout.write (which console.log funnels through) BEFORE running
    // the student code, so nothing leaks to the parent's real stdout and
    // corrupts the DELIM protocol.
    var captured = "";
    var realWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = function (chunk, encoding, cb) {
        captured += (typeof chunk === "string") ? chunk : chunk.toString();
        if (typeof encoding === "function") { encoding(); }
        else if (typeof cb === "function") { cb(); }
        return true;
    };

    var finished = false;
    function finish() {
        if (finished) return;
        finished = true;
        process.stdout.write = realWrite;
        post({ status: "OUTPUT", got: rstrip(captured) });
    }

    // The student program runs to completion asynchronously (e.g. readline
    // callbacks fire as piped stdin arrives).  'beforeExit' fires once the
    // event loop has no more work — that's when the output is complete.
    process.on("beforeExit", finish);

    // stdin is piped in by the parent (Worker { stdin: true }).
    var factory = vm.runInThisContext(wrapSource(false), { filename: "<student>" });
    factory(module.exports, require, module, "<student>", process.cwd());

    // If the student never set up a stdin consumer (e.g. a program that just
    // prints a constant), the open stdin pipe keeps the worker's event loop
    // alive forever and 'beforeExit' never fires → false TLE.  (A worker's
    // process.stdin has no .unref().)  One tick later — after any
    // readline/'data'/'readable' listeners have been registered synchronously
    // — if nothing is consuming stdin, resume() it: the already-ended pipe
    // drains straight to EOF and stops holding the loop open, so 'beforeExit'
    // can fire.  When the student IS reading, we leave stdin untouched so
    // their own reads receive the data.
    setImmediate(function () {
        var si = process.stdin;
        var reading = si.listenerCount("data") > 0 || si.listenerCount("readable") > 0;
        if (!reading) {
            try { si.resume(); } catch (e) { /* ignore */ }
        }
    });
}

// ════════════════════════════════════════════════════════════════════════
// PARENT ROLE — orchestrates workers, prints results
// ════════════════════════════════════════════════════════════════════════

// Node >= 12.16.0 supports Worker resourceLimits (per-TC memory cap).
function supportsResourceLimits() {
    var p = process.versions.node.split(".");
    var major = parseInt(p[0], 10);
    var minor = parseInt(p[1], 10);
    return major > 12 || (major === 12 && minor >= 16);
}

function runOneTC(index, results) {
    return new Promise(function (resolve) {
        var opts = {
            workerData: { tcIndex: index },
            stdin: (MODE === "stdio"),
        };
        if (supportsResourceLimits()) {
            // No-op on 12.14.0; enforced on newer Node after an image upgrade.
            opts.resourceLimits = { maxOldGenerationSizeMb: MEMORY_LIMIT_MB };
        }

        var worker;
        try {
            worker = new worker_threads.Worker(__filename, opts);
        } catch (e) {
            results[index] = { status: "ERROR", detail: "Worker spawn failed: " + formatError(e) };
            resolve();
            return;
        }

        var settled = false;
        function settle(result) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            results[index] = result;
            try { worker.terminate(); } catch (e) { /* already gone */ }
            resolve();
        }

        var timer = setTimeout(function () {
            settle({ status: "TLE", detail: "Exceeded " + PER_TC_LIMIT_S + "s" });
        }, PER_TC_LIMIT_S * 1000 + 500);

        worker.on("message", function (msg) { settle(msg); });
        worker.on("error", function (err) {
            // On Node >= 12.16 the per-Worker resourceLimits turn an
            // over-allocation into a catchable ERR_WORKER_OUT_OF_MEMORY here
            // (instead of a fatal process abort) — report it as MLE.
            if (err && err.code === "ERR_WORKER_OUT_OF_MEMORY") {
                settle({ status: "MLE", detail: "Memory limit exceeded (" + MEMORY_LIMIT_MB + " MB)" });
            } else {
                settle({ status: "ERROR", detail: formatError(err) });
            }
        });
        worker.on("exit", function (code) {
            // Normal completion settles via 'message' first; reaching here
            // unsettled means the worker died without posting (likely OOM).
            settle({ status: "ERROR", detail: "Worker exited without result (code " + code + ")" });
        });

        if (MODE === "stdio" && worker.stdin) {
            var text = (TEST_CASES[index] && TEST_CASES[index].stdin_text) || "";
            worker.stdin.write(text);
            worker.stdin.end();
        }
    });
}

function emitResult(i, result) {
    process.stdout.write(DELIM + "START_" + (i + 1) + "\n");
    process.stdout.write(JSON.stringify(result) + "\n");
    process.stdout.write(DELIM + "END_" + (i + 1) + "\n");
}

function emitAll(results, n) {
    for (var i = 0; i < n; i++) {
        var r = results[i] || { status: "ERROR", detail: "Result not collected" };
        emitResult(i, r);
    }
    process.stdout.write(DELIM + "DONE\n");
}

function runParent() {
    var n = TEST_CASES.length;
    var results = new Array(n);

    // Syntax-precheck once.  A SyntaxError here means the whole submission is
    // uncompilable → report Compilation Error for every TC (autograder maps
    // the "Compilation Error:" prefix to the CE status/label).
    try {
        new vm.Script(wrapSource(MODE !== "stdio"), { filename: "<student>" });
    } catch (e) {
        var detail = "Compilation Error:\n" + formatError(e);
        for (var k = 0; k < n; k++) {
            results[k] = { status: "ERROR", detail: detail };
        }
        emitAll(results, n);
        return;
    }

    // Run in batches of MAX_PARALLEL_TCS so one OOM can't sink more than a
    // batch, matching the time budget computed by judge0_client.
    var batchStart = 0;
    function nextBatch() {
        if (batchStart >= n) {
            emitAll(results, n);
            return;
        }
        var batchEnd = Math.min(batchStart + MAX_PARALLEL_TCS, n);
        var jobs = [];
        for (var i = batchStart; i < batchEnd; i++) {
            jobs.push(runOneTC(i, results));
        }
        batchStart = batchEnd;
        Promise.all(jobs).then(nextBatch);
    }
    nextBatch();
}

// ── Entry point ────────────────────────────────────────────────────────────
if (worker_threads.isMainThread) {
    runParent();
} else {
    runWorker();
}
