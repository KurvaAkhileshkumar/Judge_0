# Judge0 Autograder — API Usage Guide

## Base URL

```
http://<your-ec2-ip>:5001
```

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Service health check |
| `POST` | `/submit` | Submit a grading job |
| `GET` | `/results/stream/<ticket_id>` | SSE stream — fires once when result is ready |

---

## Flow

```
POST /submit  →  { ticket_id }  →  GET /results/stream/<ticket_id>  →  { tc_results, score, total }
```

---

## Request Schema

```json
{
  "student_id":      "string (required)",
  "assessment_id":   "string (required)",
  "language":        "python | c | cpp | java (required)",
  "student_code":    "string (required)",
  "test_cases":      "array (required, 1–500 entries)",
  "mode":            "function | stdio (default: function)",
  "function_name":   "string (default: solve)",
  "param_types":     "array of type strings (default: [])",
  "return_type":     "string (default: auto)",
  "per_tc_limit_s":  "int 1–30 (default: 2)",
  "memory_limit_mb": "int 16–3500 (default: 256)"
}
```

### Test Case Schema

```json
{ "inputs": [...], "expected": "string" }       // function mode
{ "stdin_text": "raw string", "expected": "string" }  // stdio mode
```

> `inputs` and `stdin_text` are mutually exclusive. Use one per test case, matching the `mode`.

---

## Field Constraints

| Field | Required | Default | Allowed Values |
|-------|----------|---------|----------------|
| `student_id` | Yes | — | any string |
| `assessment_id` | Yes | — | any string |
| `language` | Yes | — | `python` / `c` / `cpp` / `java` |
| `student_code` | Yes | — | max 1 MB total payload |
| `test_cases` | Yes | — | 1 – 500 entries |
| `mode` | No | `"function"` | `"function"` / `"stdio"` |
| `function_name` | No | `"solve"` | valid identifier |
| `param_types` | No | `[]` | list of type strings |
| `return_type` | No | `"auto"` | type string |
| `per_tc_limit_s` | No | `2` | 1 – 30 |
| `memory_limit_mb` | No | `256` | 16 – 3500 |

---

## Supported param_types

### C / C++
`int`, `long`, `long long`, `double`, `float`, `long double`, `char`, `char*`, `const char*`, `bool` (C++ only), `short`, `unsigned int`, `unsigned long`, `size_t`

### Java
`int`, `long`, `double`, `float`, `boolean`, `char`, `Integer`, `Long`, `Double`, `Float`, `Boolean`, `String`, `int[]`, `long[]`, `double[]`, `String[]`

### Python
`param_types` is ignored — Python uses introspection.

---

## Response

### POST /submit
```json
{ "ticket_id": "uuid", "status": "queued" }         // 202 — new submission
{ "ticket_id": "uuid", "status": "duplicate" }       // 200 — same code resubmitted within 2h
{ "error": "..." }                                   // 400 — validation error
{ "error": "Queue at capacity" }                     // 429 — retry later
```

### GET /results/stream/<ticket_id>
```
event: result
data: {
  "tc_results": [
    { "tc_num": 1, "status": "PASS", "got": "5", "expected": "5", "detail": null },
    { "tc_num": 2, "status": "FAIL", "got": "9", "expected": "10", "detail": null },
    { "tc_num": 3, "status": "TLE",  "got": null, "expected": "3", "detail": "Exceeded 2s" }
  ],
  "score": 1,
  "total": 3
}
```

### TC Status Values
| Status | Meaning |
|--------|---------|
| `PASS` | Output matches expected |
| `FAIL` | Output does not match expected |
| `TLE` | Time limit exceeded (`per_tc_limit_s`) |
| `MLE` | Memory limit exceeded (`memory_limit_mb`) |
| `SEGV` | Segmentation fault (C/C++) |
| `FPE` | Floating point exception (C/C++) |
| `ERROR` | Runtime error or infrastructure failure |
| `MISSING` | TC never produced output (likely crash before it ran) |

---

## Mode Decision Guide

| Scenario | Mode |
|----------|------|
| Function takes primitives (int, float, string, bool) | `function` |
| Function takes a list/array | `stdio` for C/C++, `function` for Python/Java |
| 2D arrays / matrices | `stdio` (all), `function` (Python only) |
| Student uses `input()` / `scanf` / `cin` / `Scanner` | `stdio` |
| Multiple return values | `stdio` (print space-separated) |
| Floats with precision issues | `function` — float tolerance handles up to 1e-6 relative diff |

---

## Scenario 1 — Python, Function Mode, Primitives

```json
{
  "student_id": "u1",
  "assessment_id": "a1",
  "language": "python",
  "student_code": "def solve(a, b):\n    return a + b",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int", "int"],
  "return_type": "int",
  "per_tc_limit_s": 2,
  "memory_limit_mb": 256,
  "test_cases": [
    { "inputs": [2, 3], "expected": "5" },
    { "inputs": [10, 20], "expected": "30" }
  ]
}
```

---

## Scenario 2 — Python, Function Mode, List Input

Python supports lists natively as function arguments.

```json
{
  "student_id": "u1",
  "assessment_id": "a2",
  "language": "python",
  "student_code": "def solve(arr):\n    return sum(arr)",
  "mode": "function",
  "function_name": "solve",
  "test_cases": [
    { "inputs": [[1, 2, 3]], "expected": "6" },
    { "inputs": [[10, 20, 30]], "expected": "60" }
  ]
}
```

---

## Scenario 3 — Python, Function Mode, 2D Array

```json
{
  "student_id": "u1",
  "assessment_id": "a3",
  "language": "python",
  "student_code": "def solve(matrix):\n    return matrix[0][0]",
  "mode": "function",
  "function_name": "solve",
  "test_cases": [
    { "inputs": [[[1, 2], [3, 4]]], "expected": "1" },
    { "inputs": [[[9, 8], [7, 6]]], "expected": "9" }
  ]
}
```

---

## Scenario 4 — Python, Function Mode, String Input

```json
{
  "student_id": "u1",
  "assessment_id": "a4",
  "language": "python",
  "student_code": "def solve(s):\n    return s[::-1]",
  "mode": "function",
  "function_name": "solve",
  "test_cases": [
    { "inputs": ["hello"], "expected": "olleh" },
    { "inputs": ["abc"], "expected": "cba" }
  ]
}
```

---

## Scenario 5 — Python, Function Mode, Float Output

Float tolerance: `1e-6` relative, `1e-9` absolute. `0.30000000000000004` passes against `"0.3"`.

```json
{
  "student_id": "u1",
  "assessment_id": "a5",
  "language": "python",
  "student_code": "def solve(a, b):\n    return a / b",
  "mode": "function",
  "function_name": "solve",
  "test_cases": [
    { "inputs": [1, 3], "expected": "0.3333333333" },
    { "inputs": [22, 7], "expected": "3.142857" }
  ]
}
```

---

## Scenario 6 — Python, Function Mode, Boolean Output

```json
{
  "student_id": "u1",
  "assessment_id": "a6",
  "language": "python",
  "student_code": "def solve(a, b):\n    return a > b",
  "mode": "function",
  "function_name": "solve",
  "test_cases": [
    { "inputs": [5, 3], "expected": "True" },
    { "inputs": [1, 9], "expected": "False" }
  ]
}
```

---

## Scenario 7 — Python, stdio Mode

Use when student reads from `input()` or `sys.stdin`.

```json
{
  "student_id": "u1",
  "assessment_id": "a7",
  "language": "python",
  "student_code": "a, b = map(int, input().split())\nprint(a + b)",
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "2 3\n", "expected": "5" },
    { "stdin_text": "10 20\n", "expected": "30" }
  ]
}
```

---

## Scenario 8 — Python, stdio Mode, Multi-line Input

```json
{
  "student_id": "u1",
  "assessment_id": "a8",
  "language": "python",
  "student_code": "n = int(input())\narr = list(map(int, input().split()))\nprint(sum(arr))",
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "3\n1 2 3\n", "expected": "6" },
    { "stdin_text": "4\n10 20 30 40\n", "expected": "100" }
  ]
}
```

---

## Scenario 9 — C, Function Mode, Primitives

> Lists and arrays are **not** supported in C function mode. Use stdio for those.

```json
{
  "student_id": "u1",
  "assessment_id": "a9",
  "language": "c",
  "student_code": "int solve(int a, int b) { return a + b; }",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int", "int"],
  "return_type": "int",
  "test_cases": [
    { "inputs": [2, 3], "expected": "5" },
    { "inputs": [10, 20], "expected": "30" }
  ]
}
```

---

## Scenario 10 — C, Function Mode, Float Return

```json
{
  "student_id": "u1",
  "assessment_id": "a10",
  "language": "c",
  "student_code": "double solve(double a, double b) { return a / b; }",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["double", "double"],
  "return_type": "double",
  "test_cases": [
    { "inputs": [1.0, 3.0], "expected": "0.333333" },
    { "inputs": [22.0, 7.0], "expected": "3.142857" }
  ]
}
```

---

## Scenario 11 — C, Function Mode, String Return

```json
{
  "student_id": "u1",
  "assessment_id": "a11",
  "language": "c",
  "student_code": "#include <string.h>\nchar* solve(char* s) { return s; }",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["char*"],
  "return_type": "char*",
  "test_cases": [
    { "inputs": ["hello"], "expected": "hello" },
    { "inputs": ["world"], "expected": "world" }
  ]
}
```

---

## Scenario 12 — C, stdio Mode (arrays, complex input)

Use for anything involving arrays, structs, or multi-line parsing.

```json
{
  "student_id": "u1",
  "assessment_id": "a12",
  "language": "c",
  "student_code": "#include <stdio.h>\nint main() {\n    int n;\n    scanf(\"%d\", &n);\n    int sum = 0, x;\n    for (int i = 0; i < n; i++) { scanf(\"%d\", &x); sum += x; }\n    printf(\"%d\\n\", sum);\n    return 0;\n}",
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "3\n1 2 3\n", "expected": "6" },
    { "stdin_text": "4\n10 20 30 40\n", "expected": "100" }
  ]
}
```

---

## Scenario 13 — C++, Function Mode, Primitives

```json
{
  "student_id": "u1",
  "assessment_id": "a13",
  "language": "cpp",
  "student_code": "int solve(int a, int b) { return a + b; }",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int", "int"],
  "return_type": "int",
  "test_cases": [
    { "inputs": [2, 3], "expected": "5" },
    { "inputs": [10, 20], "expected": "30" }
  ]
}
```

---

## Scenario 14 — C++, Function Mode, Boolean Return

```json
{
  "student_id": "u1",
  "assessment_id": "a14",
  "language": "cpp",
  "student_code": "bool solve(int a, int b) { return a > b; }",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int", "int"],
  "return_type": "bool",
  "test_cases": [
    { "inputs": [5, 3], "expected": "true" },
    { "inputs": [1, 9], "expected": "false" }
  ]
}
```

---

## Scenario 15 — C++, stdio Mode

Use for `cin`/`cout`, vectors, or complex data structures.

```json
{
  "student_id": "u1",
  "assessment_id": "a15",
  "language": "cpp",
  "student_code": "#include <iostream>\nusing namespace std;\nint main() {\n    int n; cin >> n;\n    int sum = 0, x;\n    for (int i = 0; i < n; i++) { cin >> x; sum += x; }\n    cout << sum << endl;\n    return 0;\n}",
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "3\n1 2 3\n", "expected": "6" },
    { "stdin_text": "4\n10 20 30 40\n", "expected": "100" }
  ]
}
```

---

## Scenario 16 — Java, Function Mode, Primitives

```json
{
  "student_id": "u1",
  "assessment_id": "a16",
  "language": "java",
  "student_code": "public class Student {\n    public int solve(int a, int b) {\n        return a + b;\n    }\n}",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int", "int"],
  "return_type": "int",
  "test_cases": [
    { "inputs": [2, 3], "expected": "5" },
    { "inputs": [10, 20], "expected": "30" }
  ]
}
```

---

## Scenario 17 — Java, Function Mode, Array Input

```json
{
  "student_id": "u1",
  "assessment_id": "a17",
  "language": "java",
  "student_code": "public class Student {\n    public int solve(int[] arr) {\n        int s = 0;\n        for (int x : arr) s += x;\n        return s;\n    }\n}",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["int[]"],
  "return_type": "int",
  "test_cases": [
    { "inputs": [[1, 2, 3]], "expected": "6" },
    { "inputs": [[10, 20, 30]], "expected": "60" }
  ]
}
```

---

## Scenario 18 — Java, Function Mode, String Input

```json
{
  "student_id": "u1",
  "assessment_id": "a18",
  "language": "java",
  "student_code": "public class Student {\n    public String solve(String s) {\n        return new StringBuilder(s).reverse().toString();\n    }\n}",
  "mode": "function",
  "function_name": "solve",
  "param_types": ["String"],
  "return_type": "String",
  "test_cases": [
    { "inputs": ["hello"], "expected": "olleh" },
    { "inputs": ["abc"], "expected": "cba" }
  ]
}
```

---

## Scenario 19 — Java, stdio Mode

Use when student uses `Scanner` / `System.in`.

```json
{
  "student_id": "u1",
  "assessment_id": "a19",
  "language": "java",
  "student_code": "import java.util.Scanner;\npublic class Student {\n    public static void main(String[] args) {\n        Scanner sc = new Scanner(System.in);\n        int a = sc.nextInt(), b = sc.nextInt();\n        System.out.println(a + b);\n    }\n}",
  "mode": "stdio",
  "test_cases": [
    { "stdin_text": "2 3\n", "expected": "5" },
    { "stdin_text": "10 20\n", "expected": "30" }
  ]
}
```

---

## Edge Cases

### Empty inputs — calls `solve()` with no arguments
```json
{ "inputs": [], "expected": "42" }
```

### Empty stdin — EOF immediately
```json
{ "stdin_text": "", "expected": "" }
```

### String with spaces
```json
{ "inputs": ["hello world"], "expected": "hello world" }
```

### Multiline expected output
```json
{ "stdin_text": "3\n", "expected": "1\n2\n3" }
```

### Float precision — passes due to tolerance
```json
{ "inputs": [0.1, 0.2], "expected": "0.3" }
```

### NaN / Inf — exact string match required
```json
{ "inputs": [1.0, 0.0], "expected": "inf" }
```

---

## Validation Errors (HTTP 400)

| Cause | Error Message |
|-------|---------------|
| Missing required field | field name in error detail |
| Invalid language | `"language must be one of: python, c, cpp, java"` |
| test_cases empty | `"test_cases must not be empty"` |
| test_cases > 500 | `"test_cases must not exceed 500 entries"` |
| per_tc_limit_s out of range | `"per_tc_limit_s must be 1–30"` |
| memory_limit_mb out of range | `"memory_limit_mb must be 16–3500"` |
| Invalid function_name | `"function_name must be a valid identifier"` |
| mode/test_case mismatch | `"mode='function' requires 'inputs'"` |
| Both inputs and stdin_text set | `"each test_case must have 'inputs' or 'stdin_text'"` |
| Queue full | HTTP 429 — retry later |

---

## Notes

- **Idempotency**: Submitting the same `student_id` + `assessment_id` + `student_code` within 2 hours returns the original `ticket_id` (no re-grading).
- **SSE in Postman**: Use "Send and Download" — regular Send will hang on the stream.
- **Result TTL**: Results are stored for 2 hours after completion.
- **Max concurrent TCs**: 200 per Judge0 submission. Batches of 500 TCs run as 3 sequential rounds internally.
- **Security**: Dangerous imports (`os`, `subprocess`, `socket`, etc.) and infinite loops are rejected before execution.
