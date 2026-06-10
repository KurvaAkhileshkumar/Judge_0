#!/usr/bin/env python3
import json, urllib.request, time, sys

bank = json.load(open('question_bank_30p_30tc.json'))
api = 'http://localhost:5001'

def post(url, data):
    req = urllib.request.Request(url, json.dumps(data).encode(), {'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def sse(url):
    req = urllib.request.Request(url, headers={'Accept':'text/event-stream'})
    deadline = time.monotonic() + 120
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            buf = b''
            while time.monotonic() < deadline:
                chunk = r.read(1024)
                if not chunk: break
                buf += chunk
                for line in buf.split(b'\n'):
                    line = line.decode(errors='replace').strip()
                    if line.startswith('data:'):
                        data = line[5:].strip()
                        if data: return json.loads(data)
    except Exception as e:
        return {'stream_error': str(e)}
    return {}

pass_count = 0
fail_count = 0
# Test TC1 of every problem
for p in bank['problems']:
    tc  = p['test_cases'][0]
    sol = p['solutions'][0]
    body = {
        'student_id':    'smoke_test',
        'assessment_id': 'smoke_' + p['id'],
        'language':      'python',
        'student_code':  sol['source_code'],
        'test_cases':    [{'stdin_text': tc['stdin_text'], 'expected': tc['expected']}],
        'mode':          'stdio',
        'per_tc_limit_s': 5,
    }
    try:
        resp   = post(api + '/submit', body)
        tid    = resp.get('ticket_id', '')
        result = sse(api + '/results/stream/' + tid)
        tcs    = result.get('tc_results', [])
        if tcs:
            status = tcs[0]['status']
            got    = tcs[0].get('got', '?')
        else:
            status = result.get('system_error', result.get('stream_error', 'NO_RESULT'))
            got    = ''
    except Exception as e:
        status = 'EXCEPTION'
        got    = str(e)[:60]

    ok = status == 'PASS'
    if ok: pass_count += 1
    else:  fail_count += 1
    icon = 'OK' if ok else 'FAIL'
    print(f"[{icon}] {p['id'][:45]:45s}  status={status}  exp={tc['expected']!r:10}  got={got!r}")

print(f"\n{pass_count}/{pass_count+fail_count} passed")
sys.exit(0 if fail_count == 0 else 1)
