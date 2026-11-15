"""Periksa detail apakah input.clientTools menghasilkan function_call atau diabaikan."""
import json
import time
import uuid
from curl_cffi import requests as cr

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")

ATTEST_ENV = {
    "nativeIntegrityOk": True,
    "reactStateHash": "b0447632072f076aa8a159db137c2e7e787cfc3212f58bb72ce26611cef138b8b",
    "sentryVersion": "10.50.0",
    "unexpectedGlobalsCount": 670,
    "webdriver": False,
}

VER = "interface@20261107.041322+9682fa7"
MODEL = "deepseek-v4-1-flash-uncensored"
BASE = "https://outerface.venice.ai"

H = {"content-type": "application/json",
     "origin": "https://venice.ai",
     "referer": "https://venice.ai/chat/classic",
     "user-agent": UA}

S = cr.Session(impersonate="chrome")
S.headers.update({"user-agent": UA})

m = S.post(BASE + "/api/app/user/client_attestation/mint",
           headers=H, json={"count": 5, "env": ATTEST_ENV}, timeout=30)
tok = (m.json().get("tokens") or [m.json().get("token")])[0]
if isinstance(tok, dict): tok = tok["token"]

hdr = {**H, "accept": "text/event-stream",
       "x-venice-client-attestation": tok,
       "x-venice-locale": "en-US",
       "x-venice-middleface-version": "0.1.1214",
       "x-venice-request-timestamp-ms": str(int(time.time() * 1000)),
       "x-venice-version": VER}


def pesan(t):
    return [{"id": str(uuid.uuid4()), "role": "user", "type": "message", "status": "completed",
             "content": [{"type": "output_text", "text": t, "annotations": []}]}]

TOOLS = [{
    "name": "kali",
    "description": "Kalikan dua bilangan bulat. Tool ini wajib dipanggil.",
    "inputSchema": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]}
}]


def make_body(with_tools=True):
    msgs = pesan("Hitung 12345 kali 67890. Wajib gunakan tool kali.")
    inp = {"messages": msgs}
    if with_tools:
        inp["clientTools"] = TOOLS
    return {"input": inp}


test_bodies = []

# 1. venice + input.clientTools
test_bodies.append(("venice input.clientTools", "/api/inference/chat/agents/venice?agentModelId=" + MODEL,
                     make_body(True)))

# 2. venice root clientTools (stripped by middleware but include for reference)
test_bodies.append(("venice root clientTools", "/api/inference/chat/agents/venice?agentModelId=" + MODEL,
                     {"input": make_body(False)["input"], "clientTools": TOOLS}))

# 3. mind-assistant + input.clientTools
test_bodies.append(("mind-assistant input.clientTools", "/api/inference/chat/agents/mind-assistant?agentModelId=" + MODEL,
                     make_body(True)))

# 4. studio + input.clientTools
test_bodies.append(("studio input.clientTools", "/api/inference/chat/agents/studio?agentModelId=" + MODEL,
                     make_body(True)))

# 5. ad-hoc baseline (no clientTools)
test_bodies.append(("ad-hoc baseline", "/api/inference/chat/agents/ad-hoc",
                     make_body(False)))

# 6. ad-hoc + input.clientTools
test_bodies.append(("ad-hoc input.clientTools", "/api/inference/chat/agents/ad-hoc",
                     make_body(True)))


def run_test(label, url, body):
    try:
        r = S.post(BASE + url, headers=hdr, json=body, timeout=(10, 30), stream=True)
        function_calls = []
        teks = []
        current_event = ""
        print("\n--- %s ---" % label)
        print("Status: %s | rem: %s | clerk: %s" % (
            r.status_code, r.headers.get("x-ratelimit-remaining"), r.headers.get("x-clerk-auth-reason")
        ))
        if r.status_code == 200:
            for ln in r.iter_lines():
                if isinstance(ln, bytes): ln = ln.decode("utf-8", "replace")
                if not ln: continue
                if ln.startswith("event: "):
                    current_event = ln[7:].strip()
                    continue
                if not ln.startswith("data: "): continue
                data = ln[6:]
                if data == "[DONE]": break
                try:
                    j = json.loads(data)
                except:
                    continue
                
                it = j.get("item")
                if isinstance(it, dict):
                    print("ITEM:", current_event, it.get("type"), it.get("name"), it.get("call_id") if it.get("type") == "function_call" else "")
                    if it.get("type") == "function_call":
                        function_calls.append(it)
                
                if current_event == "response.output_text.delta":
                    teks.append(j.get("delta") or "")
                
                if len(function_calls) >= 3 and len(teks) >= 3: break
            print("Function calls found:", len(function_calls))
            if function_calls:
                print(json.dumps(function_calls[:3], indent=2))
            print("Teks output:\n", "".join(teks[:3]))
        else:
            print("Body:", (r.text or "")[:200])
        r.close()
    except Exception as e:
        print("ERR %s: %s" % (label, e))

for label, url, body in test_bodies:
    run_test(label, url, body)