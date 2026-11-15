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

body = {
    "input": {
        "messages": pesan("Hitung 12345 kali 67890. Wajib gunakan tool kali."),
        "clientTools": TOOLS
    }
}

r = S.post(BASE + "/api/inference/chat/agents/venice?agentModelId=" + MODEL,
           headers=hdr, json=body, timeout=(10, 30), stream=True)

print("HTTP:", r.status_code)
function_calls = []
teks = []
current_event = ""

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
        print("ITEM:", current_event, it.get("type"), it.get("name"))
        if it.get("type") == "function_call":
            function_calls.append(it)
    
    if current_event == "response.output_text.delta":
        teks.append(j.get("delta") or "")

print("\nFunction calls found:", len(function_calls))
if function_calls:
    print(json.dumps(function_calls, indent=2))
print("Teks output:\n", "".join(teks))
