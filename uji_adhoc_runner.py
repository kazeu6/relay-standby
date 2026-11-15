"""Print response headers lengkap untuk 403 pada ad-hoc."""
import json
import os
import sys
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

H = {"content-type": "application/json",
     "origin": "https://venice.ai",
     "referer": "https://venice.ai/chat/classic",
     "user-agent": UA}

S = cr.Session(impersonate="chrome")
S.headers.update({"user-agent": UA})

m = S.post("https://outerface.venice.ai/api/app/user/client_attestation/mint",
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

jwt = os.environ.get("VENICE_JWT")
if jwt:
    hdr["authorization"] = "Bearer " + jwt

b_adhoc = {
    "messages": [{"id": str(uuid.uuid4()), "role": "user", "type": "message", "status": "completed",
                 "content": [{"type": "output_text", "text": "7*8", "annotations": []}]}],
    "systemPrompt": {"instructions": "Asisten."},
    "tools": [],
    "clientTools": [{"name": "kali", "description": "kali", "inputSchema": {"type": "object", "properties": {}}}],
    "reasoningMode": "disabled",
    "modelId": MODEL
}

r = S.post("https://outerface.venice.ai/api/inference/chat/agents/ad-hoc",
           headers=hdr, json=b_adhoc, timeout=(10, 25))
print("Status:", r.status_code)
print("Headers:", dict(r.headers))
print("Body:", r.text[:500])
