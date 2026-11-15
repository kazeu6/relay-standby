"""Script uji persis seperti worker.py di GitHub runner."""
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

# Mint attestation persis seperti worker.py
m = S.post("https://outerface.venice.ai/api/app/user/client_attestation/mint",
           headers=H, json={"count": 5, "env": ATTEST_ENV}, timeout=30)
print("mint status:", m.status_code)
d = m.json()
tokens = [t["token"] if isinstance(t, dict) else str(t) for t in d.get("tokens", [])] or [d.get("token")]
tok = tokens[0]
print("mint token len:", len(tok))

hdr = {**H, "accept": "text/event-stream",
       "x-venice-client-attestation": tok,
       "x-venice-locale": "en-US",
       "x-venice-middleface-version": "0.1.1214",
       "x-venice-request-timestamp-ms": str(int(time.time() * 1000)),
       "x-venice-version": VER}

TOOLS = [{
    "name": "kali",
    "description": "Kalikan dua bilangan bulat.",
    "inputSchema": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]}
}]

def pesan(t):
    return [{"id": str(uuid.uuid4()), "role": "user", "type": "message", "status": "completed",
             "content": [{"type": "output_text", "text": t, "annotations": []}]}]

# 1. Uji kontrol: /agents/venice harus 200
b_ctrl = {"input": {"messages": pesan("Halo!")}}
r_ctrl = S.post("https://outerface.venice.ai/api/inference/chat/agents/venice?agentModelId=" + MODEL,
                headers=hdr, json=b_ctrl, timeout=(10, 25), stream=True)
print("1. kontrol /agents/venice status:", r_ctrl.status_code, "rem:", r_ctrl.headers.get("x-ratelimit-remaining"))
r_ctrl.close()

# 2. Uji /agents/ad-hoc anonim
b_adhoc = {
    "messages": pesan("Berapa 7 x 8? Gunakan tool kali."),
    "systemPrompt": {"instructions": "Kamu asisten AI."},
    "tools": [],
    "clientTools": TOOLS,
    "reasoningMode": "disabled",
    "modelId": MODEL
}
r_adhoc = S.post("https://outerface.venice.ai/api/inference/chat/agents/ad-hoc",
                 headers=hdr, json=b_adhoc, timeout=(10, 25), stream=True)
print("2. /agents/ad-hoc (anon) status:", r_adhoc.status_code,
      "clerk-reason:", r_adhoc.headers.get("x-clerk-auth-reason"),
      "clerk-status:", r_adhoc.headers.get("x-clerk-auth-status"))
if r_adhoc.status_code == 200:
    for ln in r_adhoc.iter_lines():
        if isinstance(ln, bytes): ln = ln.decode("utf-8", "replace")
        if "function_call" in ln or "output_item" in ln or "delta" in ln:
            print("  DATA:", ln[:200])
r_adhoc.close()

# 3. Uji /agents/ad-hoc dengan JWT
jwt = os.environ.get("VENICE_JWT")
if jwt:
    hdr_jwt = dict(hdr)
    hdr_jwt["authorization"] = "Bearer " + jwt
    r_jwt = S.post("https://outerface.venice.ai/api/inference/chat/agents/ad-hoc",
                   headers=hdr_jwt, json=b_adhoc, timeout=(10, 25), stream=True)
    print("3. /agents/ad-hoc (jwt) status:", r_jwt.status_code,
          "rate-rem:", r_jwt.headers.get("x-ratelimit-remaining"),
          "body:", (r_jwt.text or "")[:200].replace("\n", " "))
    if r_jwt.status_code == 200:
        for ln in r_jwt.iter_lines():
            if isinstance(ln, bytes): ln = ln.decode("utf-8", "replace")
            if "function_call" in ln or "output_item" in ln or "delta" in ln:
                print("  DATA:", ln[:200])
    r_jwt.close()
