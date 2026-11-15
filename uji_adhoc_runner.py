"""Script uji langsung di GitHub Actions runner.
Menguji endpoint /api/inference/chat/agents/ad-hoc dengan berbagai kombinasi:
1. Anonim + client_attestation segar dari IP runner
2. Anonim + client_attestation + berbagai variasi header
3. Terotentikasi (JWT) + client_attestation
4. Memeriksa apakah response.output_item.added (function_call) muncul
"""
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
MIDDLEFACE = "0.1.1214"
MODEL = "deepseek-v4-1-flash-uncensored"
BASE = "https://outerface.venice.ai"

S = cr.Session(impersonate="chrome")

# 1. Mint attestation di runner IP
print("=== 1. Mint attestation di IP runner ===")
r_mint = S.post(BASE + "/api/app/user/client_attestation/mint",
                headers={"content-type": "application/json", "origin": "https://venice.ai",
                         "referer": "https://venice.ai/chat/classic", "user-agent": UA},
                json={"count": 5, "env": ATTEST_ENV}, timeout=30)
print("Mint status:", r_mint.status_code)
d_mint = r_mint.json() if r_mint.status_code == 200 else {}
tokens = d_mint.get("tokens") or [d_mint.get("token")]
tok = tokens[0]["token"] if isinstance(tokens[0], dict) else str(tokens[0])
print("Token minted (len=%d): %s..." % (len(tok), tok[:25]))

# Siapkan tool & pesan
TOOLS = [{
    "name": "kali",
    "description": "Kalikan dua bilangan bulat. Wajib dipanggil untuk menghitung.",
    "inputSchema": {
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
        "required": ["a", "b"]
    }
}]

def buat_pesan(teks):
    return [{
        "id": str(uuid.uuid4()),
        "role": "user",
        "type": "message",
        "status": "completed",
        "content": [{"type": "output_text", "text": teks, "annotations": []}]
    }]

BODY = {
    "messages": buat_pesan("Berapa 7 kali 8? Panggil tool kali."),
    "systemPrompt": {"instructions": "Kamu asisten AI teknis. Panggil tool jika diminta."},
    "tools": [],
    "clientTools": TOOLS,
    "reasoningMode": "disabled",
    "modelId": MODEL
}

def coba_request(nama, url, headers, body):
    print("\n--- Uji: %s ---" % nama)
    try:
        r = S.post(url, headers=headers, json=body, timeout=(10, 30), stream=True)
    except Exception as e:
        print("ERR:", str(e))
        return
    print("HTTP:", r.status_code)
    print("Headers:", {k: v for k, v in r.headers.items() if "rate" in k.lower() or "auth" in k.lower() or "type" in k.lower()})
    if r.status_code != 200:
        print("Body:", (r.text or "")[:300])
        r.close()
        return

    # Baca stream SSE
    ev = ""
    events = []
    items = []
    function_calls = []
    teks = []
    for ln in r.iter_lines():
        if isinstance(ln, bytes):
            ln = ln.decode("utf-8", "replace")
        if not ln:
            continue
        if ln.startswith("event: "):
            ev = ln[7:].strip()
            events.append(ev)
            continue
        if ln.startswith(":"):
            continue
        if not ln.startswith("data: "):
            continue
        data = ln[6:]
        if data == "[DONE]":
            break
        try:
            j = json.loads(data)
        except Exception:
            continue
        it = j.get("item")
        if isinstance(it, dict):
            items.append((ev, it.get("type"), it.get("name")))
            if it.get("type") == "function_call":
                function_calls.append(it)
        if ev == "response.output_text.delta":
            teks.append(j.get("delta") or "")

    r.close()
    print("Events ringkas:", list(set(events))[:10])
    print("Items:", items)
    if function_calls:
        print("\n*** FUNCTION_CALL BERHASIL MUNCUL! ***")
        print(json.dumps(function_calls, indent=2))
    else:
        print("Teks jawaban:", "".join(teks)[:200])

# Header dasar
H_BASE = {
    "accept": "text/event-stream",
    "content-type": "application/json",
    "origin": "https://venice.ai",
    "referer": "https://venice.ai/",
    "user-agent": UA,
    "x-venice-client-attestation": tok,
    "x-venice-locale": "en-US",
    "x-venice-middleface-version": MIDDLEFACE,
    "x-venice-request-timestamp-ms": str(int(time.time() * 1000)),
    "x-venice-version": VER
}

# Tes 1: ad-hoc anonim biasa
coba_request("ad-hoc (anon, attestation)", BASE + "/api/inference/chat/agents/ad-hoc", H_BASE, BODY)

# Tes 2: ad-hoc anonim tanpa modelId
b2 = dict(BODY)
del b2["modelId"]
coba_request("ad-hoc (anon, tanpa modelId)", BASE + "/api/inference/chat/agents/ad-hoc", H_BASE, b2)

# Tes 3: ad-hoc dengan JWT (jika diberikan via env VENICE_JWT)
jwt = os.environ.get("VENICE_JWT")
if jwt:
    h_jwt = dict(H_BASE)
    h_jwt["authorization"] = "Bearer " + jwt
    coba_request("ad-hoc (dengan JWT)", BASE + "/api/inference/chat/agents/ad-hoc", h_jwt, BODY)
else:
    print("\n(VENICE_JWT tidak diberikan, lewati uji JWT)")

# Tes 4: sebagai perbandingan, uji /agents/venice dengan clientTools
b_venice = {
    "input": {"messages": buat_pesan("Berapa 7 kali 8? Panggil tool kali.")},
    "clientTools": TOOLS
}
coba_request("/agents/venice (anon, dengan clientTools)", BASE + "/api/inference/chat/agents/venice?agentModelId=" + MODEL, H_BASE, b_venice)
