"""Worker standby Venice untuk GitHub Actions.

Worker dinyalakan lebih dulu, berlangganan topik perintah MQTT, lalu
menganggur sampai perintah `ask` datang. Satu worker = satu IP publik =
jatah `CAP` (default 8) chat Venice.

Env:
  TOPIC  topik MQTT untuk perintah (wajib saat menjalankan main())
  POOL   topik MQTT untuk laporan (default TOPIC + "-p")
  IDLE   detik menganggur sebelum keluar (default 600)
"""
import base64
import json
import os
import sys
import threading
import time
import uuid
import zlib

from curl_cffi import requests as cr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mqtt

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
ATTEST_ENV = {
    "nativeIntegrityOk": True,
    "reactStateHash": "b0447632072f076aa8a159db137c2e7e787cfc3212f58bb72ce26611cef138b8b",
    "sentryVersion": "10.50.0",
    "unexpectedGlobalsCount": 670,
    "webdriver": False,
}
VER = os.environ.get("VENICE_VER", "interface@20261111.090351+046c490")
MIDDLEFACE = os.environ.get("VENICE_MIDDLEFACE", "0.1.1234")
MODEL = "deepseek-v4-1-flash-uncensored"

TOPIC = os.environ.get("TOPIC", "")
POOL = os.environ.get("POOL") or ((TOPIC + "-p") if TOPIC else "")
IDLE = int(os.environ.get("IDLE", "600"))
CAP = int(os.environ.get("CAP", "8"))
MQTT_HOST = os.environ.get("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_TLS = os.environ.get("MQTT_TLS", "1").strip().lower() not in (
    "0", "false", "no", "off", "")
MQTT_USER = os.environ.get("MQTT_USER") or None
MQTT_PASS = os.environ.get("MQTT_PASS") or None
WID = uuid.uuid4().hex[:8]
VER_WORKER = "18"

_BUS = [None]
_KIRIM = []
_KIRIM_LOCK = threading.Lock()

H = {"content-type": "application/json",
     "origin": "https://venice.ai",
     "referer": "https://venice.ai/chat/classic",
     "user-agent": UA}

# Sesi HTTP per-thread agar beberapa thread `kerja` tidak saling merusak state
# libcurl. Variabel modul `S` tetap dipertahankan sebagai hook untuk unit test.
_SESI = threading.local()
S = None


def _session():
    if S is not None:
        return S
    s = getattr(_SESI, "s", None)
    if s is None:
        s = cr.Session(impersonate="chrome")
        s.headers.update({"user-agent": UA})
        _SESI.s = s
    return s


def publish(topic, payload, penting=False):
    """Kirim laporan lewat MQTT tanpa memblokir thread pembaca."""
    payload = {**payload, "ts": time.time()}
    data = json.dumps(payload)
    c = _BUS[0]
    if c and c.connected:
        try:
            c.publish(topic, data)
            return True
        except Exception as e:
            print("publish lewat bus gagal: %s" % str(e)[:80], flush=True)
    with _KIRIM_LOCK:
        if len(_KIRIM) < 200:
            _KIRIM.append((topic, data, penting))
    return False


def _pengirim(stop):
    """Kirim ulang pesan yang gagal lewat koneksi sekali pakai."""
    while not stop.is_set():
        with _KIRIM_LOCK:
            butir = _KIRIM.pop(0) if _KIRIM else None
        if not butir:
            stop.wait(0.2)
            continue
        topic, data, penting = butir
        c = _BUS[0]
        if c and c.connected:
            try:
                c.publish(topic, data)
                continue
            except Exception:
                pass
        t = None
        try:
            t = mqtt.Client(host=MQTT_HOST, port=MQTT_PORT,
                            client_id="wx" + uuid.uuid4().hex[:8],
                            tls=MQTT_TLS, username=MQTT_USER,
                            password=MQTT_PASS)
            t.connect(timeout=5)
            t.publish(topic, data)
        except Exception as e:
            print("publish cadangan gagal: %s" % str(e)[:80], flush=True)
            if penting:
                with _KIRIM_LOCK:
                    if len(_KIRIM) < 100:
                        _KIRIM.append((topic, data, penting))
            stop.wait(0.5)
        finally:
            if t is not None:
                try:
                    t.close()
                except Exception:
                    pass


class Tokens:
    """Token attestation; mint ulang otomatis saat habis atau basi (TTL 240s)."""

    def __init__(self):
        self.tokens = []
        self.minted_at = 0.0
        self.lock = threading.Lock()

    def get(self, paksa=False):
        with self.lock:
            if paksa or not self.tokens or (time.monotonic() - self.minted_at > 240):
                self.tokens.clear()
                m = _session().post(
                    "https://outerface.venice.ai/api/app/user/client_attestation/mint",
                    headers=H, json={"count": 5, "env": ATTEST_ENV}, timeout=30)
                m.raise_for_status()
                d = m.json()
                if isinstance(d.get("token"), str):
                    self.tokens = [d["token"]]
                elif isinstance(d.get("tokens"), list) and d["tokens"]:
                    self.tokens = [t["token"] if isinstance(t, dict) else str(t)
                                   for t in d["tokens"]]
                else:
                    raise RuntimeError("mint: tidak ada token (%s)" % list(d.keys()))
                self.minted_at = time.monotonic()
            return self.tokens.pop(0)


TOK = Tokens()

# Jalur murni Classic `/api/inference/chat`: nol hidden prompt Venice, nol 20
# native server tool, dan tembus 1.000.000 char verbatim dengan `isFile: true`
# + `enableLargeContextChat: true` + `includeVeniceSystemPrompt: false`.
CHAT_URL = "https://outerface.venice.ai/api/inference/chat"

_HTTP_TIMEOUT = (15, 120)

_LCC_LOCK = threading.Lock()
_LCC = [True]
_LCC_MIN_CHARS = 66000
_ISFILE_MIN_CHARS = 40000
_TAIL_USER_MAX = 35000
_SYS_MAX_CHARS = 32000
_MAX_RESP_DEFAULT = int(os.environ.get("VENICE_MAX_RESP_TOKENS", "131072"))
_SEP_PERCAKAPAN = "\n\n===== PERCAKAPAN =====\n\n"


def _opsi_reasoning(opts=None):
    """Tentukan flag `reasoning` (per-request opts > env `VENICE_REASONING` > default True)."""
    if isinstance(opts, dict) and isinstance(opts.get("reasoning"), bool):
        return opts["reasoning"]
    v = os.environ.get("VENICE_REASONING", "1").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "ya", "yes", "on"):
        return True
    return True


def _classic_body(prompt, lcc_on=True, opts=None):
    """Bangun body Classic `/api/inference/chat` tanpa truncation hingga 1M char."""
    opts = opts if isinstance(opts, dict) else {}
    body = {
        "modelId": MODEL,
        "requestId": str(uuid.uuid4()),
        "includeVeniceSystemPrompt": False,
        "webEnabled": False,
        "webScrapeEnabled": False,
        "xSearchEnabled": False,
        "reasoning": _opsi_reasoning(opts),
    }

    mt = opts.get("max_tokens")
    if isinstance(mt, int) and not isinstance(mt, bool) and mt > 0:
        body["maxResponseTokens"] = min(mt, _MAX_RESP_DEFAULT)
    elif _MAX_RESP_DEFAULT > 0:
        body["maxResponseTokens"] = _MAX_RESP_DEFAULT

    temp = opts.get("temperature")
    if isinstance(temp, (int, float)) and not isinstance(temp, bool) and 0.0 <= float(temp) <= 2.0:
        body["temperature"] = float(temp)
    tp = opts.get("top_p")
    if isinstance(tp, (int, float)) and not isinstance(tp, bool) and 0.0 < float(tp) <= 1.0:
        body["topP"] = float(tp)

    isi = prompt or ""
    if _SEP_PERCAKAPAN in isi:
        kepala, ekor = isi.split(_SEP_PERCAKAPAN, 1)
        if kepala.strip() and len(kepala) <= _SYS_MAX_CHARS and ekor.strip():
            body["systemPrompt"] = kepala.strip()
            isi = ekor

    if len(isi) >= _ISFILE_MIN_CHARS:
        idx_u = isi.rfind("\n\nUser: ")
        if idx_u >= 1000 and (len(isi) - idx_u) < _TAIL_USER_MAX:
            hist = isi[:idx_u]
            tail = isi[idx_u + 2:]
            body["prompt"] = [
                {"role": "user", "content": hist, "isFile": True},
                {"role": "user", "content": tail},
            ]
        else:
            body["prompt"] = [
                {"role": "user", "content": isi, "isFile": True},
                {"role": "user", "content": (
                    "Lanjutkan percakapan di atas dan balas langsung sebagai "
                    "Assistant sesuai instruksi sistem."
                )},
            ]
    else:
        body["prompt"] = [{"role": "user", "content": isi}]

    if lcc_on and len(prompt or "") >= _LCC_MIN_CHARS:
        body["enableLargeContextChat"] = True
    return body


def _tutup_resp(r):
    if r is not None:
        try:
            r.close()
        except Exception:
            pass


def ask(prompt, on_delta=None, on_maju=None, stop_check=None, opts=None):
    """Satu chat murni lewat Classic `/api/inference/chat`. Kembalikan (jawaban, status, sisa_kuota, err)."""
    sesi = _session()
    hdr = {**H, "accept": "text/event-stream",
           "x-venice-client-attestation": TOK.get(),
           "x-venice-locale": "en-US",
           "x-venice-middleface-version": MIDDLEFACE,
           "x-venice-request-timestamp-ms": str(int(time.time() * 1000)),
           "x-venice-version": VER}
    with _LCC_LOCK:
        lcc_on = _LCC[0]
    body = _classic_body(prompt, lcc_on=lcc_on, opts=opts)

    r = sesi.post(CHAT_URL, headers=hdr, json=body, timeout=_HTTP_TIMEOUT, stream=True)
    try:
        if r.status_code == 400 and body.get("enableLargeContextChat"):
            _tutup_resp(r)
            with _LCC_LOCK:
                _LCC[0] = False
            body.pop("enableLargeContextChat", None)
            r = sesi.post(CHAT_URL, headers=hdr, json=body, timeout=_HTTP_TIMEOUT, stream=True)
        if r.status_code == 400 and any(k in body for k in ("maxResponseTokens", "temperature", "topP")):
            _tutup_resp(r)
            for k in ("maxResponseTokens", "temperature", "topP"):
                body.pop(k, None)
            r = sesi.post(CHAT_URL, headers=hdr, json=body, timeout=_HTTP_TIMEOUT, stream=True)
        if r.status_code in (401, 403):
            try:
                tok_baru = TOK.get(paksa=True)
            except Exception:
                tok_baru = None
            if tok_baru:
                _tutup_resp(r)
                hdr["x-venice-client-attestation"] = tok_baru
                r = sesi.post(CHAT_URL, headers=hdr, json=body, timeout=_HTTP_TIMEOUT,
                              stream=True)
        if r.status_code != 200:
            try:
                err = (r.text or "")[:300].replace("\n", " ")
            except Exception:
                err = ""
            return "", r.status_code, r.headers.get("x-ratelimit-remaining"), \
                err or ("HTTP %s tanpa body" % r.status_code)

        ans = ""
        cadangan = ""
        jenis = {}
        dalam_think = [False]

        def emit(potong):
            nonlocal ans
            if not potong:
                return
            ans += potong
            if on_delta:
                on_delta(len(ans) - len(potong), potong)

        def proses(ln):
            """Urai satu baris NDJSON Classic (termasuk `reasoning_content`). Kembalikan True kalau ada kemajuan nyata."""
            nonlocal cadangan
            try:
                o = json.loads(ln)
            except Exception:
                return False
            if not isinstance(o, dict):
                return False
            k = o.get("kind") or "(tanpa kind)"
            jenis[k] = jenis.get(k, 0) + 1
            if k in ("reasoning", "thinking"):
                potong = o.get("content") or o.get("reasoning_content") or o.get("reasoning") or o.get("thinking") or ""
                if potong:
                    cadangan += potong
                    if not dalam_think[0]:
                        dalam_think[0] = True
                        emit("<think>")
                    emit(potong)
                    return True
                return False
            if k == "content":
                # Venice Classic mengirim penalaran pada field `reasoning_content` di dalam `kind="content"`.
                rc = o.get("reasoning_content")
                potong = o.get("content") or ""
                maju = False
                if isinstance(rc, str) and rc:
                    cadangan += rc
                    if not dalam_think[0]:
                        dalam_think[0] = True
                        emit("<think>")
                    emit(rc)
                    maju = True
                if potong:
                    if dalam_think[0]:
                        dalam_think[0] = False
                        emit("</think>")
                    emit(potong)
                    maju = True
                return maju
            potong = ""
            for f in ("content", "text", "reasoning_content", "reasoning", "thinking", "delta"):
                v = o.get(f)
                if isinstance(v, str) and v:
                    potong += v
            if potong:
                cadangan += potong
            return False

        for ln in r.iter_lines():
            if stop_check and stop_check():
                return "", 499, 0, "dibatalkan pengelola"
            if isinstance(ln, bytes):
                ln = ln.decode("utf-8", "replace")
            if not ln:
                continue
            teks_datang = proses(ln)
            if teks_datang and on_maju:
                on_maju(len(ln))

        if dalam_think[0]:
            dalam_think[0] = False
            emit("</think>")

        if not ans.strip() and cadangan.strip():
            ans = cadangan

        if r.status_code == 200 and not ans.strip():
            err = ("kosong; jenis=%s contoh=%s"
                   % ({k: v for k, v in list(jenis.items())[:6]},
                      cadangan[:120]))[:300]
        elif r.status_code == 200:
            err = ""
        else:
            err = "HTTP %s" % r.status_code
        return ans.strip(), r.status_code, \
            r.headers.get("x-ratelimit-remaining"), err
    finally:
        _tutup_resp(r)


def ask_ulang(prompt, coba=2, on_delta=None, on_maju=None, stop_check=None, opts=None):
    """`ask` dengan percobaan ulang kalau balasannya kosong."""
    ans, status, rem, err = ask(prompt, on_delta=on_delta,
                                on_maju=on_maju, stop_check=stop_check, opts=opts)
    for _ in range(coba):
        if ans or status != 200 or (stop_check and stop_check()):
            break
        time.sleep(0.4)
        if on_delta:
            on_delta(-1, "")
        ans, status, rem, err = ask(prompt, on_delta=on_delta,
                                    on_maju=on_maju, stop_check=stop_check, opts=opts)
    return ans, status, rem, err


def listen(stop, on_msg, topic=None):
    """Dengarkan topik perintah lewat MQTT, sambung ulang kalau putus."""
    topik = topic or TOPIC
    while not stop.is_set():
        c = None
        try:
            c = mqtt.Client(host=MQTT_HOST, port=MQTT_PORT,
                            client_id="vw" + WID, tls=MQTT_TLS,
                            username=MQTT_USER, password=MQTT_PASS)

            def cb(t, payload):
                if stop.is_set():
                    return
                try:
                    on_msg({"message": payload.decode("utf-8", "replace")})
                except Exception:
                    pass

            c.on_message = cb
            c.connect()
            c.subscribe(topik)
            c.start()
            _BUS[0] = c
            print("listen mqtt %s:%d topik=%s" % (MQTT_HOST, MQTT_PORT, topik),
                  flush=True)
            while c.connected and not stop.is_set():
                time.sleep(0.5)
            _BUS[0] = None
            c.close()
            print("mqtt putus, sambung ulang", flush=True)
        except Exception as e:
            _BUS[0] = None
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass
            print("mqtt error: %s" % str(e)[:120], flush=True)
        if not stop.is_set():
            time.sleep(1.0)


def _baca_prompt(task):
    """Ekstrak prompt dari payload tugas MQTT (mendukung `prompt_z` zlib+base64)."""
    pz = task.get("prompt_z")
    if isinstance(pz, str) and pz:
        return zlib.decompress(base64.b64decode(pz.encode("ascii"))).decode("utf-8")
    return task.get("prompt", "")


class WorkerRuntime:
    """State dan dispatcher pesan untuk satu instance worker."""

    def __init__(self, wid=None, pool_topic=None, cap=None, idle=None,
                 pub_fn=None, ask_fn=None):
        self.wid = wid or WID
        self.pool_topic = pool_topic if pool_topic is not None else POOL
        self.cap = CAP if cap is None else int(cap)
        self.idle = IDLE if idle is None else int(idle)
        self._pub = pub_fn or publish
        self._ask = ask_fn or ask_ulang

        self.stop = threading.Event()
        self.draining = threading.Event()
        now = time.monotonic()
        self.last_task = now     # HANYA diperbarui oleh tugas nyata, BUKAN ping!
        self.yatim_at = now      # Diperbarui oleh sapaan `ping` dari pengelola.
        self.used = 0
        self.doing = 0
        self.seen = {}           # tid -> payload hasil (dedupe kirim ulang)
        self.batal_set = set()   # tid yang dibatalkan pengelola
        self.kunci = threading.Lock()

    def _is_cancelled(self, tid):
        with self.kunci:
            return tid in self.batal_set

    def should_exit(self, now=None):
        now = time.monotonic() if now is None else now
        with self.kunci:
            used = self.used
            doing = self.doing
            last_task = self.last_task
        if used >= self.cap and doing == 0:
            return True
        if self.stop.is_set() and doing == 0:
            return True
        if self.draining.is_set() and doing == 0:
            return True
        if doing == 0 and (now - last_task > self.idle):
            return True
        return False

    def on_msg(self, o):
        try:
            task = json.loads(o.get("message", ""))
        except Exception:
            return
        if not isinstance(task, dict):
            return

        jenis = task.get("t")
        if jenis == "ping":
            # Sapaan pengelola: perbarui `yatim_at` SAJA, JANGAN sentuh `last_task`
            # supaya timer IDLE tetap berjalan saat tidak ada tugas nyata.
            with self.kunci:
                self.yatim_at = time.monotonic()
            return

        if jenis == "retire":
            if task.get("to") in (self.wid, "any"):
                with self.kunci:
                    d = self.doing
                print("diperintahkan pensiun (tunggu %d chat selesai)" % d,
                      flush=True)
                self.draining.set()
            return

        if jenis == "cancel":
            cid = task.get("id")
            if task.get("to") in (self.wid, "any") and cid:
                with self.kunci:
                    self.batal_set.add(cid)
            return

        if jenis != "ask":
            return
        if task.get("to") not in (self.wid, "any"):
            return

        tid = task.get("id", "")
        if self.draining.is_set():
            self._pub(self.pool_topic, {"t": "busy", "id": self.wid, "tid": tid,
                                        "why": "pensiun"}, penting=True)
            return

        tolak_penuh = False
        with self.kunci:
            ulang = tid in self.seen
            lama = self.seen.get(tid)
            if not ulang:
                if self.used + self.doing >= self.cap:
                    tolak_penuh = True
                else:
                    self.seen[tid] = None
                    self.doing += 1
                    self.last_task = time.monotonic()

        if tolak_penuh:
            self._pub(self.pool_topic, {"t": "busy", "id": self.wid, "tid": tid,
                                        "why": "jatah penuh"}, penting=True)
            return

        self._pub(self.pool_topic, {"t": "ack", "id": self.wid, "tid": tid},
                  penting=True)

        if ulang:
            if lama:
                self._pub(self.pool_topic, lama, penting=True)
            return

        threading.Thread(target=self._kerja, args=(tid, task), daemon=True).start()

    def _kerja(self, tid, task):
        t1 = time.monotonic()
        mau_stream = bool(task.get("stream"))
        nadi_stop = threading.Event()
        maju = [0]

        def nadi():
            terakhir = maju[0]
            while not nadi_stop.wait(10):
                if maju[0] != terakhir:
                    terakhir = maju[0]
                    self._pub(self.pool_topic, {"t": "n", "id": self.wid, "tid": tid})

        threading.Thread(target=nadi, daemon=True).start()

        buf = []
        buf_len = [0]
        off_awal = [0]
        last_flush = [time.monotonic()]
        kunci_delta = threading.Lock()

        def flush(paksa=False):
            with kunci_delta:
                if not buf:
                    return
                now = time.monotonic()
                if not paksa and (buf_len[0] < 120
                                  and now - last_flush[0] < 0.08):
                    return
                teks = "".join(buf)
                buf.clear()
                buf_len[0] = 0
                last_flush[0] = now
                offset_kirim = off_awal[0]
                off_awal[0] += len(teks)
            self._pub(self.pool_topic, {"t": "d", "id": self.wid, "tid": tid,
                                        "o": offset_kirim, "c": teks})

        def on_delta(off, teks):
            if not mau_stream:
                return
            if off < 0:
                with kunci_delta:
                    buf.clear()
                    buf_len[0] = 0
                    off_awal[0] = 0
                self._pub(self.pool_topic, {"t": "d", "id": self.wid, "tid": tid,
                                            "o": -1, "c": ""})
                return
            with kunci_delta:
                buf.append(teks)
                buf_len[0] += len(teks)
            flush()

        def catat_maju(n):
            maju[0] += n

        try:
            prompt_teks = _baca_prompt(task)
            task_opts = task.get("opts") if isinstance(task.get("opts"), dict) else None
            ans, status, rem, err = self._ask(
                prompt_teks,
                on_delta=on_delta,
                on_maju=catat_maju,
                stop_check=lambda: self._is_cancelled(tid),
                opts=task_opts)
        except Exception as e:
            nadi_stop.set()
            with self.kunci:
                self.doing = max(0, self.doing - 1)
                self.last_task = time.monotonic()
                res = {"t": "res", "id": self.wid, "tid": tid,
                       "err": str(e)[:150], "used": self.used}
                self.seen[tid] = res
            self._pub(self.pool_topic, res, penting=True)
            return

        if self._is_cancelled(tid):
            nadi_stop.set()
            with self.kunci:
                self.doing = max(0, self.doing - 1)
                self.last_task = time.monotonic()
                self.batal_set.discard(tid)
                self.seen.pop(tid, None)
            print("[kerja] tugas %s dibatalkan pengelola, keluar" % tid, flush=True)
            return

        nadi_stop.set()
        if mau_stream:
            flush(paksa=True)
        ms = int((time.monotonic() - t1) * 1000)
        with self.kunci:
            if status == 200 and ans:
                self.used += 1
            n = self.used
            self.doing = max(0, self.doing - 1)
            self.last_task = time.monotonic()
            res = {"t": "res", "id": self.wid, "tid": tid, "ans": ans,
                   "ms": ms, "status": status, "rem": rem,
                   "err": err, "used": n}
            self.seen[tid] = res
            if len(self.seen) > 200:
                self.seen.pop(next(iter(self.seen)))
        self._pub(self.pool_topic, res, penting=True)
        print("  %s -> %dms status=%s rem=%s err=%s ans=%r" % (
            tid, ms, status, rem, err[:60], ans[:30]), flush=True)


def main():
    topik = TOPIC or os.environ.get("TOPIC", "")
    if not topik:
        sys.exit("env TOPIC wajib diisi")
    pool_topik = POOL or os.environ.get("POOL") or (topik + "-p")

    t0 = time.monotonic()
    try:
        ip = _session().get("https://api.ipify.org", timeout=15).text.strip()
    except Exception:
        ip = "?"

    rt = WorkerRuntime(wid=WID, pool_topic=pool_topik, cap=CAP, idle=IDLE)

    threading.Thread(target=listen, args=(rt.stop, rt.on_msg, topik), daemon=True).start()
    threading.Thread(target=_pengirim, args=(rt.stop,), daemon=True).start()
    tunggu = time.monotonic() + 20
    while _BUS[0] is None and time.monotonic() < tunggu and not rt.stop.is_set():
        time.sleep(0.05)

    if _BUS[0] is None:
        print("mqtt tidak tersambung dalam 20s, keluar", flush=True)
        rt.stop.set()
        return

    try:
        TOK.get()
    except Exception as e:
        print("gagal panaskan token: %s" % str(e)[:120], flush=True)

    publish(pool_topik, {"t": "up", "id": WID, "ip": ip, "v": VER_WORKER,
                         "warm": int((time.monotonic() - t0) * 1000)})
    print("worker %s siap ip=%s warm=%dms" % (
        WID, ip, (time.monotonic() - t0) * 1000), flush=True)

    def beat():
        while not rt.stop.is_set():
            if rt.stop.wait(15):
                return
            publish(pool_topik, {"t": "up", "id": WID, "ip": ip, "beat": True,
                                 "v": VER_WORKER})
            with rt.kunci:
                yatim_age = time.monotonic() - rt.yatim_at
            if yatim_age > 90:
                print("tidak disapa pengelola 90s, keluar (yatim)", flush=True)
                rt.stop.set()
                return

    threading.Thread(target=beat, daemon=True).start()

    while not rt.should_exit():
        time.sleep(1)

    with rt.kunci:
        used_final = rt.used
    # Kirim laporan `down` SEBELUM menyetel `stop`, supaya bus/pengirim sempat mengirim.
    publish(pool_topik, {"t": "down", "id": WID, "used": used_final}, penting=True)
    time.sleep(0.2)
    rt.stop.set()
    print("worker %s keluar, dipakai %d kali" % (WID, used_final), flush=True)


if __name__ == "__main__":
    main()
