"""Worker standby Venice untuk GitHub Actions.

Kenapa ada file ini
===================
`workflow_dispatch` butuh ~4-7 detik hanya untuk menyalakan VM. Kalau job
dinyalakan saat prompt datang, latency tidak mungkin di bawah 5 detik.

Jadi dibalik: worker dinyalakan LEBIH DULU, lalu menganggur sambil
mendengarkan perintah lewat MQTT (~0,2 detik). Prompt datang ->
langsung dikerjakan (~0,7 detik) -> jawaban dikirim balik.

Satu worker = satu IP publik = jatah 8 chat Venice. Setelah 8 chat, worker
melapor "habis" lalu keluar; pengelola di mesin lokal menyalakan pengganti.

Kenapa curl_cffi
================
Chat lewat `curl` biasa memicu challenge Cloudflare (403) setiap beberapa
request, karena tiap panggilan membuka handshake TLS baru. `curl_cffi`
dengan `impersonate="chrome"` memakai satu sesi + koneksi yang dipakai ulang
dan meniru fingerprint Chrome: diukur 8/8 sukses tanpa 403.

Env:
  TOPIC  topik MQTT untuk perintah (wajib)
  POOL   topik MQTT untuk laporan (default TOPIC + "-p")
  IDLE   detik menganggur sebelum keluar (default 600)
"""
import json
import os
import sys
import threading
import time
import uuid

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
VER = "interface@20261107.041322+9682fa7"
MODEL = "deepseek-v4-1-flash-uncensored"

TOPIC = os.environ["TOPIC"]
POOL = os.environ.get("POOL") or (TOPIC + "-p")
IDLE = int(os.environ.get("IDLE", "600"))
CAP = int(os.environ.get("CAP", "8"))
MQTT_HOST = os.environ.get("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
WID = uuid.uuid4().hex[:8]
# Versi kode worker. Dinaikkan setiap kali ada perubahan yang HARUS dipakai
# semua worker. Pool menolak worker dengan versi lebih rendah, karena worker
# lama yang masih hidup bisa membuat pool mengirim tugas ke kode yang sudah
# tidak cocok — gejalanya 502 acak dan latensi 12-31 detik pada permintaan
# pertama setelah deploy.
VER_WORKER = "12"
_BUS = [None]          # klien MQTT untuk publish, diisi oleh listen()
# Antrean kirim. WAJIB: `publish` dipanggil dari dalam `on_msg`, yaitu
# thread PEMBACA mqtt. Dulu jalur cadangan di `publish` membuka koneksi baru
# (`connect()` dengan timeout bawaan 15 detik) dan itu MEMBEKUKAN thread
# pembaca selama belasan detik. Selama beku, worker tidak membaca satu pun
# perintah masuk — termasuk `ask` berikutnya — sehingga pool melihatnya
# "tuli" dan melaporkan "tidak mengaku dalam 5s" padahal prosesnya hidup dan
# detak `up` masih terkirim lewat jalur tulis yang sama.
#
# Sekarang pengiriman penting tidak pernah menunggu di jalur pembaca: pesan
# dimasukkan ke antrean, satu thread pengirim yang mengurus koneksi cadangan.
_KIRIM = []
_KIRIM_LOCK = threading.Lock()

H = {"content-type": "application/json",
     "origin": "https://venice.ai",
     "referer": "https://venice.ai/chat/classic",
     "user-agent": UA}

S = cr.Session(impersonate="chrome")
S.headers.update({"user-agent": UA})


def publish(topic, payload, penting=False):
    """Kirim laporan lewat MQTT. Tanpa ini, pengelola tidak tahu hasilnya.

    `ts` disertakan supaya penerima bisa membuang pesan yang diputar ulang.
    `penting=True` (untuk ack & hasil) menjamin pesan tidak hilang: kalau bus
    utama sedang putus, pesan masuk antrean dan dikirim lewat jalur cadangan
    oleh thread terpisah.

    Penting: fungsi ini TIDAK PERNAH menunggu koneksi baru. Ia dipanggil dari
    thread pembaca MQTT (`on_msg`), jadi menunggu di sini berarti berhenti
    membaca perintah — lihat catatan di `_KIRIM`.
    """
    payload = {**payload, "ts": time.time()}
    data = json.dumps(payload)
    c = _BUS[0]
    if c and c.connected:
        try:
            c.publish(topic, data)
            return True
        except Exception as e:
            print("publish lewat bus gagal: %s" % str(e)[:80], flush=True)
    # Bus putus: serahkan ke pengirim latar, jangan blokir pemanggil.
    with _KIRIM_LOCK:
        if len(_KIRIM) < 200:
            _KIRIM.append((topic, data, penting))
    return False


def _pengirim(stop):
    """Kirim ulang pesan yang gagal lewat koneksi sekali pakai.

    Timeout `connect` dipersingkat ke 5 detik: jalur ini hanya dipakai saat
    bus utama putus, dan satu broker yang tidak bisa dijangkau tidak boleh
    menahan antrean lebih lama dari itu.
    """
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
        try:
            t = mqtt.Client(host=MQTT_HOST, port=MQTT_PORT,
                            client_id="wx" + uuid.uuid4().hex[:8])
            t.connect(timeout=5)
            t.publish(topic, data)
            t.close()
        except Exception as e:
            print("publish cadangan gagal: %s" % str(e)[:80], flush=True)
            # Pesan hasil/ack tidak boleh hilang diam-diam; coba sekali lagi
            # kecuali antrean sudah terlalu panjang.
            if penting:
                with _KIRIM_LOCK:
                    if len(_KIRIM) < 100:
                        _KIRIM.append((topic, data, penting))
            stop.wait(0.5)


class Tokens:
    """Token attestation; mint ulang otomatis saat habis atau basi (TTL 240s).

    Dikunci karena satu worker sekarang melayani beberapa chat serentak.
    Tanpa kunci, dua thread bisa mengambil token yang sama.
    """

    def __init__(self):
        self.tokens = []
        self.minted_at = 0.0
        self.lock = threading.Lock()

    def get(self, paksa=False):
        with self.lock:
            # Token Venice hanya berlaku 5 menit (300 detik). Worker yang lama
            # menganggur sebelum chat pertama datang akan memegang token basi jika
            # tidak dicek TTL-nya.
            if paksa or not self.tokens or (time.monotonic() - self.minted_at > 240):
                self.tokens.clear()
                m = S.post("https://outerface.venice.ai/api/app/user/client_attestation/mint",
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

# Jalur agen: Venice menjalankan tool-nya sendiri (web search, scrape, cuaca,
# kalkulator, image). Matikan lewat `VENICE_AGENTIC=0` untuk kembali ke
# `/api/inference/chat` (classic) tanpa mengubah kode.
AGENTIC = os.environ.get("VENICE_AGENTIC", "1").strip().lower() not in (
    "0", "", "false", "no", "off")
CHAT_URL = "https://outerface.venice.ai/api/inference/chat"
AGENT_URL = ("https://outerface.venice.ai/api/inference/chat/agents/venice"
             "?agentModelId=" + MODEL)


def _agentic_body(prompt):
    """Body agentic: `content` WAJIB array parts, bukan string (kalau string: 500)."""
    return {"input": {"messages": [{
        "id": str(uuid.uuid4()),
        "role": "user",
        "type": "message",
        "status": "completed",
        "content": [{"type": "output_text", "text": prompt,
                     "annotations": []}],
    }]}}


def ask(prompt, on_delta=None, on_maju=None, stop_check=None):
    """Satu chat. Kembalikan (jawaban, status, sisa_kuota, err).

    Kalau `on_delta` diberikan, potongan teks dikirim begitu diterima Venice
    (`on_delta(offset, teks)`) alih-alih menunggu jawaban utuh. Offset dipakai
    supaya penerima bisa menyusun ulang walau ada potongan yang hilang —
    MQTT di sini QoS 0, jadi kehilangan potongan memang mungkin.

    Dua protokol didukung: agentic (Responses API, `response.output_text.*`)
    dan classic (`{"kind": "content"}`) saat `VENICE_AGENTIC=0`.
    """
    hdr = {**H, "accept": "text/event-stream",
           "x-venice-client-attestation": TOK.get(),
           "x-venice-locale": "en-US",
           "x-venice-middleface-version": "0.1.1214",
           "x-venice-request-timestamp-ms": str(int(time.time() * 1000)),
           "x-venice-version": VER}
    # Timeout TERPISAH: (sambung, baca). Dulu `timeout=180` tunggal, dan itu
    # menyebabkan worker menahan tugas yang sudah ditinggalkan pengelola.
    # Rantainya: pool menyerah pada detik 15/45 lalu mengalihkan tugas ke worker
    # lain, TAPI worker lama masih menunggu upstream sampai 180 detik dengan
    # `doing[0]` tetap 1. Worker yang menggantung seperti itu terlihat "sehat"
    # (detak `up` tetap terkirim) padahal tidak menerima tugas baru, dan kalau
    # beberapa worker kena, pool kehabisan kapasitas — pengguna melihat
    # "Working..." lalu gagal.
    #
    # Batas baca 25 detik memutus stream yang benar-benar diam jauh sebelum
    # `_DIAM` pool (45 detik), jadi worker membebaskan slotnya sendiri. Tidak
    # berbahaya untuk generasi panjang: token datang terus, dan tiap baris SSE
    # mengulang hitungan timeout.
    if AGENTIC:
        url, body = AGENT_URL, _agentic_body(prompt)
    else:
        url, body = CHAT_URL, {"modelId": MODEL,
                               "prompt": [{"role": "user", "content": prompt}],
                               "requestId": str(uuid.uuid4())}
    r = S.post(url, headers=hdr, json=body, timeout=(10, 25), stream=True)
    if r.status_code in (401, 403):
        # Token basi: paksa mint token baru, lalu ulangi sekali.
        try:
            hdr["x-venice-client-attestation"] = TOK.get(paksa=True)
            r = S.post(url, headers=hdr, json=body, timeout=(10, 25),
                       stream=True)
        except Exception:
            pass
    if r.status_code != 200:
        try:
            err = (r.text or "")[:300].replace("\n", " ")
        except Exception:
            err = ""
        return "", r.status_code, r.headers.get("x-ratelimit-remaining"), \
            err or ("HTTP %s tanpa body" % r.status_code)

    ans = ""
    cadangan = ""          # teks dari jenis selain jawaban (classic)
    jenis = {}             # penghitung event untuk pesan error
    tipe_item = {}         # item_id -> message/reasoning (agentic)
    pesan = []             # teks final item `message` (agentic)
    ev = ""

    def proses(ln):
        """Urai satu baris. Kembalikan True kalau ada TEKS yang dihasilkan."""
        nonlocal ans, cadangan, ev
        if AGENTIC:
            if ln.startswith("event: "):
                ev = ln[7:].strip()
                jenis[ev] = jenis.get(ev, 0) + 1
                return False
            if ln.startswith(":"):
                # Keepalive: server masih mengerjakan, tapi belum ada teks.
                # Dihitung sebagai bukti hidup supaya pool tidak membunuh agen
                # yang sedang berpikir panjang sebelum byte pertama.
                return True
            if not ln.startswith("data: "):
                return False
            data = ln[6:]
            if data == "[DONE]":
                return False
            try:
                o = json.loads(data)
            except Exception:
                return False
            if not isinstance(o, dict):
                return False
            if ev == "response.output_item.added":
                it = o.get("item") or {}
                if it.get("id") and it.get("type"):
                    tipe_item[it["id"]] = it["type"]
                return False
            if ev == "response.output_text.delta":
                # Delta reasoning jangan diteruskan: itu proses berpikir.
                if tipe_item.get(o.get("item_id") or "") == "reasoning":
                    return False
                potong = o.get("delta") or ""
                if potong:
                    ans += potong
                    if on_delta:
                        on_delta(len(ans) - len(potong), potong)
                    return True
                return False
            if ev == "response.output_item.done":
                it = o.get("item") or {}
                if it.get("type") == "message":
                    t = "".join(p.get("text") or ""
                                for p in (it.get("content") or [])
                                if p.get("type") == "output_text")
                    if t:
                        pesan.append(t)
                return False
            if ev in ("response.failed", "error"):
                cadangan = str(o.get("message") or o.get("error") or "")[:300]
            return False

        # Jalur classic.
        try:
            o = json.loads(ln)
        except Exception:
            return False
        if not isinstance(o, dict):
            return False
        k = o.get("kind") or "(tanpa kind)"
        jenis[k] = jenis.get(k, 0) + 1
        if k == "content":
            potong = o.get("content") or ""
            if potong:
                ans += potong
                if on_delta:
                    on_delta(len(ans) - len(potong), potong)
                return True
            return False
        potong = ""
        for f in ("content", "text", "reasoning", "thinking", "delta"):
            v = o.get(f)
            if isinstance(v, str) and v:
                potong += v
        if potong:
            cadangan += potong
        return False

    for ln in r.iter_lines():
        if stop_check and stop_check():
            r.close()
            return "", 499, 0, "dibatalkan pengelola"
        if isinstance(ln, bytes):
            ln = ln.decode("utf-8", "replace")
        if not ln:
            continue
        teks_datang = proses(ln)
        # Bukti kemajuan HANYA kalau ada teks yang benar-benar dihasilkan.
        #
        # Dulu `on_maju(len(ln))` dipanggil untuk SETIAP baris, termasuk
        # pembuka SSE yang cuma berisi metadata (`kind: model`, versi, dsb).
        # Akibatnya worker yang upstream-nya menggantung SETELAH metadata
        # tetap dianggap "sudah mulai", pengelola memakai batas `_DIAM`
        # (45 detik) alih-alih `_AWAL` (15 detik), dan satu chat biasa bisa
        # tampil "Working..." 56 detik. Terukur: 56,4 detik berulang untuk
        # prompt "hi".
        if teks_datang and on_maju:
            on_maju(len(ln))

    # Item `message` (agentic) lebih dipercaya daripada akumulasi delta.
    if pesan:
        ans = "".join(pesan)
    elif not ans.strip() and cadangan.strip():
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


def ask_ulang(prompt, coba=2, on_delta=None, on_maju=None, stop_check=None):
    """`ask` dengan percobaan ulang kalau balasannya kosong.

    Venice kadang membalas 200 dengan token terpakai tetapi tanpa teks.
    Ini bukan kuota habis dan bukan salah prompt — modelnya saja yang
    berhenti tanpa mengeluarkan apa pun. Satu percobaan ulang hampir selalu
    berhasil, dan jauh lebih murah daripada mengganti worker (yang berarti
    membuang satu IP dengan jatah 8 chat).

    Kalau `on_delta` dipakai, potongan hanya diteruskan dari percobaan yang
    akhirnya dipakai. Percobaan yang gagal bisa saja sudah mengirim sebagian
    teks, jadi pemanggil diberi penanda `on_delta(-1, "")` untuk membuang
    potongan lama sebelum percobaan baru dimulai.
    """
    ans, status, rem, err = ask(prompt, on_delta=on_delta,
                                on_maju=on_maju, stop_check=stop_check)
    for _ in range(coba):
        if ans or status != 200 or (stop_check and stop_check()):
            break
        time.sleep(0.4)
        if on_delta:
            on_delta(-1, "")
        ans, status, rem, err = ask(prompt, on_delta=on_delta,
                                    on_maju=on_maju, stop_check=stop_check)
    return ans, status, rem, err


def listen(stop, on_msg):
    """Dengarkan topik perintah lewat MQTT, sambung ulang kalau putus."""
    while not stop.is_set():
        try:
            c = mqtt.Client(host=MQTT_HOST, port=MQTT_PORT,
                            client_id="vw" + WID)

            def cb(topic, payload):
                if stop.is_set():
                    return
                try:
                    on_msg({"message": payload.decode()})
                except Exception:
                    pass

            c.on_message = cb
            c.connect()
            c.subscribe(TOPIC)
            c.start()
            _BUS[0] = c
            print("listen mqtt %s:%d topik=%s" % (MQTT_HOST, MQTT_PORT, TOPIC),
                  flush=True)
            while c.connected and not stop.is_set():
                time.sleep(0.5)
            c.close()
            print("mqtt putus, sambung ulang", flush=True)
        except Exception as e:
            print("mqtt error: %s" % str(e)[:120], flush=True)
        time.sleep(1.0)


def main():
    t0 = time.monotonic()
    S.get("https://venice.ai/chat/classic", timeout=25)

    try:
        ip = S.get("https://api.ipify.org", timeout=15).text.strip()
    except Exception:
        ip = "?"

    # JANGAN melapor "siap" di sini. Dulu urutannya: publish up, lalu baru
    # menyalakan thread `listen`. Selama celah itu (~0,2-1 detik) pool sudah
    # menganggap worker ini siap dan mengirim tugas ke topik yang belum
    # didengarkan — tugasnya hilang, pool menunggu pengakuan 12 detik,
    # pengguna melihat 502 "tidak mengaku dalam 12s" padahal worker sehat.
    # Terukur: 2 dari 20 tugas agentik gagal karena celah ini.
    # Sekarang lapor "siap" SETELAH MQTT benar-benar tersambung (lihat bawah).

    stop = threading.Event()
    draining = threading.Event()     # diperintahkan pensiun: tunggu chat selesai
    last = [time.monotonic()]
    used = [0]                       # chat yang benar-benar dijawab
    doing = [0]                      # chat yang sedang dikerjakan
    seen = {}                        # tid -> payload hasil (dedupe kirim ulang)
    batal_set = set()                # tid yang dibatalkan pengelola
    kunci = threading.Lock()
    # Kapan terakhir pengelola menyapa. Worker yang tidak pernah disapa lagi
    # berarti pengelolanya sudah mati atau berganti topik: ia harus keluar
    # sendiri, bukan menunggu IDLE 300 detik sambil menahan kapasitas job
    # GitHub (terukur: 13 job yatim berumur 81 menit, membuat pool kosong
    # dan semua permintaan 502 selama menit-menitan).
    yatim = [time.monotonic()]

    def on_msg(o):
        last[0] = time.monotonic()
        try:
            task = json.loads(o.get("message", ""))
        except Exception:
            return
        if task.get("t") == "ping":
            # Pengelola menyapa: worker ini masih terhubung ke topik yang
            # benar. Catat waktu SEBUAH (monotonic), bukan 0.0 — dulu di sini
            # `yatim[0] = 0.0`, dan karena `time.monotonic()` selalu jauh di
            # atas 90, setiap sapaan justru membuat worker langsung mengira
            # dirinya yatim lalu bunuh diri.
            yatim[0] = time.monotonic()
            return

        if task.get("t") == "retire":
            if task.get("to") in (WID, "any"):
                print("diperintahkan pensiun (tunggu %d chat selesai)"
                      % doing[0], flush=True)
                # Dulu stop.set() di sini langsung mematikan proses, jadi
                # chat yang masih berjalan hilang dan pengelola menunggu
                # sampai timeout. Sekarang berhenti menerima tugas baru,
                # lalu keluar setelah yang sedang jalan selesai.
                draining.set()
            return

        if task.get("t") == "cancel":
            cid = task.get("id")
            if task.get("to") in (WID, "any") and cid:
                with kunci:
                    batal_set.add(cid)
            return

        if task.get("t") != "ask":
            return
        if task.get("to") not in (WID, "any"):
            return

        tid = task.get("id", "")

        # Dulu `draining` langsung `return` tanpa memberi tahu siapa pun.
        # Pool lalu menunggu pengakuan 12 detik untuk tugas yang sudah pasti
        # tidak akan dikerjakan, menandai worker ini "tuli", dan mengulanginya
        # ke worker lain — pengguna melihat 502 "tidak mengaku dalam 12s"
        # padahal cuma salah pilih worker. Sekarang penolakan DILAPORKAN
        # seketika supaya pool bisa memindahkan tugas tanpa menunggu.
        if draining.is_set():
            publish(POOL, {"t": "busy", "id": WID, "tid": tid,
                           "why": "pensiun"}, penting=True)
            return

        with kunci:
            ulang = tid in seen
            lama = seen.get(tid)
            if not ulang:
                # Jangan terima kalau jatah IP sudah penuh. Tanpa ini, tiga
                # chat serentak saat used=7 membuat worker mengirim 10 chat
                # dan dua di antaranya kena 429 (jatah 8 per IP).
                #
                # Penolakan ini juga harus DILAPORKAN. Dulu `return` diam-di
                # sini, jadi pool menunggu 12 detik lalu membuang worker yang
                # sebenarnya sehat dan masih punya sisa jatah — terukur satu
                # kegagalan tugas agentik dari 15 percobaan.
                if used[0] + doing[0] >= CAP:
                    publish(POOL, {"t": "busy", "id": WID, "tid": tid,
                                   "why": "jatah penuh"}, penting=True)
                    return
                seen[tid] = None
                doing[0] += 1

        # Akui segera. Tanpa ini pengelola tidak bisa membedakan "worker
        # sedang bekerja" dari "worker sudah mati", dan harus menunggu
        # timeout panjang sebelum mengalihkan prompt ke worker lain.
        publish(POOL, {"t": "ack", "id": WID, "tid": tid}, penting=True)

        if ulang:
            # Pengelola mengirim ulang karena ack/hasil sebelumnya hilang.
            # Jangan jalankan chat yang sama dua kali (itu membakar jatah
            # IP). Kalau hasilnya sudah ada, kirim ulang hasil itu.
            if lama:
                publish(POOL, lama, penting=True)
            return

        def kerja():
            t1 = time.monotonic()
            mau_stream = bool(task.get("stream"))

            # Detak per tugas, TAPI hanya kalau ada kemajuan nyata.
            #
            # Riwayatnya penting. Potongan teks hanya ada kalau klien minta
            # streaming, jadi permintaan NON-stream panjang tidak mengirim
            # apa pun lalu dibunuh di `_DIAM`. Perbaikan pertama saya kirim
            # detak buta tiap 10 detik — dan itu SALAH: upstream yang
            # menggantung (koneksi terbuka, tidak mengirim apa pun) tetap
            # dianggap hidup sampai jatah 90 detik habis. Terukur: satu
            # putaran tugas makan 91,68 detik.
            #
            # Sekarang `maju[0]` hanya naik kalau ada data yang benar-benar
            # diterima dari upstream. Tidak ada kemajuan = tidak ada detak =
            # pengelola berhak membunuh dan memindahkan tugas ke worker lain.
            nadi_stop = threading.Event()
            maju = [0]

            def nadi():
                # Mulai dari kemajuan saat ini, bukan -1: kalau upstream
                # menggantung sejak awal, jangan sampai detak pertama
                # terkirim padahal belum ada satu byte pun.
                terakhir = maju[0]
                while not nadi_stop.wait(10):
                    if maju[0] != terakhir:
                        terakhir = maju[0]
                        publish(POOL, {"t": "n", "id": WID, "tid": tid})

            threading.Thread(target=nadi, daemon=True).start()

            # Potongan teks dikirim ke pengelola supaya klien melihat teks
            # muncul bertahap, bukan menunggu jawaban utuh. Digabung dulu
            # (~120 char / 80 ms) supaya tidak membanjiri broker: satu
            # jawaban 2.000 char bisa jadi 60 pesan MQTT kalau dikirim
            # per potongan.
            buf = []
            buf_len = [0]
            off_awal = [0]      # offset kumulatif yang sudah dikirim ke pengelola
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
                publish(POOL, {"t": "d", "id": WID, "tid": tid,
                               "o": off_awal[0], "c": teks})
                off_awal[0] += len(teks)

            def on_delta(off, teks):
                if not mau_stream:
                    return
                if off < 0:
                    # Percobaan sebelumnya gagal; buang potongan lamanya.
                    with kunci_delta:
                        buf.clear()
                        buf_len[0] = 0
                    off_awal[0] = 0
                    publish(POOL, {"t": "d", "id": WID, "tid": tid,
                                   "o": -1, "c": ""})
                    return
                with kunci_delta:
                    buf.append(teks)
                    buf_len[0] += len(teks)
                flush()

            def catat_maju(n):
                maju[0] += n

            try:
                ans, status, rem, err = ask_ulang(task.get("prompt", ""),
                                                  on_delta=on_delta,
                                                  on_maju=catat_maju,
                                                  stop_check=lambda: tid in batal_set)
            except Exception as e:
                nadi_stop.set()
                with kunci:
                    doing[0] = max(0, doing[0] - 1)
                    res = {"t": "res", "id": WID, "tid": tid,
                           "err": str(e)[:150], "used": used[0]}
                    seen[tid] = res
                publish(POOL, res, penting=True)
                return

            if tid in batal_set:
                nadi_stop.set()
                with kunci:
                    doing[0] = max(0, doing[0] - 1)
                    batal_set.discard(tid)
                print("[kerja] tugas %s dibatalkan pengelola, keluar" % tid, flush=True)
                return
            # Tugas selesai: hentikan detak sebelum hasil dikirim, supaya
            # tidak ada "n" yang datang setelah "res" dan membingungkan
            # pengelola.
            nadi_stop.set()
            if mau_stream:
                flush(paksa=True)
            ms = int((time.monotonic() - t1) * 1000)
            with kunci:
                if status == 200 and ans:
                    used[0] += 1
                n = used[0]
                doing[0] = max(0, doing[0] - 1)
                res = {"t": "res", "id": WID, "tid": tid, "ans": ans,
                       "ms": ms, "status": status, "rem": rem,
                       "err": err, "used": n}
                seen[tid] = res
                # Jaga memori: satu worker hanya punya 8 chat sah, sisanya
                # dari percobaan gagal. 200 sudah jauh lebih dari cukup.
                if len(seen) > 200:
                    seen.pop(next(iter(seen)))
            publish(POOL, res, penting=True)
            print("  %s -> %dms status=%s rem=%s err=%s ans=%r" % (
                tid, ms, status, rem, err[:60], ans[:30]), flush=True)

        # Satu worker melayani beberapa chat serentak. Tanpa ini, 6 permintaan
        # bersamaan hanya 4 yang dapat 1,4 detik; sisanya mengantre sampai
        # 65 detik walau jatah IP worker masih ada. Jatah 8 chat per IP tidak
        # berubah — yang berubah hanya kemampuan melayani bersamaan.
        threading.Thread(target=kerja, daemon=True).start()

    # Sambungkan MQTT LEBIH DULU, tunggu benar-benar berlangganan, baru
    # lapor "siap". `_BUS[0]` diisi oleh listen() tepat setelah subscribe
    # berhasil, jadi ini penanda yang jujur: selama masih None, worker belum
    # bisa menerima perintah.
    threading.Thread(target=listen, args=(stop, on_msg), daemon=True).start()
    threading.Thread(target=_pengirim, args=(stop,), daemon=True).start()
    tunggu = time.monotonic() + 20
    while _BUS[0] is None and time.monotonic() < tunggu and not stop.is_set():
        time.sleep(0.05)

    if _BUS[0] is None:
        print("mqtt tidak tersambung dalam 20s, keluar", flush=True)
        return

    # Panaskan token attestation SEBELUM melapor siap. Mint butuh 1-2 detik
    # dan dulu baru dilakukan saat chat pertama datang, sehingga permintaan
    # pertama ke worker baru terukur 7,2 detik sementara berikutnya 1,1 detik.
    # Dengan dipanaskan lebih dulu, biaya itu tidak lagi dibayar pengguna.
    try:
        TOK.get()
    except Exception as e:
        print("gagal panaskan token: %s" % str(e)[:120], flush=True)

    publish(POOL, {"t": "up", "id": WID, "ip": ip, "v": VER_WORKER,
                   "warm": int((time.monotonic() - t0) * 1000)})
    print("worker %s siap ip=%s warm=%dms" % (
        WID, ip, (time.monotonic() - t0) * 1000), flush=True)

    # Detak berkala. Tanpa ini, kalau pengelola di mesin lokal restart, worker
    # yang masih hidup jadi yatim: pool baru tidak tahu mereka ada, lalu
    # menyalakan worker tambahan (terbukti 6 worker padahal pool 3).
    def beat():
        while not stop.is_set():
            time.sleep(15)
            if stop.is_set():
                return
            publish(POOL, {"t": "up", "id": WID, "ip": ip, "beat": True,
                           "v": VER_WORKER})
            # Tidak ada sapaan selama 90 detik = yatim. Keluar sendiri
            # supaya tidak menahan kapasitas job GitHub.
            if time.monotonic() - yatim[0] > 90:
                print("tidak disapa pengelola 90s, keluar (yatim)", flush=True)
                stop.set()
                return

    threading.Thread(target=beat, daemon=True).start()

    while used[0] < CAP:
        time.sleep(1)
        # Yatim: pengelola tidak menyapa lagi (lihat `beat()`). Keluar
        # segera, tapi tetap tunggu chat yang sedang jalan selesai.
        if stop.is_set() and doing[0] == 0:
            break
        # Pensiun: keluar setelah chat yang sedang jalan selesai.
        if draining.is_set() and doing[0] == 0:
            break
        # Idle: jangan keluar di tengah chat yang masih dikerjakan.
        if doing[0] == 0 and time.monotonic() - last[0] > IDLE:
            break

    stop.set()
    publish(POOL, {"t": "down", "id": WID, "used": used[0]})
    print("worker %s keluar, dipakai %d kali" % (WID, used[0]), flush=True)


if __name__ == "__main__":
    main()
