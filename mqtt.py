"""Klien MQTT 3.1.1 minimal — tanpa dependensi, hardened untuk koneksi jangka panjang.

Dipakai oleh `worker.py` (di runner GitHub) dan `pool.py` (di mesin lokal).
Mendukung CONNECT/CONNACK, PUBLISH (QoS 0/1 PUBACK), SUBSCRIBE/SUBACK,
PINGREQ/PINGRESP, deteksi koneksi half-open TCP, batas ukuran paket maksimum,
`selectors` tanpa batas 1024 FD, enkripsi AEAD Encrypt-then-MAC (`seal_frame` /
`open_frame` + `ReplayGuard`), serta penutupan socket yang thread-safe.
"""
import base64
import hashlib
import hmac
import json
import math
import secrets
import selectors
import socket
import ssl
import struct
import threading
import time
import uuid

CONNECT, CONNACK, PUBLISH, PUBACK, SUBSCRIBE = 1, 2, 3, 4, 8
SUBACK, PINGREQ, PINGRESP, DISCONNECT = 9, 12, 13, 14

# Batas atas payload satu paket MQTT (16 MiB) untuk mencegah OOM dari frame korup.
MAX_PACKET_SIZE = 16 * 1024 * 1024


# ------------------------------------------------ autentikasi & enkripsi frame
def _stream_xor(key_b, nonce, data):
    """Stream cipher CTR berbasis HMAC-SHA256 + SHA-256 (stdlib murni)."""
    if not data:
        return b""
    enc_key = hmac.new(key_b, b"venice-enc-v1:" + nonce, hashlib.sha256).digest()
    n = len(data)
    blocks = (n + 31) // 32
    ks = b"".join(
        hashlib.sha256(enc_key + struct.pack("!Q", i)).digest()
        for i in range(blocks)
    )[:n]
    x = int.from_bytes(data, "big") ^ int.from_bytes(ks, "big")
    return x.to_bytes(n, "big")


class ReplayGuard:
    """Pelindung replay nonce berbatas TTL & ukuran memori (thread-safe)."""

    def __init__(self, ttl=240.0, max_size=8192):
        self.ttl = float(ttl)
        self.max_size = int(max_size)
        self._seen = {}
        self._lock = threading.Lock()

    def check_and_add(self, nonce_bytes):
        """True jika nonce baru (sah); False jika replay/duplikat."""
        now = time.monotonic()
        with self._lock:
            if len(self._seen) >= self.max_size:
                for k in [k for k, exp in list(self._seen.items()) if exp <= now]:
                    self._seen.pop(k, None)
                kelebihan = len(self._seen) - self.max_size + 1
                if kelebihan > 0:
                    for k in sorted(list(self._seen),
                                    key=lambda x: self._seen.get(x, 0))[:kelebihan]:
                        self._seen.pop(k, None)
            exp = self._seen.get(nonce_bytes)
            if exp is not None and exp > now:
                return False
            self._seen[nonce_bytes] = now + self.ttl
            return True


def seal_frame(key, topic, payload):
    """Bungkus payload JSON/str ke amplop terenkripsi + HMAC-SHA256 (Encrypt-then-MAC)."""
    key_b = key.encode("utf-8") if isinstance(key, str) else bytes(key)
    if isinstance(payload, bytes):
        pt = payload
    elif isinstance(payload, str):
        pt = payload.encode("utf-8")
    else:
        pt = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    nonce = secrets.token_bytes(16)
    ts_ms = int(time.time() * 1000)
    ct = _stream_xor(key_b, nonce, pt)
    mac_key = hmac.new(key_b, b"venice-mac-v1:" + nonce, hashlib.sha256).digest()
    aad = ("v1|%s|%d|" % (topic or "", ts_ms)).encode("utf-8")
    tag = hmac.new(mac_key, aad + nonce + b"|" + ct, hashlib.sha256).digest()
    return json.dumps({
        "enc": "v1",
        "n": base64.b64encode(nonce).decode("ascii"),
        "ts": ts_ms,
        "ct": base64.b64encode(ct).decode("ascii"),
        "mac": base64.b64encode(tag).decode("ascii"),
    }, separators=(",", ":"))


def open_frame(key, topic, raw_msg, replay_guard=None, max_skew_s=120.0):
    """Verifikasi HMAC + kesegaran timestamp + anti-replay, lalu dekripsi frame."""
    key_b = key.encode("utf-8") if isinstance(key, str) else bytes(key)
    if isinstance(raw_msg, bytes):
        raw_msg = raw_msg.decode("utf-8", "replace")
    try:
        env = json.loads(raw_msg)
    except Exception:
        raise ValueError("frame MQTT bukan JSON valid")
    if not isinstance(env, dict) or env.get("enc") != "v1":
        raise ValueError("frame MQTT tidak terenkripsi (wajib enc=v1)")
    ts_ms = env.get("ts")
    if isinstance(ts_ms, bool) or not isinstance(ts_ms, (int, float)) or not math.isfinite(float(ts_ms)):
        raise ValueError("timestamp frame tidak valid")
    if abs(time.time() * 1000.0 - float(ts_ms)) > float(max_skew_s) * 1000.0:
        raise ValueError("timestamp frame di luar jendela toleransi")
    try:
        nonce = base64.b64decode(env["n"].encode("ascii"), validate=True)
        ct = base64.b64decode(env["ct"].encode("ascii"), validate=True)
        tag = base64.b64decode(env["mac"].encode("ascii"), validate=True)
    except Exception:
        raise ValueError("encoding amplop frame rusak")
    if len(nonce) != 16 or len(tag) != 32:
        raise ValueError("panjang nonce/mac frame tidak valid")
    mac_key = hmac.new(key_b, b"venice-mac-v1:" + nonce, hashlib.sha256).digest()
    aad = ("v1|%s|%d|" % (topic or "", int(ts_ms))).encode("utf-8")
    expected = hmac.new(mac_key, aad + nonce + b"|" + ct, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, tag):
        raise ValueError("tanda tangan HMAC frame tidak cocok")
    if replay_guard is not None and not replay_guard.check_and_add(nonce):
        raise ValueError("frame MQTT replay terdeteksi")
    pt = _stream_xor(key_b, nonce, ct)
    return pt.decode("utf-8")


def _str(s):
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


def _varint(n):
    out = bytearray()
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out.append(d)
        if not n:
            return bytes(out)


def _packet(ptype, flags, body):
    return bytes([(ptype << 4) | flags]) + _varint(len(body)) + body


class Client:
    """Satu koneksi MQTT. `on_message(topic, payload_bytes)` dipanggil di thread pembaca."""

    def __init__(self, host="broker.hivemq.com", port=8883,
                 client_id=None, keepalive=60, tls=True,
                 username=None, password=None, ping_timeout=None):
        self.host = host
        self.port = port
        self.client_id = client_id or ("vq" + uuid.uuid4().hex[:10])
        self.keepalive = keepalive
        self.tls = tls
        if password and not username:
            raise ValueError("MQTT 3.1.1 melarang password tanpa username")
        self.username = username
        self.password = password
        self.sock = None
        self.on_message = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.connected = False
        self._buf = b""
        self._pkt_id = 0
        self._loop_started = False
        self._subacks = {}       # pkt_id -> Event
        self._suback_rc = {}     # pkt_id -> return code byte
        self._ping_timeout = (float(ping_timeout) if ping_timeout is not None
                              else min(15.0, max(5.0, float(keepalive) * 0.4)))

    # ------------------------------------------------------------- koneksi
    def connect(self, timeout=15):
        s = socket.create_connection((self.host, self.port), timeout=timeout)
        try:
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except OSError:
                pass
            s.settimeout(timeout)
            if self.tls:
                # Default port 8883 memakai TLS; jangan fallback ke plaintext.
                s = ssl.create_default_context().wrap_socket(
                    s, server_hostname=self.host)
                s.settimeout(timeout)
            self.sock = s
            self._buf = b""
            self._stop.clear()
            with self._lock:
                self._loop_started = False
                self._subacks.clear()
                self._suback_rc.clear()
            flags = 0x02  # clean session
            payload = _str(self.client_id)
            if self.username:
                flags |= 0x80
                payload += _str(self.username)
            if self.password:
                flags |= 0x40
                payload += _str(self.password)
            body = (_str("MQTT") + bytes([4, flags])
                    + struct.pack("!H", max(1, int(self.keepalive))) + payload)
            s.sendall(_packet(CONNECT, 0, body))
            # CONNACK: 20 02 00 00
            hdr = self._recv_exact(4, timeout=timeout)
            if hdr[0] >> 4 != CONNACK or hdr[3] != 0:
                raise RuntimeError("CONNACK gagal: %r" % (hdr,))
            self.connected = True
            return self
        except Exception:
            self.connected = False
            self.sock = None
            try:
                s.close()
            except Exception:
                pass
            raise

    def _recv_exact(self, n, timeout=None):
        """Ambil tepat `n` byte; sisa yang berlebih disimpan di penyangga."""
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        while len(self._buf) < n:
            if deadline is not None:
                sisa = deadline - time.monotonic()
                if sisa <= 0:
                    raise socket.timeout("timeout membaca %d byte MQTT" % n)
                try:
                    self.sock.settimeout(sisa)
                except OSError:
                    pass
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("koneksi MQTT ditutup")
            self._buf += chunk
            if len(self._buf) > MAX_PACKET_SIZE + 16:
                raise ConnectionError("penyangga MQTT melebihi batas")
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _parse(self):
        """Urai satu paket dari penyangga TANPA memotongnya kalau belum utuh."""
        b = self._buf
        if len(b) < 2:
            return None
        mult, val, i = 1, 0, 1
        while True:
            if i >= len(b):
                return None
            d = b[i]
            i += 1
            val += (d & 0x7F) * mult
            if not d & 0x80:
                break
            mult *= 128
            if mult > 128 ** 3:
                raise ConnectionError("panjang paket MQTT tidak masuk akal")
        if val > MAX_PACKET_SIZE:
            raise ConnectionError("paket MQTT terlalu besar: %d byte" % val)
        total = i + val
        if len(b) < total:
            return None
        b1 = b[0]
        body = b[i:total]
        self._buf = b[total:]
        return b1 >> 4, b1 & 0x0F, body

    def _read_packet(self, timeout=2.0):
        """Tunggu sampai satu paket utuh tanpa memblokir `publish()` pada SSLSocket.

        Pada CPython/OpenSSL, `SSLSocket.recv()` dan `SSLSocket.sendall()` tidak
        aman/saling mengunci bila berjalan serentak dari dua thread. Memakai
        `selectors.DefaultSelector()` di luar `_lock` (bebas limit 1024 FD
        `select.select()`) lalu membaca singkat di dalam `_lock` guna menjaga
        mutual exclusion OpenSSL.
        """
        p = self._parse()
        if p is not None:
            return p
        deadline = time.monotonic() + max(0.05, float(timeout))
        sel = selectors.DefaultSelector()
        reg_sock = None
        try:
            while p is None:
                if self._stop.is_set():
                    raise ConnectionError("MQTT dihentikan")
                with self._lock:
                    s = self.sock
                    if s is None or not self.connected:
                        raise ConnectionError("koneksi MQTT ditutup")
                    has_pending = getattr(s, "pending", lambda: 0)() > 0
                if not has_pending:
                    sisa = deadline - time.monotonic()
                    if sisa <= 0:
                        raise socket.timeout("belum ada paket MQTT")
                    try:
                        if reg_sock is not s:
                            if reg_sock is not None:
                                try:
                                    sel.unregister(reg_sock)
                                except Exception:
                                    pass
                            sel.register(s, selectors.EVENT_READ)
                            reg_sock = s
                        ready = sel.select(min(0.05, sisa))
                    except (OSError, ValueError):
                        raise ConnectionError("socket MQTT ditutup")
                    if not ready:
                        continue
                with self._lock:
                    s = self.sock
                    if s is None or not self.connected:
                        raise ConnectionError("koneksi MQTT ditutup")
                    try:
                        s.settimeout(0.25)
                        chunk = s.recv(65536)
                    except (socket.timeout, ssl.SSLWantReadError,
                            ssl.SSLWantWriteError, BlockingIOError):
                        continue
                if not chunk:
                    raise ConnectionError("koneksi MQTT ditutup")
                self._buf += chunk
                if len(self._buf) > MAX_PACKET_SIZE + 16:
                    raise ConnectionError("penyangga MQTT melebihi batas")
                p = self._parse()
            return p
        finally:
            try:
                sel.close()
            except Exception:
                pass

    def _dispatch_publish(self, flags, body):
        if len(body) < 2:
            raise ConnectionError("paket PUBLISH MQTT terlalu pendek")
        tlen = struct.unpack("!H", body[:2])[0]
        qos = (flags >> 1) & 3
        off = 2 + tlen + (2 if qos else 0)
        if off > len(body):
            raise ConnectionError("panjang topic PUBLISH MQTT rusak")
        if qos == 1:
            pkt_id = struct.unpack("!H", body[2 + tlen:2 + tlen + 2])[0]
            try:
                with self._lock:
                    self._sendall_locked(_packet(PUBACK, 0, struct.pack("!H", pkt_id)))
            except Exception:
                pass
        topic = body[2:2 + tlen].decode("utf-8", errors="replace")
        if self.on_message:
            try:
                self.on_message(topic, body[off:])
            except Exception:
                pass

    def _sendall_locked(self, data):
        if not self.sock or not self.connected:
            raise ConnectionError("MQTT tidak tersambung")
        try:
            # Timeout tulis longgar untuk payload besar terkompresi (hingga beberapa MB).
            self.sock.settimeout(30.0)
            self.sock.sendall(data)
        except Exception:
            self.connected = False
            raise

    # ------------------------------------------------------------- kirim
    def publish(self, topic, payload):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        pkt = _packet(PUBLISH, 0, _str(topic) + payload)
        with self._lock:
            self._sendall_locked(pkt)

    def subscribe(self, topic, qos=0, timeout=10.0):
        with self._lock:
            self._pkt_id = (self._pkt_id % 65535) + 1
            pid = self._pkt_id
            loop_aktif = self._loop_started
            ev = threading.Event() if loop_aktif else None
            if ev is not None:
                self._subacks[pid] = ev
            body = struct.pack("!H", pid) + _str(topic) + bytes([qos])
            try:
                self._sendall_locked(_packet(SUBSCRIBE, 0x02, body))
            except Exception:
                if ev is not None:
                    self._subacks.pop(pid, None)
                raise

        if loop_aktif:
            if not ev.wait(timeout):
                with self._lock:
                    self._subacks.pop(pid, None)
                    self._suback_rc.pop(pid, None)
                raise TimeoutError("SUBACK timeout untuk topik %s" % topic)
            with self._lock:
                self._subacks.pop(pid, None)
                rc = self._suback_rc.pop(pid, 0x80)
            if rc == 0x80:
                raise RuntimeError("SUBACK ditolak broker untuk topik %s" % topic)
            return

        # Sebelum loop() dijalankan, baca sinkron sampai SUBACK cocok tiba.
        deadline = time.monotonic() + timeout
        while True:
            sisa = deadline - time.monotonic()
            if sisa <= 0:
                self.connected = False
                raise TimeoutError("SUBACK timeout untuk topik %s" % topic)
            try:
                ptype, flags, body = self._read_packet(timeout=min(2.0, sisa))
            except socket.timeout:
                continue
            if ptype == SUBACK and len(body) >= 3:
                rpid = struct.unpack("!H", body[:2])[0]
                if rpid == pid:
                    if body[2] == 0x80:
                        raise RuntimeError("SUBACK ditolak broker untuk topik %s" % topic)
                    return
            elif ptype == PUBLISH:
                self._dispatch_publish(flags, body)

    def ping(self):
        pkt = _packet(PINGREQ, 0, b"")
        with self._lock:
            self._sendall_locked(pkt)

    def close(self):
        self._stop.set()
        with self._lock:
            s = self.sock
            self.sock = None
            self.connected = False
            evs = list(self._subacks.values())
            self._subacks.clear()
            self._suback_rc.clear()
            if s is not None:
                try:
                    s.settimeout(0.5)
                    s.sendall(_packet(DISCONNECT, 0, b""))
                except Exception:
                    pass
                try:
                    s.close()
                except Exception:
                    pass
        for ev in evs:
            ev.set()

    # ------------------------------------------------------------- terima
    def loop(self):
        """Baca paket sampai koneksi putus. Dipanggil di thread sendiri."""
        with self._lock:
            self._loop_started = True
        last_ping = time.monotonic()
        awaiting_pingresp = False
        ping_sent_at = 0.0
        read_to = min(2.0, max(0.1, self._ping_timeout / 3.0))

        while not self._stop.is_set():
            s = self.sock
            if s is None or not self.connected:
                return
            try:
                ptype, flags, body = self._read_packet(timeout=read_to)
            except socket.timeout:
                ptype = None
            except Exception:
                self.close()
                return

            if ptype == PUBLISH:
                try:
                    self._dispatch_publish(flags, body)
                except ConnectionError:
                    self.close()
                    return
            elif ptype == PINGRESP:
                awaiting_pingresp = False
            elif ptype == SUBACK and len(body) >= 3:
                rpid = struct.unpack("!H", body[:2])[0]
                rc = body[2]
                with self._lock:
                    self._suback_rc[rpid] = rc
                    ev = self._subacks.get(rpid)
                    if ev is not None:
                        self._suback_rc[rpid] = rc
                if ev:
                    ev.set()
            elif ptype == CONNACK:
                pass

            now = time.monotonic()
            if awaiting_pingresp and (now - ping_sent_at >= self._ping_timeout):
                # Koneksi TCP half-open: broker tidak membalas PINGREQ.
                self.close()
                return

            if not awaiting_pingresp and (now - last_ping >= self.keepalive * 0.6):
                try:
                    self.ping()
                except Exception:
                    self.close()
                    return
                awaiting_pingresp = True
                ping_sent_at = now
                last_ping = now

    def start(self):
        with self._lock:
            self._loop_started = True
        t = threading.Thread(target=self.loop, daemon=True)
        t.start()
        return t


def make(on_message=None, host="broker.hivemq.com", port=8883, client_id=None,
         tls=True, username=None, password=None):
    """Buat klien yang sudah tersambung (belum subscribe)."""
    c = Client(host=host, port=port, client_id=client_id, tls=tls,
               username=username, password=password)
    c.on_message = on_message
    c.connect()
    return c
