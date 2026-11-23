"""Klien MQTT 3.1.1 minimal — tanpa dependensi.

Kenapa file ini ada
===================
Bus pesan sebelumnya ntfy.sh. Setelah dipakai beberapa jam, ntfy membalas:

    {"code":42908,"http":429,"error":"limit reached: daily message quota
     reached; increase your limits with a paid plan"}

Kuota pesan harian habis — dihabiskan detak 15 detik dari tiap worker.
Begitu itu terjadi, perintah `ask` tidak pernah sampai ke worker, dan
setiap prompt menggantung sampai timeout 60 detik.

MQTT tidak punya kuota pesan. Broker publik (HiveMQ, mosquitto) menerima
publish sebanyak apa pun. Jadi bus dipindah ke MQTT.

Kenapa ditulis sendiri: mesin ini tidak punya pip (PEP 668), dan protokol
yang kita butuh cuma CONNECT / PUBLISH / SUBSCRIBE / PINGREQ — sekitar 150
baris. Menambah dependensi untuk itu tidak sepadan.

Dipakai oleh `worker.py` (di runner GitHub) dan `pool.py` (di mesin lokal).
"""
import socket
import ssl
import struct
import threading
import time

CONNECT, CONNACK, PUBLISH, SUBSCRIBE = 1, 2, 3, 8
SUBACK, PINGREQ, PINGRESP, DISCONNECT = 9, 12, 13, 14


def _str(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def _varint(n):
    out = b""
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out += bytes([d])
        if not n:
            return out


def _packet(ptype, flags, body):
    return bytes([(ptype << 4) | flags]) + _varint(len(body)) + body


class Client:
    """Satu koneksi MQTT. `on_message(topic, payload_bytes)` dipanggil di thread pembaca."""

    def __init__(self, host="broker.hivemq.com", port=8883,
                 client_id=None, keepalive=60, tls=True,
                 username=None, password=None):
        self.host = host
        self.port = port
        self.client_id = client_id or ("vq" + str(int(time.time() * 1000))[-10:])
        self.keepalive = keepalive
        self.tls = tls
        self.username = username
        self.password = password
        self.sock = None
        self.on_message = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.connected = False
        # Penyangga baca. WAJIB ada dan WAJIB bertahan antar-panggilan.
        #
        # Dulu pembacaan langsung dari socket dengan `_recv_exact(n)` yang
        # mengumpulkan byte di variabel lokal. Kalau `recv` kena timeout di
        # TENGAH paket (header sudah terbaca, body belum), pengecualiannya
        # naik ke `loop` yang memperlakukannya sebagai "tidak ada paket" —
        # dan byte yang sudah diterima HILANG. Aliran MQTT jadi bergeser
        # satu paket: byte berikutnya dibaca sebagai tipe paket yang salah.
        # Gejalanya persis "worker hidup tapi tuli": detak `up` tetap
        # terkirim (jalur tulis sehat), pool menganggapnya siap, tapi
        # perintah `ask` tidak pernah sampai sehingga pengelola menunggu
        # pengakuan lalu melaporkan "tidak mengaku dalam 5s".
        self._buf = b""

    # ------------------------------------------------------------- koneksi
    def connect(self, timeout=15):
        s = socket.create_connection((self.host, self.port), timeout=timeout)
        # Biarkan timeout tetap aktif selama handshake CONNACK. Dulu di sini
        # langsung `settimeout(None)`, jadi broker yang menerima TCP tetapi
        # tidak membalas (penuh/diblokir) membuat connect menggantung
        # selamanya dan worker/pool tidak pernah menyambung ulang.
        s.settimeout(timeout)
        if self.tls:
            # Default port 8883 memakai TLS. Jangan pernah fallback diam-diam
            # ke 1883: itu mengirim prompt lewat plaintext.
            s = ssl.create_default_context().wrap_socket(
                s, server_hostname=self.host)
            s.settimeout(timeout)
        self.sock = s
        flags = 0x02  # clean session
        payload = _str(self.client_id)
        if self.username:
            flags |= 0x80
            payload += _str(self.username)
        if self.password:
            flags |= 0x40
            payload += _str(self.password)
        body = (_str("MQTT") + bytes([4, flags])
                + struct.pack("!H", self.keepalive) + payload)
        s.sendall(_packet(CONNECT, 0, body))
        # CONNACK: 20 02 00 00
        hdr = self._recv_exact(4)
        if hdr[0] >> 4 != CONNACK or hdr[3] != 0:
            raise RuntimeError("CONNACK gagal: %r" % (hdr,))
        self.connected = True
        return self

    def _recv_exact(self, n, timeout=None):
        """Ambil tepat `n` byte; sisa yang berlebih disimpan di penyangga."""
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("koneksi MQTT ditutup")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _parse(self):
        """Urai satu paket dari penyangga TANPA memotongnya kalau belum utuh.

        Ini kunci ketahanan pembaca. Dulu pembacaan langsung dari socket:
        `socket.timeout` di tengah paket membuang byte yang sudah diterima,
        sehingga aliran MQTT bergeser satu paket dan byte body berikutnya
        dibaca sebagai header. Gejalanya "worker hidup tapi tuli": detak
        terkirim, tetapi perintah `ask` tidak pernah sampai.

        Dengan parser ini, timeout tidak menghilangkan apa pun — paket
        setengah jadi tetap di penyangga dan dilanjutkan pada pembacaan
        berikutnya.
        """
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
        total = i + val
        if len(b) < total:
            return None
        b1 = b[0]
        body = b[i:total]
        self._buf = b[total:]
        return b1 >> 4, b1 & 0x0F, body

    def _read_packet(self):
        """Tunggu sampai satu paket utuh. `socket.timeout` = belum ada paket."""
        p = self._parse()
        while p is None:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("koneksi MQTT ditutup")
            self._buf += chunk
            p = self._parse()
        return p

    # ------------------------------------------------------------- kirim
    def publish(self, topic, payload):
        if isinstance(payload, str):
            payload = payload.encode()
        with self._lock:
            self.sock.sendall(_packet(PUBLISH, 0, _str(topic) + payload))

    def subscribe(self, topic, qos=0):
        body = struct.pack("!H", 1) + _str(topic) + bytes([qos])
        with self._lock:
            self.sock.sendall(_packet(SUBSCRIBE, 0x02, body))

    def ping(self):
        with self._lock:
            self.sock.sendall(_packet(PINGREQ, 0, b""))

    def close(self):
        self._stop.set()
        try:
            with self._lock:
                self.sock.sendall(_packet(DISCONNECT, 0, b""))
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass
        self.connected = False

    # ------------------------------------------------------------- terima
    def loop(self):
        """Baca paket sampai koneksi putus. Dipanggil di thread sendiri."""
        # monotonic: kalau jam dinding mundur, `time.time() - last_ping`
        # negatif dan PINGREQ tidak pernah terkirim sampai broker memutus.
        last_ping = time.monotonic()
        while not self._stop.is_set():
            # Timeout baca pendek supaya PINGREQ tetap terkirim walau tidak
            # ada pesan masuk. Dulu `_recv_exact` memblokir tanpa batas, jadi
            # saat topik sepi tidak ada PINGREQ sama sekali; broker memutus
            # koneksi karena keepalive, worker jadi "hidup tapi tuli", dan
            # pengelola menunggu ack 6 detik x 4 percobaan = 24,8 detik
            # sebelum menyerah. Itu persis angka DONE 24879ms yang dilaporkan.
            try:
                self.sock.settimeout(2.0)
            except Exception:
                pass
            try:
                ptype, flags, body = self._read_packet()
            except socket.timeout:
                # Tidak ada paket utuh dalam 2 detik. Kalau penyangga masih
                # berisi potongan paket, JANGAN anggap sepi — lanjut membaca
                # sisa paketnya di putaran berikutnya. Penyangga membuat
                # pembacaan bisa dilanjutkan tanpa kehilangan apa pun.
                ptype = None
            except Exception:
                self.connected = False
                return

            if ptype == PUBLISH:
                tlen = struct.unpack("!H", body[:2])[0]
                topic = body[2:2 + tlen].decode(errors="replace")
                # QoS 0 tidak punya packet id; QoS>0 punya 2 byte
                qos = (flags >> 1) & 3
                off = 2 + tlen + (2 if qos else 0)
                if self.on_message:
                    try:
                        self.on_message(topic, body[off:])
                    except Exception:
                        pass
            elif ptype in (PINGRESP, CONNACK, SUBACK):
                pass

            if time.monotonic() - last_ping > self.keepalive * 0.6:
                try:
                    self.ping()
                except Exception:
                    self.connected = False
                    return
                last_ping = time.monotonic()

    def start(self):
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
