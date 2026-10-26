#!/bin/bash
set -e

# 1. 清理 9410 端口上的遗留占位进程与残留服务
#    容器内没有 CAP_SYS_PTRACE，fuser 读不到别人的 /proc/PID/fd，
#    以 /proc/PID/cmdline 匹配为准（root 可读）。
pkill -f /opt/squatter.py 2>/dev/null || true
pkill -f "/opt/ingest-ledger/venv/bin/python /opt/ingest-ledger/app.py" 2>/dev/null || true
pkill -f "/opt/ingest-ledger/app.py" 2>/dev/null || true
fuser -k 9410/tcp 2>/dev/null || true

# 2. 有界等待端口释放（最多 15 秒，提前释放立即返回）
port_free=false
for i in $(seq 1 150); do
    if python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.2); sys.exit(0 if s.connect_ex(('127.0.0.1',9410))!=0 else 1)"; then
        port_free=true
        break
    fi
    sleep 0.1
done
$port_free || echo "[solve] WARN: port 9410 still busy after 15s" >&2

# 3. 修复配置文件：原文件含重复键（后出现的键值生效），端口与数据目录都指向旧环境
cat << 'EOF' > /etc/ingest-ledger/config.json
{
  "server": {
    "port": 9410,
    "data_dir": "/var/lib/ingest-ledger"
  }
}
EOF

# 4. 修复权限：数据目录需可进入、journal 需可被 ledgersvc 读取、日志目录需可写
chmod 755 /var/lib/ingest-ledger
chmod 644 /var/lib/ingest-ledger/journal.jsonl
mkdir -p /var/log/ingest-ledger
chown -R ledgersvc:ledgersvc /var/log/ingest-ledger

# 5. 重建服务实现（不沿用上一任的遗留代码）：
#    journal 重放语义 + 增量 tail-follow + 事务缓冲 + CKPT 基线重置
#    + 全局单飞锁 + flock 等锁上限。
cat << 'PYEOF' > /opt/ingest-ledger/app.py
"""ingest-ledger —— 参考实现（Oracle）。

要点：
  1. journal 语义按 samples/ 样本与题面闭环表实现：BEGIN..COMMIT 之间的事务块
     在 COMMIT 后生效；ABORT 块整体丢弃；CKPT 将重放基线重置为其快照。
  2. 紧邻重复行（写入端重试，同一 seq 连续出现）只计一次。
  3. 文件末尾未完成的行不参与重放；未提交的事务块缓冲至 COMMIT。
  4. 增量 tail-follow：按 (ino, size, mtime) 感知变化，只解析新增字节；
     inode 变化（压缩重写，原子替换）才从头重建。
  5. 全局单飞锁保证 20 并发下不重复累加；flock 等锁超时直接返回上一次完整快照。
"""
import fcntl
import http.server
import json
import os
import sys
import threading
import time
import urllib.parse

try:
    import ujson as _json
except ImportError:                       # pragma: no cover - 镜像内必有 ujson
    import json as _json

CONFIG_PATH = "/etc/ingest-ledger/config.json"
DEFAULT_DATA_DIR = "/var/lib/ingest-ledger"
DEFAULT_PORT = 9410
JOURNAL_NAME = "journal.jsonl"
LOCK_WAIT_SEC = 1.0
CHUNK = 8 << 20


def load_config():
    port = DEFAULT_PORT
    data_dir = DEFAULT_DATA_DIR
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                server = _json.load(f).get("server", {})
            port = int(server.get("port", port))
            data_dir = server.get("data_dir", data_dir)
        except Exception:
            sys.stderr.write("[ledger] config load failed; using defaults\n")
    return port, data_dir


class Ledger(object):
    """journal 重放状态机。ingest_lines 之外保持无副作用。"""

    __slots__ = ("balances", "touched", "records", "txns", "last_seq",
                 "open_tx", "pending")

    def __init__(self):
        self.balances = {}
        self.touched = {}
        self.records = 0
        self.txns = 0
        self.last_seq = 0
        self.open_tx = None
        self.pending = None

    def reset_from_ckpt(self, obj):
        bal = {}
        raw = obj.get("balances")
        if isinstance(raw, dict):
            for k, v in raw.items():
                if isinstance(k, str) and type(v) is int:
                    bal[k] = v
        self.balances = bal
        self.touched = dict(bal)
        for k in self.touched:
            self.touched[k] = obj.get("seq", 0)
        self.records = obj.get("committed_records", 0)
        if type(self.records) is not int:
            self.records = 0
        self.txns = obj.get("committed_txns", 0)
        if type(self.txns) is not int:
            self.txns = 0
        self.open_tx = None
        self.pending = None
        self.last_seq = obj.get("seq", 0)

    def ingest(self, lines):
        """把一批完整行并入状态。返回 None。"""
        balances = self.balances
        touched = self.touched
        records = self.records
        txns = self.txns
        last_seq = self.last_seq
        open_tx = self.open_tx
        pending = self.pending
        loads = _json.loads
        for line in lines:
            try:
                obj = loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            seq = obj.get("seq")
            if type(seq) is not int:
                continue
            if seq == last_seq:
                continue                          # 写入端重试：紧邻重复行
            op = obj.get("op")
            if op == "PUT":
                key = obj.get("key")
                if not isinstance(key, str):
                    continue
                delta = obj.get("delta", 0)
                if type(delta) is not int:
                    continue
                if open_tx is not None:
                    pending.append((key, delta, seq))
                else:
                    balances[key] = balances.get(key, 0) + delta
                    touched[key] = seq
                    records += 1
            elif op == "DEL":
                key = obj.get("key")
                if not isinstance(key, str):
                    continue
                if open_tx is not None:
                    pending.append((None, key, seq))
                else:
                    balances.pop(key, None)
                    touched[key] = seq
                    records += 1
            elif op == "BEGIN":
                open_tx = obj.get("tx")
                pending = []
            elif op == "COMMIT":
                if open_tx is not None:
                    for item in pending:
                        if item[0] is None:
                            balances.pop(item[1], None)
                            touched[item[1]] = item[2]
                            records += 1
                        else:
                            balances[item[0]] = balances.get(item[0], 0) + item[1]
                            touched[item[0]] = item[2]
                            records += 1
                    txns += 1
                    open_tx = None
                    pending = None
            elif op == "ABORT":
                if open_tx is not None:
                    open_tx = None
                    pending = None
            elif op == "CKPT":
                bal = {}
                raw = obj.get("balances")
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if isinstance(k, str) and type(v) is int:
                            bal[k] = v
                balances = bal
                touched = dict(bal)
                for k in touched:
                    touched[k] = seq
                r = obj.get("committed_records", 0)
                t = obj.get("committed_txns", 0)
                records = r if type(r) is int else 0
                txns = t if type(t) is int else 0
                open_tx = None
                pending = None
            else:
                continue                          # 未知 op：非法行
            last_seq = seq
        self.balances = balances
        self.touched = touched
        self.records = records
        self.txns = txns
        self.last_seq = last_seq
        self.open_tx = open_tx
        self.pending = pending

    def summary(self):
        bal = self.balances
        top = sorted(bal.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        return {
            "records": self.records,
            "txns": self.txns,
            "keys": len(bal),
            "total_balance": sum(bal.values()),
            "top_keys": [[k, v] for k, v in top],
        }


class Svc(object):
    def __init__(self, data_dir):
        self.path = os.path.join(data_dir, JOURNAL_NAME)
        self.lock = threading.RLock()
        self.ino = None
        self.offset = 0
        self.tail = b""                # 未完成的最后一行（写入端停顿期间）
        self.ledger = Ledger()

    def _rebuild(self, st, f):
        self.ino = st.st_ino
        self.tail = b""
        self.ledger = Ledger()
        f.seek(0)
        self._ingest_stream(f, 0)

    def _ingest_stream(self, f, start):
        # start：本次读取的绝对起点。offset 一律按"起点 + 已读 - 尾巴"推进，
        # 增量路径与重建路径共用同一套记账，避免把绝对偏移覆盖成相对读取量。
        ledger = self.ledger
        tail = self.tail
        offset = start
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            offset += len(chunk)
            data = tail + chunk if tail else chunk
            end = data.rfind(b"\n")
            if end < 0:
                tail = data
                continue
            text = data[:end].decode("utf-8", "replace")
            ledger.ingest(text.split("\n"))
            tail = data[end + 1:]
        # 已处理位置必须回退到尾巴行起点：停顿期间的未完成行要在补全后重读
        self.offset = offset - len(tail)
        self.tail = tail

    def refresh(self):
        try:
            st = os.stat(self.path)
        except OSError:
            return
        if self.ino is not None and st.st_ino == self.ino and st.st_size == self.offset \
                and not self.tail:
            return
        f = None
        try:
            f = open(self.path, "rb")
        except OSError:
            return
        try:
            deadline = time.monotonic() + LOCK_WAIT_SEC
            while True:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        return                  # 写入端长持锁：保留旧状态
                    time.sleep(0.01)
            st = os.fstat(f.fileno())
            if self.ino is None or st.st_ino != self.ino:
                self._rebuild(st, f)            # 首次 / 压缩重写（原子替换，新 inode）
                return
            if st.st_size < self.offset:
                self._rebuild(st, f)            # 就地截断：重建
                return
            if st.st_size == self.offset and not self.tail:
                return
            f.seek(self.offset)
            self._ingest_stream(f, self.offset)
        finally:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            f.close()


SVC = None


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "IngestLedger/2.0"

    def _send(self, code, body, send_body=True, extra=None):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Ledger-Status", "Verified")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if send_body and body:
            self.wfile.write(body)

    def _json(self, obj, send_body=True, code=200):
        body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        self._send(code, body, send_body)

    def _route(self, send_body):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/healthz":
            self._json({"status": "healthy"}, send_body)
            return
        if path == "/api/v1/ledger":
            with SVC.lock:
                SVC.refresh()
                data = SVC.ledger.summary()
            self._json(data, send_body)
            return
        if path == "/api/v1/key":
            raw = urllib.parse.parse_qs(parsed.query).get("name", [""])[0]
            if not raw:
                self._json({"error": "missing name"}, send_body, 400)
                return
            with SVC.lock:
                SVC.refresh()
                bal = SVC.ledger.balances
                if raw not in bal:
                    self._send(404, b"", send_body)
                    return
                data = {"name": raw, "balance": bal[raw],
                        "last_seq": SVC.ledger.touched.get(raw, 0)}
            self._json(data, send_body)
            return
        self._send(404, b"", send_body)

    def do_GET(self):
        self._route(True)

    def do_HEAD(self):
        self._route(False)

    def _drain(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length > 0:
            self.rfile.read(min(length, 1 << 20))

    def do_POST(self):
        self._drain()
        self._send(405, b"", True, {"Allow": "GET, HEAD"})

    def do_PUT(self):
        self._drain()
        self._send(405, b"", True, {"Allow": "GET, HEAD"})

    def do_DELETE(self):
        self._send(405, b"", True, {"Allow": "GET, HEAD"})

    def do_OPTIONS(self):
        self._send(405, b"", True, {"Allow": "GET, HEAD"})

    def do_PATCH(self):
        self._send(405, b"", True, {"Allow": "GET, HEAD"})

    def log_message(self, fmt, *args):
        return


def main():
    global SVC
    port, data_dir = load_config()
    SVC = Svc(data_dir)
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.daemon_threads = True
    httpd.serve_forever()


if __name__ == "__main__":
    main()
PYEOF
chmod 755 /opt/ingest-ledger/app.py

# 6. 虚拟环境（题面运维规范要求用 venv 解释器启动）；ujson 为 apt 渠道的
#    系统包（构建期已安装），venv 复用系统站点包。硬校验失败即退出。
if [ ! -x /opt/ingest-ledger/venv/bin/python ]; then
    python3 -m venv --system-site-packages /opt/ingest-ledger/venv
fi
/opt/ingest-ledger/venv/bin/python -c "import ujson" 2>/dev/null || {
    echo "[solve] ERROR: ujson unavailable in venv" >&2
    exit 1
}
chown -R ledgersvc:ledgersvc /opt/ingest-ledger

# 7. 以 ledgersvc 身份拉起常驻服务，然后有界等待"我们自己的服务"就绪
#    （只探测端口连通性不够：占位进程也可能在 9410 上应答）
su -s /bin/bash ledgersvc -c 'nohup /opt/ingest-ledger/venv/bin/python /opt/ingest-ledger/app.py >> /var/log/ingest-ledger/app.log 2>&1 &'

cat > /tmp/_healthcheck.py <<'PYCHK'
import json
import urllib.request
r = urllib.request.urlopen("http://127.0.0.1:9410/healthz", timeout=1)
assert r.getcode() == 200
assert json.loads(r.read().decode()) == {"status": "healthy"}
assert r.headers.get("X-Ledger-Status") == "Verified"
PYCHK

ready=false
for i in $(seq 1 300); do
    if python3 /tmp/_healthcheck.py 2>/dev/null; then
        ready=true
        break
    fi
    sleep 0.1
done
rm -f /tmp/_healthcheck.py
if ! $ready; then
    echo "[solve] ERROR: service did not answer /healthz on 9410 within 30s" >&2
    tail -20 /var/log/ingest-ledger/app.log >&2 || true
    exit 1
fi
echo "[solve] service is up on 9410"
