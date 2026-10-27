#!/bin/bash
# ingest-ledger-incident verifier
# 判分结构：3 项 x 1/3 -> 可能值 0 / 0.33 / 0.67 / 1.00
# 各项的验收口径、时延门槛与宽限时长均以 instruction.md 为准；
# 本脚本不额外引入 instruction.md 之外的判定标准。
set -u

REWARD_FILE="/logs/verifier/reward.txt"
DATA_DIR="/var/lib/ingest-ledger"
JOURNAL="$DATA_DIR/journal.jsonl"
BASE="http://127.0.0.1:9410"
VENV_PY="/opt/ingest-ledger/venv/bin/python"
APP_PY="/opt/ingest-ledger/app.py"

mkdir -p /logs/verifier
PASS=0
TOTAL=3

export DATA_DIR JOURNAL BASE VENV_PY APP_PY

# 备份 journal，退出时还原（题面口径：服务必须始终等价于对当前文件从头重放）
rm -f /tmp/journal.bak
cp -a "$JOURNAL" /tmp/journal.bak 2>/dev/null || true
trap 'cp -a /tmp/journal.bak "$JOURNAL" 2>/dev/null || true' EXIT

# ---- 共享真值模块：所有真值由此独立计算，不信任被测服务自报 ----
cat > /tmp/walib.py <<'PYLIB'
import fcntl
import json
import os
import time
import urllib.request
import urllib.error

try:
    import ujson as _json          # 解析用（镜像内 apt 渠道 ujson）
except ImportError:
    import json as _json
import json as _sj                 # 序列化用（ujson.dumps 不支持 separators）

BASE = os.environ["BASE"]
JOURNAL = os.environ["JOURNAL"]

APPEND_SLA = 1.5     # 常规变更（追加事务块）后的首个请求
WARMUP_SLA = 20.0    # 服务启动后的首次统计请求（建立初始状态）
STRUCT_SLA = 30.0    # 结构性变更（压缩重写，原子替换）后的首个请求
HDR = "X-Ledger-Status"


# ---------------- 写入端（模拟现场写入行为） ----------------
class Writer(object):
    """追加行时持排他锁并 fsync；seq 沿用文件中已接受的最大 seq 继续分配。"""

    def __init__(self, path, seq):
        self.path = path
        self.seq = seq
        self.history = []          # 评测期间追加过的全部行（压缩重写时复用）

    def emit(self, objs):
        lines = []
        for o in objs:
            if "seq" in o:
                self.seq = max(self.seq, o["seq"])
            else:
                self.seq += 1
                o["seq"] = self.seq
            lines.append(_sj.dumps(o, separators=(",", ":")))
            self.history.append(lines[-1])
        with open(self.path, "r+b") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0, 2)
                f.write(("\n".join(lines) + "\n").encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def emit_raw(self, raw_lines):
        """写入无法通过 JSON 解析或字段非法的行（不占用 seq）。"""
        with open(self.path, "r+b") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0, 2)
                f.write(("\n".join(raw_lines) + "\n").encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def compact(self, snapshot, include_history=True):
        """压缩重写：以检查点为基线整体替换文件（原子替换，新 inode）。
        快照为当前已提交状态；此后写入端从检查点继续追加。"""
        obj = {"seq": self.seq + 1, "op": "CKPT",
               "committed_records": snapshot["records"],
               "committed_txns": snapshot["txns"],
               "balances": snapshot["balances"]}
        self.seq = obj["seq"]
        lines = [_sj.dumps(obj, separators=(",", ":"))]
        if include_history and self.history:
            lines.extend(self.history)
            self.history = []
        tmp = self.path + ".compact.tmp"
        with open(tmp, "wb") as f:
            f.write(("\n".join(lines) + "\n").encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, self.path)
        self.history = []


# ---------------- 真值状态机（独立重放，不信任服务） ----------------
class Truth(object):
    def __init__(self, path):
        self.path = path
        self.bal = {}
        self.touched = {}
        self.records = 0
        self.txns = 0
        self.last_seq = 0
        self.open_tx = None
        self.pending = []
        self.ino = None
        self.pos = 0

    def ingest(self, lines):
        for line in lines:
            try:
                obj = _json.loads(line)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            seq = obj.get("seq")
            if type(seq) is not int or seq == self.last_seq:
                continue
            op = obj.get("op")
            if op == "PUT":
                key = obj.get("key")
                delta = obj.get("delta", 0)
                if not isinstance(key, str) or type(delta) is not int:
                    continue
                if self.open_tx is not None:
                    self.pending.append(("PUT", key, delta, seq))
                else:
                    self.bal[key] = self.bal.get(key, 0) + delta
                    self.touched[key] = seq
                    self.records += 1
            elif op == "DEL":
                key = obj.get("key")
                if not isinstance(key, str):
                    continue
                if self.open_tx is not None:
                    self.pending.append(("DEL", key, None, seq))
                else:
                    if self.bal.pop(key, None) is not None:
                        self.touched[key] = seq
                        self.records += 1
            elif op == "BEGIN":
                self.open_tx = obj.get("tx")
                self.pending = []
            elif op == "COMMIT":
                if self.open_tx is not None:
                    for kind, key, delta, s in self.pending:
                        if kind == "PUT":
                            self.bal[key] = self.bal.get(key, 0) + delta
                            self.touched[key] = s
                            self.records += 1
                        elif self.bal.pop(key, None) is not None:
                            self.touched[key] = s
                            self.records += 1
                    self.txns += 1
                    self.open_tx = None
                    self.pending = []
            elif op == "ABORT":
                self.open_tx = None
                self.pending = []
            elif op == "CKPT":
                bal = {}
                raw = obj.get("balances")
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if isinstance(k, str) and type(v) is int:
                            bal[k] = v
                self.bal = bal
                self.touched = {k: seq for k in bal}
                r = obj.get("committed_records", 0)
                t = obj.get("committed_txns", 0)
                self.records = r if type(r) is int else 0
                self.txns = t if type(t) is int else 0
                self.open_tx = None
                self.pending = []
            else:
                continue
            self.last_seq = seq

    def full(self):
        """从文件头全量重放（流式分块，避免整文件载入内存）。"""
        self.bal = {}
        self.touched = {}
        self.records = 0
        self.txns = 0
        self.last_seq = 0
        self.open_tx = None
        self.pending = []
        tail = b""
        with open(self.path, "rb") as f:
            while True:
                chunk = f.read(8 << 20)
                if not chunk:
                    break
                data = tail + chunk if tail else chunk
                end = data.rfind(b"\n")
                if end < 0:
                    tail = data
                    continue
                self.ingest(data[:end].decode("utf-8", "replace").split("\n"))
                tail = data[end + 1:]
        # 末尾未完成的行（写入端停顿期间）不参与重放
        st = os.stat(self.path)
        self.ino = st.st_ino
        self.pos = st.st_size - len(tail)

    def refresh(self):
        """增量对账：只解析新增字节；inode 变化（压缩重写）才全量重放。"""
        st = os.stat(self.path)
        if self.ino is not None and st.st_ino != self.ino:
            self.full()
            return
        if st.st_size <= self.pos:
            return
        with open(self.path, "rb") as f:
            f.seek(self.pos)
            data = f.read()
        end = data.rfind(b"\n")
        if end < 0:
            return
        self.ingest(data[:end].decode("utf-8", "replace").split("\n"))
        self.pos += end + 1

    def snapshot(self):
        return {"records": self.records, "txns": self.txns,
                "balances": dict(self.bal)}

    def summary(self):
        top = sorted(self.bal.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        return {
            "records": self.records,
            "txns": self.txns,
            "keys": len(self.bal),
            "total_balance": sum(self.bal.values()),
            "top_keys": [[k, v] for k, v in top],
        }


def txn_block(tx, puts, dels=(), op_end="COMMIT"):
    """一个事务块的行序列（seq 由 Writer 分配，dels 为 key 列表）。"""
    lines = [{"op": "BEGIN", "tx": tx}]
    for key, delta in puts:
        lines.append({"op": "PUT", "tx": tx, "key": key, "delta": delta})
    for key in dels:
        lines.append({"op": "DEL", "tx": tx, "key": key})
    lines.append({"op": op_end, "tx": tx})
    return lines


# ---------------- HTTP ----------------
def get(path, timeout=30, method="GET"):
    t0 = time.monotonic()
    req = urllib.request.Request(BASE + path, method=method)
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        body = r.read()
        return r.getcode(), r.headers, body, time.monotonic() - t0
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read(), time.monotonic() - t0
    except Exception as e:
        return -1, {}, ("ERR:%s" % e).encode(), time.monotonic() - t0


def get_json(path, timeout=30):
    code, headers, body, elapsed = get(path, timeout)
    if code != 200:
        raise RuntimeError("%s -> HTTP %s %r" % (path, code, body[:160]))
    return _json.loads(body.decode("utf-8", "replace")), elapsed, headers, body


def cmp_summary(got, want, label, fails):
    for k in ("records", "txns", "keys", "total_balance"):
        v = got.get(k)
        if isinstance(v, bool) or not isinstance(v, int):
            fails.append("%s: %s not integer (%r)" % (label, k, v))
        elif v != want[k]:
            fails.append("%s: %s=%r want %r" % (label, k, v, want[k]))
    if got.get("top_keys") != want["top_keys"]:
        fails.append("%s: top_keys=%r want %r" % (label, got.get("top_keys"),
                                                  want["top_keys"]))


def cmp_key(got, want, label, fails):
    for k in ("name", "balance", "last_seq"):
        if got.get(k) != want[k]:
            fails.append("%s: %s=%r want %r" % (label, k, got.get(k), want[k]))
PYLIB

# 等待被测服务就绪（最多 120 秒：服务启动时可能要做首次全量重放）
for i in $(seq 1 1200); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(0.2); s.connect(('127.0.0.1', 9410))" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

# 预热：服务启动后的首次统计请求（建立初始状态）。耗时落盘供 ITEM2 校验
# 20 秒门槛（instruction.md：首次统计请求最长 20 秒）。
python3 - <<'PY' 2>&1 | sed 's/^/[warmup] /'
import os, sys, time
sys.path.insert(0, "/tmp")
import walib
t0 = time.monotonic()
try:
    data, elapsed, _h, _b = walib.get_json("/api/v1/ledger", timeout=30)
    with open("/tmp/warmup_elapsed.txt", "w") as f:
        f.write("%.2f" % elapsed)
    print("first ledger in %.2fs, records=%s" % (elapsed, data.get("records")))
except Exception as e:
    with open("/tmp/warmup_elapsed.txt", "w") as f:
        f.write("FAIL")
    print("warmup failed after %.2fs: %s" % (time.monotonic() - t0, e))
PY

echo
echo "===== ITEM 1/3: protocol, endpoints and runtime identity ====="
ITEM1=$(python3 - <<'PY'
import json, os, pwd, sys
sys.path.insert(0, "/tmp")
import walib

BASE = walib.BASE
HDR = walib.HDR
fails = []

# --- /healthz ---
code, h, body, _ = walib.get("/healthz")
if code != 200:
    fails.append("/healthz status=%s" % code)
if h.get(HDR) != "Verified":
    fails.append("/healthz header=%r" % h.get(HDR))
try:
    if json.loads(body) != {"status": "healthy"}:
        fails.append("/healthz body=%r" % body[:120])
except Exception:
    fails.append("/healthz body not json: %r" % body[:120])
hz_len = len(body)

# --- /api/v1/ledger 结构与紧凑输出 ---
code, h, body, _ = walib.get("/api/v1/ledger", timeout=30)
if code != 200:
    fails.append("/api/v1/ledger status=%s" % code)
if h.get(HDR) != "Verified":
    fails.append("/api/v1/ledger header=%r" % h.get(HDR))
try:
    res = json.loads(body)
    if not isinstance(res, dict):
        fails.append("ledger body is not object")
    else:
        for k in ("records", "txns", "keys", "total_balance", "top_keys"):
            if k not in res:
                fails.append("ledger missing field %s" % k)
        for k in ("records", "txns", "keys"):
            if not isinstance(res.get(k), int) or isinstance(res.get(k), bool):
                fails.append("ledger %s is not int" % k)
        td = res.get("top_keys")
        if not isinstance(td, list):
            fails.append("ledger top_keys is not list")
        else:
            for item in td:
                if (not isinstance(item, list) or len(item) != 2
                        or not isinstance(item[0], str)
                        or isinstance(item[1], bool) or not isinstance(item[1], int)):
                    fails.append("ledger top_keys item malformed: %r" % (item,))
                    break
        if json.dumps(res, separators=(",", ":")) != body.decode("utf-8", "replace"):
            fails.append("ledger body is not compact JSON (no spaces allowed)")
except Exception:
    fails.append("ledger body not json: %r" % body[:120])

# --- /api/v1/key 结构 ---
code, h, body, _ = walib.get("/api/v1/key?name=acct-0000", timeout=10)
if code != 200:
    fails.append("/api/v1/key status=%s" % code)
if h.get(HDR) != "Verified":
    fails.append("/api/v1/key header=%r" % h.get(HDR))
try:
    res = json.loads(body)
    for k in ("name", "balance", "last_seq"):
        if k not in res:
            fails.append("key missing field %s" % k)
    if json.dumps(res, separators=(",", ":")) != body.decode("utf-8", "replace"):
        fails.append("key body is not compact JSON")
except Exception:
    fails.append("key body not json: %r" % body[:120])

# --- 参数校验 ---
for q in ("/api/v1/key", "/api/v1/key?name="):
    code, h, _b, _ = walib.get(q)
    if code != 400:
        fails.append("%s status=%s (want 400)" % (q, code))

code, h, _b, _ = walib.get("/api/v1/key?name=no-such-key-xyz")
if code != 404:
    fails.append("key unknown status=%s (want 404)" % code)
if h.get(HDR) != "Verified":
    fails.append("key unknown header=%r" % h.get(HDR))

# --- 404 / 405 / HEAD / 查询串 ---
code, h, _b, _ = walib.get("/no-such-path")
if code != 404:
    fails.append("/no-such-path status=%s (want 404)" % code)
if h.get(HDR) != "Verified":
    fails.append("/no-such-path header=%r" % h.get(HDR))

code, h, _b, _ = walib.get("/api/v1/ledger/extra")
if code != 404:
    fails.append("/api/v1/ledger/extra status=%s (want 404)" % code)

code, h, _b, _ = walib.get("/healthz?verbose=1")
if code != 200:
    fails.append("/healthz?verbose=1 status=%s (want 200)" % code)

code, h, body, _ = walib.get("/healthz", method="HEAD")
if code != 200:
    fails.append("HEAD /healthz status=%s (want 200)" % code)
if h.get(HDR) != "Verified":
    fails.append("HEAD /healthz header=%r" % h.get(HDR))
if body:
    fails.append("HEAD /healthz returned a body")
try:
    if int(h.get("Content-Length") or -1) != hz_len:
        fails.append("HEAD /healthz Content-Length=%r (want %d)"
                     % (h.get("Content-Length"), hz_len))
except ValueError:
    fails.append("HEAD /healthz Content-Length not numeric")

import urllib.request
for method in ("POST", "PUT", "DELETE", "OPTIONS", "PATCH"):
    try:
        req = urllib.request.Request(BASE + "/api/v1/ledger", data=b"", method=method)
        resp = urllib.request.urlopen(req, timeout=10)
        code, allow = resp.getcode(), resp.headers.get("Allow")
    except Exception as e:
        code = getattr(e, "code", -1)
        allow = e.headers.get("Allow") if hasattr(e, "headers") else None
    if code != 405:
        fails.append("%s /api/v1/ledger status=%s (want 405)" % (method, code))
    elif "GET" not in (allow or "") or "HEAD" not in (allow or ""):
        fails.append("%s 405 Allow=%r (want GET, HEAD)" % (method, allow))

# --- 运维真实性：端口归属 + 运行身份 + venv ---
def listen_rows(port=9410):
    rows = []
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = open(path).read().splitlines()[1:]
        except Exception:
            continue
        for ln in lines:
            p = ln.split()
            if len(p) < 10:
                continue
            try:
                if int(p[1].split(":")[1], 16) == port and p[3] == "0A":
                    rows.append({"uid": int(p[7])})
            except Exception:
                continue
    return rows

try:
    svc_uid = pwd.getpwnam("ledgersvc").pw_uid
except Exception:
    svc_uid = -1
    fails.append("system user ledgersvc not found")

rows = listen_rows()
if not rows:
    fails.append("nothing is LISTENing on :9410")
for r in rows:
    if r["uid"] != svc_uid:
        fails.append(":9410 LISTEN socket owned by uid=%s (want ledgersvc=%s)"
                     % (r["uid"], svc_uid))

cands = []
for d in os.listdir("/proc"):
    if not d.isdigit():
        continue
    try:
        argv = [a for a in open("/proc/%s/cmdline" % d, "rb").read()
                .decode("utf-8", "replace").split("\x00") if a]
    except Exception:
        continue
    if not argv:
        continue
    if "/opt/ingest-ledger/app.py" in argv and "python" in os.path.basename(argv[0]):
        try:
            uid = os.stat("/proc/%s" % d).st_uid
        except Exception:
            uid = -1
        cands.append({"pid": d, "argv": argv, "uid": uid})

owned = [c for c in cands if c["uid"] == svc_uid]
if not owned:
    fails.append("no ledgersvc-owned process running /opt/ingest-ledger/app.py")
else:
    argv = owned[0]["argv"]
    want = os.environ["VENV_PY"]
    if os.path.realpath(argv[0]) != os.path.realpath(want):
        fails.append("argv[0]=%r (want %s)" % (argv[0], want))

# 遗留占位进程必须清理干净
for d in os.listdir("/proc"):
    if not d.isdigit():
        continue
    try:
        raw = open("/proc/%s/cmdline" % d, "rb").read().decode("utf-8", "replace")
    except Exception:
        continue
    if "/opt/squatter.py" in raw:
        fails.append("leftover squatter process still running (pid %s)" % d)
        break

print("OK" if not fails else "FAIL: " + "; ".join(fails))
PY
)
if [ "$ITEM1" = "OK" ]; then
    PASS=$((PASS + 1))
    echo "ITEM 1: PASS"
elif [ -z "$ITEM1" ]; then
    echo "ITEM 1: FAIL - verifier script aborted before producing a verdict"
else
    echo "ITEM 1: FAIL - $ITEM1"
fi

echo
echo "===== ITEM 2/3: journal replay correctness (baseline + mutations) ====="
ITEM2=$(python3 - <<'PY'
import os, sys, time
sys.path.insert(0, "/tmp")
import walib

JOURNAL = walib.JOURNAL
fails = []

def ledger_now(timeout=10):
    return walib.get_json("/api/v1/ledger", timeout=timeout)[0]

# ---------- 2a. 基线：全量重放必须与独立真值完全一致，且首次请求不超 24s ----------
try:
    with open("/tmp/warmup_elapsed.txt") as f:
        warm = f.read().strip()
except Exception:
    warm = "FAIL"
if warm == "FAIL":
    fails.append("2a first ledger request failed entirely (no response within 30s)")
elif float(warm) > walib.WARMUP_SLA:
    fails.append("2a first request %.2fs > %.1fs SLA (full replay of ~9.5M lines "
                 "must fit the 20s budget)" % (float(warm), walib.WARMUP_SLA))
t0 = time.monotonic()
truth = walib.Truth(JOURNAL)
truth.full()
print("[truth] baseline replay %.1fs, records=%d txns=%d keys=%d"
      % (time.monotonic() - t0, truth.records, truth.txns, len(truth.bal)),
      file=sys.stderr)
writer = walib.Writer(JOURNAL, truth.last_seq)
want = truth.summary()
try:
    got = ledger_now(timeout=walib.WARMUP_SLA)
except Exception as e:
    got = None
    fails.append("2a baseline ledger request failed: %s" % e)
if got is not None:
    walib.cmp_summary(got, want, "2a baseline", fails)

# ---------- 2b. 写入中事务块：BEGIN+PUT 已落盘、COMMIT 未落盘 ----------
open_tx = truth.open_tx
try:
    got = ledger_now(timeout=10)
    walib.cmp_summary(got, want, "2b during-pause value", fails)
except Exception as e:
    fails.append("2b query failed: %s" % e)
writer.emit([{"op": "COMMIT", "tx": open_tx}])
truth.refresh()
want = truth.summary()
try:
    got = ledger_now(timeout=10)
    walib.cmp_summary(got, want, "2b after resume-commit", fails)
except Exception as e:
    fails.append("2b after-commit query failed: %s" % e)

# ---------- 2c. 追加一个已提交事务块 ----------
tx = 724002
puts = [("acct-%04d" % (i % 5000), (i * 37) % 20000 - 10000) for i in range(5000)]
writer.emit(walib.txn_block(tx, puts))
truth.refresh()
want = truth.summary()
try:
    walib.cmp_summary(ledger_now(), want, "2c after committed txn", fails)
except Exception as e:
    fails.append("2c query failed: %s" % e)

# ---------- 2d. 追加一个被放弃的事务块（内含合法数据） ----------
tx = 724003
puts = [("acct-%04d" % (i % 5000), 777) for i in range(800)]
writer.emit(walib.txn_block(tx, puts, op_end="ABORT"))
truth.refresh()
want = truth.summary()
try:
    walib.cmp_summary(ledger_now(), want, "2d after aborted txn", fails)
except Exception as e:
    fails.append("2d query failed: %s" % e)

# ---------- 2e. 写入端重试：紧邻重复行只计一次 ----------
tx = 724004
put = {"op": "PUT", "tx": tx, "key": "acct-0100", "delta": 500}
put["seq"] = writer.seq + 1               # 显式占用 seq，重试行复用同一 seq
dup = dict(put)
lines = [{"op": "BEGIN", "tx": tx}, put, dup,
         {"op": "COMMIT", "tx": tx}]
writer.emit(lines)
truth.refresh()
want = truth.summary()
try:
    walib.cmp_summary(ledger_now(), want, "2e after duplicate retry", fails)
except Exception as e:
    fails.append("2e query failed: %s" % e)

# ---------- 2f. 墓碑：DEL 移除 key（含不存在的 key） ----------
tx = 724005
dels = ["acct-0000", "acct-0001", "acct-9999-does-not-exist"]
writer.emit(walib.txn_block(tx, [], dels=dels))
truth.refresh()
want = truth.summary()
try:
    walib.cmp_summary(ledger_now(), want, "2f after tombstones", fails)
except Exception as e:
    fails.append("2f query failed: %s" % e)

# ---------- 2g. 非法行：JSON 损坏 / 未知 op（缺 seq）/ 布尔 delta / 负 delta ----------
tx = 724006
lines = [{"op": "BEGIN", "tx": tx},
         {"op": "PUT", "tx": tx, "key": "acct-0200", "delta": -1200},
         {"op": "PUT", "tx": tx, "key": "acct-0201", "delta": 300},
         {"op": "COMMIT", "tx": tx}]
writer.emit(lines)
writer.emit_raw(['{"seq":9,"op":"PUT","tx":%d,"key":"acct-0202","delta":12' % tx,
                 '{"op":"FLUSH","tx":%d}' % tx,
                 '{"seq":0,"op":"PUT","tx":%d,"key":"acct-0203","delta":true}' % tx])
truth.refresh()
want = truth.summary()
try:
    walib.cmp_summary(ledger_now(), want, "2g after illegal lines", fails)
except Exception as e:
    fails.append("2g query failed: %s" % e)

# ---------- 2h. /api/v1/key 单 key 查询 ----------
for key in ("acct-0200", "acct-0201", "acct-0100"):
    want_k = {"name": key, "balance": truth.bal.get(key, 0),
              "last_seq": truth.touched.get(key, 0)}
    if key not in truth.bal:
        continue
    got_k, _e, _h, _b = walib.get_json("/api/v1/key?name=%s" % key, timeout=10)
    walib.cmp_key(got_k, want_k, "2h key %s" % key, fails)

print("OK" if not fails else "FAIL: " + "; ".join(fails))
PY
)
if [ "$ITEM2" = "OK" ]; then
    PASS=$((PASS + 1))
    echo "ITEM 2: PASS"
elif [ -z "$ITEM2" ]; then
    echo "ITEM 2: FAIL - verifier script aborted before producing a verdict"
else
    echo "ITEM 2: FAIL - $ITEM2"
fi

echo
echo "===== ITEM 3/3: incremental SLA, compaction, concurrency, flock ====="
ITEM3=$(python3 - <<'PY'
import concurrent.futures as cf
import fcntl
import json
import os
import sys
import threading
import time
sys.path.insert(0, "/tmp")
import walib

JOURNAL = walib.JOURNAL
APPEND_SLA = walib.APPEND_SLA
STRUCT_SLA = walib.STRUCT_SLA
fails = []

def ledger(timeout=30):
    return walib.get_json("/api/v1/ledger", timeout=timeout)

# ---------- 3a. 增量 SLA：追加事务块后首个请求 <= 1.5s 且值正确 ----------
truth = walib.Truth(JOURNAL)
truth.full()
writer = walib.Writer(JOURNAL, truth.last_seq)
tx = 724100
puts = [("acct-%04d" % (i % 5000), (i * 53) % 30000 - 15000) for i in range(5000)]
writer.emit(walib.txn_block(tx, puts))
truth.refresh()
want = truth.summary()
got, elapsed, _h, _b = ledger(timeout=8)
if elapsed > APPEND_SLA:
    fails.append("3a append SLA: %.2fs > %.1fs (must follow the journal tail, "
                 "not replay the file)" % (elapsed, APPEND_SLA))
walib.cmp_summary(got, want, "3a after append", fails)

# ---------- 3b. 压缩重写（原子替换，新 inode）：首个请求 <= 30s 且值正确 ----------
writer.compact(truth.snapshot(), include_history=False)
truth.full()                            # 压缩后从新文件头重放（文件已大幅缩短）
want = truth.summary()
got, elapsed, _h, _b = ledger(timeout=45)
if elapsed > STRUCT_SLA:
    fails.append("3b compaction SLA: %.2fs > %.1fs" % (elapsed, STRUCT_SLA))
walib.cmp_summary(got, want, "3b after compaction", fails)

# ---------- 3c. 20 并发：不得重复累加，全部一致 ----------
tx = 724101
puts = [("acct-%04d" % (i % 5000), (i * 71) % 20000 - 10000) for i in range(2000)]
writer.emit(walib.txn_block(tx, puts))
truth.refresh()
want = truth.summary()

for _ in range(2):                      # 消除冷页/调度抖动，SLA 只衡量增量能力
    try:
        ledger()
    except Exception:
        pass

with cf.ThreadPoolExecutor(max_workers=20) as ex:
    futs = [ex.submit(lambda: ledger(timeout=8)) for _ in range(20)]
    results = [f.result() for f in futs]
slow = [e for _g, e, _h, _b in results if e > 2.0]
if slow:
    fails.append("3c concurrency: %d/20 requests exceeded 2.0s (max %.2fs)"
                 % (len(slow), max(slow)))
first = results[0][0]
for g, _e, _h, _b in results[1:]:
    if g != first:
        fails.append("3c concurrency: inconsistent responses across concurrent requests")
        break
walib.cmp_summary(first, want, "3c concurrent value (no double counting)", fails)

# ---------- 3d. 写入端长持 flock：必须 <= 2s 返回上一次完整快照 ----------
primed = ledger()[0]
holder = threading.Event()
release = threading.Event()

def long_writer():
    tx = 724102
    lines = [{"op": "BEGIN", "tx": tx}]
    for i in range(120):
        lines.append({"op": "PUT", "tx": tx, "key": "acct-%04d" % (i % 5000),
                      "delta": 9000})
    body = ("\n".join(json.dumps(o, separators=(",", ":")) for o in lines)
            + "\n").encode()
    tail = ('\n{"op":"COMMIT","tx":%d}\n' % tx).encode()
    with open(JOURNAL, "r+b") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0, 2)
            f.write(body)
            f.flush()
            os.fsync(f.fileno())
            holder.set()
            release.wait(timeout=15)      # 长持锁：事务块处于写入中
            f.write(tail)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

th = threading.Thread(target=long_writer, daemon=True)
th.start()
holder.wait(timeout=15)
time.sleep(0.2)
got, elapsed, _h, _b = ledger(timeout=6)
if elapsed > 2.0:
    fails.append("3d lock contention: %.2fs to respond (must be <= 2.0s)" % elapsed)
if got != primed:
    fails.append("3d during long lock hold: response is not the last complete snapshot")
release.set()
th.join(timeout=20)
time.sleep(0.3)
truth.refresh()
walib.cmp_summary(ledger()[0], truth.summary(), "3d after writer released", fails)

# ---------- 3e. 压缩重写与并发重叠：每个响应必须精确等于旧态或新态 ----------
truth_before = walib.Truth(JOURNAL)
truth_before.full()
writer = walib.Writer(JOURNAL, truth_before.last_seq)
old_expected = truth_before.summary()

def compactor():
    time.sleep(0.05)
    try:
        writer.compact(truth_before.snapshot(), include_history=False)
    except Exception:
        pass

sw = threading.Thread(target=compactor, daemon=True)
sw.start()
with cf.ThreadPoolExecutor(max_workers=20) as ex:
    futs = [ex.submit(lambda: ledger(timeout=40)) for _ in range(20)]
    results = [f.result() for f in futs]
sw.join(timeout=60)
truth_after = walib.Truth(JOURNAL)
truth_after.full()
new_expected = truth_after.summary()
for g, _e, _h, _b in results:
    if g != old_expected and g != new_expected:
        fails.append("3e compaction+concurrency: torn response (records=%r)"
                     % g.get("records"))
        break

print("OK" if not fails else "FAIL: " + "; ".join(fails))
PY
)
if [ "$ITEM3" = "OK" ]; then
    PASS=$((PASS + 1))
    echo "ITEM 3: PASS"
elif [ -z "$ITEM3" ]; then
    echo "ITEM 3: FAIL - verifier script aborted before producing a verdict"
else
    echo "ITEM 3: FAIL - $ITEM3"
fi

REWARD=$(awk -v s="$PASS" -v t="$TOTAL" 'BEGIN {printf "%.2f", s/t}')
echo
echo "===== RESULT: $PASS/$TOTAL -> reward=$REWARD ====="

# harbor 通过下载容器内 /logs/verifier 目录来收集判分结果，
# reward.txt 必须写在 stdout 同目录下，否则 RewardFileNotFoundError
mkdir -p "$(dirname "$REWARD_FILE")"
printf '%s' "$REWARD" > "$REWARD_FILE"
echo "[reward] written to $REWARD_FILE"
