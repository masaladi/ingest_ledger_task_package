#!/usr/bin/env python3
"""上一任部署工程师遗留的账本服务（现场保留物）。

这是一个"能跑但不正确"的初始实现，供值班工程师排查参考：
  - 只按普通 JSONL 逐行处理：看到 PUT 就累加，不认识事务边界，
    BEGIN/COMMIT/ABORT/CKPT 行一律当作普通行跳过；
  - 不做任何重复行甄别，也不感知文件被整文件重写；
  - 配置沿用旧环境（旧端口与旧数据目录），没有考虑权限与并发。
它的输出必然与题面验收标准不符，修复或重写由值班工程师决定。
"""
import http.server
import json
import os
import urllib.parse

CONFIG_PATH = "/etc/ingest-ledger/config.json"


def load_config():
    port = 9409
    data_dir = "/var/lib/ingest-ledger/legacy"
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            server = json.load(f).get("server", {})
        port = int(server.get("port", port))
        data_dir = server.get("data_dir", data_dir)
    except Exception:
        pass
    return port, data_dir


class State(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.records = 0
        self.keys = {}
        self.last_seq = 0

    def rescan(self):
        path = os.path.join(self.data_dir, "journal.jsonl")
        self.records = 0
        self.keys = {}
        self.last_seq = 0
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    op = obj.get("op")
                    if op not in ("PUT", "DEL"):
                        continue
                    self.records += 1
                    key = obj.get("key")
                    if not isinstance(key, str):
                        continue
                    if op == "PUT":
                        delta = obj.get("delta", 0)
                        if isinstance(delta, (int, float)):
                            self.keys[key] = self.keys.get(key, 0) + delta
                    else:
                        self.keys.pop(key, None)
                    seq = obj.get("seq")
                    if isinstance(seq, int):
                        self.last_seq = max(self.last_seq, seq)
        except OSError:
            pass

    def summary(self):
        self.rescan()
        top = sorted(self.keys.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        return {
            "records": self.records,
            "keys": len(self.keys),
            "total_balance": round(sum(self.keys.values()), 3),
            "top_keys": [[k, round(v, 3)] for k, v in top],
        }


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            self._send(200, b'{"status":"healthy"}')
            return
        if parsed.path == "/api/v1/ledger":
            self._send(200, json.dumps(STATE.summary(), separators=(",", ":")).encode())
            return
        self._send(404, b"")

    def log_message(self, fmt, *args):
        return


PORT, DATA_DIR = load_config()
STATE = State(DATA_DIR)

if __name__ == "__main__":
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    httpd.daemon_threads = True
    httpd.serve_forever()
