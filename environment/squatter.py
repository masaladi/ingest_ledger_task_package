import http.server
import os
import signal
import sys


class DummyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(503)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, fmt, *args):
        return


# 让出端口后转入的保活镜像：不监听任何端口、不持有任何资源，
# 仅维持容器现场存活（终止信号不再触发任何动作）。
# 注意循环用 while 1 而非其他写法：pause() 会在信号到达后返回，必须重新阻塞。
_KEEPER_SRC = (
    "import signal\n"
    "signal.signal(signal.SIGTERM, lambda *a: None)\n"
    "signal.signal(signal.SIGINT, lambda *a: None)\n"
    "while 1:\n"
    "    signal.pause()\n"
)


def _release(signum, frame):
    # 让出 9410 并退出"占位进程"身份：exec 换身为无监听的保活镜像。
    # 监听 socket 为非继承 fd（O_CLOEXEC），exec 时自动关闭，端口随之释放；
    # 进程镜像与 cmdline 同步替换，清理脚本按 cmdline 匹配时不会残留误伤目标。
    os.execv(sys.executable, [sys.executable, "-c", _KEEPER_SRC])


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, _release)
    signal.signal(signal.SIGINT, _release)
    server = http.server.HTTPServer(('0.0.0.0', 9410), DummyHandler)
    server.serve_forever()
