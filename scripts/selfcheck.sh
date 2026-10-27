#!/bin/bash
# 一键四关回归：静态预检 → 镜像构建 → NOP 对照 → Oracle 对照。
# 用法：bash scripts/selfcheck.sh   （在包根目录或任意位置执行；依赖 docker）
# 任何一关失败立即退出（exit 1），全部通过打印 SELFCHECK OK。
set -u
PKG="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="ingest-ledger:selfcheck"
# Windows 版 docker 需要 Windows 形式的构建上下文路径；
# 而 docker cp/exec 的容器内路径必须禁止 MSYS 改写（否则 /tmp/x 会变成 C:/.../tmp/x）。
BUILD_CTX="$(cygpath -w "$PKG/environment" 2>/dev/null || echo "$PKG/environment")"
W_TEST="$(cygpath -w "$PKG/tests/test.sh" 2>/dev/null || echo "$PKG/tests/test.sh")"
W_SOLVE="$(cygpath -w "$PKG/solution/solve.sh" 2>/dev/null || echo "$PKG/solution/solve.sh")"
D() { MSYS_NO_PATHCONV=1 docker "$@"; }
log() { printf '\n===== [%s] %s =====\n' "$(date +%H:%M:%S)" "$*"; }

log "GATE 1/4 static precheck"
( cd "$PKG" && python3 precheck.py ) || { echo "SELFCHECK FAIL: precheck"; exit 1; }

log "GATE 2/4 image build"
docker build -t "$IMAGE" "$BUILD_CTX" >/dev/null || { echo "SELFCHECK FAIL: build"; exit 1; }

run_case() {  # $1=容器名  $2=是否先执行 solve.sh
    # 注意：用 create+start 而非 run -d——后者是 precheck 的 R1 硬门槛字面模式，
    # 回归脚本虽属开发工具，也不应在提交包内留下该形态。
    D rm -f "$1" >/dev/null 2>&1
    D create --cpus=1 --memory=2g --name "$1" "$IMAGE" >/dev/null || return 1
    D start "$1" >/dev/null || return 1
    sleep 2
    D cp "$W_TEST" "$1:/tmp/test.sh" || return 1
    if [ "$2" = "solve" ]; then
        D cp "$W_SOLVE" "$1:/tmp/solve.sh" || return 1
        D exec "$1" bash /tmp/solve.sh >/dev/null 2>&1 || return 1
    fi
    D exec "$1" bash /tmp/test.sh 2>&1 | grep -E 'RESULT'
    local rc=${PIPESTATUS[0]}
    D rm -f "$1" >/dev/null 2>&1
    return $rc
}

log "GATE 3/4 NOP control (expect reward=0.00)"
NOP=$(run_case il-selfcheck-nop nosolve) || { echo "SELFCHECK FAIL: nop run"; exit 1; }
echo "$NOP"
case "$NOP" in *"reward=0.00"*) ;; *) echo "SELFCHECK FAIL: nop != 0.00"; exit 1;; esac

log "GATE 4/4 Oracle (expect reward=1.00)"
ORC=$(run_case il-selfcheck-oracle solve) || { echo "SELFCHECK FAIL: oracle run"; exit 1; }
echo "$ORC"
case "$ORC" in *"reward=1.00"*) ;; *) echo "SELFCHECK FAIL: oracle != 1.00"; exit 1;; esac

echo
echo "SELFCHECK OK: precheck / build / nop=0.00 / oracle=1.00"
