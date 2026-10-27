# 基础镜像按摘要锁定，保证各次构建输入一致（可复现性）
FROM ubuntu:22.04@sha256:0a9887931b0e38af2e3487ed39e29436b0535c4f3f52e57745baaf79d1898956

RUN sed -i \
    -e 's|http://archive.ubuntu.com|http://mirrors.tuna.tsinghua.edu.cn|g' \
    -e 's|http://security.ubuntu.com|http://mirrors.tuna.tsinghua.edu.cn|g' \
    /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    python3-ujson \
    procps \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 唯一允许运行该服务的系统账号（题面运维规范要求服务以它运行）
RUN useradd --system --create-home --shell /usr/sbin/nologin ledgersvc

# /opt/ingest-ledger/app.py 是"上一任遗留"的实现：可运行，但它按普通 JSONL 逐行
# 累加，不认识事务边界、放弃事务与压缩检查点，配置指向的也是旧环境。
# 修复它（含整体重写）是任务的一部分。
# 注意：任务包中的 tests/ 与 solution/ 不打进镜像——评测框架（harbor）会在评测
# 阶段分别注入容器：solution/solve.sh 由 Oracle 代理执行，tests/test.sh 由
# verifier 在代理结束后执行。
COPY app.py /opt/ingest-ledger/app.py
RUN chmod 755 /opt/ingest-ledger/app.py

COPY config.json /etc/ingest-ledger/config.json
COPY squatter.py /opt/squatter.py
RUN chmod 755 /opt/squatter.py

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 755 /usr/local/bin/entrypoint.sh

# 数据摄取管道的基线事务日志（约 950 万行 / 约 600 MB），外加 samples/ 下一份
# 小样本 journal 与其逐条语义标注 —— 数据团队没有留下格式文档，样本是唯一的
# 语义依据。生成脚本在构建期用完即删，不进运行时现场。
COPY gen_journal.py /opt/gen_journal.py
RUN mkdir -p /var/lib/ingest-ledger /opt/ingest-ledger/samples \
    && python3 /opt/gen_journal.py \
    && rm -f /opt/gen_journal.py

# 注意：verifier 使用的真值基线副本与摘要不写入镜像（跨界原则——测试侧材料
# 不得进入被测环境）。真值锚点为烧录进判分脚本（tests/test.sh，评测阶段注入）
# 的 journal 摘要常量，评测开始时对 live journal 做摘要校验。

# 上一任留下的权限现场：数据目录不可进入、journal 不可读、日志目录不可写。
# 服务以 ledgersvc 运行时读不到数据文件，也写不出日志。
RUN mkdir -p /var/log/ingest-ledger \
    && chown -R root:root /var/lib/ingest-ledger \
    && chmod 700 /var/lib/ingest-ledger \
    && chmod 600 /var/lib/ingest-ledger/journal.jsonl \
    && chmod 644 /opt/ingest-ledger/samples/* \
    && chown -R root:root /var/log/ingest-ledger \
    && chmod 755 /var/log/ingest-ledger

CMD ["/usr/local/bin/entrypoint.sh"]
