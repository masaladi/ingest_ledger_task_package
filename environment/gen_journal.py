#!/usr/bin/env python3
"""生成数据摄取管道的基线事务日志（journal）与一份用于语义自证的样本。

journal 为 JSONL：每行一个独立 JSON 对象。记录类型（题面"输入规格"一节定义）：
  BEGIN / COMMIT / ABORT : 事务边界，携带 tx
  PUT / DEL              : 数据记录，携带 key（DEL 移除该 key），PUT 携带整数 delta
  CKPT                   : 压缩检查点，携带 balances / committed_records / committed_txns

基线里刻意混入的形态（题面以"闭环表"说明预期处理）：
  - ABORT 事务块（内含合法数据记录，整体无效）；
  - 写入端重试导致的紧邻重复行（同一 seq 连续出现两次）；
  - PUT 的 delta 缺失 / 布尔值 / 负数等边界形态；
  - 少量 JSON 损坏行、未知 op 行、seq 非法行；
  - 文件末尾是一个写入中的事务块（BEGIN 与数据记录已落盘、COMMIT 尚未落盘）。

samples/ 下附一份小样本与其逐条语义标注，作为 journal 语义的唯一依据。
"""
import json
import os
import random

OUT_DIR = "/var/lib/ingest-ledger"
SAMPLE_DIR = "/opt/ingest-ledger/samples"
JOURNAL = os.path.join(OUT_DIR, "journal.jsonl")

SEED = 20261026
KEY_COUNT = 5000
TXNS = 724000                 # 已提交事务数
OPEN_TAIL_RECORDS = 137       # 末尾未提交事务内的数据记录数
DUP_EVERY = 1900              # 每隔多少条数据记录出现一次紧邻重试行

KEYS = ["acct-%04d" % i for i in range(KEY_COUNT)]


def put_line(seq, tx, key, delta):
    return {"seq": seq, "op": "PUT", "tx": tx, "key": key, "delta": delta}


def begin_line(seq, tx):
    return {"seq": seq, "op": "BEGIN", "tx": tx}


def commit_line(seq, tx):
    return {"seq": seq, "op": "COMMIT", "tx": tx}


def dumps(obj):
    return json.dumps(obj, separators=(",", ":"))


def gen():
    rnd = random.Random(SEED)
    seq = 0
    data_records = 0          # 已提交事务内的 PUT+DEL（重试行不计）
    dups = 0
    total_lines = 0
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    with open(JOURNAL, "w", encoding="utf-8", newline="\n") as f:
        buf = []

        def emit(obj):
            nonlocal seq, total_lines
            seq += 1
            obj["seq"] = seq
            buf.append(dumps(obj))
            total_lines += 1

        def flush():
            if buf:
                f.write("\n".join(buf) + "\n")
                del buf[:]

        for t in range(1, TXNS + 1):
            emit(begin_line(0, t))
            n = rnd.randint(8, 14)
            for _ in range(n):
                rec = put_line(0, t, rnd.choice(KEYS), rnd.randint(-50000, 50000))
                if rnd.random() < 0.005:
                    # 附加 meta 子对象：内部可能也带 delta / op 等同名键，
                    # 统计一律以顶层字段为准（题面闭环表）
                    rec["meta"] = {"op": "FLUSH", "delta": rnd.randint(-9, 9),
                                   "note": "upstream-retry-metadata"}
                emit(rec)
                data_records += 1
                if data_records % DUP_EVERY == 0:
                    # 写入端重试：同一 seq 的行紧邻再次出现
                    buf.append(dumps(rec))
                    dups += 1
                    total_lines += 1
            if rnd.random() < 0.02:
                emit({"seq": 0, "op": "DEL", "tx": t, "key": rnd.choice(KEYS)})
                data_records += 1
            emit(commit_line(0, t))
            if t % 400 == 0:
                flush()

            if t % 60 == 0:
                # 被放弃的事务块：内含合法数据记录，整体无效
                tx = 10 ** 7 + t             # 独立 tx 号段
                emit(begin_line(0, tx))
                for _ in range(rnd.randint(5, 9)):
                    emit(put_line(0, tx, rnd.choice(KEYS), rnd.randint(-50000, 50000)))
                emit({"seq": 0, "op": "ABORT", "tx": tx})
                flush()

            if t % 7000 == 0:
                # 非法形态：JSON 损坏行 / 未知 op（缺 seq）/ 布尔 delta
                flush()
                f.write('{"seq":9,"op":"PUT","tx":%d,"key":"acct-0001","delta":12\n' % t)
                total_lines += 1
                buf.append('{"op":"FLUSH","tx":%d}' % t)
                buf.append('{"seq":0,"op":"PUT","tx":%d,"key":"acct-0002","delta":true}' % t)
                total_lines += 2
                flush()

        # 末尾：写入中的事务块（BEGIN + 数据记录已落盘，COMMIT 未落盘）
        tx = TXNS + 1
        emit(begin_line(0, tx))
        for _ in range(OPEN_TAIL_RECORDS):
            emit(put_line(0, tx, rnd.choice(KEYS), rnd.randint(-50000, 50000)))
        flush()

    # ---- 语义自证样本（37 行，覆盖全部记录类型与异常形态，含无效果墓碑口径演示） ----
    sample = [
        '{"seq":1,"op":"BEGIN","tx":501}',
        '{"seq":2,"op":"PUT","tx":501,"key":"acct-0007","delta":120}',
        '{"seq":3,"op":"PUT","tx":501,"key":"acct-0007","delta":-45}',
        '{"seq":4,"op":"COMMIT","tx":501}',
        '{"seq":5,"op":"BEGIN","tx":502}',
        '{"seq":6,"op":"PUT","tx":502,"key":"acct-0013","delta":300}',
        '{"seq":7,"op":"ABORT","tx":502}',
        '{"seq":8,"op":"BEGIN","tx":503}',
        '{"seq":9,"op":"PUT","tx":503,"key":"acct-0002","delta":80}',
        '{"seq":9,"op":"PUT","tx":503,"key":"acct-0002","delta":80}',
        '{"seq":10,"op":"COMMIT","tx":503}',
        '{"seq":11,"op":"BEGIN","tx":504}',
        '{"seq":12,"op":"PUT","tx":504,"key":"acct-0009","delta":60}',
        '{"seq":13,"op":"COMMIT","tx":504}',
        '{"seq":14,"op":"BEGIN","tx":505}',
        '{"seq":15,"op":"PUT","tx":505,"key":"acct-0004","delta":90}',
        '{"seq":16,"op":"DEL","tx":505,"key":"acct-0007"}',
        '{"seq":17,"op":"COMMIT","tx":505}',
        '{"seq":18,"op":"BEGIN","tx":506}',
        '{"seq":19,"op":"PUT","tx":506,"key":"acct-0005","delta":70}',
        '{"seq":20,"op":"PUT","tx":506,"key":"acct-0005"}',
        '{"seq":21,"op":"PUT","tx":506,"key":"acct-0006","delta":true}',
        '{"seq":22,"op":"PUT","tx":506,"key":"acct-0008","delta":-15}',
        '{"seq":23,"op":"COMMIT","tx":506}',
        '{"seq":24,"op":"COMMIT","tx":999}',
        '{"seq":25,"op":"BEGIN","tx":507}',
        '{"seq":26,"op":"PUT","tx":507,"key":"acct-0011","delta":40}',
        '{"seq":27,"op":"COMMIT","tx":507}',
        '{"seq":28,"op":"CKPT","committed_records":10,"committed_txns":6,'
        '"balances":{"acct-0002":80,"acct-0004":90,"acct-0005":70,'
        '"acct-0008":-15,"acct-0009":60,"acct-0011":40}}',
        '{"seq":29,"op":"BEGIN","tx":508}',
        '{"seq":30,"op":"PUT","tx":508,"key":"acct-0012","delta":500}',
        '{"seq":30,"op":"PUT","tx":508,"key":"acct-0012","delta":500}',
        '{"seq":31,"op":"COMMIT","tx":508}',
        '{"seq":32,"op":"BEGIN","tx":509}',
        '{"seq":33,"op":"DEL","tx":509,"key":"acct-0007"}',
        '{"seq":34,"op":"DEL","tx":509,"key":"acct-0777"}',
        '{"seq":35,"op":"COMMIT","tx":509}',
    ]
    with open(os.path.join(SAMPLE_DIR, "journal_sample.jsonl"), "w",
              encoding="utf-8", newline="\n") as f:
        f.write("\n".join(sample) + "\n")

    notes = [
        {"line": 1, "note": "事务 501 开始"},
        {"line": 2, "note": "acct-0007 += 120"},
        {"line": 3, "note": "同 key 再次累加：acct-0007 累计 75"},
        {"line": 4, "note": "事务 501 提交，2 条数据记录计入有效记录"},
        {"line": 5, "note": "事务 502 开始"},
        {"line": 6, "note": "acct-0013 += 300"},
        {"line": 7, "note": "事务 502 被放弃：块内记录不产生任何效果"},
        {"line": 8, "note": "事务 503 开始"},
        {"line": 9, "note": "acct-0002 += 80"},
        {"line": 10, "note": "写入端重试：seq 9 紧邻重复出现，只计一次"},
        {"line": 11, "note": "事务 503 提交，acct-0002 = 80（1 条数据记录）"},
        {"line": 12, "note": "事务 504 开始"},
        {"line": 13, "note": "acct-0009 += 60"},
        {"line": 14, "note": "事务 504 提交"},
        {"line": 15, "note": "事务 505 开始"},
        {"line": 16, "note": "acct-0004 += 90"},
        {"line": 17, "note": "墓碑：acct-0007 从账本移除（该 key 不再出现在统计中）"},
        {"line": 18, "note": "事务 505 提交，2 条数据记录计入"},
        {"line": 19, "note": "事务 506 开始"},
        {"line": 20, "note": "acct-0005 += 70"},
        {"line": 21, "note": "PUT 缺 delta：按 0 计入，该行仍生效（acct-0005 += 0）"},
        {"line": 22, "note": "delta 为布尔值：该行非法，静默过滤"},
        {"line": 23, "note": "delta 为负数：正常累加，acct-0008 = -15"},
        {"line": 24, "note": "事务 506 提交，3 条数据记录计入（70 / 0 / -15）"},
        {"line": 25, "note": "孤立 COMMIT（无对应 BEGIN）：该行非法，静默过滤"},
        {"line": 26, "note": "事务 507 开始"},
        {"line": 27, "note": "acct-0011 += 40"},
        {"line": 28, "note": "事务 507 提交"},
        {"line": 29, "note": "压缩检查点：此前已提交历史以该快照为基线，"
                             "从快照继续重放（acct-0007 已被移除、acct-0013 已被放弃，均不在快照内）"},
        {"line": 30, "note": "事务 508 开始"},
        {"line": 31, "note": "acct-0012 += 500"},
        {"line": 32, "note": "写入端重试：seq 30 紧邻重复，只计一次"},
        {"line": 33, "note": "事务 508 提交，acct-0012 = 500（1 条数据记录）"},
        {"line": 34, "note": "事务 509 开始"},
        {"line": 35, "note": "无效果 DEL：acct-0007 已在 seq 16 被墓碑移除，不在此"
                             "账本中。该行合法、被正常消费（seq 推进、事务正常"
                             "提交），但不产生任何效果——不改变账本、不计入有效"
                             "数据记录（与 CKPT 的 committed_records 同口径），"
                             "该 key 的 last_seq 也不更新"},
        {"line": 36, "note": "无效果 DEL：acct-0777 从未出现在账本中，口径同上行"
                             "——行合法但无效果，不计入有效数据记录"},
        {"line": 37, "note": "事务 509 提交：块内两条 DEL 均无效果，0 条数据记录计入"},
    ]
    with open(os.path.join(SAMPLE_DIR, "journal_sample.decoded.json"), "w",
              encoding="utf-8", newline="\n") as f:
        json.dump(notes, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(JOURNAL)
    print("journal: %d lines (%d committed data records, %d duplicate lines), %.1f MB"
          % (total_lines, data_records, dups, size / 1048576.0))
    print("committed_txns=%d open_tail_records=%d keys=%d"
          % (TXNS, OPEN_TAIL_RECORDS, KEY_COUNT))


if __name__ == "__main__":
    gen()
