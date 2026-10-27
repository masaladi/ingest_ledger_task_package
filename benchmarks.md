# 性能实测记录（benchmarks）

本文档固定记录关键性能数字的实测环境、方法与结果，供质检与回归对照。
所有实测均在单核（`--cpus=1`）、2 GB 内存约束下进行，与 `task.toml` 的资源配额一致。

## 环境

- 基础镜像：`ubuntu:22.04`（按 Dockerfile 摘要锁定），Python 3.10 + apt 渠道 ujson
- 基线数据：`environment/gen_journal.py` 在镜像构建期生成
  （`random.Random(20261026)`，确定性），约 950 万行 / 约 600 MB，
  其中已提交有效数据记录约 800 万条，外加 `samples/` 下的语义样本与逐条标注
- 运行方式：`docker run --cpus=1 --memory=2g`，容器内单进程服务

## 重放与聚合实测（ujson 逐行解析 + 增量 tail-follow）

| 路径 | 耗时 | 对应门槛 | 余量 |
|---|---|---|---|
| 全量重放建态（约 950 万行，stdlib 逐行解析） | 23.9s（单核实测） | 首次请求 20s | 超限，被门槛确定性淘汰 |
| 全量重放建态（apt 渠道 ujson） | 12.6-12.7s（单核实测） | 首次请求 20s | 参考解 warmup 实测 12.98-14.06s（多轮），约 1.4-1.5x |
| verifier 基线真值重放 | 15.3-16.6s（单核实测） | verifier 总限时 600s | verifier 全流程实测约 2-3 分钟 |
| 尾部增量（追加 5000 条事务块） | <0.05s | 常规变更 1.5s | 全量重放（20s 级）与"每段重放"均远超 1.5s，不可行 |
| 压缩重写后重建（新 inode，文件缩短为检查点基线） | <1s | 结构性变更 30s | 主要考察重建触发与正确性，非耗时 |
| flock 等锁超时回退 | ≤1s 等锁 + 立即返回快照 | 请求总耗时 2s | — |

## 结论

- 1.5 秒常规门槛使 **tail-follow 增量**成为唯一可行路径（全量重放 20s 级不可行）；
- 20 秒首次门槛要求**优化解析路径**（stdlib 逐行解析实测 23.9s 超限，
  apt 渠道 ujson 实测 12.6-12.7s，余量约 1.6x；镜像内 apt 渠道 ujson 为"机器上现成工具"）；
- 30 秒结构性门槛为诚实实现的重建留有余量，主要惩罚"未检测 inode 变化"与
  "重建后状态错误"两类实现。

## 复现方式

```bash
docker build -t ingest-ledger environment
docker run --rm --cpus=1 --memory=2g ingest-ledger python3 - <<'PY'
# 逐行解析 /var/lib/ingest-ledger/journal.jsonl（分别以 stdlib json 与
# apt 渠道 ujson），对全量重放、尾部增量、检查点重放分别计时多轮
PY
```

实测数据随每次发布前回归记录更新（README「发布前回归清单」第 5 步）。

## 回归记录

- 2026-10-26 v1：镜像 ingest-ledger:v1（约 953.7 万行 / 607MB；已提交数据记录
  7,976,143、事务 724,000、存活 key 4,991、末尾未提交事务 137 条）。precheck OK；
  NOP reward=0.00；Oracle reward=1.00（warmup 12.98s / 13.20s / 13.37s / 14.06s 多轮）。

- 2026-10-27 v2（no-op DEL 轴翻转，仅改判分/题面/参考解，数据与镜像未变）：
  T1 两连 1.000 归因为题面第 85 行把 no-op DEL 计数口径规范化（第四处同型泄露）。
  records 真值改为「pop 成功才计数」（无效果 DEL 不计入）：truth/oracle 实测
  records=7,976,106（=7,976,143 - 37 条 no-op DEL），txns/keys/total/top_keys 不变；
  本地差分对拍 Truth vs oracle Ledger 逐字段一致。题面异常表删除该行、records
  定义去掉「有效」与「PUT 与 DEL」枚举，恢复无先验口径歧义轴。

- 2026-10-27 v3（R2 整改：防篡改真值基线 / mtime 检测 / 样本对齐 / 依赖锁版本，
  镜像重建 ingest-ledger:selfcheck）：precheck OK（15 文件）；NOP reward=0.00；
  Oracle reward=1.00（warmup 13.49s，records=7,976,106）。回归工具入口
  `scripts/selfcheck.sh`（预检 → 构建 → NOP → Oracle 一键四关）。
