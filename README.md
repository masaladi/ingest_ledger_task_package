# ingest-ledger-incident（chaotic-environment / hard）

数据工程方向的单机故障现场修复题。数据摄取管道的对账账本服务监听 9410 端口，
以事务日志（journal，JSONL）为唯一真值来源，服务、配置、权限与数据链路存在多处
相互交织的缺陷，且数据文件（约 950 万行 / 约 600 MB，其中已提交有效数据记录约
800 万条，单核全量重放 20 秒以上）在运行期被持续修改，变更后的统计请求须在
1.5 秒内返回且数值正确。

## 包结构

| 路径 | 角色 | 谁使用 | 何时使用 |
|---|---|---|---|
| `instruction.md` | 题面：验收标准、journal 记录类型、输入契约（含异常闭环表）、写入端行为与正确性契约、运维规范 | 被测代理 | 评测开始时注入 |
| `environment/` | 初始环境：`Dockerfile`（含缺陷现场与基线数据生成，基础镜像按摘要锁定）、`app.py`（上一任遗留的缺陷服务）、`config.json`（含重复键陷阱）、`squatter.py`（端口占用进程）、`entrypoint.sh`、`gen_journal.py` | 构建镜像 | 评测开始前 |
| `solution/solve.sh` | Oracle 解法：修复现场并实现满足全部契约的增量重放服务 | Oracle 代理 | Oracle trial 执行 |
| `tests/test.sh` | Verifier：3 项 × 1/3 判分，全部真值由 verifier 侧独立计算，不信任服务自报 | 评测框架（代理结束后注入并执行） | 判分阶段 |
| `docs/scoring-rationale.md` | 判分粒度设计说明：3 项二元结构与 ≤0.700 难度门的绑定论证、真值计算方法 | 质检审阅 | R2 质检 |
| `benchmarks.md` | 性能实测记录：重放耗时、门槛余量的方法、数据与复现命令 | 质检审阅 | R2 质检 |
| `precheck.py` | 发布前静态预检：文件完整性、硬门槛模式扫描、三方数字对齐（无 Docker / 无常驻进程 / 无网络） | 出题人 | 每次送测前 |

注：`tests/` 与 `solution/` 不打进镜像，由评测框架（harbor）在对应阶段分别注入容器。

## 判分口径速览

- ITEM 1：HTTP 协议与端点（healthz / ledger 结构与紧凑输出 / key 查询与 400/404 /
  405+Allow 覆盖全部非 GET/HEAD 方法 / HEAD / 查询串 / 自定义响应头）
- ITEM 2：重放正确性（基线真值比对 + 写入中事务块 + 追加/放弃事务 + 紧邻重试
  去重 + 墓碑 + 非法行闭环）+ 首次请求 20 秒门槛
- ITEM 3：增量 SLA（变更后首请求 ≤1.5s）+ 压缩重写 ≤30s + 20 并发响应与真值
  全量一致 + flock 等锁上限与快照回退 + 压缩与并发重叠不撕裂

时延口径以 `instruction.md` 为唯一标准：常规变更后 1.5 秒；服务启动后的首次
统计请求 20 秒；压缩重写后的首个请求 30 秒。
性能数字的实测方法、历史数据与复现命令见 `benchmarks.md`。

## 运维观测与容器生命周期

- **健康检查**：`GET /healthz` 返回 `{"status":"healthy"}`；就绪判定以 9410
  端口可接受连接为准。
- **日志**：遗留占位进程与服务进程均前台运行、输出到容器 stdout/stderr，
  `docker logs` 可直接观测；服务不落本地日志文件。
- **容器停止**：占位进程收到 SIGTERM 后让出 9410 端口并转为无监听保活镜像；
  保活镜像再次收到终止信号即正常退出，`docker stop` 可优雅完成。
- **防篡改基线**：容器启动时 journal 被冻结为只读副本与 SHA-256 摘要（见
  `docs/scoring-rationale.md` §3b），评测真值与该基线解耦于可写的 live 文件。
- **一键回归**：`bash scripts/selfcheck.sh` 依次执行预检 → 构建 → NOP →
  Oracle 四关，输出各关判定。

## 发布前回归清单（送测前）

每次送测前依次执行，任何一步失败不提交（平台规定检测未通过后需等待 30 分钟，
勿用平台试错）：

1. **静态预检**：`python3 precheck.py` —— 文件完整性、R1 硬门槛模式（常驻容器、
   无限等待类命令）扫描、题面/README/判分口径三方数字对齐、历次迭代残留清查。
2. **镜像构建**：`docker build -t ingest-ledger environment` —— 构建期 apt 安装
   `python3-ujson`、生成基线 journal，确认成功。
3. **NOP 对照**：干净容器直接跑 `tests/test.sh`，预期 0/3（reward ≤ 0.100）。
4. **Oracle 对照**：容器内执行 `solution/solve.sh` 后跑 `tests/test.sh`，
   预期 3/3（reward = 1.000）。
5. **性能抽查**：对照 `benchmarks.md` 记录，确认参考解 warmup / 增量耗时仍在
   门槛余量内。

其中 2-4 的判分数字以 `/logs/verifier/reward.txt` 为准；本清单为包外流程的
固定记录模板。
