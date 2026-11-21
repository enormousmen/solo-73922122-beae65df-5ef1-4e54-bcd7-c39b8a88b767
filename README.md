# Canary Rollout API

灰度发布控制服务：运维员上报副本状态与窗口指标，服务根据**目标版本、每批副本上限、
最短稳定窗口、错误率阈值**自动决定 **放量(promote) / 暂停(hold) / 回滚(rollback) /
完成(complete)**，全部决策与执行结果持久化在 SQLite，重启后可继续发布。

## 1. 启动（Docker Compose）

```bash
docker compose up -d --build
curl -s http://127.0.0.1:8000/health      # {"status":"ok"}
# 交互式文档：http://127.0.0.1:8000/docs
```

SQLite 数据库位于命名卷 `canary-data`（容器内 `/data/canary.db`，WAL 模式）。
容器重启后状态不丢失；`docker compose down` 不删卷，`docker compose down -v` 才清空。

本地无 Docker 时也可直接运行：

```bash
pip install -r requirements.txt
CANARY_DB_PATH=./data/canary.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 2. 工作模型与代次(generation)语义

- 创建发布时给定 `total_replicas`、`max_replicas_per_batch`、
  `min_stable_window_seconds`、`error_rate_threshold`，初始代次为 `0`。
- 运维员持续上报**带代次**的副本状态和指标窗口。
- `POST /evaluate` 按当前代次的观测做决策，优先级：
  1. **回滚**：错误率 `error_count / request_count` **严格大于**阈值（无流量不触发）；
  2. **完成**：最后一批已生效、全部副本 ready、且有满足最短窗口的观测；
  3. **放量**：窗口达标、无 unready 副本、未到总副本数，放量步长为
     `min(总副本数, 当前副本数 + 每批上限)`；
  4. **暂停(hold)**：条件不满足时保持当前规模，并在 `reason` 中说明阻塞项。
- `promote` / `rollback` 决策产生后进入 `pending`，由平台执行后通过
  `.../result` 回报 `succeeded` 或 `failed`：
  - 成功：放量推进副本数，回滚进入终态 `rolled_back`，**代次 +1**；
  - 失败：**保留上一个可执行状态**（副本数与代次均不变），决策标记 `failed`，
    放量失败转 `paused`、回滚失败转终态 `failed` 等待人工介入。
- `pause/hold` 决策不占用代次，可在同代次继续观测后重新评估。
- **旧代次/迟到观测**（`generation < current_generation`）会入库但标记
  `ignored=1`，绝不参与聚合、不能改写已提交状态；未来代次（`> current`）直接 422。
- 执行结果只接受一次，重复回报返回 `409`。

## 2.1 评估并发语义（长扫描不阻塞写入）

灰度积累大量观测后，`POST /evaluate` 的聚合扫描不再持有 SQLite 写锁：

1. **无锁快照扫描**：评估先在一个 WAL 只读快照事务里读取发布状态、副本与
   指标明细并在内存中完成决策计算；此期间同库**其他发布的上报与执行回执、
   以及本发布的新上报都可以正常提交**（WAL 读写互不阻塞）。
2. **短写事务乐观提交**：只有最后的决策落库走串行写事务，提交时重新校验
   - 发布状态元组（`status` / 代次 / 副本数 / pending / last_decision /
     失败信息及配置列）与快照一致；
   - 本代次有效观测指纹（两张观测表中该代 `ignored=0` 行的最大行 id；
     观测表只追加不删除，最大 id 即变更令牌，并有覆盖索引支撑点查）。
3. **冲突处理**：若同一发布在扫描期间提交了**新的有效观测**或发生了**状态
   变更**，本次算出的决定不落库，自动用最新快照**重新计算**（例如新指标让
   累计错误率超过阈值时，原先的放量决定会被回滚决定取代）；连续 3 次都冲突
   则返回 `409`，提示调用方重试且**本次不写任何决定**。
4. **不触发重试的写入**：旧代次补报入库即标记 `ignored=1`，不参与指纹；其他
   发布的写入行 id 互不相关——两者都不会让评估重试。

接口路径、请求/响应结构与决策规则均保持不变；无需迁移，旧 SQLite 库直接
可用，服务启动时会幂等补建两个覆盖索引。

## 3. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/rollouts` | 创建灰度发布 |
| GET | `/rollouts` / `/rollouts/{id}` | 列表 / 详情 |
| POST | `/rollouts/{id}/replicas` | 上报副本状态（带代次） |
| POST | `/rollouts/{id}/metrics` | 上报指标窗口（带代次） |
| POST | `/rollouts/{id}/evaluate` | 评估并产生决策 |
| POST | `/rollouts/{id}/decisions/{did}/result` | 回报决策执行结果 |
| POST | `/rollouts/{id}/pause` / `/resume` | 人工暂停 / 恢复 |
| GET | `/rollouts/{id}/decisions` | 全部决策（SQLite 持久化） |
| GET | `/rollouts/{id}/observations?kind=replicas\|metrics` | 原始观测（含 ignored 行） |

## 4. 可直接调用的示例

```bash
# 依赖 curl + jq；覆盖 hold→放量→执行→旧观测忽略→第二批→阈值回滚全流程
bash examples/api_examples.sh

# 仅依赖 Python 标准库的等价示例
BASE=http://127.0.0.1:8000 python3 examples/client_demo.py

# 多线程上报/评估/执行回执交错、旧代补报、409 重试约定的并发演示
BASE=http://127.0.0.1:8000 python3 examples/concurrency_demo.py
```

典型放量决策响应：

```json
{
  "evaluated": true,
  "message": "stable for 30s (>= 30s) with error rate 0.0040 <= 0.0500; promote 0 -> 2 replicas",
  "decision": {
    "id": "dec_...", "generation": 0, "action": "promote",
    "from_replicas": 0, "to_replicas": 2,
    "execution_status": "pending",
    "detail": { "ready_replicas": 2, "metrics": { "error_rate": 0.004, "max_window_seconds": 30.0 } }
  },
  "rollout": { "status": "pending", "current_generation": 0, "current_replicas": 0, "pending_decision_id": "dec_..." }
}
```

## 5. 自测

```bash
pip install -r requirements-dev.txt
python3 -m pytest -q        # 28 个用例：放量/完成、阈值回滚、旧观测、执行失败、
                            # 重启恢复、校验、旧库兼容，以及评估乐观提交与读写交错并发
```

`tests/test_evaluate_concurrency.py` 专门覆盖：扫描期间同发布新有效观测触发
重算（放量→回滚）、状态变更触发重算、持续冲突返回 409 且不落决定、旧代补报
与其他发布写入不触发重试、长扫描不阻塞并发写入、真实多线程下每代至多一个
pending 决定。

## 6. 目录结构

```
app/            FastAPI 应用（main 路由 / rollout 状态机 / db SQLite / models 模型）
tests/          pytest 自测
examples/       curl+jq 与纯标准库 Python 两套可调用示例
Dockerfile, docker-compose.yml, requirements.txt
```
