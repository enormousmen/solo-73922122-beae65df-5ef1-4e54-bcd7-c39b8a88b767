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
python3 -m pytest -q        # 18 个用例：放量/完成、阈值回滚、旧观测、执行失败、重启恢复、校验
```

## 6. 目录结构

```
app/            FastAPI 应用（main 路由 / rollout 状态机 / db SQLite / models 模型）
tests/          pytest 自测
examples/       curl+jq 与纯标准库 Python 两套可调用示例
Dockerfile, docker-compose.yml, requirements.txt
```
