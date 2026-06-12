# RelayRPC Scheduler

RelayRPC 调度中心。接收 HTTP 任务请求，通过 WebSocket 调度给在线 Worker 执行并返回结果。

纯内存运行，零外部依赖，单二进制部署。

## 性能

真机（iOS 设备，局域网 WiFi，零延迟任务处理）实测，数据反映端到端转发 + 调度开销。成功率 100%，无数据竞争（`go build -race` 压测验证）。

| 部署 | 稳态吞吐 | 峰值吞吐 | 串行 P50 延迟 |
|---|---|---|---|
| 单设备 | ~130 req/s | ~141 req/s | ~8 ms |
| 双设备 | ~243 req/s | ~263 req/s | ~8 ms |

### 双设备

| 并发 | 请求数 | 成功率 | 吞吐量 | P50 | P90 | P99 |
|---|---|---|---|---|---|---|
| 1 | 50 | 100% | 78 req/s | 8.6 ms | 11.9 ms | 104 ms |
| 10 | 100 | 100% | 222 req/s | 45 ms | 51 ms | 58 ms |
| 50 | 200 | 100% | 224 req/s | 214 ms | 246 ms | 249 ms |
| 100 | 300 | 100% | 263 req/s | 361 ms | 394 ms | 401 ms |
| 200 | 400 | 100% | 246 req/s | 793 ms | 819 ms | 880 ms |
| 50（持续） | 500 | 100% | 243 req/s | 205 ms | 231 ms | 241 ms |

### 单设备

| 并发 | 请求数 | 成功率 | 吞吐量 | P50 | P90 | P99 |
|---|---|---|---|---|---|---|
| 1 | 50 | 100% | 106 req/s | 8.1 ms | 9.6 ms | 17 ms |
| 10 | 100 | 100% | 139 req/s | 71 ms | 78 ms | 80 ms |
| 50 | 200 | 100% | 134 req/s | 371 ms | 389 ms | 397 ms |
| 100 | 300 | 100% | 141 req/s | 676 ms | 708 ms | 804 ms |
| 50（持续） | 500 | 100% | 126 req/s | 370 ms | 463 ms | 561 ms |

- 近线性扩展：双设备达到单设备的 ~1.9 倍，调度层开销可忽略，瓶颈在单设备串行执行能力。
- 轮询调度负载均匀（400 任务串行测试在两台设备上 200:200 平分）。
- 故障转移对调用方透明：Worker 执行中断开，任务自动重新调度到存活 Worker（at-least-once 交付）。
- 容量估算：目标吞吐 ÷ ~130 ≈ 所需设备数。

## 快速开始

### 1. 生成 Token

```bash
./scripts/gen-tokens.sh
```

按提示输入需要的 token 数量，UUID 会自动追加到 `configs/config.yaml`。

### 2. 启动

```bash
go run ./cmd/relayrpc-server
```

### 3. 连接 Worker

Worker Simulator:

```bash
go run ./cmd/relayrpc-worker-sim --token <uuid>
```

或使用 iOS Worker tweak（见 `workers/ios/`）。

### 4. 提交任务

提交任务（同步等待结果）：

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"action":"demo","params":{}},"wait_timeout_ms":30000}'
```

成功返回：

```json
{"task_id":"...","success":true,"status":"succeeded","result":{...}}
```

超时未完成返回 HTTP 202：

```json
{"task_id":"...","status":"pending","message":"task is still processing"}
```

## 鉴权

Token 是 UUID，配置在 `configs/config.yaml` 的 `tokens` 列表中。任何有效 token 既能提交任务也能连接 WebSocket 作为 Worker，token 不区分角色。作为 Worker 连接时，token 同时用作该 Worker 的身份标识。

## API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查（无需认证） |
| POST | `/api/v1/tasks` | 提交任务并同步等待结果 |
| GET | `/api/v1/workers/ws` | Worker WebSocket 连接 |

## 调度规则

- 任务按全局递增 seq FIFO 调度
- 每个 Worker 同一时间只执行一个任务
- 多 Worker 并行处理不同任务，按轮询（round-robin）顺序分配，负载均匀
- Worker 失败后进入 cooling，冷却时间随连续失败次数递增：5s → 30s → 2m → 10m → `worker_cooldown`
- 每次下发生成唯一 attempt_id，旧结果被忽略
- 所有 Worker 都失败后任务最终 failed
- Worker 断开时正在执行的任务自动重新调度

## 配置

`configs/config.yaml`:

```yaml
server:
  listen_addr: ":8080"
  shutdown_grace_period: "30s"

scheduler:
  worker_cooldown: "1m"        # 连续失败达上限后的冷却时长
  task_run_timeout: "120s"
  task_ack_timeout: "3s"
  heartbeat_timeout: "30s"
  poll_interval: "50ms"
  default_task_timeout: "120s"
  default_task_deadline: "10m"

tokens:
  - "uuid-1"
  - "uuid-2"
```

## 错误码

| 错误码 | 说明 |
|--------|------|
| ALL_WORKERS_FAILED | 所有 Worker 都尝试失败 |
| TASK_DEADLINE_EXCEEDED | 任务超过截止时间 |
| TASK_CANCELED | 主动取消 |
| ACK_TIMEOUT | Worker 未及时 ACK |
| TASK_RUN_TIMEOUT | Worker 执行超时 |
| WORKER_DISCONNECTED | Worker 断连 |

## Worker Simulator

用于测试，模拟 Worker 接收并执行任务：

```bash
go run ./cmd/relayrpc-worker-sim \
  --token <uuid> \
  --success-rate 1.0 \
  --min-delay 1s \
  --max-delay 3s
```

设置 `--success-rate 0.5` 可模拟失败场景，观察 cooling 和重试行为。

## 社区交流

`RelayRPC` 已经聚集了不少开发者和用户持续交流，目前已建立多个微信交流群。

| 微信交流群（7群开放中） | 公众号 |
|---|---|
| 1群：已满<br>2群：已满<br>3群：已满<br>4群：已满<br>5群：已满<br>6群：已满<br>7群：开放中 | `移动端Android和iOS开发技术分享` |
| <img src="https://raw.githubusercontent.com/witchan/Imgur/main/group6_qr.JPG" alt="微信 6 群二维码" width="260"> | <img src="https://raw.githubusercontent.com/witchan/ios-mcp/refs/heads/main/prefs/Resources/wechat_qr.jpg" alt="移动端Android和iOS开发技术分享 公众号二维码" width="220"> |

> 6群二维码如已过期，请添加微信 `witchan028` 或关注公众号 `移动端Android和iOS开发技术分享` 获取最新入群方式。

欢迎添加微信或关注公众号，获取最新动态与入群方式。

- 微信：`witchan028`
- 邮箱：`witchan028@126.com`

## 许可

本项目自有代码使用 MIT License，详见 [LICENSE](../LICENSE)。

使用、修改、分发或合并本项目自有源码及其重要部分时，应保留版权声明和许可证文本。[NOTICE](../NOTICE) 提供项目出处和免责说明。

本项目按 “AS IS” 方式提供，不提供任何明示或暗示担保。因使用、修改、分发、部署或运行本项目导致的设备异常、数据丢失、服务中断、账号风险、系统损坏、安全问题、商业损失或其他直接/间接影响，作者不承担责任。
