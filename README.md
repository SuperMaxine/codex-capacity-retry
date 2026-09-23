# codex-capacity-retry

Windows 上针对 Codex 桌面版的 **capacity 限流自动重试工具**。

当本地桌面任务因 `Selected model is at capacity. Please try a different model.` 失败时，
本工具自动发现失败任务、等待可配置的时间，然后通过桌面 App **自己的后台**发起一次
**空输入新轮次**（`input: []`）恢复任务——语义接近原生 Retry 按钮，但**不插入可见的
“继续”消息、不切换模型、不另启 Codex 后台、不控制窗口 UI**。

> 状态：**实验性**。它依赖 Codex 桌面版的内部本地 IPC 协议（在 `26.915.4065.0` 上实测验证），
> 不是 OpenAI 公布的稳定 API。桌面 App 升级后可能需要适配。

---

## 目录

- [原理说明](#原理说明)
  - [背景](#背景)
  - [整体流程](#整体流程)
  - [三步详解](#三步详解)
  - [关键设计](#关键设计)
  - [IPC 协议要点](#ipc-协议要点)
- [使用指南](#使用指南)
  - [环境要求](#环境要求)
  - [快速开始](#快速开始)
  - [参数说明](#参数说明)
  - [只读查看任务状态](#只读查看任务状态)
  - [运行测试](#运行测试)
- [项目结构](#项目结构)
- [已知限制](#已知限制)

---

## 原理说明

### 背景

Codex 桌面任务遇到模型容量不足时会把轮次置为 `failed`、任务状态置为
`systemError`，任务就此停住。原生解决方式是手动点 Retry，但多任务、长时间运行时
手动操作不现实。本工具的目标是：**在任务仍停留在同一个 capacity 失败轮次上时，
替用户完成一次等效于 Retry 的空输入重发**。

### 整体流程

```text
┌─────────────────────────────────────────────────────────────────────┐
│  codex_native_retry.py（Python，标准库，调度器）                     │
│                                                                     │
│  1. 发现候选        2. 实时核实              3. 计时 + 发重试       │
│  ~~~~~~~~~~~~~~~~~  ~~~~~~~~~~~~~~~~~~        ~~~~~~~~~~~~~~~~~~~~   │
│  只读打开 Codex     通过 Node IPC 适配器      每个失败轮次独立计时；  │
│  本地数据库，列出   连接 \\.\pipe\codex-ipc，  到期后经桌面 owner    │
│  未归档桌面主任务； 找到任务 owner，拉取     发 thread-follower-    │
│  读各任务 rollout   v11 实时状态快照，严格   start-turn（空输入）    │
│  日志尾部 1 MiB，   校验仍是同一 capacity    由桌面 App 自己的后台   │
│  找结构化 capacity  失败轮次                  执行 turn/start       │
│  失败终止事件                                                        │
└─────────────────────────────────────────────────────────────────────┘
              两个独立信息源交叉验证，全部通过才允许发送
```

### 三步详解

**1. 发现候选（静态，只读）**

- 以只读模式（`?mode=ro` + `PRAGMA query_only`）打开 `~/.codex/state_5.sqlite`，
  列出未归档的桌面主任务（`originator = 'Codex Desktop'`，排除子 Agent）。
  全程不修改 Codex 的任何文件。
- 读取每个任务 rollout 日志的**最后 1 MiB**，只把结构化的
  `event_msg / task_complete` 事件中的 capacity 错误当作候选。
  聊天文本里引用的报错、工具输出中的报错**一律不算**。
- 日志末行未写完、格式无法识别时**失败关闭**（不重试），等下一次扫描。

**2. 实时核实（动态，权威）**

- 启动随附的 Node 适配器（`codex_native_pipe.mjs`）连接 Windows 命名管道
  `\\.\pipe\codex-ipc`——这是 Codex 桌面 App 自带的内部本地 IPC 路由。
- 适配器以独立客户端身份注册（`clientType: "capacity-retry-supervisor"`），
  对桌面的所有权发现询问一律回答 `canHandle: false`：**不认领任何任务、
  不冒充其他客户端**。
- 通过 `thread-owner-discovery` 找到任务的实时桌面 owner；**没有实时 owner
  的任务（App 未加载）直接跳过**。
- 订阅 `thread-stream-following-changed` 拿到 v11 实时快照，只有同时满足以下
  条件才算合格：任务为 `systemError` 且无活动标志、无待审批/待输入请求、
  无未确认提交、最新轮次就是候选的失败轮次、错误仍是精确的 capacity 文本。

**3. 计时并发重试**

- 每个 `(任务 ID, 失败轮次 ID)` 独立计时：首次观察到合格失败后至少等待
  `--retry-delay` 秒（默认 60）；到期后**再拉一次新快照复查**，才发送。
- 发送的请求走内部 `thread-follower-start-turn` 路径，载荷固定为：

  ```json
  {
    "conversationId": "任务ID",
    "turnStart": {
      "request": {
        "threadId": "任务ID",
        "input": [],
        "turnTrigger": "capacity_retry_automatic"
      }
    }
  }
  ```

  不携带模型/推理强度/权限/上下文覆盖参数——由桌面 App 自己的管理器继承
  原有配置执行 `turn/start`。
- 重试后如果再次 capacity，那是一个**新的失败轮次**，重新计时、继续循环；
  如果成功，任务恢复运行，工具不再干预。

### 关键设计

| 机制 | 说明 |
| --- | --- |
| 双源交叉验证 | 静态日志只产生“候选”，动态 IPC 快照才是“权威”；两者一致才发送 |
| 失败关闭 | 末行残缺、未知消息版本、未知历史结构、快照超时 → 一律不发送 |
| 去重日志 | 本工具的 SQLite（只存任务 ID、失败轮次 ID、时间、发送状态，**不存聊天正文**）。发送前持久化占位；请求超时/断连/结果不明 → 标记 `unknown_do_not_resend`，重启后也不盲目重发 |
| 全局限速 | 所有任务合计每分钟最多 `--max-per-minute` 次（默认 6） |
| 单实例 | 操作系统文件锁（进程退出自动释放），同一状态目录只允许一个监控进程 |
| 默认 dry-run | 不传 `--execute` 只预览、零发送 |
| 不碰敏感面 | 不读登录令牌、不开调试端口、不控制窗口、不转发管道到网络 |

### IPC 协议要点

（实验性、未公开，以下均来自本机只读检查 + 实测验证。）

- 端点：`\\.\pipe\codex-ipc`（Windows 命名管道）
- 帧格式：4 字节小端长度前缀 + JSON 帧
- 关键消息：
  - `initialize`（`clientType`）→ 分配 `clientId`
  - `thread-owner-discovery`（`hostId=local`, `conversationId`）→ 返回持有任务的桌面 owner
  - `thread-stream-following-changed`（广播，跟随/取消跟随）
  - `thread-stream-state-changed`（广播，`change.type = "snapshot"`，`version = 11`）
  - `thread-follower-start-turn`（`version = 2`，指定 `targetClientId` 为 owner）
- 任何一项与 `26.915.4065.0` 实测值不符，程序拒绝工作并报错，不做猜测。

---

## 使用指南

### 环境要求

- Windows，Codex 桌面 App **保持运行**（任务需在 App 中已加载、有实时 owner）
- Python 3.11+（本工具只使用标准库，**无需安装任何 pip 包**）
- Node.js 20+（运行随附的 IPC 适配器）

### 快速开始

```powershell
# 1. 只检测，不重试（一次性扫描）
.\start-retry.ps1 -Once

# 2. 持续预览（dry-run，仍不发送）
.\start-retry.ps1

# 3. 启用真实重试：每个失败任务每次至少等待 60 秒后发空输入重试
.\start-retry.ps1 -RetryDelay 60 -Execute
```

`Ctrl+C` 停止监控；已经恢复运行的 Codex 任务会继续执行。

等价 Python 命令（无需 PowerShell 封装）：

```powershell
python codex_native_retry.py --retry-delay 60 --execute
```

只处理指定任务（可重复 `--thread`）：

```powershell
.\start-retry.ps1 -RetryDelay 90 -ThreadId '任务ID一','任务ID二' -Execute
```

### 参数说明

| PowerShell 参数 | Python 参数 | 含义 |
| --- | --- | --- |
| `-RetryDelay 60` | `--retry-delay` | 每次新失败被确认后至少等待的秒数，默认 60，最小 1 |
| `-PollInterval 5` | `--poll-interval` | 扫描间隔秒数，默认 5 |
| `-MaxPerMinute 6` | `--max-per-minute` | 所有任务合计每分钟最多发送次数，默认 6 |
| `-ThreadId 'ID'` | `--thread` | 只处理指定本机任务（可重复） |
| `-Execute` | `--execute` | 允许真正发送重试；不传则只预览 |
| `-Once` | `--once` | 只扫描一次就退出 |
| `-RunFor 300` | `--run-for` | 约 300 秒后自动退出 |

Python 入口另有：`--exclude-thread ID`（排除任务；从 Codex 自身启动时会自动
排除启动它的当前任务）、`--database PATH`（覆盖 Codex 数据库路径）、
`--state-dir PATH`（覆盖本工具状态目录）、`--node PATH`（指定 Node 可执行文件）。

等待时间是**最小值**，实际发送可能因扫描间隔、全局限速、IPC 延迟稍晚。
多个任务按历史更新时间从旧到新检查，避免反复失败的新任务抢占老任务。

### 只读查看任务状态

```powershell
python codex_native_retry.py --status 任务ID
```

打印任务的实时快照（状态、最新轮次、错误、模型等），不发送任何东西。

### 运行测试

```powershell
python -m unittest discover -s tests -v
node --test tests\test_native_pipe.mjs
```

覆盖：独立计时、可调延迟、连续失败重新计时、只认结构化错误、状态变更/归档跳过、
子 Agent 排除、预览零发送、全局限速、跨重启去重、结果不明不重发、单实例、
空输入载荷、IPC 分帧/owner 路由、发送前实时复查。

---

## 项目结构

```text
.
├── codex_native_retry.py    # 主程序：候选发现 + 调度 + 去重日志（Python 标准库）
├── codex_native_pipe.mjs    # Node IPC 适配器：连接 \\.\pipe\codex-ipc（无第三方依赖）
├── start-retry.ps1          # PowerShell 入口（参数透传给主程序）
└── tests/
    ├── test_native_retry.py # 主程序单元测试
    └── test_native_pipe.mjs # IPC 适配器单元测试（node:test）
```

本工具自身的状态（`%LOCALAPPDATA%\CodexRetrySupervisor\`）：

- `native-attempts.sqlite3`：发送记录（仅元数据）
- `native-watcher.lock`：单实例锁（进程退出后 OS 自动释放，文件残留无害）

---

## 已知限制

1. **实验性内部协议**：不是 OpenAI 公布的稳定 Retry API。桌面升级（管道消息、
   快照版本 11、历史结构、数据库 schema）可能需要适配；未知格式会拒绝工作。
2. **只处理本机任务**：不处理 SSH 远程主机或云端任务；仅重试在 App 中有实时
   owner 的任务，不主动加载已卸载的历史任务。
3. **不解决根因**：不切换模型。如果模型持续饱和，工具会按“等待 → 重试 →
   再失败 → 再等待”循环，成功率取决于服务容量恢复。持续大量 capacity 时建议
   等待时间设为 60–300 秒，或改用其他模型。
4. **微小竞态窗口**：内部协议没有“仅当失败轮次仍为 X 才启动”的原子操作，
   最后一次检查与发送之间存在极短窗口。监控开启时避免手动点同一任务的 Retry。
5. **`retry_submitted` 不等于恢复成功**：只表示请求已返回。可用 `--status`
   查看后续状态；`unknown_do_not_resend` 状态的任务需要人工检查，不要删库强迫重发。
6. **Windows 专用**：管道名与文件锁均为 Windows 实现。
7. **不处理**额度耗尽、认证错误、网络错误、手动停止、等待审批、等待用户输入
   等其他失败类型。

## 免责声明

本仓库是对已安装 Codex 桌面 App 内部接口的实验性适配，仅用于恢复自己的本机任务。
IPC 协议细节来自本机只读检查与有限实测，不构成官方兼容承诺；桌面 App 更新后请
先重新运行只读测试。不要把本地 IPC 管道转发为任何网络服务。
