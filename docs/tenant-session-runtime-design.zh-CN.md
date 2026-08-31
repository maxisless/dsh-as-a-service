# 租户、会话与运行时设计

[English version](tenant-session-runtime-design.md)

> **状态：目标架构。** 本文描述托管、多租户平台的最终形态；它不表示当前 v1 Worker 已经具备这些边界。运行时复用的具体机制须以验证上游 DSH 是否支持会话级 workspace 为准。

## 一句话决策

最终平台将 **租户** 作为治理与资源归属边界，将 **会话** 作为对话、文件与记忆边界，将 **Harness/runtime** 作为可复用的执行资源。一个租户不拥有一个永久 Harness；一个会话也不需要一个永久 runtime 进程。

~~~text
租户
  拥有：身份策略、额度、允许的模型和 Skill、租户存储命名空间
  │
  ├── 会话 A + 模型 fast
  │     拥有：对话、DSH runtime session、workspace、产物
  │     使用：模型池中兼容的 runtime 执行容量
  │
  ├── 会话 B + 模型 fast
  │     拥有：对话、DSH runtime session、workspace、产物
  │     使用：模型池中兼容的 runtime 执行容量
  │
  └── 会话 C + 模型 pro
        拥有：对话、DSH runtime session、workspace、产物
        使用：pro 模型池中兼容的 runtime 执行容量
~~~

这个设计保留轻量、可复用的 DSH runtime session，同时消除不同会话共用 workspace 的边界缺口。

~~~mermaid
flowchart TB
    T["租户<br/>策略 · 配额 · 模型与 Skill 白名单"]
    P["主体<br/>已认证调用方"]
    S1["会话 A<br/>对话 · workspace · 产物"]
    S2["会话 B<br/>对话 · workspace · 产物"]
    R1["运行 A-1"]
    R2["运行 B-1"]
    X["隔离的执行器租约"]
    H["兼容的 DSH Harness / runtime 容量"]

    T --> P
    T --> S1
    T --> S2
    P -->|授权访问| S1
    P -->|授权访问| S2
    S1 --> R1
    S2 --> R2
    R1 --> X
    R2 --> X
    X -. 兼容模型容量 .-> H
~~~

**读图方式：** 租户负责访问与额度；会话拥有状态和文件；运行是一次请求；执行器租约提供隔离执行。只有先建立执行器边界，runtime 容量才可以安全复用。

## 为什么这是最终方向

一个 DSH Harness 对应一个长期存活的 JSON-RPC runtime 子进程。SDK 可以通过该进程发送多个 session prompt：JSON-RPC 响应由 request ID 分流，流式通知由 DSH session ID 过滤。因此，runtime session 比 runtime 子进程轻量得多，并且可以并发。

如果每个租户固定一个永久 Harness，会为空闲租户浪费进程；如果每个会话固定一个永久 Harness，浪费会进一步放大。真正必须隔离的是会话的文件、记忆、授权和产物，而不是一定要为每个会话永久独占模型 runtime 进程。

## 当前状态与缺口

v1 Worker 已具备：同一 public session 的 turn 串行、按 session 持久化对话记忆、会话模型绑定、不同会话并发。当前仍是每个模型别名一个 Harness，而 Harness 固定使用一个共享 workspace。

~~~text
当前 Worker
  模型别名
    → 共享 Harness / JSON-RPC runtime
       → DSH session A
       → DSH session B
       → DSH session C
    → 共享 /data/workspace                 ← 缺口
~~~

- server.py:271 在创建 Harness 时固定其工作目录。
- server.py:308 让同一会话请求串行；server.py:265 限制不同会话的全局活跃 turn 数。
- server.py:667 为每一组模型别名与 public session ID 创建独立 DSH runtime session ID。

共享 workspace 意味着两个独立会话仍能读取或覆盖同一个文件。workspace-write 只能阻止写出 workspace，不能区分 workspace 内的不同会话。

~~~mermaid
flowchart LR
    subgraph Current["当前 v1 Worker"]
      A1["会话 A"] --> W1["/data/workspace"]
      B1["会话 B"] --> W1
      W1 --> F["report.md · inbound/ · artifacts"]
    end
    subgraph Target["目标执行边界"]
      A2["会话 A"] --> WA["租户 A / 会话 A / workspace"]
      B2["会话 B"] --> WB["租户 A / 会话 B / workspace"]
      WA --> FA["inbox/run A · artifacts/run A"]
      WB --> FB["inbox/run B · artifacts/run B"]
    end
~~~

## 最终归属模型

| 层级 | 稳定标识 | 拥有的内容 | 不拥有的内容 |
| --- | --- | --- | --- |
| 租户 | tenant_id | 策略、计费/配额、模型与 Skill 白名单、存储命名空间、审计保留规则 | 单一永久 Harness 或共享对话 |
| 主体 | principal_id | 调用方身份及租户角色 | 任意租户或会话的访问权 |
| 会话 | 服务端签发的 session_id | 模型绑定、对话记忆、DSH runtime session ID、workspace、会话锁 | 其他会话的文件或记忆 |
| 运行 | 服务端签发的 run_id | 一次请求、事件流、输入清单、输出产物、生命周期状态 | 长期对话状态 |
| runtime 租约 | 内部 runtime_id | 一个活跃 DSH Harness 子进程和兼容模型配置 | 租户归属或永久产物 |

控制面从已认证调用方导出 tenant ID 与 principal ID。公开客户端不能自行指定裸 session ID 进而认领历史。飞书映射由服务端导出：私聊映射到用户会话，群聊映射到群会话，话题映射到话题会话。

## 存储布局

每个会话在租户命名空间下拥有独立根目录。目录名使用服务端生成的不透明 ID 或 hash；不能把用户输入的原始字符串直接作为路径。

~~~text
tenants/<tenant-id>/
  sessions/<session-id>/
    conversation.json                 # 模型绑定和有上限的对话记忆
    dsh-state/                        # 本会话的 DSH JSONL / checkpoint
    workspace/                        # Agent 唯一可写的项目目录
      inbox/<run-id>/                 # 本次运行可信输入附件
      artifacts/<run-id>/             # 本次运行输出文件
      scratch/                        # 会话级临时工作区
  audit/<run-id>.jsonl                # 不可变运行事件与用量引用
~~~

附件和生成文件都注册为 artifact。响应返回 artifact ID 或签名下载 URL，不能返回任意共享 workspace 路径。保留和删除策略归租户所有。

~~~mermaid
flowchart TD
    Tenant["tenant_id"]
    Tenant --> Session["session_id"]
    Session --> Memory["conversation.json"]
    Session --> DSH["DSH 状态与 checkpoint"]
    Session --> Workspace["workspace"]
    Workspace --> Inbox["inbox/run_id"]
    Workspace --> Output["artifacts/run_id"]
    Output --> Registry["artifact 注册表"]
    Registry --> Client["授权下载或消息投递"]
~~~

## 执行模型

控制面只有在解析出租户、主体、会话和已绑定模型之后才调度一次运行。

~~~text
1. 客户端 → 控制面：已认证消息或飞书事件
2. 控制面 → 会话存储：授权主体，并解析/创建会话
3. 控制面 → 运行存储：创建 run_id，持久化 queued 状态
4. 调度器：
     同会话正在执行？       排到该会话后面
     已达租户活跃上限？     排到租户队列
     已达全局活跃上限？     排到全局队列
     其他情况              租用兼容执行容量
5. 隔离执行器：
     仅挂载本会话 workspace 与 dsh-state
     使用绑定模型和 DSH runtime session ID 执行
6. 执行器 → Artifact 存储：登记输出
7. 执行器 → 事件流：状态、工具事件、增量文本、完成/失败
8. 控制面 → 客户端：最终结果和 artifact 引用
~~~

同一会话始终串行；不同会话可在租户与全局配额内并发。长时间的视频/音频生成在提交后成为异步 artifact job，等待供应商渲染期间不占用交互式 DSH runtime 租约。

~~~mermaid
sequenceDiagram
    participant C as 客户端或飞书
    participant CP as 控制面
    participant Q as 调度器与队列
    participant E as 隔离执行器
    participant D as DSH runtime
    participant A as Artifact 存储

    C->>CP: 已认证消息与附件
    CP->>CP: 授权租户与主体
    CP->>Q: 创建 run_id 并入队
    Q-->>C: SSE 返回 queued 或 running
    Q->>E: 租用会话 workspace 与 DSH state
    E->>D: 提示绑定的 runtime session
    D-->>E: 增量文本与工具事件
    E-->>C: 流式状态与增量文本
    E->>A: 登记生成文件
    E-->>C: done + artifact 引用
~~~

## Runtime 池与执行隔离

runtime 池按兼容模型配置组织，而不是按租户组织。

~~~text
池键 = 模型别名 + runtime 镜像/配置版本 + 工具策略版本

deepseek-v4-flash 池
  ├── runtime 1：会话 A、B、D 在活跃期使用
  └── runtime 2：容量需要时供会话 C、E 使用

seed-2.1-pro 池
  └── runtime 3：只供兼容会话使用
~~~

runtime 租约是短期可复用资源。只有当 runtime API 能安全复用多个 DSH session ID，**且**执行器阻止这些会话之间看到彼此文件时，一个 runtime 才能承载多个会话。

当前 SDK 在创建 Harness 时固定 cwd、DSH_CWD 和 DSH_SESSION_ROOT；公开 run API 只接收 session ID 与输入。不能通过运行中动态修改这些进程级变量来模拟会话 workspace，因为并发 turn 会产生竞态。

最终不变量是 **会话隔离执行**，不是必须共享 runtime。存在两条合法的最终实现路径：

| 机制 | 适用前提 | runtime 复用 |
| --- | --- | --- |
| 会话感知 DSH runtime pool | 上游 DSH 支持 session 级 workspace 与 state root，并能并发复用 session | 兼容 runtime 可服务多个会话 |
| 隔离会话执行器池 | DSH 继续使用进程级 cwd 或其他进程级工具状态 | 容量可复用，但每个活跃会话获得独立进程/容器租约 |

第二条路径对当前 SDK 是安全的。它不等于一会话永久一个 Harness：进程/容器只在会话活跃期间创建或租用，空闲到期后停止并回收。只有验证上游存在会话 workspace 契约后，第一条路径才更优。

~~~mermaid
flowchart LR
    S["会话隔离执行<br/>不可妥协的不变量"]
    S --> U{"上游 DSH 是否支持<br/>会话级 workspace 和 state？"}
    U -->|是，已验证| P["会话感知 runtime pool<br/>共享兼容 runtime 容量"]
    U -->|否或不确定| I["隔离会话执行器池<br/>每个活跃会话一份进程/容器租约"]
    P --> G["同样满足租户/会话/运行安全保证"]
    I --> G
~~~

## 隔离方案对照

| 方案 | runtime 复用 | 文件隔离 | 使用范围 |
| --- | --- | --- | --- |
| 一个共享 Worker workspace | 高 | 会话之间无隔离 | 当前可信单域模式 |
| 一个会话一个 Harness、同一容器 | 低 | 除非增加 mount/用户，否则仅逻辑隔离 | 不建议作为默认方案 |
| 会话感知 DSH runtime pool | 高 | 上游支持会话级 workspace root 时可强隔离 | 验证能力后优先 |
| 隔离会话执行器池 | 容量级复用 | 强文件隔离 | 当前 SDK 下推荐 |
| 每运行一个 MicroVM / sandboxed pod | 较低 | 最强 | 高风险工具或不可信租户 |

推荐的执行单元是短生命周期容器或 sandboxed worker，只挂载：

~~~text
/workspace  → tenants/<tenant>/sessions/<session>/workspace
/state      → tenants/<tenant>/sessions/<session>/dsh-state
/skills     → 只读的已批准 Skill bundle
~~~

它不能拿到 Docker socket、其他租户根目录、长期模型密钥或不受限网络出口。Model Gateway 可针对所选模型发放短期、带策略限制的凭据。

## 并发与公平性

当前部署的全局活跃 turn 上限是 10。最终保留全局上限，但把立即拒绝替换为有界调度队列。

| 范围 | 规则 | 初始策略 |
| --- | --- | --- |
| 会话 | 最多一个活跃运行 | 后续消息按顺序排队 |
| 租户 | 有界活跃运行数 | 先从 2 开始，按套餐配置 |
| 全局 | 有界活跃运行数 | 当前容量：10 |
| 队列 | 有界等待运行数与 TTL | 返回 queued + run ID，满了才拒绝 |
| 媒体渲染 | 提交后异步 job | 不占用交互式 slot |

这样可防止一个高频租户占满全部容量，并让客户端通过 SSE 观察排队，而不是遇到 429 后自行重试。

~~~mermaid
flowchart TB
    In["进入的 run"]
    In --> Same{"同一会话正在执行？"}
    Same -->|是| SQ["会话 FIFO 队列"]
    Same -->|否| Tenant{"达到租户活跃运行上限？"}
    Tenant -->|是| TQ["租户队列"]
    Tenant -->|否| Global{"全局有可用容量？"}
    Global -->|是| Lease["租用执行器容量"]
    Global -->|否| GQ["全局有界队列"]
    SQ --> Global
    TQ --> Global
    GQ --> Lease
~~~

## 对外 API 方向

v1 接口继续服务于可信本地兼容场景。托管 API 应使用服务端拥有的资源：

~~~text
POST /v1/sessions
  → 201 { session_id, model, created_at }

POST /v1/sessions/{session_id}/runs
  → 202 { run_id, status: queued or running }

GET /v1/runs/{run_id}/events
  → SSE: queued, status, assistant.delta, tool.call, tool.result, artifact, done, error

GET /v1/artifacts/{artifact_id}
  → 授权下载，或跳转到短期对象存储 URL
~~~

控制面从凭据中导出租户和主体。它拒绝访问属于其他租户或主体的会话。v1 的 chat / chat-stream 接口在此边界存在前应保持仅 loopback 可见。

## 不变量

1. 一个运行不能通过执行挂载读取或写入其他会话的 workspace。
2. 调用方不能绕过 tenant/principal 绑定访问会话。
3. 会话的模型绑定不可变；选择另一模型必须创建新会话。
4. 一个会话最多一个活跃运行；其记忆和 DSH 事件顺序确定。
5. 所有生成文件在返回给调用方前都登记为 artifact。
6. runtime 复用不能削弱文件、凭据或事件流边界。
7. 执行器失败或重启后，可从 run store 恢复 queued/running 状态，不重复投递已经完成的 artifact。

## 迁移计划

### 阶段 1：建立服务端身份

- 在控制面增加 tenant、principal、session、run 与 artifact 记录。
- 新 API 之后先继续复用当前 Worker 作为可信单租户执行器。
- 从飞书 sender、chat 与 thread 元数据导出飞书会话归属。

### 阶段 2：分离状态与产物

- 将 conversation 和 DSH persistence 从进程全局根目录移动到 tenant/session 根目录。
- 将飞书入站媒体从共享 inbound 目录移到 workspace/inbox/<run-id>/。
- 将输出文件登记为 artifact；保留现有异步媒体投递 worker，但让它从 artifact API 获取结果。

### 阶段 3：增加调度与配额

- 将即时 429 busy 改成持久化 queue 状态和 SSE status 事件。
- 落地会话串行、租户活跃运行上限、全局容量、超时与 artifact 保留规则。

### 阶段 4：隔离执行 Worker

- 每份会话租约在容器或 pod 中执行，只挂载该会话 workspace 和 DSH state。
- 增加执行器池容量和空闲回收。只有验证上游 session-scoped workspace 契约后，才升级为跨会话共享的 per-model DSH runtime pool。
- 通过带凭据感知的 Model Gateway 调用模型。

### 阶段 5：加固与运营

- 增加审计日志保留、按租户可观测性、用量计费、重试、取消、清理、备份和灾备演练。
- 高风险 Skill 或不可信工作负载按需进入更强的 sandbox 类型。

## 验收标准

最终架构完成时，应可以证明：

1. 两个并发会话无法列出、读取、修改或返回彼此的附件与产物。
2. 即使知道标识符，两个租户也不能解析彼此的会话、运行或 artifact。
3. 多个会话可通过有界执行池并发执行，不发生跨会话 SSE 事件或记忆泄漏。
4. Worker 重启后可恢复持久化队列和 artifact 投递状态，不重复产生用户可见结果。
5. 可按 tenant、model、session 与 run 观测容量、公平性、成本、时延和失败。
