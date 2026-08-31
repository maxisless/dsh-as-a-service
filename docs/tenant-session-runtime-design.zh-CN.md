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
| Agent / App | `agent_id` + 不可变 `agent_version` | 助手身份、系统指令、模型/Skill 策略、检索集合、工具策略、artifact 默认规则 | 租户成员、账单账户、原始凭据 |
| 会话 | 绑定一个 Agent 版本的服务端签发 `session_id` | 模型绑定、对话记忆、DSH runtime session ID、workspace、会话锁 | 其他会话的文件或记忆 |
| 运行 | 服务端签发的 run_id | 一次请求、事件流、输入清单、输出产物、生命周期状态 | 长期对话状态 |
| runtime 租约 | 内部 runtime_id | 一个活跃 DSH Harness 子进程和兼容模型配置 | 租户归属或永久产物 |

控制面从已认证调用方导出 tenant ID 与 principal ID。公开客户端不能自行指定裸 session ID 进而认领历史。飞书映射由服务端导出：私聊映射到用户会话，群聊映射到群会话，话题映射到话题会话。会话还记录 `agent_id` 与 `agent_version`；因此一个租户可以托管多个助手，且不会混合它们的 prompt、工具、知识或 artifact 策略。

~~~mermaid
flowchart TB
    Tenant["租户<br/>身份 · 成员 · 账单 · Vault 命名空间"]
    AgentA["Agent A v17<br/>销售助手<br/>知识：销售<br/>工具：报价"]
    AgentB["Agent B v5<br/>研发助手<br/>知识：代码<br/>工具：代码与部署"]
    SessionA["会话 A<br/>绑定 Agent A v17"]
    SessionB["会话 B<br/>绑定 Agent B v5"]

    Tenant --> AgentA
    Tenant --> AgentB
    AgentA --> SessionA
    AgentB --> SessionB
~~~

## 租户控制面

租户级资源是控制面的一级记录。会话通过引用消费已发布的租户资源；不能把密钥或可变的全局配置复制进对话记忆或 workspace。

~~~mermaid
flowchart TB
    Tenant["租户控制面"]
    Policy["策略与配额<br/>模型 · Skill · 网络 · 保留规则"]
    Config["版本化配置<br/>已发布租户配置版本"]
    Memory["租户记忆与知识<br/>已整理事实 · 文档 · 检索索引"]
    Vault["租户 Vault<br/>仅保存密钥引用"]
    Audit["审计与用量<br/>不可变事件 · 计费维度"]
    Session["会话<br/>私有对话与 workspace"]
    Executor["隔离执行器"]
    Gateway["模型与工具网关"]

    Tenant --> Policy
    Tenant --> Config
    Tenant --> Memory
    Tenant --> Vault
    Tenant --> Audit
    Policy --> Session
    Config --> Session
    Memory -->|仅授权检索| Executor
    Session --> Executor
    Vault -->|短期、带范围 capability| Gateway
    Executor -->|仅 capability 引用| Gateway
    Executor --> Audit
~~~

### 租户记忆与知识

租户记忆是租户拥有的共享知识，不是把所有用户对话拼接在一起。它保存经过整理、可追溯的记录，例如已批准指令、组织事实、项目文档、按策略提升的工具输出和检索索引。每个记忆项都有 owner、来源、访问范围、修订版本、保留策略和 provenance。

~~~text
租户记忆                    会话记忆
────────────────────────    ────────────────────────────────
由授权用户共享              仅属于一个会话
整理过或显式提升            原始多轮对话
有版本且可归因              有上限的滚动上下文
按策略检索                  仅注入自身会话
~~~

一次运行只能获得其有权限使用的检索结果，不能将整个租户记忆库挂载或倾倒到 workspace。把会话或运行产物提升到租户记忆必须是显式、可审计的动作；默认禁止自动提升。

### 配置继承

配置一旦发布即不可变。一条运行需要存储已解析的版本标识，使租户修改模型策略或 Skill 设置后，历史运行仍可复现。

~~~text
平台基线
  → 租户已发布配置版本
    → 会话绑定与少量可审计覆盖
      → 运行已解析配置快照
~~~

租户配置可包含允许的模型别名、默认模型、启用的 Skill 版本、检索集合、出站网络策略、artifact 保留规则、限流、成本预算和媒体默认参数。会话创建时可选择一个被允许的模型，但不能弱化租户策略，也不能在创建后改变绑定模型。运行要记录所使用的精确 policy/configuration/Skill 版本。

### 密钥与凭据管理

密钥归租户 Vault 所有，但会话或执行器永远不能读取原始长期值。持久化的只有密钥引用和用途、provider、轮换状态、允许模型/工具、过期策略等元数据。

~~~text
Tenant Vault
  secret_ref: vault://tenant/t-123/model-provider/primary
       │
       ├─ 控制面校验租户策略和运行意图
       ├─ 模型/工具网关发放短期、带范围 capability
       └─ 执行器仅将 capability 用于已批准请求

绝不暴露到执行器 workspace、会话记忆、prompt、artifact、日志或客户端响应：
原始 API key、refresh token、Vault master key
~~~

控制面同时支持租户自带凭据与平台托管凭据。两种情况下，计费、授权、轮换、撤销和审计都留在控制面/网关边界。执行器只获得完成一次模型或工具请求所必需的最小权限 capability，不获得通用租户凭据。

### 管理生命周期与权限

租户全局资源通过控制面管理，而不是由 Agent 工具直接管理。租户管理员可以创建草稿、上传来源、申请轮换或发布经过审核的版本。执行器只能读取自己被分配运行的已解析配置、授权检索结果和短期 capability；它不能修改租户配置、直接写入租户记忆、枚举密钥或轮换凭据。

~~~mermaid
sequenceDiagram
    participant Admin as 租户管理员
    participant CP as 控制面
    participant V as 租户 Vault
    participant M as 租户记忆服务
    participant S as 会话/运行
    participant E as 隔离执行器

    Admin->>CP: 创建配置草稿或策略变更
    CP->>CP: 校验 schema、配额、模型/Skill 策略
    Admin->>CP: 发布配置版本
    CP-->>S: 会话/运行创建时固定 config_version
    Admin->>V: 写入或轮换密钥值
    V-->>CP: 仅保存 secret_ref 与版本元数据
    Admin->>M: 导入或审批租户知识
    M-->>CP: 发布已索引的记忆修订
    S->>CP: 使用固定版本启动运行
    CP->>E: 已解析配置 + 授权检索 + 带范围 capability
    E-->>CP: 追加审计、用量与 artifact 元数据
~~~

| 全局资源 | 生命周期与管理 | 执行器可见内容 | 不可突破的边界 |
| --- | --- | --- | --- |
| 租户资料与成员 | 控制面 CRUD、角色分配、停用、删除流程 | tenant ID 与有效角色 | 执行器不能创建成员或扩大租户范围 |
| 策略、配额与网络出口 | 草稿 → 校验 → 不可变已发布版本 | 已解析策略快照 | 运行不能放宽限制或编辑策略 |
| 模型与 Skill 配置 | 白名单、默认模型、固定 Skill bundle 版本、媒体默认参数 | 已批准模型/Skill ID 与配置快照 | 运行不能任意指定模型 endpoint 或上传 Skill |
| 租户记忆与知识 | 导入 → 提取/索引 → 审核/批准 → 发布修订 → 保留/删除 | 仅限范围内的检索结果 | 不挂载原始存储；不从聊天历史隐式提升 |
| 密钥引用 | 在 Vault 创建/轮换/撤销；控制面保存版本元数据 | 一次批准调用对应的短期 capability | 原始密钥不进入 prompt、workspace、DSH state、日志、artifact 或 API 响应 |
| 审计与用量 | 仅追加事件与用量账本；按策略保留/导出 | 不允许直接修改 | 运行只能经控制面追加事件 |

一次配置发布必须是原子的：要么生成一个新的不可变租户配置版本，要么完全不改变。创建会话时固定允许模型与有效配置版本；创建运行时还要记录该运行使用的 policy 版本、Skill bundle 版本、记忆集合修订和密钥引用版本。

### 租户管理 API 方向

最终控制面应将管理资源与运行执行分开。代表性操作如下：

~~~text
租户管理
  管理租户资料、成员、角色、策略草稿、配额和已发布配置版本

记忆管理
  导入来源 → 索引 → 审核/发布 → 范围化检索 → 过期/删除
  显式提升（会话/运行 artifact）→ 审核/发布；默认不自动提升

凭据管理
  创建密钥引用 → 在 Vault 写入/轮换值 → 撤销 → 审计访问
  执行路径只签发 capability，永远不返回原始值

执行管理
  安装/固定已批准 Skill bundle → 选择模型白名单 → 查看运行/审计/用量 → 取消或保留 artifact
~~~

具体 REST 或 RPC 路径可以变化，但必须保留上述权限边界。特别是，租户管理员可管理租户内的全局资源；只有聊天权限的主体可创建或继续已授权会话，但不能发布配置、访问密钥，或在没有相应角色时提升共享记忆。

## 持久化控制面与运行状态

在 Worker 横向扩展之前，架构必须先具备持久化控制面。进程内 map、lock 和 queue 是 v1 有用的实现细节，但当存在多个 Worker 时，不能继续充当租户访问、会话串行、运行归属或投递状态的权威来源。

~~~mermaid
flowchart LR
    API["API 与飞书入口"]
    DB["控制面数据库<br/>权威元数据与状态"]
    Q["持久化队列"]
    EX["执行器池"]
    OBJ["对象存储"]
    IDX["知识索引"]
    VAULT["Vault"]

    API --> DB
    API --> Q
    DB --> Q
    Q --> EX
    EX --> DB
    EX --> OBJ
    EX --> IDX
    DB --> VAULT
~~~

| 权威系统 | 权威内容 | 不能单独决定的事情 |
| --- | --- | --- |
| 控制面数据库 | tenant/principal/role、Agent version、session/run 状态、lease、幂等键、artifact ACL/元数据、policy/config 版本、用量账本 | 原始 artifact 字节、原始密钥值、向量相似度排序 |
| 持久化队列 | 待执行运行、延迟重试、异步媒体/索引/清理 job | 授权、最终运行成功、artifact 访问权 |
| 对象存储 | 附件、artifact、导出快照、记忆源文件 | 调用方是否有权读取对象 |
| 知识索引 | 有范围的检索候选和 embedding | 可信策略或权威运行状态 |
| Vault | 原始密钥值与轮换材料 | 会话归属、账单或执行历史 |

每个运行都有持久化状态机和幂等键。最小状态路径如下：

~~~text
CREATED → QUEUED → LEASED → RUNNING → SUCCEEDED
                             ├→ FAILED
                             ├→ CANCELED
                             └→ EXPIRED
~~~

lease 带有 `lease_epoch`、`executor_id`、过期时间和 attempt 编号。执行器只有持有当前 lease epoch 时才能更新运行；过期执行器不能覆盖已重试的运行。所有带副作用的操作使用由 tenant、run 和 action identity 派生的幂等键。外部 task ID、文档/消息 ID 和 artifact 投递 ID 都要在重试前持久化。

~~~mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> QUEUED
    QUEUED --> LEASED
    LEASED --> RUNNING
    RUNNING --> SUCCEEDED
    RUNNING --> FAILED
    RUNNING --> CANCELED
    LEASED --> QUEUED: 启动前 lease 过期
    RUNNING --> QUEUED: 可重试失败 / 执行器丢失
    QUEUED --> EXPIRED: 队列 TTL 到期
~~~

## 上下文信任边界

授权检索并不等于文本可信。即使某段内容存储在租户授权集合中，prompt builder 也必须区分可信平台控制与不可信数据。

~~~text
可信控制指令
  平台策略 → 已发布 Agent 版本 → 已解析运行策略

不可信数据块
  授权租户检索 → 用户消息/附件 → 网页内容 → 工具输出

模型只能把数据块当作参考材料，不能把其当作修改策略、调用隐藏工具、泄露凭据
或覆盖指令的授权。
~~~

每个检索 chunk 和外部附件都带 source、collection、revision、content type、trust class 与 access decision。文档未授权、过期、恶意或不属于 Agent 允许集合时，检索可以返回空结果。工具输出也遵循相同的不可信数据边界。

## Artifact、事件与成本生命周期

Artifact、事件流和成本需要各自独立的持久化契约：

| 关注点 | 必需契约 |
| --- | --- |
| Artifact ACL | artifact 归属于 tenant + Agent + session + run；下载检查所有适用范围规则 |
| Artifact 安全 | 校验大小/类型，扫描适用的上传/输出，加密静态存储，且不内联执行主动内容 |
| 下载 | 签名 URL 有短 TTL；支持时绑定 audience；支持撤销检查并写审计事件 |
| 保留/删除 | 租户策略级联 workspace、DSH state、artifact、记忆源/索引、Vault references 与备份；已删除租户不能在恢复时重新出现 |
| SSE 恢复 | 每个事件有单调递增 `event_id`；Last-Event-ID 从持久化事件日志恢复；未知/过期 cursor 返回明确 resync 状态 |
| 取消 | 客户端取消写入持久化 cancel intent；执行器/工具/媒体 job 确认取消或报告不可取消的外部工作 |
| 预算 | dispatch 前预留估算成本，异步记录实际用量，对账有延迟的 provider 用量；策略预算耗尽后阻止新工作 |
| 异步媒体 | 使用幂等键只提交一次 task，持久化 provider task ID，只轮询/投递一次，并与交互 token 用量分开计费 |

运行进入终态本身不代表业务完成：artifact 登记与面向用户的投递都要各自拥有幂等完成记录。Worker 在 provider 成功但投递前崩溃时，应恢复投递，而不是重新生成 artifact。

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

- 在控制面增加 tenant、principal、Agent/App、不可变 Agent version、session、run、lease、幂等键与 artifact 记录。
- 将控制面数据库、持久化队列、对象存储、知识索引和 Vault references 建立为独立的权威系统。
- 新 API 之后先继续复用当前 Worker 作为可信单租户执行器。
- 从飞书 sender、chat 与 thread 元数据导出飞书会话归属。

### 阶段 2：分离状态与产物

- 将 conversation 和 DSH persistence 从进程全局根目录移动到 tenant/Agent/session 根目录。
- 将飞书入站媒体从共享 inbound 目录移到 workspace/inbox/<run-id>/。
- 将输出文件登记为 artifact；保留现有异步媒体投递 worker，但让它从 artifact API 获取结果。
- 构建可信上下文组装器以及显式的租户记忆提升/审核路径。

### 阶段 3：增加调度与配额

- 将即时 429 busy 改成持久化运行状态、lease、fencing、幂等 action 记录和 SSE status 事件。
- 落地会话串行、租户活跃运行上限、全局容量、队列 TTL、取消、成本预留与 artifact 保留规则。
- 增加 Last-Event-ID 恢复和恰好一次 artifact 投递记录。

### 阶段 4：隔离执行 Worker

- 每份会话租约在容器或 pod 中执行，只挂载该会话 workspace 和 DSH state。
- 增加执行器池容量和空闲回收。只有验证上游 session-scoped workspace 契约后，才升级为跨会话共享的 per-model DSH runtime pool。
- 通过带凭据感知的 Model/Tool Gateway 调用模型和工具，签发带范围 capability，而非下发原始租户密钥。

### 阶段 5：加固与运营

- 增加审计日志保留、按租户可观测性、用量计费、重试、取消、清理、备份和灾备演练。
- 高风险 Skill 或不可信工作负载按需进入更强的 sandbox 类型。

## 验收标准

最终架构完成时，应可以证明：

1. 两个并发会话无法列出、读取、修改或返回彼此的附件与产物。
2. 即使知道标识符，两个租户也不能解析彼此的会话、运行或 artifact。
3. 同一租户的两个 Agent 不会混合各自已发布的 prompt、Skill、检索集合或 artifact 策略。
4. 多个会话可通过有界执行器/runtime 池并发执行，不发生跨会话 SSE 事件、记忆泄漏或 workspace 可见性。
5. Worker 重启后可恢复持久化队列、lease 和 artifact 投递状态，不重复产生用户可见结果或外部提交。
6. 检索文档、附件、网页和工具输出保持不可信数据块，不能覆盖已发布控制指令。
7. 无法从执行器 workspace、会话状态、prompt、artifact、日志或公开 API 响应恢复原始租户凭据。
8. 可按 tenant、Agent、model、session 与 run 观测容量、公平性、预留/实际成本、时延、取消和失败。
