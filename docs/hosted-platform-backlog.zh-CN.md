# 托管平台待办清单

[English version](hosted-platform-backlog.md) · [目标架构](tenant-session-runtime-design.zh-CN.md)

> 本文记录从当前单机、可信部署走向托管多租户平台仍需完成的工作。
> 它是 backlog，不代表下列能力已经上线。优先级按"若不做，会不会导致
> 跨租户越权、重复扣费/投递、数据丢失，或无法安全扩容"排序。

## 当前已具备的基础

- 控制面已持久化 tenant、principal、Agent version、session、run、lease、
  输入 manifest、action、artifact、delivery、usage 与 audit 元数据。
- 飞书会话会映射到服务端 session；附件会进入 `inbox/<run-id>/`，并以
  hash 和 manifest 固化后才允许执行。
- 文本 delivery 使用稳定幂等标识重放；媒体 delivery 失败会保留 `RETRY`
  状态，不会触发重新生成。
- 私有媒体 runner 只继承声明过的环境变量和当前 session workspace。

这些能力仍运行在单节点 SQLite、单容器 Worker 与服务端托管凭据之上；
它们是下一阶段的地基，而不是最终隔离保证。

## P0：在扩大真实用户范围前完成

### [ ] 媒体 delivery 的完整重放

**现在的缺口：** 文本消息可以按 delivery ID 重发；图片、音频、视频和
文件需要卡片 ID、上传阶段、附件 message ID 等上下文。失败后目前保留为
`RETRY`，但还不能自动安全重放。

**要做：** 为每个媒体 delivery 持久化阶段状态：下载 artifact、上传飞书
资源、更新卡片/回复消息、确认 delivery。每步使用稳定 idempotency key。

**完成条件：** 在"飞书已接收上传、服务尚未写回状态"时重启服务，最终用户
只看到一份媒体，不重新生成，也不会丢失 delivery。

### [ ] Provider Job 与 Run Action 统一

**现在的缺口：** Seedance 异步任务仍有私有 SQLite job 表。控制面的
`run_actions` 能记录 tool call 和部分 task ID，但不是所有媒体 provider job
的唯一权威记录。

**要做：** 把 `provider_job` 作为 control-plane action 的持久化扩展：
provider、外部 task ID、轮询游标、可取消性、重试策略、最终 artifact 与
delivery ID。

**完成条件：** Worker 或桥接在任意阶段重启后，只会继续同一个 provider
task 的轮询/投递，不会再次提交收费任务。

### [ ] 公网身份、租户成员与撤销

**现在的缺口：** 当前 bearer token 映射适合开发和受信任入口；飞书身份也
是服务端 hash principal，不是完整的租户成员系统。

**要做：** 接入生产身份提供方；维护 tenant membership、角色、会话授权、
停用和撤销时间。外部会话绑定需要处理群成员变化、用户迁移与 thread 生命周期。

**完成条件：** 用户被移出租户、token 被撤销或群权限改变后，不能继续读取
旧 session、run、artifact 或触发新任务。

### [ ] Tenant 策略、配额和成本预留

**现在的缺口：** 已记录部分 usage/cost 元数据，但没有在 dispatch 前预留
预算，也没有 tenant 并发/速率/媒体额度的硬执行。

**要做：** 发布不可变 tenant policy version，包含模型/Skill 白名单、
并发、请求速率、Token、存储、媒体次数和成本预算；Run 创建时冻结该版本并
预留成本，完成后回写实际用量。

**完成条件：** 一个高频或高成本 tenant 不能耗尽全局容量或超出预算；被拒绝
的原因可由用户和运营人员分别看到。

## P1：托管多租户与可扩容执行

### [ ] PostgreSQL、持久化队列与对象存储

**现在的缺口：** SQLite、进程内 scheduler、Docker volume 只适用于单节点。

**要做：** 将控制面迁移到 PostgreSQL，将 Run/Delivery/Provider Job 放入
持久化队列，将输入和 artifact 移至对象存储；保留 lease epoch 与幂等语义。

**完成条件：** 多个 Worker 可同时领取任务；实例故障不会丢 Run、输入、
artifact 或 delivery 状态。

### [ ] 会话隔离执行器池

**现在的缺口：** session 有独立目录和 Harness，但仍共处一个容器和 Unix
权限边界；桥接还需要 Docker 操作能力。

**要做：** 每个活跃 session/run 使用短生命周期容器或 sandbox worker，
只挂载该 workspace、DSH state 和只读批准 Skill bundle；移除执行器的 Docker
socket、其他 session 根目录和长期密钥访问。

**完成条件：** 恶意 Skill、提示注入或被攻破的执行器都无法读取其他 session
文件、宿主机状态或长期凭据。

### [ ] Vault 与 Model/Tool Gateway

**现在的缺口：** Runner 环境已缩小，但仍使用服务端长期 provider 凭据。

**要做：** 将原始密钥移入 Vault；控制面按 Run policy 向模型/工具网关签发
短期、范围化 capability。支持轮换、撤销、审计和平台托管/BYOK 两种模式。

**完成条件：** 原始密钥不进入 executor、workspace、DSH state、prompt、
artifact、日志或 API 响应。

### [ ] 网络出口与 Tool Policy Enforcement

**现在的缺口：** 工具参数有 schema 和部分 URL 校验，但没有统一的 egress
allowlist、SSRF/DNS rebinding 防护、按 tenant 的网络策略和副作用审批。

**要做：** 在 tool dispatch 与网络出口增加策略执行点：目标 allowlist、
解析后 IP 校验、连接时复核、请求大小限制、风险等级、审批/预算检查与审计。

**完成条件：** 来自附件、网页或工具输出的提示注入不能绕过网络、计费或
副作用策略。

### [ ] Runtime 生命周期与容量池

**现在的缺口：** 当前每个活跃 session 可能保留独立 Harness；没有 idle TTL、
LRU 回收或 executor drain。

**要做：** 定义 runtime/executor lease、冷启动、空闲回收、最大进程数和
优雅 drain。只有验证 DSH 的 session-scoped workspace 契约后，才引入共享
per-model runtime pool。

**完成条件：** 长时间空闲 session 不泄漏进程；扩容和缩容不打断持久 Run。

## P2：共享知识、治理与运营

### [ ] 租户记忆与知识发布流程

**现在的缺口：** 表结构预留了 tenant memory metadata，但没有导入、索引、
检索、审核、版本冻结或显式提升流程。

**要做：** 建立 source → index → review → publish 的知识生命周期；Run 只
接收被授权的检索结果，不能挂载整个知识库。会话/Run 的提升必须显式且可审计。

**完成条件：** 同一 tenant 的共享知识可用但不串会话；未授权、过期或未发布
内容不会进入 prompt。

### [ ] 数据保留、删除、备份与灾备

**现在的缺口：** 当前没有租户级 retention、purge、legal hold、备份删除
传播或 RPO/RTO 承诺。

**要做：** 定义 `ACTIVE → ARCHIVED → PENDING_PURGE → PURGED` 生命周期，
覆盖 workspace、DSH state、输入、artifact、索引、日志、队列与备份。

**完成条件：** 删除租户后不会因恢复备份而重新出现；可以给出可验证的删除和
恢复演练结果。

### [ ] 可观测性、告警和 SLO

**现在的缺口：** 有 audit/usage 基础，但没有 tenant/Agent/model/run 维度的
指标、trace、告警和 SLO。

**要做：** 增加 Run 延迟、队列等待、失败率、delivery retry、provider 错误、
成本、取消率和 executor 饱和度指标；建立告警、runbook 和分租户排障视图。

**完成条件：** 能在不查看用户内容的前提下定位"哪个 tenant/模型/Skill/版本
导致失败或成本异常"。

### [ ] Agent / Skill 发布治理

**现在的缺口：** Agent version 不可变，但还没有 Skill manifest hash、签名、
SBOM、评测门禁、灰度和一键回滚流程。

**要做：** 发布包冻结 Agent prompt、Skill bundle、工具策略、runner image 和
模型路由版本；经过安全扫描、评测、canary 后发布。

**完成条件：** 可以复现历史 Run 使用的执行环境；有问题的 Agent/Skill 可以
停止新流量并回滚，不影响已有审计记录。

## 建议实施顺序

1. 完整媒体 delivery replay 与 Provider Job 统一。
2. 生产身份、tenant policy、成本预留和配额。
3. PostgreSQL / 队列 / 对象存储，再进入多 Worker。
4. 会话隔离执行器、Vault 和网络策略执行点。
5. 知识发布、数据治理、可观测性和发布治理。

每一项开始前，都要先更新 [目标架构](tenant-session-runtime-design.zh-CN.md)
中的"当前状态与缺口"，并为该项增加单机/故障恢复/越权三类验收测试。
