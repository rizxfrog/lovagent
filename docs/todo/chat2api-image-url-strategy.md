# Chat2API 图片输入兼容性待办

日期：2026-04-12

## 当前结论

已通过 IntelliJ IDEA HTTP Client 手工验证：

- Chat2API 的 `/v1/chat/completions` 可以处理远程图片 URL
- Chat2API 当前不接受 `data:image/...;base64,...` 形式的 `image_url`

验证文件：

- [chat2api-vision-smoke.http](F:\MyDepository\lovagent\docs\http\chat2api-vision-smoke.http)

## 对当前项目的影响

lovagent 现在在多模态回复链路里，会把 `http://` / `https://` 图片先下载，再转成 `data:` URL 后发给模型。

这条策略对 Chat2API 不合适，因为它已经验证为：

- 远程 URL 可用
- data URL 不可用

因此，如果后续接入 Chat2API，应该优先直接把远程图片 URL 透传给模型，而不是统一转 base64。

## 暂不修改的原因

- 当前只是完成协议验证，还没有正式切换到 Chat2API 作为生产链路
- 现有逻辑可能是为了兼容其他 provider，直接改会影响已有行为
- 更合适的做法是按 provider 区分图片输入策略，而不是全局一刀切

## 后续建议实现

建议按 provider / transport 做图片输入策略分支：

- `openai_compatible` + Chat2API：优先透传远程 `image_url`
- `glm` 原生或其他特殊 provider：保留现有 base64 / data URL 兼容逻辑
- `base64://...` 输入：继续作为兜底兼容保留

## 预计改动点

- [inbound_actor_service.py](F:\MyDepository\lovagent\app\services\inbound_actor_service.py:890)
  - 调整 `_resolve_image_reference()`
- [attachment_executor_service.py](F:\MyDepository\lovagent\app\services\attachment_executor_service.py:78)
  - 确认多模态消息组装仍符合 OpenAI-compatible 格式
- [test_inbound_actor_service.py](F:\MyDepository\lovagent\tests\test_inbound_actor_service.py:867)
  - 补充 `openai_compatible` / Chat2API 场景下不转 data URL 的回归测试

## 目标行为

当 provider 为 Chat2API 对应的 `openai_compatible` 路径时：

1. 收到 `https://...` 图片地址
2. 不下载、不转 base64
3. 直接作为 `image_url.url` 发送到 `/v1/chat/completions`

这样应与已验证通过的手工测试保持一致。
