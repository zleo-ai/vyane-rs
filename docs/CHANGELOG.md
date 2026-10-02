# 变更日志

## Unreleased

- 将 `async-trait` 的最低版本升级至 `0.1.92`，移除宏生成的冗余 `#[must_use]`，修复新 Rust 工具链下的 Clippy `double_must_use` 错误。
# 更新记录

## Unreleased

- 修正 Anthropic Messages 用量归一化：输入总量包含普通输入、缓存创建和缓存读取，缓存子计数仅包含读取，避免缓存读取多于普通输入时少计费用；缓存创建仍按普通输入费率估算。
- 修正费用估算：为缓存输入／推理输出配置专用费率时，从输入／输出总量中扣除对应子计数后分别计费，避免重复收费；超出总量的异常子计数钳制到对应总量。
# 更新日志

## Unreleased

- 补齐安全路由对 `authenticate` / `authorize` 及其过去式、第三人称单数、进行时的识别；保留完整词边界，避免 `author`、`authority`、`authoritative` 和嵌词误报。
