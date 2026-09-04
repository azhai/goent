# Task Plan: 代码审查 + 优化 + 测试 + 文档更新

## Goal
审查 goent 仓库代码，修复发现的 bug，补充必要优化与测试，并同步更新 README 与相关文档；全部门禁（build / vet / test / race）保持全绿。

## Phases
- [x] 1. 记录 — 建立 `.planning/` 三件套与 `.codebuddy/memory/` 记忆文件
- [x] 2. 审查 — 通读 `utils/`、`plus/`、核心 ORM 文件，定位 bug
- [x] 3. 修复 — 修复已确认 bug（含一处公开 API 调整：ShardedTable.Update/DeleteByPK 增加 pk 参数）
- [x] 4. 优化 — 缩写排序缓存（utils/strcase.go）、Builder 池归还路径
- [x] 5. 测试 — 新增 utils/strcase_test.go、utils/parse_test.go、tests/plus/sharding_ops_test.go、tests/plus/nested_set_extra_test.go、tests/bugfix_builder_pool_test.go、tests/concurrent_builder_test.go 已存在
- [x] 6. 文档 — 更新 README（Plus Extensions 章节 + Code Quality & Tests + 修复清单，Go 版本 1.27）
- [x] 7. 验证 — `go build ./...`、`go vet ./...`、`go test ./...`、`go test -race ./...` 全绿

## Open questions
- `go.mod` 中 `replace github.com/azhai/gobus => ../gobus` 依赖工作区外路径 → 需在本地存在（已确认存在），CI 上是否有隐患？→ 记录到 findings，不擅自改动。

## Resume point
任务已完成，无需继续。
