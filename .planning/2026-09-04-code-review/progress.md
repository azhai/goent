# Progress Log

## 2026-09-04
- Ran: `go build ./...` → exit 0
- Ran: `go vet ./...` → exit 0
- Ran: `go test ./...` → exit 0（backup / cmd/goent-sql / rename / tests / tests/plus 通过；utils、model、reverse、drivers、plus、根包无测试文件）
- Read: `AGENTS.md`、`.ai-memory/*` 全部、README.md、Makefile、utils/strcase.go
- Finding: `utils.ToPlural` 元音判断下标错误；`utils.ToSnakeCase` 缩写替换无词边界且每次排序
- Fixed: `utils/strcase.go`（ToPlural/ToSingular/ToCamelCase/ToSnakeCase 重写 + 缩写快照原子发布）
- Fixed: `plus/sharding.go`、`plus/sharding_strategy.go`、`plus/table_cache.go`、`plus/sharding_query.go`、`plus/nested_set.go`
- Fixed: `condition.go`（Not 空条件、applyFilter 切片别名）
- Fixed: `insert.go` + `select.go` + `builder.go`（Builder 双归还）
- Ran: `go build ./... && go vet ./...` → exit 0
- Ran: `go test ./...` ×6 → 全绿（修复前 3/3 失败）
- Ran: `go test ./... -race` ×3 → 全绿
- Added: `tests/plus/nested_set_extra_test.go`（int64/uint 字段往返、nil 根、畸形行、空、round-trip）
- Added: `tests/bugfix_builder_pool_test.go`（Insert.All 单元素双归还回归 + Select.All 并发池压力）
- Fixed: `table.go` SimpleTable 缺失空 db 守卫 + 缺失列初始化（createTableMeta）
- Ran: `go test ./utils/ ./tests/plus/ ./tests/ -run TestBugfix -count=3 -race` → 全绿
- Ran: `go vet ./...` → 全绿；`go test ./...` → 全绿
- Updated: README.md（新增 Plus Extensions 与 Code Quality & Tests 章节、修复清单，Go 要求改为 1.27）
- Task3 性能优化: `TableInfo.GetBulkInsertFields()` 缓存批量插入字段列表（`insert.go` `All` 复用）；`And`/`Or` 改用 `strings.Builder`
- Added: `table_internal_test.go`（`GetBulkInsertFields` 缓存与自增主键排除回归）
- Ran: `go test ./... -race` → 全绿
- Task complete: 全部门禁通过。

## 2026-09-04（续三）— 补充必要单元测试
- 盘点已修复 bug 的回归测试覆盖：`sharding_ops_test.go` 已 DB 覆盖 `ModuloHex` 边界、`ShardedTable.Update/DeleteByPK`、`TableCache.Refresh`；但 `condition.Not` 空条件、`applyFilter` 写坏调用方切片、`Field.String()` 缓存路径仍缺断言测试。
- 新增 `condition_internal_test.go`（package goent 白盒）：`TestNot_EmptyCondition`（防 `NOT ()`）、`TestNot_WrapsNonEmpty`、`TestApplyFilter_DoesNotMutateCallerSlice`（用 len-2 reslice 共享底层数组观察溢出写）、`TestApplyFilter_EmptyCondsReturnsW`。
- 新增 `field_internal_test.go`（package goent）：`TestField_String_Cache`、`TestField_String_Fallback`。
- 顺手优化：`Field.String()` 的 Function 分支原本不走缓存、仍走注册表查表；改为聚合字段也复用 `qualifiedName` 缓存（结果字符串与注册表一致，避免冗余锁+格式化）。
- 验证：`go build ./...`、`go vet ./...`、`go test ./... -race` 全绿；`BenchmarkFieldStringCached` 1.76ns/0alloc 无回归。
