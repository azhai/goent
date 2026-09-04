# Findings

## 2026-09-04 — 仓库基线与门禁
- Learned: 仓库门禁命令为 `go build ./...`、`go vet ./...`、`go test ./...`；无 lint/knip 配置，`Makefile` 只负责交叉编译。
- Source: `Makefile`、`go.mod`（`replace github.com/azhai/gobus => ../gobus`）
- Implication: 验证阶段以 go 原生工具链为准，不引入新 lint 器。

## 2026-09-04 — 测试覆盖缺口
- Learned: `utils/`（strcase/parse/imports）、`model/`、`reverse/`、`drivers/` 均无测试文件；`plus/` 的测试放在 `tests/plus/`。
- Source: `go test ./...` 输出中的 `[no test files]`
- Implication: 命名转换（ToSnakeCase/ToPlural/ToSingular）是迁移与代码生成的核心，零覆盖 = 高风险区，优先补测。

## 2026-09-04 — ToPlural 元音判断下标错误（BUG）
- Learned: `utils/strcase.go` `ToPlural` 中 `last := name[len(name)-1]`，而函数已进入 `HasSuffix(name,"y")` 分支，`last` 恒为 `'y'`，永远不等于 a/e/i/o/u。
- 后果: `Day` → `Daies`（应为 `Days`）、`Boy` → `Boies`（应为 `Boys`）、`Key` → `Keies`。
- Source: `utils/strcase.go:214-219`
- Implication: 影响自动生成的表名复数化，需修正为取倒数第二个字符。

## 2026-09-04 — ToSnakeCase 缩写替换缺少词边界（BUG）
- Learned: `ToSnakeCase` 用 `strings.ReplaceAll(word, abbr, ...)` 无边界匹配，缩写 "ID" 会命中 "Identity" → `_id_entity` → `id_entity`（应为 `identity`）。
- Source: `utils/strcase.go:138-158`
- Implication: 字段名含 Identity/Item/Idle 等会被错误拆分；需要按 Go 标识符词边界匹配。

## 2026-09-04 — ToSnakeCase 每次调用都排序缩写表（性能）
- Learned: `sortedAbbreviationKeys()` 每次调用都分配切片并 `sort.Slice`，而 `ToSnakeCase` 在迁移/反射映射中被高频调用。
- Source: `utils/strcase.go:96-107`
- Implication: 可缓存排序结果，仅在 `RegisterAbbreviations` 时失效；同时消除循环内反复加解锁。

## 2026-09-04 — sync.Pool 双归还导致 Builder 被两个 goroutine 共享（严重 BUG）
- Learned: `StateInsert.All` 先 `defer PutBuilder(s.builder)`，又在 `len(data)==1` 分支里 `return s.One(data[0])`，而 `One` 自己也 `defer PutBuilder`。同一个 *Builder 被 Put 两次。
- 后果: sync.Pool 把同一个 Builder 交给两个 goroutine → `clear(b.Changes)` 与并发写入同一 map → 运行时 `fatal error: concurrent map writes`，不可 recover。
- Source: `insert.go:111-117`（修复前）、`builder.go:255`
- Implication: 这是全仓库最严重的缺陷；修复方式改为三条分支各自只归还一次。修复前 `go test ./...` 3/3 失败（基线 1/3），修复后 6/6 通过。

## 2026-09-04 — IterRows 重复遍历会双归还 Builder（BUG）
- Learned: `StateSelect.IterRows` 把 `defer PutBuilder(builder)` 放在返回的 `iter.Seq2` 闭包里，闭包被 range 两次就会 Put 两次。
- Source: `select.go:170-177`（修复前）
- Implication: 与上面同源；改为闭包内先取走 builder 指针并置 nil，保证最多归还一次。

## 2026-09-04 — plus.TableCache.Refresh 在 PostgreSQL 上必然失败（BUG）
- Learned: SQL 用了 `$1` 占位符，却 `model.CreateQuery(sqlStr, nil)` 传了 nil 参数；pgx 会拒绝该查询。
- Source: `plus/table_cache.go:86`（修复前）
- Implication: 新增 `RefreshSchema(ctx, db, schema)`，`Refresh` 默认用 "public" 并绑定参数。

## 2026-09-04 — plus.ShardedTable 的 Update/DeleteByPK 把分片值当主键（BUG）
- Learned: `DeleteByPK(shardValue)` 与 `Update(shardValue, pairs...)` 都用 `Equals(id, shardValue)` 作为过滤条件，分片值被当成主键，会命中错误的行。
- Source: `plus/sharding.go:93-102`（修复前）
- Implication: 补齐 pk 参数，与 `FindByPK(shardValue, pk)` 保持一致。属于 API 变更，需在 README 变更记录里说明。

## 2026-09-04 — ModuloHex 位移越界（BUG）
- Learned: `1<<m.Bits`：Bits<0 会 panic（negative shift amount）；Bits>=63 时 mask 溢出为 -1，负的分片值会生成 `t_x_-10` 这样的表名。
- Source: `plus/sharding_strategy.go:22-26`（修复前）

## 2026-09-04 — NestedSet 只对 int 类型写回 Lft/Rgt（BUG）
- Learned: `setIntFieldValue` 只在 `field.Kind()==reflect.Int` 时写入，int32/int64 字段会被静默跳过，导致树结构写回不完整。
- Source: `plus/nested_set.go:162-173`（修复前）

## 2026-09-04 — condition.Not 对空条件生成非法 SQL（BUG）
- Learned: `Not(Condition{})` 生成 `NOT ()`。
- Source: `condition.go:146-148`（修复前）

## 2026-09-04 — applyFilter 可能写坏调用方切片（隐患）
- Learned: `conds = append(conds, *w)` 中 conds 是变参切片，调用方用 `Filter(s...)` 传入时 append 可能写入调用方底层数组。
- Source: `condition.go:380-389`（修复前）

## 2026-09-04 — ToCamelCase 会截断多字节首字符（BUG）
- Learned: `word[:1]` 按字节切分，遇到非 ASCII 首字符（如 `é_user`）会产生非法 UTF-8。
- Source: `utils/strcase.go:123-134`（修复前）

## Errors logged
- `go test ./...` 间歇性 `fatal error: concurrent map writes` → 已定位为 Builder 双归还，已修复。
- 早期用 `go test ./tests/ -count=2` 复现出的 `TestO2M...`/`TestOneWith...` 失败是测试自身不幂等（同一 fixture 跑两遍），不是产品缺陷；不做修改。

## 2026-09-04（续）— 任务3：性能优化
- `StateInsert.All` 每次批量插入都重建 `VisitFields`（每列一次 `Field()` 查表）。新增 `TableInfo.GetBulkInsertFields()`，按表缓存该切片（自动递增主键排除），`All` 直接复用。新增回归测试 `table_internal_test.go`。
- `And`/`Or` 原用 `res.Template += ...` 拼接，复合条件为 O(n²)；改为 `strings.Builder` 一次拼装。`condition.go`。
- `utils/strcase.go` 缩写排序结果已缓存（上一轮）。
- 验证：`go build ./...`、`go vet ./...`、`go test ./...`、`go test ./... -race` 全绿。
- 未做：`Field.String()` 的 registry 查表缓存——存在 simpleTable 场景下 `String()` 返回 "" 的语义差异与跨 goroutine 缓存竞态风险，收益低且易引入回归，暂不动。
