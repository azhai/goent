# goent/plus 包设计文档

## 概述

`goent/plus` 是 GoEnt 的子包，提供两种进阶数据模型的封装：
1. **分表 (Sharding)** — 根据索引字段自动路由到对应的子表
2. **嵌套集 (Nested Set)** — 用 Nested Set 模型存储和操作树状数据

## 包结构

```
plus/
├── sharding.go            # ShardedTable[T] 核心
├── sharding_strategy.go   # ShardingStrategy 接口与实现
├── sharding_query.go      # 跨表查询（分页、聚合）
├── table_cache.go         # 表元数据缓存
├── nestedset.go           # NestedSet[T] 核心
└── nestedset_ops.go       # 树操作方法

---

## 一、分表模块 (Sharding)

### 1.1 ShardingStrategy 接口

```go
type ShardingStrategy interface {
    ResolveTableName(base string, shardValue any) string
    MatchPattern(base string) string // 返回 glob 模式如 "t_order_*"
}
```

### 1.2 ModuloHex 策略 — 按余数分表

按分片字段的值对 16/64/256 取余，表名格式为 `base_XX`（两位十六进制 + 下划线）。

```go
type ModuloHex struct {
    Bits int // 4(16张), 6(64张), 8(256张)
}
```

**表名示例：**

| Bits | 分片数 | 表名范围 | 示例 |
|------|--------|----------|------|
| 4 | 16 | `t_order_00` ~ `t_order_0f` | user_id=15 → `t_order_0f` |
| 6 | 64 | `t_order_00` ~ `t_order_3f` | user_id=63 → `t_order_3f` |
| 8 | 256 | `t_order_00` ~ `t_order_ff` | user_id=255 → `t_order_ff` |

```go
func (m ModuloHex) ResolveTableName(base string, val any) string {
    n := toInt64(val)
    suffix := fmt.Sprintf("_%02x", n&(1<<m.Bits-1))
    return base + suffix
}

func (m ModuloHex) MatchPattern(base string) string {
    return base + "_*"
}
```

### 1.3 TimeGranularity 策略 — 按时间分表

```go
type TimeGranularity int

const (
    ByDay   TimeGranularity = iota // t_order_20250521
    ByMonth                        // t_order_202505
    ByYear                         // t_order_2025
)
```

**表名示例：**

| 粒度 | 格式 | 示例 |
|------|------|------|
| ByDay | `base_YYYYMMDD` | `t_order_20250521` |
| ByMonth | `base_YYYYMM` | `t_order_202505` |
| ByYear | `base_YYYY` | `t_order_2025` |

```go
func (g TimeGranularity) ResolveTableName(base string, val any) string {
    t := toTime(val)
    switch g {
    case ByDay:   return base + "_" + t.Format("20060102")
    case ByMonth: return base + "_" + t.Format("200601")
    case ByYear:  return base + "_" + t.Format("2006")
    }
}
```

### 1.4 TableCache — 表元数据缓存

缓存数据库中已存在的表列表，避免每次查询都扫描 information_schema。

```go
type TableCache struct {
    mu        sync.RWMutex
    tables    map[string]bool     // 已存在的表名 → true
    schemaMap map[string][]string // schema → 表名列表
    refreshed time.Time           // 上次刷新时间
    ttl       time.Duration       // 缓存有效期，默认 5 分钟
}

func NewTableCache(ttl time.Duration) *TableCache
func (c *TableCache) Exists(table string) bool
func (c *TableCache) Refresh(ctx context.Context, db *DB) error
func (c *TableCache) Match(pattern string) []string // glob 匹配返回匹配的表名列表
func (c *TableCache) Add(table ...string)
func (c *TableCache) Remove(table ...string)
```

- 首次调用时从数据库加载已有表并缓存
- TTL 过期后自动刷新（下次访问时惰性刷新）
- `Match()` 使用 glob 模式匹配（如 `t_order_*`），返回排序后的表名列表
- 支持手动增删（建表/删表时同步更新）

### 1.5 ShardedTable[T] — 统一分表接口

```go
type ShardedTable[T any] struct {
    db       *DB
    baseName string              // 基础表名，如 "t_order"
    shardKey string              // 分片字段名，如 "user_id"
    strategy ShardingStrategy    // 分片策略
    cache    *TableCache         // 表元数据缓存（共享实例）
    tables   sync.Map            // map[string]*Table[T] 懒创建子表
}
```

#### 构造函数

```go
func NewShardedTable[T any](db *DB, baseName, shardKey string,
    strategy ShardingStrategy, cache *TableCache) *ShardedTable[T]
```

#### 单表路由

根据分片字段的值自动选择目标子表：

```go
// 根据分片值获取对应的子表（懒创建 + 缓存）
func (s *ShardedTable[T]) ResolveTable(shardValue any) *Table[T]
```

内部逻辑：
1. 调用 `strategy.ResolveTableName()` 计算表名
2. 从 `sync.Map` 查找或创建 `SimpleTable[T]`
3. 返回该子表实例

#### 单表操作

自动路由到对应子表：

```go
func (s *ShardedTable[T]) Insert(row *T) error
func (s *ShardedTable[T]) InsertMany(rows []*T) error
func (s *ShardedTable[T]) Update(shardValue any, pairs ...Pair) error
func (s *ShardedTable[T]) DeleteByPK(shardValue any) (*T, error)
func (s *ShardedTable[T]) FindByPK(shardValue any, pk any) (*T, error)
func (s *ShardedTable[T]) FindOne(shardValue any, filters ...Condition) (*T, error)
```

#### 跨表操作

遍历所有匹配的子表执行查询：

```go
// 查询所有子表
func (s *ShardedTable[T]) SelectAll(filters ...Condition, orders ...Order) ([]*T, error)
func (s *ShardedTable[T]) CountAll(filters ...Condition) (int64, error)

// 分页查询（核心方法）
func (s *ShardedTable[T]) SelectPage(page, size int,
    filters []Condition, orders []Order) ([]*T, int64, error)

// 执行器模式：遍历所有子表，对每个表执行回调
func (s *ShardedTable[T]) EachTable(fn func(*Table[T]) error) error
```

### 1.6 分页查询算法

跨表分页是分表场景的核心难点。算法如下：

```
输入: page=2, size=20, orders=[id ASC], filters=[status='active']

步骤 1: 获取所有匹配的表名（从缓存）
  tables = cache.Match("t_order_*")  // ["t_order_00", "t_order_01", ..., "t_order_0f"]
  按 orders 方向排序（ASC 则表名升序）

步骤 2: 前向扫描确定 offset 位置
  offset = (page - 1) * size  // = 20
  remaining = offset
  for each table in tables:
      count = COUNT(*) FROM table WHERE status='active'
      if count >= remaining:
          targetTable = table
          tableOffset = remaining - 1
          break
      remaining -= count

步骤 3: 从目标表取数据
  result = SELECT * FROM targetTable WHERE status='active'
          ORDER BY id ASC LIMIT size OFFSET tableOffset

步骤 4: 如果目标表数据不足 size 条，继续从下一张表补充
  while len(result) < size and has next table:
      more = SELECT * FROM nextTable WHERE status='active'
             ORDER BY id ASC LIMIT (size - len(result))
      result = append(result, more...)

步骤 5: 总数统计（可异步/缓存）
  totalCount = SUM(COUNT(*) FROM each table WHERE ...)
  totalPages = ceil(totalCount / size)

返回: result, totalCount
```

**性能优化点：**
- `totalCount` 可缓存（短 TTL 或由调用方指定是否需要精确值）
- 当 filters 为空且只需 Count 时，可直接用 cache 中记录的行数近似
- 时间分表通常只查最近 N 张表（可配置 `MaxTables` 限制范围）

---

## 二、Nested Set 模块

### 2.1 数据模型要求

用户 struct 需包含左值(lft)、右值(rgt) 字段：

```go
type Category struct {
    ID       int64 `goe:"pk"`
    Name     string
    ParentID int64
    Lft      int `goe:"lft"`  // 左值
    Rgt      int `goe:"rgt"`  // 右值
}
```

### 2.2 NestedSet[T] 结构

```go
type NestedSet[T any] struct {
    table       *Table[T]
    leftField   string // 默认 "lft"
    rightField  string // 默认 "rgt"
    parentField string // 默认 "parent_id"
    pkField     string // 默认 "id"
}

func NewNestedSet[T any](table *Table[T]) *NestedSet[T]
func NewNestedSetWithFields[T any](table *Table[T], left, right, parent, pk string) *NestedSet[T]
```

### 2.3 写操作

所有写操作在事务中执行，保证 lft/rgt 的一致性。

```go
// 插入为 parent 的最后一个子节点
func (n *NestedSet[T]) InsertNode(ctx context.Context, parentID int64, data *T) (*T, error)

// 插入到 sibling 之前
func (n *NestedSet[T]) InsertNodeBefore(ctx context.Context, siblingID int64, data *T) (*T, error)

// 整棵树批量插入（优化版：预先计算所有 lft/rgt，一次性写入）
func (n *NestedSet[T]) BatchInsertTree(ctx context.Context, root *TreeNode[T]) error

// 移动子树到新父节点下
func (n *NestedSet[T]) MoveSubtree(ctx context.Context, nodeID, newParentID int64) error

// 删除子树，返回被删除的节点
func (n *NestedSet[T]) DeleteSubtree(ctx context.Context, nodeID int64) ([]*T, error)
```

**InsertNode 内部流程（以插入到最后一个子节点为例）：**

```
1. BEGIN TRANSACTION
2. SELECT lft, rgt FROM table WHERE id = parentID  -- 获取父节点
3. UPDATE table SET lft = lft + 2 WHERE lft > parent.rgt  -- 右侧节点右移
4. UPDATE table SET rgt = rgt + 2 WHERE rgt >= parent.rgt
5. INSERT INTO table (..., lft, rgt) VALUES (..., parent.rgt, parent.rgt + 1)
6. COMMIT
```

**MoveSubtree 内部流程：**

```
1. 计算子树宽度: width = node.rgt - node.lft + 1
2. 确定移动方向和距离
3. 三步更新（在事务中）:
   a. 先腾出目标空间
   b. 再移动子树
   c. 最后填补原位置空隙
```

### 2.4 读操作

```go
// 单节点
func (n *NestedSet[T]) GetNode(ctx context.Context, nodeID int64) (*T, error)
func (n *NestedSet[T]) GetRoot(ctx context.Context) (*T, error)
func (n *NestedSet[T]) GetParent(ctx context.Context, nodeID int64) (*T, error)
func (n *NestedSet[T]) GetLevel(ctx context.Context, nodeID int64) (int, error)

// 层级关系
func (n *NestedSet[T]) GetAncestors(ctx context.Context, nodeID int64) ([]*T, error)
func (n *NestedSet[T]) GetDescendants(ctx context.Context, nodeID int64) ([]*T, error)
func (n *NestedSet[T]) GetChildren(ctx context.Context, nodeID int64) ([]*T, error)
func (n *NestedSet[T]) GetSubtree(ctx context.Context, nodeID int64, maxDepth int) ([]*T, error)

// 完整树
func (n *NestedSet[T]) GetTree(ctx context.Context, rootID int64) (*TreeNode[T], error)
func (n *NestedSet[T]) GetFullTree(ctx context.Context) (*TreeNode[T], error) // 多根时返回虚拟根
```

**SQL 映射：**

| 方法 | SQL 要点 |
|------|----------|
| GetAncestors | `WHERE lft < node.lft AND rgt > node.rgt ORDER BY lft ASC` |
| GetDescendants | `WHERE lft BETWEEN node.lft AND node.rgt ORDER BY lft ASC` |
| GetChildren | `WHERE parent_id = node.id ORDER BY lft ASC` |
| GetSubtree(maxDepth) | `WHERE lft BETWEEN node.lft AND node.rgt AND (rgt-lft)/2 < maxDepth` |
| GetLevel(node) | `SELECT COUNT(*) FROM table WHERE lft < node.lft AND rgt > node.rgt` |

### 2.5 TreeNode 辅助结构

```go
type TreeNode[T any] struct {
    Data     *T
    Children []*TreeNode[T]
    Level    int // 根节点 Level=0
}

// 从扁平列表构建树（GetTree 内部使用）
func BuildTree(nodes []*T, getID func(*T) int64,
    getParentID func(*T) int64) *TreeNode[T]
```

---

## 使用示例

### 分表示例

```go
// 定义订单 model
type Order struct {
    ID      int64 `goe:"pk"`
    OrderNo string
    UserID  int64 // 分片键
    Total   float64
}

// 创建分表
cache := plus.NewTableCache(5 * time.Minute)
shardedOrder := plus.NewShardedTable[Order](db, "t_order", "user_id",
    &plus.ModuloHex{Bits: 4}, cache)

// 刷新表缓存（启动时调用一次）
shardedOrder.RefreshCache(context.Background())

// 单表操作 — 自动路由
order := &Order{OrderNo: "ORD-001", UserID: 255, Total: 99.9}
err := shardedOrder.Insert(order)  // 写入 t_order_ff

// 查询
found, err := shardedOrder.FindByPK(255, order.ID)

// 跨表分页
orders, total, err := shardedOrder.SelectPage(1, 20,
    []Condition{}, []Order{{Column: "id", Direction: "ASC"}})
```

### Nested Set 示例

```go
// 定义分类 model
type Category struct {
    ID       int64 `goe:"pk"`
    Name     string
    ParentID int64
    Lft      int
    Rgt      int
}

categoryTable := db.Category
ns := plus.NewNestedSet[Category](categoryTable)

// 构建分类树
electronics, _ := ns.InsertNode(ctx, 0, &Category{Name: "电子产品"})
phones, _ := ns.InsertNode(ctx, electronics.ID, &Category{Name: "手机"})
laptops, _ := ns.InsertNode(ctx, electronics.ID, &Category{Name: "笔记本"})

// 查询子树
descendants, _ := ns.GetDescendants(ctx, electronics.ID)
// [{电子产品}, {手机}, {笔记本}]

// 获取完整树
tree, _ := ns.GetTree(ctx, 0)
// TreeNode{
//   Data: {电子产品, Lft:1, Rgt:6},
//   Level: 0,
//   Children: [
//     {Data: {手机, Lft:2, Rgt:3}, Level: 1},
//     {Data: {笔记本, Lft:4, Rgt:5}, Level: 1},
//   ],
// }
```

---

## 设计决策

1. **为什么用 sync.Map 而不是普通 map？** 子表实例是懒创建的，高并发下 sync.Map 减少锁竞争
2. **为什么 TableCache 是独立结构而非嵌入 ShardedTable？** 多个 ShardedTable 可共享同一个 TableCache（同一数据库连接），减少重复查询
3. **为什么分页算法是顺序扫描而非并行查询？** 保持结果有序；并行需要额外合并排序，复杂度高；后续可加可选的并行模式
4. **Nested Set 为什么不直接修改 lft/rgt 字段标签？** 保持用户 struct 的通用性，通过构造函数参数灵活配置字段名
