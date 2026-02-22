# 数据库迁移

本目录包含蜂巢套利系统的 SQL 迁移文件，连接宿主机 PostgreSQL 自动执行。

## 数据表

### 1. positions
存储所有仓位记录，包含完整的交易详情。

**索引:**
- `idx_positions_status` - 按仓位状态查询
- `idx_positions_symbol` - 按交易对查询
- `idx_positions_asset_class` - 按资产类别查询
- `idx_positions_entry_timestamp` - 按开仓时间查询
- `idx_positions_exchange` - 按交易所查询
- `idx_positions_funding_anomaly` - 查询资金费异常的仓位

### 2. funding_logs
存储资金费率支付记录，关联仓位。

**索引:**
- `idx_funding_logs_position` - 按仓位 ID 查询
- `idx_funding_logs_timestamp` - 按时间戳查询

### 3. daily_pnl
存储每日盈亏汇总，日期唯一约束。

**索引:**
- `idx_daily_pnl_date` - 按日期查询 (唯一)

### 4. system_logs
存储系统事件日志。

**索引:**
- `idx_system_logs_level` - 按日志级别查询
- `idx_system_logs_timestamp` - 按时间戳查询
- `idx_system_logs_module` - 按模块名查询

## 执行迁移

```bash
# 从项目根目录执行
make migrate

# 或直接运行
python -m backend.storage.migrations.run_migrations
```

迁移工具会：
- 自动连接 `.env` 中配置的 PostgreSQL
- 创建 `_migrations` 记录表追踪已执行的迁移
- 按文件名顺序执行未应用的 `.sql` 文件
- 每个文件在独立事务中执行，失败自动回滚
- 重复运行是安全的（幂等）

## 验证

```sql
-- 检查表是否创建
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN ('positions', 'funding_logs', 'daily_pnl', 'system_logs');

-- 检查索引
SELECT indexname, tablename
FROM pg_indexes
WHERE schemaname = 'public'
AND tablename IN ('positions', 'funding_logs', 'daily_pnl', 'system_logs');

-- 检查迁移记录
SELECT * FROM _migrations ORDER BY applied_at;
```

## 迁移文件

- `001_create_tables.sql` - 初始表结构 (positions, funding_logs, daily_pnl, system_logs)
- `002_add_cross_exchange_fields.sql` - 跨市套利字段

## 注意事项

- 所有表使用 UUID 主键 (`gen_random_uuid()`)
- 时间戳使用 `TIMESTAMPTZ` 支持时区
- 金额字段使用合适的精度
- 外键约束确保数据完整性
