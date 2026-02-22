#!/bin/bash
# ============================================================
# 蜂巢套利系统 - 数据库初始化脚本
# 读取 backend/.env 中的 PostgreSQL 配置，按顺序导入 migrations
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/backend/.env"
MIGRATIONS_DIR="$SCRIPT_DIR/backend/storage/migrations"

# 检查 .env 文件
if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] 找不到 .env 文件: $ENV_FILE"
    exit 1
fi

# 检查 migrations 目录
if [ ! -d "$MIGRATIONS_DIR" ]; then
    echo "[ERROR] 找不到 migrations 目录: $MIGRATIONS_DIR"
    exit 1
fi

# 从 .env 读取变量（忽略注释和空行）
load_env() {
    local key="$1"
    grep -E "^${key}=" "$ENV_FILE" | head -1 | cut -d'=' -f2-
}

PG_HOST=$(load_env "POSTGRES_HOST")
PG_PORT=$(load_env "POSTGRES_PORT")
PG_DB=$(load_env "POSTGRES_DATABASE")
PG_USER=$(load_env "POSTGRES_USER")
PG_PASS=$(load_env "POSTGRES_PASSWORD")

# 校验必要参数
if [ -z "$PG_HOST" ] || [ -z "$PG_DB" ] || [ -z "$PG_USER" ]; then
    echo "[ERROR] .env 中缺少必要的 PostgreSQL 配置 (POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USER)"
    exit 1
fi

PG_PORT="${PG_PORT:-5432}"

echo "============================================================"
echo " 蜂巢套利系统 - 数据库初始化"
echo "============================================================"
echo " Host:     $PG_HOST"
echo " Port:     $PG_PORT"
echo " Database: $PG_DB"
echo " User:     $PG_USER"
echo "============================================================"

export PGPASSWORD="$PG_PASS"

# 按文件名排序，依次执行 SQL
SQL_FILES=$(find "$MIGRATIONS_DIR" -name '*.sql' | sort)

if [ -z "$SQL_FILES" ]; then
    echo "[WARN] migrations 目录中没有找到 .sql 文件"
    exit 0
fi

for sql_file in $SQL_FILES; do
    filename=$(basename "$sql_file")
    echo ""
    echo ">> 执行: $filename"
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -f "$sql_file" -v ON_ERROR_STOP=1
    echo "   [OK] $filename 完成"
done

unset PGPASSWORD

echo ""
echo "============================================================"
echo " 全部 migration 执行完成!"
echo "============================================================"
