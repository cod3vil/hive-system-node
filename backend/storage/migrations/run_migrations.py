"""
数据库迁移工具 — 连接本地 Docker PostgreSQL 执行 SQL 迁移文件。

用法:
    python -m backend.storage.migrations.run_migrations
    # 或直接运行
    python backend/storage/migrations/run_migrations.py
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from backend.config.settings import get_config

try:
    import asyncpg
except ImportError:
    print("错误: 缺少 asyncpg 依赖，请执行: pip install asyncpg")
    sys.exit(1)


async def run_migrations():
    """按顺序执行所有 SQL 迁移文件。"""
    config = get_config()

    dsn = (
        f"postgresql://{config.postgres_user}:{config.postgres_password}"
        f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
    )

    print(f"连接数据库: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")

    try:
        conn = await asyncpg.connect(dsn)
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}")
        sys.exit(1)

    try:
        # 创建迁移记录表 (幂等)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id SERIAL PRIMARY KEY,
                filename TEXT UNIQUE NOT NULL,
                applied_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # 查询已执行过的迁移
        applied = {
            row["filename"]
            for row in await conn.fetch("SELECT filename FROM _migrations")
        }

        # 收集待执行的迁移文件
        migrations_dir = Path(__file__).parent
        migration_files = sorted(migrations_dir.glob("*.sql"))

        if not migration_files:
            print("未找到 SQL 迁移文件")
            return

        print(f"共发现 {len(migration_files)} 个迁移文件，已执行 {len(applied)} 个\n")

        executed = 0
        for migration_file in migration_files:
            name = migration_file.name

            if name in applied:
                print(f"  ✓ {name} (已执行，跳过)")
                continue

            print(f"  ▶ 正在执行 {name} ...")
            sql = migration_file.read_text(encoding="utf-8")

            try:
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO _migrations (filename) VALUES ($1)", name
                    )
                print(f"  ✓ {name} 执行成功")
                executed += 1
            except Exception as e:
                print(f"  ✗ {name} 执行失败: {e}")
                sys.exit(1)

        print(f"\n{'=' * 50}")
        if executed:
            print(f"本次执行了 {executed} 个迁移")
        else:
            print("所有迁移均已是最新状态，无需操作")
        print(f"{'=' * 50}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())
