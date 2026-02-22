.PHONY: help install run stop migrate logs test docker-build docker-up docker-down docker-logs docker-reload docker-restart

# ── 帮助 ──
help:
	@echo "蜂巢套利系统 - 常用命令"
	@echo ""
	@echo "本地运行 (推荐):"
	@echo "  make install     - 安装 Python 依赖"
	@echo "  make run         - 启动应用"
	@echo "  make stop        - 停止应用 (发送 SIGTERM 优雅关闭)"
	@echo "  make migrate     - 执行数据库迁移"
	@echo "  make logs        - 查看日志 (Ctrl+C 退出)"
	@echo "  make test        - 运行测试"
	@echo ""
	@echo "前置服务:"
	@echo "  make check-deps  - 检查 PostgreSQL 和 Redis 是否运行"
	@echo "  make redis-cli   - 打开 Redis 命令行"
	@echo "  make psql        - 打开 PostgreSQL 命令行"
	@echo ""
	@echo "Docker 部署:"
	@echo "  make docker-build   - 构建 Docker 镜像"
	@echo "  make docker-up      - 启动 Docker 容器"
	@echo "  make docker-down    - 停止 Docker 容器"
	@echo "  make docker-logs    - 查看 Docker 日志"
	@echo "  make docker-reload  - 重启容器 (改了 .env 或代码后用这个)"
	@echo "  make docker-restart - 重建并重启容器 (改了 Dockerfile/依赖后用)"

# ── 本地运行 ──
install:
	pip install -r requirements.txt
	@echo "✓ 依赖安装完成"

run:
	@make check-deps --silent 2>/dev/null || true
	python -m backend.main

stop:
	@pkill -f "python -m backend.main" && echo "✓ 已发送停止信号" || echo "未找到运行中的进程"

migrate:
	python -m backend.storage.migrations.run_migrations

logs:
	tail -f logs/*.log

test:
	pytest tests/ -v

# ── 前置服务检查 ──
check-deps:
	@echo "检查前置服务..."
	@redis-cli ping > /dev/null 2>&1 && echo "  ✓ Redis 运行中" || echo "  ✗ Redis 未运行 — 请启动: brew services start redis / sudo systemctl start redis"
	@pg_isready > /dev/null 2>&1 && echo "  ✓ PostgreSQL 运行中" || echo "  ✗ PostgreSQL 未运行 — 请启动: brew services start postgresql / sudo systemctl start postgresql"

redis-cli:
	redis-cli

psql:
	psql -h localhost -U postgres -d stable_cash_carry

# ── Docker 部署 (仅容器化后端应用，PG/Redis 使用宿主机) ──
docker-build:
	docker compose build

docker-up:
	docker compose up -d
	@echo "✓ 容器已启动，查看日志: make docker-logs"

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

docker-reload:
	docker compose restart
	@echo "✓ 容器已重启 (适用于修改 .env 或 Python 代码)"

docker-restart:
	docker compose up -d --build
	@echo "✓ 容器已重建并启动 (适用于修改 Dockerfile 或 requirements.txt)"
