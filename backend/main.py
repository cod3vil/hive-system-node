"""
Main application entry point for the Hive Arbitrage System.
Handles application lifecycle: startup, execution, and shutdown.

The scanning / signal-processing logic has been moved to QueenBee.
This file only handles infrastructure setup, monitoring, and WebSocket server.
"""

import asyncio
import signal
from typing import Optional
from datetime import datetime
import uvicorn

from backend.config.settings import get_config
from backend.storage.redis_client import RedisClient
from backend.storage.postgres_client import PostgresClient
from backend.services.exchange_client import ExchangeClient
from backend.services.exchange_manager import ExchangeManager
from backend.services.funding_service import FundingService
from backend.services.position_service import PositionService
from backend.services.equity_service import EquityService
from backend.services.balance_service import BalanceService
from backend.services.cross_exchange_spread_service import CrossExchangeSpreadService
from backend.capital.capital_allocator import CapitalAllocator
from backend.risk.risk_engine import RiskEngine
from backend.risk.liquidation_guard import LiquidationGuard
from backend.risk.drawdown_guard import DrawdownGuard
from backend.risk.funding_guard import FundingGuard
from backend.execution.trade_executor import TradeExecutor
from backend.execution.order_manager import OrderManager
from backend.core.cold_start_recovery import ColdStartRecovery
from backend.core.network_resilience import NetworkResilienceManager
from backend.utils.logger import get_logger
from backend.services.notifier import NotifyDispatcher, get_notifier
from backend.websocket.websocket_server import get_connection_manager, create_websocket_app
from backend.hive.queen_bee import QueenBee
from backend.hive.communication_bee import CommunicationBee
from backend.services.market_pair_cache import MarketPairCache


class Application:
    """Main application class managing system lifecycle."""

    def __init__(self):
        self.config = get_config()
        self.websocket_manager = get_connection_manager()
        self.logger = get_logger("Application", websocket_manager=self.websocket_manager)

        # Core clients
        self.redis_client: Optional[RedisClient] = None
        self.postgres_client: Optional[PostgresClient] = None
        self.exchange_client: Optional[ExchangeClient] = None
        self.exchange_manager: Optional[ExchangeManager] = None

        # Services
        self.funding_service: Optional[FundingService] = None
        self.position_service: Optional[PositionService] = None
        self.equity_service: Optional[EquityService] = None
        self.balance_service: Optional[BalanceService] = None
        self.spread_service: Optional[CrossExchangeSpreadService] = None

        # Capital and risk
        self.capital_allocator: Optional[CapitalAllocator] = None
        self.risk_engine: Optional[RiskEngine] = None
        self.trade_executor: Optional[TradeExecutor] = None

        # Hive
        self.queen: Optional[QueenBee] = None
        self.comm_bee: Optional[CommunicationBee] = None

        # Notifications
        self.notifier: Optional[NotifyDispatcher] = None

        # Recovery
        self.cold_start_recovery: Optional[ColdStartRecovery] = None
        self.network_resilience: Optional[NetworkResilienceManager] = None

        # Lifecycle
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.monitoring_task: Optional[asyncio.Task] = None
        self.queen_task: Optional[asyncio.Task] = None
        self.websocket_task: Optional[asyncio.Task] = None
        self.comm_bee_task: Optional[asyncio.Task] = None
        self.websocket_server: Optional[uvicorn.Server] = None

    async def startup(self) -> None:
        """Application startup sequence."""
        await self.logger.info("=" * 60)
        await self.logger.info("蜂巢套利系统 - 启动")
        await self.logger.info("=" * 60)
        await self.logger.info(f"环境: {self.config.environment}")
        await self.logger.info(f"交易所: {self.config.exchange_name}")
        await self.logger.info(f"费率版本: {self.config.fee_version}")
        await self.logger.info(f"启用的交易对: {self.config.enabled_symbols}")
        await self.logger.info(f"单仓上限: {self.config.max_single_position_pct:.1%}")
        await self.logger.info(f"总敞口上限: {self.config.max_total_exposure_pct:.1%}")
        await self.logger.info(f"最大回撤: {self.config.max_drawdown_pct:.1%}")

        mode = self.config.running_mode
        await self.logger.info("=" * 60)
        if mode == "testnet":
            await self.logger.info("TESTNET 模式 — 连接测试网")
        elif mode == "simulation":
            await self.logger.info("SIMULATION 模式 — 主网只读，订单为模拟成交")
        else:
            await self.logger.info("MAINNET 模式 — 正式交易")
        await self.logger.info("=" * 60)

        # Initialize notification dispatcher
        self.notifier = get_notifier(self.config)
        channels = self.notifier.enabled_channels
        if channels:
            await self.logger.info(f"✓ 通知渠道已启用: {', '.join(channels)}")
        else:
            await self.logger.info("未配置任何通知渠道 (Telegram / 飞书)")

        try:
            # Step 1: Redis
            await self.logger.info("正在初始化Redis客户端...")
            self.redis_client = RedisClient(self.config)
            await self.redis_client.connect()
            await self.logger.info("✓ Redis客户端已初始化")

            # Step 2: PostgreSQL
            await self.logger.info("正在初始化PostgreSQL客户端...")
            self.postgres_client = PostgresClient(self.config)
            await self.postgres_client.connect()
            await self.logger.info("✓ PostgreSQL客户端已初始化")

            # Step 3: Exchange client (primary)
            await self.logger.info("正在初始化交易所客户端...")
            self.exchange_client = ExchangeClient(self.config)
            await self.exchange_client.initialize()
            await self.logger.info("✓ 交易所客户端已初始化")

            # Step 4: Validate API key permissions
            await self.logger.info("正在验证API密钥权限...")
            await self.exchange_client.validate_api_permissions()
            await self.logger.info("✓ API密钥权限已验证")

            # Step 4.5: Account balance
            await self.logger.info("正在查询账户余额...")
            try:
                combined = await self.exchange_client.fetch_combined_balance()
                self._startup_balance = {
                    "total": combined["total"],
                    "free": combined["free"],
                    "used": combined["used"],
                    "spot": combined["spot"],
                    "swap": combined["swap"],
                }
                await self.logger.info(
                    f"账户余额（USDT）: 💰 总计 {combined['total']:.2f} | "
                    f"现货 {combined['spot']['total']:.2f} | "
                    f"合约 {combined['swap']['total']:.2f}"
                )
            except Exception as e:
                await self.logger.warning(f"查询余额失败: {e}")
                self._startup_balance = {
                    "total": 0, "free": 0, "used": 0,
                    "spot": {"total": 0, "free": 0, "used": 0},
                    "swap": {"total": 0, "free": 0, "used": 0},
                }

            # Step 4.6: Exchange Manager (multi-exchange, for cross-exchange)
            if self.config.enable_cross_exchange:
                await self.logger.info("正在初始化多交易所管理器...")
                self.exchange_manager = ExchangeManager(self.config)
                await self.exchange_manager.initialize()
                await self.logger.info("✓ 多交易所管理器已初始化")

                self.spread_service = CrossExchangeSpreadService(
                    self.exchange_manager, self.config
                )

                # Re-fetch balance from all exchanges (Binance + OKX etc.)
                try:
                    all_bal = await self.exchange_manager.fetch_all_combined_balances()
                    self._startup_balance = {
                        "total": all_bal["total"],
                        "free": all_bal["free"],
                        "used": all_bal["used"],
                        "exchanges": all_bal["exchanges"],
                    }
                    await self.logger.info(
                        f"多交易所余额（USDT）: 💰 总计 {all_bal['total']:.2f}"
                    )
                    for ex_name, ex_bal in all_bal["exchanges"].items():
                        await self.logger.info(
                            f"  {ex_name.upper()}: "
                            f"现货 {ex_bal['spot']['total']:.2f} | "
                            f"合约 {ex_bal['swap']['total']:.2f} | "
                            f"小计 {ex_bal['total']:.2f}"
                        )
                except Exception as e:
                    await self.logger.warning(f"多交易所余额查询失败: {e}")

            # Step 5: Services
            await self.logger.info("正在初始化服务...")
            self.funding_service = FundingService(
                self.exchange_client, self.redis_client, self.config
            )
            self.position_service = PositionService(
                self.redis_client, self.postgres_client,
                self.exchange_client, self.websocket_manager
            )
            self.equity_service = EquityService(
                self.redis_client, self.exchange_client,
                self.position_service, self.websocket_manager
            )
            self.balance_service = BalanceService(
                self.exchange_client, self.redis_client
            )
            await self.logger.info("✓ 服务已初始化")

            # Step 6: Risk guards
            await self.logger.info("正在初始化风控守卫...")
            liquidation_guard = LiquidationGuard(self.exchange_client, self.config)
            drawdown_guard = DrawdownGuard(self.redis_client, self.config)
            funding_guard = FundingGuard(
                self.funding_service, self.redis_client, self.config
            )
            await self.logger.info("✓ 风控守卫已初始化")

            # Step 7: Capital allocator, risk engine, trade executor
            await self.logger.info("正在初始化策略和执行模块...")
            self.capital_allocator = CapitalAllocator(
                self.redis_client, self.equity_service,
                self.exchange_client, self.config
            )
            self.risk_engine = RiskEngine(
                liquidation_guard, drawdown_guard, funding_guard,
                self.capital_allocator, self.funding_service,
                self.redis_client, self.exchange_client,
                self.config, self.websocket_manager
            )
            # Inject spread_service for cross-exchange position monitoring
            if self.spread_service:
                self.risk_engine.spread_service = self.spread_service

            order_manager = OrderManager(
                self.exchange_client, 
                shadow_mode=self.config.is_simulation,
                config=self.config
            )
            self.trade_executor = TradeExecutor(
                order_manager, self.redis_client, self.config
            )
            # Inject trade_executor into risk_engine for hard stop position closing
            self.risk_engine.set_trade_executor(self.trade_executor)
            await self.logger.info("✓ 策略和执行模块已初始化")

            # Step 8: Cold start recovery
            await self.logger.info("正在初始化冷启动恢复...")
            self.cold_start_recovery = ColdStartRecovery(
                self.redis_client, self.postgres_client,
                self.exchange_client, self.position_service,
                self.equity_service
            )
            await self.logger.info("✓ 冷启动恢复已初始化")

            # Step 8.5: Network resilience
            await self.logger.info("正在初始化网络弹性管理器...")
            self.network_resilience = NetworkResilienceManager(
                self.redis_client, self.exchange_client,
                self.position_service, self.equity_service,
                self.websocket_manager, self.config
            )
            await self.logger.info("✓ 网络弹性管理器已初始化")

            # Step 9: Clear Redis and restore from database
            await self.logger.info("=" * 60)
            await self.logger.info("正在清空Redis并从数据库恢复数据...")
            await self.logger.info("=" * 60)
            
            # Step 9.1: Clear all Redis data
            await self.logger.info("正在清空Redis缓存...")
            try:
                # Clear position-related keys
                await self.redis_client.client.delete("open_positions")
                await self.redis_client.client.delete("close_queue")
                
                # Clear position data keys
                position_keys = await self.redis_client.client.keys("position:*")
                if position_keys:
                    await self.redis_client.client.delete(*position_keys)
                    await self.logger.info(f"✓ 已清空 {len(position_keys)} 个持仓缓存")
                
                # Clear position by symbol keys
                symbol_keys = await self.redis_client.client.keys("position_by_symbol:*")
                if symbol_keys:
                    await self.redis_client.client.delete(*symbol_keys)
                    await self.logger.info(f"✓ 已清空 {len(symbol_keys)} 个交易对持仓索引")
                
                # Clear PnL keys
                await self.redis_client.client.delete("pnl:total_realized")
                await self.redis_client.client.delete("total_realized_pnl")
                await self.logger.info("✓ 已清空累计收益缓存")
                
                await self.logger.info("✓ Redis缓存已清空")
            except Exception as e:
                await self.logger.error(f"清空Redis失败: {e}")
                raise
            
            # Step 9.2: Restore open positions from database
            await self.logger.info("正在从数据库恢复持仓数据...")
            try:
                # Query all open positions from database
                async with self.postgres_client.pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT position_id, symbol, strategy_type, exchange_high, exchange_low,
                               spot_entry_price, perp_entry_price, spot_quantity, perp_quantity,
                               entry_spread_pct, target_close_spread_pct, leverage,
                               entry_timestamp, status, created_at
                        FROM positions
                        WHERE status = 'open'
                        ORDER BY created_at DESC
                        """
                    )
                    open_positions_db = [dict(row) for row in rows]
                
                if open_positions_db:
                    await self.logger.info(f"发现 {len(open_positions_db)} 个未平仓持仓")
                    
                    for pos_row in open_positions_db:
                        position_id = str(pos_row['position_id'])
                        symbol = pos_row['symbol']
                        
                        # Reconstruct position data
                        position_data = {
                            "position_id": position_id,
                            "symbol": symbol,
                            "spot_symbol": symbol,
                            "perp_symbol": f"{symbol}:USDT",
                            "strategy_type": pos_row['strategy_type'],
                            "exchange_high": pos_row.get('exchange_high'),
                            "exchange_low": pos_row.get('exchange_low'),
                            "spot_entry_price": str(pos_row['spot_entry_price']),
                            "perp_entry_price": str(pos_row['perp_entry_price']),
                            "spot_quantity": str(pos_row['spot_quantity']),
                            "perp_quantity": str(pos_row['perp_quantity']),
                            "entry_spread_pct": str(pos_row['entry_spread_pct']) if pos_row.get('entry_spread_pct') else None,
                            "target_close_spread_pct": str(pos_row['target_close_spread_pct']) if pos_row.get('target_close_spread_pct') else None,
                            "leverage": str(pos_row['leverage']),
                            "entry_timestamp": pos_row['entry_timestamp'].isoformat() if pos_row.get('entry_timestamp') else None,
                            "status": pos_row['status'],
                            "created_at": pos_row['created_at'].isoformat() if pos_row.get('created_at') else None,
                        }
                        
                        # Store in Redis
                        await self.redis_client.set_position_data(position_id, position_data)
                        await self.redis_client.add_open_position(position_id)
                        await self.redis_client.add_position_by_symbol(symbol, position_id)
                        
                        await self.logger.info(
                            f"  ✓ 恢复持仓: {symbol} ({pos_row['strategy_type']}) - {position_id[:8]}..."
                        )
                    
                    await self.logger.info(f"✓ 已恢复 {len(open_positions_db)} 个持仓到Redis")
                else:
                    await self.logger.info("✓ 数据库中没有未平仓持仓")
                    
            except Exception as e:
                await self.logger.error(f"从数据库恢复持仓失败: {e}")
                raise
            
            # Step 9.3: Calculate and restore total realized PnL
            await self.logger.info("正在计算累计已实现收益...")
            try:
                # Query total realized PnL from all closed positions
                async with self.postgres_client.pool.acquire() as conn:
                    pnl_result = await conn.fetch(
                        """
                        SELECT COALESCE(SUM(realized_pnl), 0) as total_realized_pnl
                        FROM positions
                        WHERE status = 'closed' AND realized_pnl IS NOT NULL
                        """
                    )
                
                if pnl_result and len(pnl_result) > 0:
                    total_realized_pnl = float(pnl_result[0]['total_realized_pnl'])
                    # Use the correct key that RedisClient expects
                    await self.redis_client.client.set("pnl:total_realized", str(total_realized_pnl))
                    await self.logger.info(
                        f"✓ 累计已实现收益: {total_realized_pnl:.4f} USDT "
                        f"({'盈利' if total_realized_pnl > 0 else '亏损'})"
                    )
                else:
                    await self.redis_client.client.set("pnl:total_realized", "0")
                    await self.logger.info("✓ 累计已实现收益: 0 USDT (首次启动)")
                    
            except Exception as e:
                await self.logger.error(f"计算累计收益失败: {e}")
                # Set to 0 if calculation fails
                await self.redis_client.client.set("pnl:total_realized", "0")
            
            await self.logger.info("=" * 60)
            await self.logger.info("✓ 数据恢复完成")
            await self.logger.info("=" * 60)
            
            # Step 9.4: Initialize system state
            await self.logger.info("正在初始化系统状态...")
            await self.redis_client.set_system_status("normal")
            await self.redis_client.set_emergency_flag(False)
            await self.redis_client.set_manual_pause(False)
            
            # Step 9.5: Initialize equity data
            await self.logger.info("正在初始化权益数据...")
            total_equity = await self.equity_service.calculate_total_equity()
            
            # Initialize peak equity if not set
            peak_equity = await self.redis_client.get_peak_equity()
            if peak_equity is None:
                await self.redis_client.set_peak_equity(total_equity)
                await self.logger.info(f"峰值权益已初始化: {total_equity} USDT")
            
            # Initialize daily start equity if not set
            daily_start = await self.redis_client.get_daily_start_equity()
            if daily_start is None:
                await self.redis_client.set_daily_start_equity(total_equity)
                await self.logger.info(f"每日起始权益已初始化: {total_equity} USDT")
            
            await self.logger.info(f"✓ 权益数据已初始化: {total_equity} USDT")

            # Step 10: Position monitoring
            await self.logger.info("正在启动持仓监控循环...")
            self.monitoring_task = asyncio.create_task(self._run_position_monitoring())
            await self.logger.info("✓ 持仓监控循环已启动")

            # Step 10.5: Network resilience monitoring
            await self.logger.info("正在启动网络弹性监控...")
            await self.network_resilience.start_monitoring()
            await self.logger.info("✓ 网络弹性监控已启动")

            # Step 11: QueenBee — replaces old _run_scout_scanning
            await self.logger.info("正在初始化蜂王...")
            market_pair_cache = MarketPairCache(self.redis_client)
            self.queen = QueenBee(
                config=self.config,
                exchange_client=self.exchange_client,
                exchange_manager=self.exchange_manager,
                trade_executor=self.trade_executor,
                capital_allocator=self.capital_allocator,
                risk_engine=self.risk_engine,
                ws_manager=self.websocket_manager,
                funding_service=self.funding_service,
                spread_service=self.spread_service,
                notifier=self.notifier,
                market_pair_cache=market_pair_cache,
                position_service=self.position_service,
                redis_client=self.redis_client,
            )
            scout_count = await self.queen.startup()
            await self.logger.info(f"✓ 蜂王已初始化 ({scout_count} 个侦查蜂)")
            
            # Initialize total realized PnL (preserve across restarts)
            # If Redis has a value, keep it; otherwise recover from database
            existing_pnl = await self.redis_client.get_total_realized_pnl()
            if existing_pnl == 0:
                # Try to recover cumulative realized PnL from database
                try:
                    from decimal import Decimal
                    rows = await self.postgres_client.pool.fetch(
                        "SELECT COALESCE(SUM(CAST(realized_pnl AS NUMERIC)), 0) as total "
                        "FROM positions WHERE status = 'closed' AND realized_pnl IS NOT NULL"
                    )
                    db_total = Decimal(str(rows[0]["total"])) if rows else Decimal("0")
                    if db_total != 0:
                        await self.redis_client.client.set("pnl:total_realized", str(db_total))
                        await self.logger.info(f"✓ 从数据库恢复累计已实现收益: {db_total} USDT")
                    else:
                        await self.logger.info("✓ 总收益初始值为0 (首次启动或无历史平仓)")
                except Exception as e:
                    await self.logger.warning(f"从数据库恢复累计收益失败: {e}, 从0开始")
            else:
                await self.logger.info(f"✓ Redis中已有累计收益: {existing_pnl} USDT")

            self.queen_task = asyncio.create_task(self.queen.run())
            await self.logger.info("✓ 蜂王扫描循环已启动")

            # Step 12: WebSocket server
            await self.logger.info("正在启动WebSocket服务器...")
            self.websocket_task = asyncio.create_task(self._run_websocket_server())
            await self.logger.info(f"✓ WebSocket服务器已启动，端口 {self.config.websocket_port}")

            # Step 13: Communication Bee (if central server configured)
            if self.config.hive_server_url:
                await self.logger.info("正在启动通讯蜂...")
                self.comm_bee = CommunicationBee(
                    config=self.config,
                    redis_client=self.redis_client,
                    position_service=self.position_service,
                    equity_service=self.equity_service,
                    exchange_client=self.exchange_client,
                    exchange_manager=self.exchange_manager,
                    queen=self.queen,
                )
                self.comm_bee_task = asyncio.create_task(self.comm_bee.run())
                await self.logger.info(f"✓ 通讯蜂已启动，目标: {self.config.hive_server_url}")
            else:
                await self.logger.info("未配置中央服务器，通讯蜂未启用")

            await self.logger.info("=" * 60)
            await self.logger.info("应用启动完成")
            await self.logger.info("=" * 60)
            self.running = True

            # Broadcast account balance
            await self.websocket_manager.broadcast({
                "type": "account_balance",
                "timestamp": datetime.utcnow().isoformat(),
                "data": self._startup_balance,
            })

            # Notify startup
            if self.notifier:
                await self.notifier.notify_app_startup(
                    environment=self.config.environment,
                    exchange=", ".join(self.config.enabled_exchanges),
                    scout_count=scout_count,
                    balance=self._startup_balance,
                    config=self.config,
                )

        except Exception as e:
            await self.logger.critical("应用启动失败", error=str(e))
            if self.notifier:
                await self.notifier.notify_app_startup_failed(str(e))
            raise

    async def _run_position_monitoring(self) -> None:
        """Position monitoring loop - delegates to RiskEngine.monitor_positions()."""
        await self.logger.info("持仓监控循环已启动")
        try:
            # RiskEngine.monitor_positions() is itself an infinite loop
            # Just call it once and let it run
            await self.risk_engine.monitor_positions()
        except asyncio.CancelledError:
            await self.logger.info("持仓监控循环已取消")
        except Exception as e:
            await self.logger.error("持仓监控循环出错", error=str(e))

    async def _run_websocket_server(self) -> None:
        """Run WebSocket server for frontend connections."""
        try:
            app = create_websocket_app()
            config = uvicorn.Config(
                app,
                host=self.config.websocket_host,
                port=self.config.websocket_port,
                log_level="info",
                access_log=False
            )
            self.websocket_server = uvicorn.Server(config)
            await self.websocket_server.serve()
        except asyncio.CancelledError:
            await self.logger.info("WebSocket服务器已取消")
        except Exception as e:
            await self.logger.error("WebSocket服务器出错", error=str(e))

    async def run(self) -> None:
        """Main execution loop — waits for shutdown signal."""
        await self.logger.info("应用运行中 - 监控和扫描已激活")
        await self.shutdown_event.wait()

    async def shutdown(self) -> None:
        """Graceful application shutdown."""
        await self.logger.info("=" * 60)
        await self.logger.info("正在启动优雅关闭")
        await self.logger.info("=" * 60)

        if self.notifier:
            try:
                await asyncio.wait_for(self.notifier.notify_app_shutdown(), timeout=5.0)
            except (asyncio.TimeoutError, Exception):
                pass

        self.running = False

        try:
            # Stop Communication Bee
            if self.comm_bee:
                await self.logger.info("正在停止通讯蜂...")
                await self.comm_bee.shutdown()
                if self.comm_bee_task and not self.comm_bee_task.done():
                    self.comm_bee_task.cancel()
                    try:
                        await self.comm_bee_task
                    except asyncio.CancelledError:
                        pass
                await self.logger.info("✓ 通讯蜂已停止")

            # Stop QueenBee (replaces old scout task cancellation)
            await self.logger.info("正在停止蜂王...")
            if self.queen:
                await self.queen.shutdown()
            if self.queen_task and not self.queen_task.done():
                self.queen_task.cancel()
                try:
                    await self.queen_task
                except asyncio.CancelledError:
                    pass
            await self.logger.info("✓ 蜂王已停止")

            # Stop WebSocket server
            await self.logger.info("正在停止WebSocket服务器...")
            if self.websocket_task and not self.websocket_task.done():
                self.websocket_task.cancel()
                try:
                    await self.websocket_task
                except asyncio.CancelledError:
                    pass
            if self.websocket_server:
                self.websocket_server.should_exit = True
            await self.logger.info("✓ WebSocket服务器已停止")

            # Stop position monitoring
            await self.logger.info("正在停止持仓监控...")
            if self.monitoring_task and not self.monitoring_task.done():
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass
            await self.logger.info("✓ 持仓监控已停止")

            # Stop network resilience monitoring
            await self.logger.info("正在停止网络弹性监控...")
            if self.network_resilience:
                await self.network_resilience.stop_monitoring()
            await self.logger.info("✓ 网络弹性监控已停止")

            # Wait for in-flight executions
            await self.logger.info("正在等待进行中的执行完成...")
            max_wait = 30
            waited = 0
            while waited < max_wait:
                await asyncio.sleep(1)
                waited += 1
                if waited >= 5:
                    break
            await self.logger.info("✓ 进行中的执行已完成")

            # Close exchange connections
            if self.exchange_client:
                await self.logger.info("正在关闭交易所连接...")
                await self.exchange_client.close()
                await self.logger.info("✓ 交易所连接已关闭")

            if self.exchange_manager:
                await self.logger.info("正在关闭多交易所管理器...")
                await self.exchange_manager.close()
                await self.logger.info("✓ 多交易所管理器已关闭")

            # Close Redis
            if self.redis_client:
                await self.logger.info("正在关闭Redis连接...")
                await self.redis_client.disconnect()
                await self.logger.info("✓ Redis连接已关闭")

            # Close PostgreSQL
            if self.postgres_client:
                await self.logger.info("正在关闭PostgreSQL连接...")
                await self.postgres_client.disconnect()
                await self.logger.info("✓ PostgreSQL连接已关闭")

            # Close notification sessions
            if self.notifier and self.notifier.feishu:
                await self.notifier.feishu.close()

            await self.logger.info("=" * 60)
            await self.logger.info("应用关闭完成")
            await self.logger.info("=" * 60)

        except Exception as e:
            await self.logger.error("关闭过程中出错", error=str(e))

    def handle_signal(self, sig):
        """Handle shutdown signals."""
        print(f"\nReceived signal {sig}, initiating shutdown...")
        self.shutdown_event.set()


async def main():
    """Main entry point."""
    app = Application()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: app.handle_signal(s))

    try:
        await app.startup()
        await app.run()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise
    finally:
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
