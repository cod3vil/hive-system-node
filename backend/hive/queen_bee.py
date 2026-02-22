"""
QueenBee - the central controller of the Hive architecture.

Responsibilities:
- Reads enabled strategies from StrategyRegistry
- Creates N Scout instances per strategy
- Runs the main scan loop: distribute symbols → gather signals → dispatch Workers
- Controls Worker concurrency via asyncio.Semaphore
"""

import asyncio
import math
from datetime import datetime
from typing import List, Dict, Optional

from core.base_strategy import BaseScout, BaseSignal
from core.strategy_registry import StrategyRegistry
from utils.logger import get_logger

logger = get_logger("QueenBee")


class QueenBee:
    """Central controller: coordinates Scouts and dispatches Workers."""

    def __init__(
        self,
        config,
        exchange_client,
        exchange_manager,
        trade_executor,
        capital_allocator,
        risk_engine,
        ws_manager,
        funding_service=None,
        spread_service=None,
        notifier=None,
        market_pair_cache=None,
        position_service=None,
        redis_client=None,
    ):
        self.config = config
        self.exchange_client = exchange_client      # single-exchange client (Binance)
        self.exchange_manager = exchange_manager    # multi-exchange manager (may be None)
        self.trade_executor = trade_executor
        self.capital_allocator = capital_allocator
        self.risk_engine = risk_engine
        self.ws_manager = ws_manager
        self.funding_service = funding_service
        self.spread_service = spread_service
        self.notifier = notifier
        self.market_pair_cache = market_pair_cache
        self.position_service = position_service
        self.redis_client = redis_client

        self.worker_semaphore = asyncio.Semaphore(
            getattr(config, "max_concurrent_workers", 5)
        )

        # strategy_type -> [Scout, ...]
        self._scouts: Dict[str, List[BaseScout]] = {}
        self._worker_tasks: List[asyncio.Task] = []
        self._monitor_tasks: Dict[str, asyncio.Task] = {}
        self._running = False

    async def startup(self):
        """Discover enabled strategies and create Scout instances."""
        # Strategy packages are imported by backend/strategies/__init__.py,
        # which triggers self-registration into StrategyRegistry.
        import strategies  # noqa: F401

        enabled = StrategyRegistry.get_enabled_strategies(self.config)
        await logger.info(f"蜂王启动 — 启用的策略: {enabled}")

        # scout_max_workers is the TOTAL scout count across all strategies.
        # Distribute using round-robin alternating allocation.
        total_scouts_cfg = getattr(self.config, "scout_max_workers", 3)
        num_strategies = len(enabled)
        if num_strategies == 0:
            await logger.warning("蜂王: 没有启用的策略")
            return 0

        counts = self._compute_alternating_counts(total_scouts_cfg, num_strategies)

        for idx, st in enumerate(enabled):
            scout_cls = StrategyRegistry.get_scout_class(st)
            count = counts[idx]
            scouts = []

            for i in range(1, count + 1):
                # Choose the right exchange handle
                if st == "cash_carry":
                    em = self.exchange_client
                elif st == "cross_exchange":
                    em = self.exchange_manager
                else:
                    em = self.exchange_client

                scout = scout_cls(
                    scout_id=i,
                    config=self.config,
                    exchange_manager=em,
                    ws_manager=self.ws_manager,
                )
                # Inject strategy-specific services
                if st == "cash_carry" and self.funding_service:
                    scout.set_funding_service(self.funding_service)
                if st == "cross_exchange" and self.spread_service:
                    scout.set_spread_service(self.spread_service)

                # Inject market pair cache
                if self.market_pair_cache and hasattr(scout, "set_market_pair_cache"):
                    scout.set_market_pair_cache(self.market_pair_cache)

                scouts.append(scout)

            self._scouts[st] = scouts
            await logger.info(f"  策略 [{st}]: 创建 {len(scouts)} 个侦查蜂")

        total_scouts = sum(len(s) for s in self._scouts.values())
        await logger.info(f"蜂王就绪 — 共 {total_scouts} 个侦查蜂")
        return total_scouts

    async def run(self):
        """Main scan loop — runs until cancelled.

        Workers are dispatched *immediately* when a scout discovers a valid
        signal (via the on_signal callback), eliminating the delay between
        scan discovery and trade execution.
        """
        self._running = True
        await logger.info("蜂王主循环启动")

        while self._running:
            try:
                scan_start = datetime.utcnow()
                all_signals: List[BaseSignal] = []
                total_markets_scanned = 0

                # Collect streaming-dispatched worker tasks
                streaming_worker_tasks: List[asyncio.Task] = []
                streaming_signals: List[BaseSignal] = []

                async def _on_signal(sig: BaseSignal):
                    """Callback: immediately dispatch a worker for a valid signal."""
                    if not self._running:
                        return
                    task = asyncio.create_task(self._dispatch_worker(sig))
                    streaming_worker_tasks.append(task)
                    streaming_signals.append(sig)

                for strategy_type, scouts in self._scouts.items():
                    if not scouts:
                        continue

                    # Get symbols from the first scout
                    symbols = await scouts[0].get_scannable_symbols()
                    num_scouts = len(scouts)
                    total_markets_scanned += len(symbols)

                    await logger.info(
                        f"[{strategy_type}] 获取到 {len(symbols)} 个市场, "
                        f"分片给 {num_scouts} 个侦查蜂"
                    )

                    # Split symbols into chunks for each scout
                    if symbols:
                        chunk_size = math.ceil(len(symbols) / num_scouts)
                        chunks = [
                            symbols[i : i + chunk_size]
                            for i in range(0, len(symbols), chunk_size)
                        ]
                    else:
                        chunks = [[] for _ in scouts]

                    # Run scouts in parallel — workers fire as signals are found
                    tasks = [
                        scout.scan(chunk, on_signal=_on_signal)
                        for scout, chunk in zip(scouts, chunks)
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            await logger.error(
                                f"[{strategy_type}] 侦查蜂 #{i+1} 出错: {result}"
                            )
                        else:
                            all_signals.extend(result)

                # Wait for any still-running streaming worker tasks
                worker_results = []
                if streaming_worker_tasks:
                    raw_results = await asyncio.gather(
                        *streaming_worker_tasks, return_exceptions=True
                    )
                    for i, r in enumerate(raw_results):
                        sig = streaming_signals[i] if i < len(streaming_signals) else None
                        if isinstance(r, Exception):
                            await logger.error(f"工蜂任务异常: {r}")
                            worker_results.append({
                                "status": "failed",
                                "symbol": sig.symbol if sig else "?",
                                "strategy_type": sig.strategy_type if sig else "?",
                                "error": str(r),
                                "signal": sig,
                            })
                        elif r is not None:
                            if sig:
                                r["signal"] = sig
                            worker_results.append(r)

                # Compute stats
                scan_duration = (datetime.utcnow() - scan_start).total_seconds()
                valid_signals = [s for s in all_signals if s.is_valid()]

                # Broadcast aggregated status (scout_id=0 for summary)
                await self.ws_manager.broadcast_scout_status({
                    "scout_id": 0,
                    "total_markets": total_markets_scanned,
                    "total_scanned": len(all_signals),
                    "valid_opportunities": len(valid_signals),
                    "scan_duration_seconds": scan_duration,
                    "last_scan_time": scan_start.isoformat(),
                    "opportunities": [s.to_dict() for s in valid_signals[:10]],
                })

                # Refresh and broadcast account balance
                await self._refresh_balance()

                if valid_signals:
                    await logger.info(
                        f"蜂王汇总: {len(valid_signals)}/{len(all_signals)} 个有效机会, "
                        f"已实时派遣 {len(streaming_worker_tasks)} 个工蜂 "
                        f"(耗时 {scan_duration:.1f}秒)"
                    )

                # Send per-worker notifications + hive report
                await self._send_worker_notifications(
                    worker_results, scan_duration, total_markets_scanned, len(valid_signals),
                )

                # Process close queue (fallback for legacy/edge cases)
                await self._process_close_queue()

                # Update position prices and PnL
                await self._update_positions_pnl()

                await asyncio.sleep(self.config.funding_update_interval_seconds)

            except asyncio.CancelledError:
                await logger.info("蜂王主循环已取消")
                break
            except Exception as e:
                await logger.error(f"蜂王主循环出错: {e}")
                await asyncio.sleep(self.config.funding_update_interval_seconds)

    async def _refresh_balance(self):
        """Refresh and broadcast account balance after each scan cycle."""
        try:
            if self.exchange_manager and hasattr(self.exchange_manager, 'fetch_all_combined_balances'):
                data = await self.exchange_manager.fetch_all_combined_balances()
            else:
                data = await self.exchange_client.fetch_combined_balance()
            await self.ws_manager.broadcast({
                "type": "account_balance",
                "timestamp": datetime.utcnow().isoformat(),
                "data": data,
            })
        except Exception:
            pass

    async def _dispatch_worker(self, signal: BaseSignal) -> Optional[dict]:
        """Dispatch a single worker for a signal, respecting the semaphore."""
        async with self.worker_semaphore:
            try:
                worker_cls = StrategyRegistry.get_worker_class(signal.strategy_type)

                # Choose exchange handle
                if signal.strategy_type == "cash_carry":
                    em = self.exchange_client
                else:
                    em = self.exchange_manager

                worker = worker_cls(
                    config=self.config,
                    exchange_manager=em,
                    trade_executor=self.trade_executor,
                    capital_allocator=self.capital_allocator,
                    risk_engine=self.risk_engine,
                    ws_manager=self.ws_manager,
                    position_service=self.position_service,
                    redis_client=self.redis_client,
                )

                # Inject strategy-specific services for autonomous monitoring
                if signal.strategy_type == "cross_exchange" and self.spread_service:
                    worker.set_spread_service(self.spread_service)
                if signal.strategy_type == "cash_carry" and self.funding_service:
                    worker.set_funding_service(self.funding_service)
                if self.notifier:
                    worker.set_notifier(self.notifier)

                result = await worker.execute(signal)

                # On successful open, launch autonomous monitor task
                if result and result.get("status") == "opened":
                    position_id = result.get("position_id")
                    if position_id:
                        task = asyncio.create_task(
                            worker.monitor_and_close(position_id, signal)
                        )
                        self._monitor_tasks[position_id] = task
                        # Auto-cleanup when task finishes
                        task.add_done_callback(
                            lambda t, pid=position_id: self._monitor_tasks.pop(pid, None)
                        )
                        await logger.info(
                            f"[自治] 启动监控任务: {position_id} ({signal.strategy_type})"
                        )

                return result
            except Exception as e:
                await logger.error(
                    f"工蜂派遣失败 ({signal.strategy_type}/{signal.symbol}): {e}"
                )
                return None

    async def _send_worker_notifications(
        self,
        worker_results: list,
        scan_duration: float,
        total_markets: int,
        valid_count: int,
    ) -> None:
        """Send per-worker notifications and the hive summary report."""
        if not self.notifier:
            return

        opened = 0
        rejected = 0
        failed = 0
        opened_positions = []  # 只记录新开仓的持仓

        for r in worker_results:
            status = r.get("status", "")
            symbol = r.get("symbol", "?")
            strategy_type = r.get("strategy_type", "?")
            signal = r.get("signal")

            if status == "opened":
                opened += 1
                
                # 提取价格信息
                kwargs = {
                    "symbol": symbol,
                    "strategy_type": strategy_type,
                    "position_size": r.get("position_size", "?"),
                }
                
                # 根据策略类型添加不同的价格信息
                if strategy_type == "cross_exchange":
                    kwargs["exchange_high"] = r.get("exchange_high", "")
                    kwargs["exchange_low"] = r.get("exchange_low", "")
                    if signal:
                        kwargs["price_high"] = float(signal.price_high)
                        kwargs["price_low"] = float(signal.price_low)
                        kwargs["spread_pct"] = float(signal.spread_pct) * 100
                elif strategy_type == "cash_carry" and signal:
                    kwargs["spot_price"] = float(signal.spot_price)
                    kwargs["perp_price"] = float(signal.perp_price)
                
                # 只通知新开仓的持仓
                await self.notifier.notify_worker_opened(**kwargs)
                
                opened_positions.append({
                    "symbol": symbol,
                    "strategy_type": strategy_type,
                    "position_id": r.get("position_id", ""),
                })
            elif status == "rejected":
                rejected += 1
                # 不再通知被拒绝的持仓（避免重复通知已有持仓）
            else:
                failed += 1

        # 获取所有当前持仓信息
        all_positions = []
        if self.position_service:
            try:
                positions = await self.position_service.get_open_positions()
                all_positions = positions
            except Exception as e:
                await logger.error(f"获取持仓信息失败: {e}")

        # 获取交易所余额信息
        exchange_balances = {}
        try:
            if self.exchange_manager and hasattr(self.exchange_manager, 'fetch_all_combined_balances'):
                exchange_balances = await self.exchange_manager.fetch_all_combined_balances()
            else:
                exchange_balances = await self.exchange_client.fetch_combined_balance()
        except Exception as e:
            await logger.error(f"获取交易所余额失败: {e}")
        
        # 获取总已实现收益
        total_realized_pnl = 0
        daily_realized_pnl = 0
        monthly_realized_pnl = 0
        try:
            if self.redis_client:
                from decimal import Decimal
                total_realized_pnl = float(await self.redis_client.get_total_realized_pnl())
        except Exception as e:
            await logger.error(f"获取总收益失败: {e}")

        # 从数据库获取今日和本月已实现收益
        try:
            if self.position_service and hasattr(self.position_service, 'db') and self.position_service.db.pool:
                pool = self.position_service.db.pool
                from datetime import datetime
                now = datetime.utcnow()
                today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                rows = await pool.fetch(
                    "SELECT "
                    "  COALESCE(SUM(CASE WHEN updated_at >= $1 THEN CAST(realized_pnl AS NUMERIC) ELSE 0 END), 0) AS daily, "
                    "  COALESCE(SUM(CASE WHEN updated_at >= $2 THEN CAST(realized_pnl AS NUMERIC) ELSE 0 END), 0) AS monthly "
                    "FROM positions WHERE status = 'closed' AND realized_pnl IS NOT NULL",
                    today_start, month_start
                )
                if rows:
                    daily_realized_pnl = float(rows[0]["daily"])
                    monthly_realized_pnl = float(rows[0]["monthly"])
        except Exception as e:
            await logger.error(f"获取日/月收益失败: {e}")

        dispatched = len(worker_results)
        await self.notifier.notify_hive_report(
            scan_duration=scan_duration,
            total_markets=total_markets,
            valid_count=valid_count,
            dispatched=dispatched,
            opened=opened,
            rejected=rejected,
            failed=failed,
            all_positions=all_positions,
            opened_positions=opened_positions,
            exchange_balances=exchange_balances,
            total_realized_pnl=total_realized_pnl,
            daily_realized_pnl=daily_realized_pnl,
            monthly_realized_pnl=monthly_realized_pnl,
        )

    async def _process_close_queue(self):
        """Process positions in close queue."""
        try:
            close_queue = await self.redis_client.get_close_queue()
            
            if not close_queue:
                return
            
            await logger.info(f"处理平仓队列: {len(close_queue)} 个持仓待平仓")
            
            for position_id in close_queue:
                try:
                    # Get position data
                    position_data = await self.redis_client.get_position(position_id)
                    if not position_data:
                        await logger.warning(f"持仓 {position_id} 不存在，从队列移除")
                        await self.redis_client.remove_from_close_queue(position_id)
                        continue
                    
                    # Convert to Position object
                    from core.models import Position
                    position = Position.from_dict(position_data)
                    
                    await logger.info(
                        f"执行平仓: {position_id} ({position.symbol}), "
                        f"策略: {position.strategy_type}"
                    )
                    
                    # Execute close based on strategy type
                    if position.strategy_type == "cross_exchange":
                        result = await self._execute_cross_exchange_close(position)
                    else:
                        result = await self.trade_executor.execute_close_position(position)
                    
                    if result.success:
                        await logger.info(f"平仓成功: {position_id}")
                        
                        # Calculate holding time
                        from datetime import datetime
                        holding_hours = 0
                        if hasattr(position, 'entry_timestamp') and position.entry_timestamp:
                            if isinstance(position.entry_timestamp, datetime):
                                holding_hours = (datetime.utcnow() - position.entry_timestamp).total_seconds() / 3600
                            else:
                                try:
                                    entry_dt = datetime.fromisoformat(str(position.entry_timestamp).replace('Z', '+00:00'))
                                    holding_hours = (datetime.utcnow() - entry_dt).total_seconds() / 3600
                                except Exception:
                                    pass
                        
                        # Update position status in database and get realized PnL
                        realized_pnl = await self.position_service.close_position(
                            position_id=position_id,
                            spot_exit_price=result.spot_fill_price,
                            perp_exit_price=result.perp_fill_price,
                            close_reason=position_data.get("close_reason", "manual")
                        )

                        # Remove from close queue
                        await self.redis_client.remove_from_close_queue(position_id)

                        # Notify with actual realized PnL (not stale unrealized_pnl)
                        await self.notifier.notify_position_closed(
                            position_id=position_id,
                            symbol=position.symbol,
                            strategy_type=position.strategy_type,
                            pnl=float(realized_pnl),
                            close_reason=position_data.get("close_reason", "manual"),
                            holding_hours=holding_hours,
                            exchange_high=getattr(position, 'exchange_high', ''),
                            exchange_low=getattr(position, 'exchange_low', ''),
                        )
                    else:
                        await logger.error(
                            f"平仓失败: {position_id}, 错误: {result.error_message}"
                        )
                        # Keep in queue for retry
                        
                except Exception as e:
                    await logger.error(f"处理平仓 {position_id} 时出错: {e}")
                    # Keep in queue for retry
                    
        except Exception as e:
            await logger.error(f"处理平仓队列时出错: {e}")

    async def _execute_cross_exchange_close(self, position):
        """
        Execute cross-exchange position close via TradeExecutor.

        Delegates to TradeExecutor.execute_cross_exchange_close() which provides:
        - Shadow mode support
        - Atomic execution with rollback
        - Proper slippage control and fill waiting
        """
        await logger.info(
            f"执行跨市平仓(via TradeExecutor): {position.position_id}, "
            f"{position.exchange_low} (平多) ↔ {position.exchange_high} (平空)"
        )

        return await self.trade_executor.execute_cross_exchange_close(
            position, self.exchange_manager
        )

    async def _update_positions_pnl(self):
        """Update prices and PnL for all open positions."""
        if not self.position_service:
            return

        try:
            positions = await self.position_service.get_open_positions()
            if not positions:
                return

            await logger.debug(f"更新 {len(positions)} 个持仓的价格和PnL")

            total = len(positions)
            failed = 0
            for position in positions:
                try:
                    # 根据策略类型使用不同的价格更新逻辑
                    if position.strategy_type == "cross_exchange":
                        await self._update_cross_exchange_position_pnl(position)
                    else:
                        # cash_carry 使用单交易所
                        await self.position_service.update_position_prices(position)
                except Exception as e:
                    failed += 1
                    await logger.error(f"更新持仓 {position.position_id} 价格失败: {e}")

            # Alert if failure rate exceeds 30%
            if total > 0 and failed / total > 0.3:
                msg = (
                    f"持仓价格更新失败率过高: {failed}/{total} ({failed/total:.0%}), "
                    f"风控可能基于过时数据决策"
                )
                await logger.error(msg)
                if self.notifier:
                    await self.notifier.notify_warning(msg)

        except Exception as e:
            await logger.error(f"更新持仓PnL失败: {e}")
    
    async def _update_cross_exchange_position_pnl(self, position):
        """Update PnL for cross-exchange position by fetching prices from both exchanges."""
        try:
            if not self.exchange_manager:
                return
            
            # Get clients for both exchanges
            low_client = self.exchange_manager.get_client(position.exchange_low)
            high_client = self.exchange_manager.get_client(position.exchange_high)
            
            if not low_client or not high_client:
                await logger.warning(
                    f"无法获取交易所客户端: {position.exchange_low}, {position.exchange_high}"
                )
                return
            
            # Fetch current prices from both exchanges
            from decimal import Decimal
            low_ticker = await low_client.fetch_ticker(position.symbol)
            high_ticker = await high_client.fetch_ticker(position.symbol)
            
            current_spot_price = Decimal(str(low_ticker['last']))  # price on low exchange
            current_perp_price = Decimal(str(high_ticker['last']))  # price on high exchange
            
            # Update position with current prices
            position.current_spot_price = current_spot_price
            position.current_perp_price = current_perp_price
            
            # Calculate current spread
            if current_spot_price > 0:
                current_spread_pct = (current_perp_price - current_spot_price) / current_spot_price
                position.current_spread_pct = current_spread_pct
            
            # Calculate PnL
            unrealized_pnl = position.calculate_current_pnl()
            position.unrealized_pnl = unrealized_pnl
            
            # Update in database and Redis
            updates = {
                "current_spot_price": str(position.current_spot_price),
                "current_perp_price": str(position.current_perp_price),
                "current_spread_pct": str(position.current_spread_pct) if position.current_spread_pct else None,
                "unrealized_pnl": str(unrealized_pnl)
            }
            
            await self.position_service.update_position(position.position_id, updates)
            
            await logger.debug(
                f"跨市持仓PnL已更新: {position.position_id}, "
                f"价差={current_spread_pct:.4%}, PnL={unrealized_pnl:.2f}"
            )
            
        except Exception as e:
            await logger.error(f"更新跨市持仓PnL失败: {position.position_id}, {e}")

    async def shutdown(self):
        """Cancel all in-flight worker tasks and monitor tasks."""
        self._running = False

        # Cancel worker tasks
        for task in self._worker_tasks:
            if not task.done():
                task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

        # Cancel all autonomous monitor tasks
        monitor_tasks = list(self._monitor_tasks.values())
        for task in monitor_tasks:
            if not task.done():
                task.cancel()
        if monitor_tasks:
            await asyncio.gather(*monitor_tasks, return_exceptions=True)
        self._monitor_tasks.clear()
        await logger.info(f"蜂王已关闭 (清理了 {len(monitor_tasks)} 个监控任务)")

    def get_total_scout_count(self) -> int:
        return sum(len(s) for s in self._scouts.values())

    @staticmethod
    def _compute_alternating_counts(total: int, num_strategies: int) -> List[int]:
        """
        Round-robin distribution of *total* scouts across *num_strategies*.

        Returns a list of counts where each strategy gets at least 1 scout,
        and remaining scouts are distributed in alternating (round-robin) order.
        """
        if num_strategies == 0:
            return []
        counts = [1] * num_strategies          # each strategy gets at least 1
        remaining = total - num_strategies
        idx = 0
        while remaining > 0:
            counts[idx % num_strategies] += 1
            idx += 1
            remaining -= 1
        return counts
