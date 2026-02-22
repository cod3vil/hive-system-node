"""
Cross-Exchange Worker - executes trades based on CrossExchangeSignal.

For cross-exchange arb, the execution is different from cash-carry:
we buy on the low exchange and sell on the high exchange.
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Optional

from backend.core.base_strategy import BaseWorker, BaseSignal
from backend.core.models import Position, PositionStatus
from backend.strategies.cross_exchange.signal import CrossExchangeSignal
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class CrossExchangeWorker(BaseWorker):
    """Worker that executes cross-exchange arbitrage trades."""
    strategy_type = "cross_exchange"

    async def _has_open_position(self, symbol: str) -> bool:
        """Check if there is already an open position for this symbol+strategy."""
        if not self.redis_client:
            return False
        
        # First check Redis (fast path)
        position_ids = await self.redis_client.get_positions_by_symbol(symbol)
        if position_ids:
            # Check if any are still open or closing (via position data in Redis)
            for pid in position_ids:
                data = await self.redis_client.get_position_data(pid)
                if data and data.get("strategy_type") == "cross_exchange":
                    status = data.get("status")
                    # Block new positions if existing position is open or closing
                    if status in [PositionStatus.OPEN, "closing"]:
                        return True
        
        # Fallback to database check if Redis is empty
        # This handles cases where Redis cache was cleared but DB has positions
        if not position_ids and self.position_service:
            try:
                db_positions = await self.position_service.db.get_open_positions()
                for db_pos in db_positions:
                    if (db_pos.get("symbol") == symbol and 
                        db_pos.get("strategy_type") == "cross_exchange"):
                        # Found open position in DB - rebuild Redis cache
                        await self.logger.warning(
                            f"发现Redis缓存缺失，从数据库恢复持仓索引: {symbol}",
                            position_id=str(db_pos.get("position_id"))
                        )
                        # Rebuild Redis cache for this position
                        position_id = str(db_pos.get("position_id"))
                        await self.redis_client.set_position_data(position_id, db_pos)
                        await self.redis_client.add_open_position(position_id)
                        await self.redis_client.add_position_by_symbol(symbol, position_id)
                        return True
            except Exception as e:
                await self.logger.error(
                    f"数据库持仓检查失败: {symbol}",
                    error=e
                )
        
        return False

    async def execute(self, signal: BaseSignal) -> Optional[dict]:
        sig: CrossExchangeSignal = signal  # type: ignore[assignment]
        symbol = sig.symbol

        await logger.info(
            f"工蜂处理跨市信号: {symbol} "
            f"({sig.exchange_low}\u2192{sig.exchange_high}, 价差={sig.spread_pct:.4%})"
        )

        try:
            # ============================================================
            # SAFETY CHECK: Validate signal freshness
            # ============================================================
            import time
            signal_age = time.time() - sig.timestamp
            max_age = self.config.cross_exchange_price_validity_seconds
            
            if signal_age > max_age:
                await logger.warning(
                    f"跨市信号过期: {symbol}, 信号年龄={signal_age:.1f}秒 > {max_age}秒"
                )
                await self.ws_manager.broadcast_worker_status({
                    "status": "rejected",
                    "symbol": symbol,
                    "reason": "signal_expired",
                    "strategy_type": "cross_exchange",
                })
                return {
                    "status": "rejected",
                    "reason": f"信号过期 ({signal_age:.1f}秒)",
                    "symbol": symbol,
                    "strategy_type": "cross_exchange"
                }
            
            # Duplicate check: skip if already holding this symbol
            if await self._has_open_position(symbol):
                await logger.info(f"跨市跳过: {symbol} 已有持仓")
                await self.ws_manager.broadcast_worker_status({
                    "status": "skipped",
                    "symbol": symbol,
                    "reason": "duplicate_position",
                    "strategy_type": "cross_exchange",
                })
                return {"status": "rejected", "reason": "已有持仓，跳过重复开仓", "symbol": symbol, "strategy_type": "cross_exchange"}

            await self.ws_manager.broadcast_worker_status({
                "status": "processing",
                "symbol": symbol,
                "action": "validating",
                "strategy_type": "cross_exchange",
            })

            # Calculate dynamic position size based on equity
            try:
                equity = await self.capital_allocator.get_total_equity()
                max_pct = Decimal(str(self.config.max_single_position_pct))
                requested_amount = equity * max_pct
                if requested_amount <= 0:
                    requested_amount = Decimal("10000")
            except Exception:
                requested_amount = Decimal("10000")
            allocation = await self.capital_allocator.allocate_capital(
                "cross_exchange",
                symbol,
                requested_amount,
                "crypto",
                0,
            )

            if not allocation.approved:
                await logger.info(f"跨市资本分配被拒: {symbol}")
                await self.ws_manager.broadcast_worker_status({
                    "status": "rejected",
                    "symbol": symbol,
                    "reason": "insufficient_capital",
                })
                return {"status": "rejected", "reason": "资本分配不足", "symbol": symbol, "strategy_type": "cross_exchange"}

            position_size = allocation.allocated_amount
            perp_symbol = f"{symbol}:USDT"
            leverage = self.config.default_leverage
            entry_price = sig.price_low

            # Risk check
            risk_check = await self.risk_engine.validate_pre_trade(
                symbol,
                perp_symbol,
                position_size,
                "crypto",
                leverage,
                entry_price,
                strategy_type="cross_exchange",
            )

            if not risk_check.passed:
                await logger.info(f"跨市风险拒绝: {symbol}, {risk_check.failed_checks}")
                await self.ws_manager.broadcast_worker_status({
                    "status": "rejected",
                    "symbol": symbol,
                    "reason": "risk_check_failed",
                    "failed_checks": risk_check.failed_checks,
                })
                failed = ", ".join(risk_check.failed_checks) if risk_check.failed_checks else "风控未通过"
                return {"status": "rejected", "reason": failed, "symbol": symbol, "strategy_type": "cross_exchange"}

            quantity = position_size / entry_price if entry_price > 0 else Decimal("0")

            await logger.info(f"工蜂执行跨市开仓: {symbol} (仓位={position_size} USDT)")
            await self.ws_manager.broadcast_worker_status({
                "status": "executing",
                "symbol": symbol,
                "action": "opening_position",
                "size": str(position_size),
            })

            # Execute: open perpetual positions on both exchanges
            # - Long (buy) on low-price exchange
            # - Short (sell) on high-price exchange
            result = await self.trade_executor.execute_cross_exchange_open(
                signal=sig,
                quantity=quantity,
                exchange_manager=self.exchange_manager
            )

            if result.success:
                # Persist position to Redis + DB
                await self._persist_position(result, sig, symbol, perp_symbol, position_size, quantity, leverage)

                await logger.info(f"✓ 跨市持仓开启: {result.position_id} ({symbol})")
                await self.ws_manager.broadcast_worker_status({
                    "status": "success",
                    "symbol": symbol,
                    "position_id": result.position_id,
                })
                return {
                    "status": "opened",
                    "position_id": result.position_id,
                    "symbol": symbol,
                    "strategy_type": "cross_exchange",
                    "position_size": str(position_size),
                    "exchange_high": sig.exchange_high,
                    "exchange_low": sig.exchange_low,
                    "is_shadow": self.config.is_simulation,
                }
            else:
                await logger.error(f"× 跨市执行失败: {symbol}, {result.error_message}")
                await self.ws_manager.broadcast_worker_status({
                    "status": "failed",
                    "symbol": symbol,
                    "error": result.error_message,
                })
                return {"status": "failed", "symbol": symbol, "strategy_type": "cross_exchange", "error": result.error_message}

        except Exception as e:
            await logger.error(f"跨市工蜂处理 {symbol} 出错: {e}")
            await self.ws_manager.broadcast_worker_status({
                "status": "error",
                "symbol": symbol,
                "error": str(e),
            })
            return {"status": "failed", "symbol": symbol, "strategy_type": "cross_exchange", "error": str(e)}

    def set_spread_service(self, spread_service):
        """Inject spread service for monitoring."""
        self.spread_service = spread_service

    def set_notifier(self, notifier):
        """Inject notifier for close notifications."""
        self.notifier = notifier

    async def monitor_and_close(self, position_id: str, signal: BaseSignal) -> None:
        """
        Monitor cross-exchange position and close when conditions met.

        Checks every 10 seconds:
        - Redis emergency flag / position status "closing" (risk engine trigger)
        - Spread convergence via spread_service.should_close_position()
        """
        sig: CrossExchangeSignal = signal  # type: ignore[assignment]
        await logger.info(f"[自治] 跨市监控启动: {position_id} ({sig.symbol})")

        try:
            while True:
                await asyncio.sleep(10)

                # --- Fetch position data from Redis ---
                if not self.redis_client:
                    await logger.warning(f"[自治] redis_client 未注入，停止监控: {position_id}")
                    return
                pos_data = await self.redis_client.get_position_data(position_id)
                if not pos_data:
                    await logger.info(f"[自治] 持仓已不存在，停止监控: {position_id}")
                    return

                status = pos_data.get("status")
                # Already closed by another process
                if status == PositionStatus.CLOSED:
                    await logger.info(f"[自治] 持仓已平仓，停止监控: {position_id}")
                    return

                # --- Check emergency flag ---
                emergency = await self.redis_client.get_emergency_flag()

                # --- Determine if we should close ---
                should_close = False
                close_reason = None

                if emergency:
                    should_close = True
                    close_reason = "emergency_flag"
                elif status == "closing":
                    should_close = True
                    close_reason = pos_data.get("close_reason", "risk_triggered")
                elif hasattr(self, "spread_service") and self.spread_service:
                    # Check spread convergence
                    spread_info = await self.spread_service.get_spread_info(sig.symbol)
                    if spread_info:
                        current_spread_pct = spread_info["spread_pct"]
                        entry_ts = pos_data.get("entry_timestamp")
                        if entry_ts:
                            if isinstance(entry_ts, str):
                                entry_ts = datetime.fromisoformat(entry_ts.replace("Z", "+00:00"))
                        else:
                            entry_ts = datetime.utcnow()

                        should, reason = await self.spread_service.should_close_position(
                            current_spread_pct, entry_ts
                        )
                        if should:
                            should_close = True
                            close_reason = reason

                if not should_close:
                    continue

                # --- Execute close ---
                await logger.info(
                    f"[自治] 跨市平仓触发: {position_id}, 原因={close_reason}"
                )

                position = Position.from_dict(pos_data)
                result = await self.trade_executor.execute_cross_exchange_close(
                    position, self.exchange_manager
                )

                if result.success:
                    # Calculate holding time
                    holding_hours = 0.0
                    entry_ts = pos_data.get("entry_timestamp")
                    if entry_ts:
                        try:
                            if isinstance(entry_ts, str):
                                entry_dt = datetime.fromisoformat(entry_ts.replace("Z", "+00:00"))
                            else:
                                entry_dt = entry_ts
                            holding_hours = (datetime.utcnow() - entry_dt).total_seconds() / 3600
                        except Exception:
                            pass

                    # Update DB + get realized PnL
                    realized_pnl = Decimal("0")
                    if self.position_service:
                        realized_pnl = await self.position_service.close_position(
                            position_id=position_id,
                            spot_exit_price=result.spot_fill_price,
                            perp_exit_price=result.perp_fill_price,
                            close_reason=close_reason,
                        )

                    # Clean up Redis close queue
                    try:
                        await self.redis_client.remove_from_close_queue(position_id)
                    except Exception:
                        pass

                    # Notify
                    if hasattr(self, "notifier") and self.notifier:
                        await self.notifier.notify_position_closed(
                            position_id=position_id,
                            symbol=sig.symbol,
                            strategy_type="cross_exchange",
                            pnl=float(realized_pnl),
                            close_reason=close_reason,
                            holding_hours=holding_hours,
                            exchange_high=sig.exchange_high,
                            exchange_low=sig.exchange_low,
                        )

                    await logger.info(
                        f"[自治] 跨市平仓完成: {position_id}, PnL={realized_pnl}"
                    )
                else:
                    await logger.error(
                        f"[自治] 跨市平仓失败: {position_id}, {result.error_message}"
                    )
                return

        except asyncio.CancelledError:
            await logger.info(f"[自治] 监控任务已取消: {position_id}")
        except Exception as e:
            await logger.error(f"[自治] 监控异常: {position_id}, {e}")

    async def _persist_position(self, result, sig, symbol, perp_symbol, position_size, quantity, leverage):
        """Create a Position record and persist via PositionService."""
        if not self.position_service:
            await logger.warning(f"position_service 未注入，跳过持仓持久化: {symbol}")
            return

        position = Position(
            position_id=result.position_id,
            symbol=symbol,
            spot_symbol=symbol,
            perp_symbol=perp_symbol,
            strategy_type="cross_exchange",
            exchange_high=sig.exchange_high,
            exchange_low=sig.exchange_low,
            entry_spread_pct=sig.spread_pct,
            target_close_spread_pct=self.config.cross_exchange_close_spread_pct,
            spot_entry_price=result.spot_fill_price or sig.price_low,
            perp_entry_price=result.perp_fill_price or sig.price_high,
            spot_quantity=result.spot_quantity or quantity,
            perp_quantity=result.perp_quantity or quantity,
            leverage=leverage,
            status=PositionStatus.OPEN,
            asset_class="crypto",
            strategy_id="cross_exchange",
            is_shadow=self.config.is_simulation,
        )
        try:
            await self.position_service.create_position(position)
        except Exception as e:
            await logger.error(f"持仓持久化失败: {symbol} {result.position_id}: {e}")
