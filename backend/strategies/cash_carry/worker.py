"""
Cash & Carry Worker - executes trades based on CashCarrySignal.

Calls capital_allocator, risk_engine, and trade_executor with correct signatures.
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Optional

from backend.core.base_strategy import BaseWorker, BaseSignal
from backend.core.models import Position, PositionStatus
from backend.strategies.cash_carry.signal import CashCarrySignal
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class CashCarryWorker(BaseWorker):
    """Worker that executes cash-and-carry arbitrage trades."""
    strategy_type = "cash_carry"

    async def _has_open_position(self, symbol: str) -> bool:
        """Check if there is already an open position for this symbol+strategy."""
        if not self.redis_client:
            return False
        
        # First check Redis (fast path)
        position_ids = await self.redis_client.get_positions_by_symbol(symbol)
        if position_ids:
            for pid in position_ids:
                data = await self.redis_client.get_position_data(pid)
                if data and data.get("strategy_type") == "cash_carry":
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
                        db_pos.get("strategy_type") == "cash_carry"):
                        # Found open position in DB - rebuild Redis cache
                        await logger.warning(
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
                await logger.error(
                    f"数据库持仓检查失败: {symbol}",
                    error=e
                )
        
        return False

    async def execute(self, signal: BaseSignal) -> Optional[dict]:
        sig: CashCarrySignal = signal  # type: ignore[assignment]
        symbol = sig.symbol
        # Construct spot_symbol and perp_symbol from symbol to ensure they're never empty
        spot_symbol = symbol if symbol else sig.spot_symbol
        perp_symbol = f"{symbol}:USDT" if symbol else sig.perp_symbol
        asset_class = sig.asset_class
        spot_price = sig.spot_price
        perp_price = sig.perp_price

        await logger.info(
            f"工蜂处理套利信号: {symbol} (年化={sig.annualized_rate:.2%}), "
            f"spot_symbol={spot_symbol}, perp_symbol={perp_symbol}"
        )

        try:
            # Duplicate check: skip if already holding this symbol
            if await self._has_open_position(symbol):
                await logger.info(f"跳过: {symbol} 已有持仓")
                await self.ws_manager.broadcast_worker_status({
                    "status": "skipped",
                    "symbol": symbol,
                    "reason": "duplicate_position",
                    "strategy_type": "cash_carry",
                })
                return {"status": "rejected", "reason": "已有持仓，跳过重复开仓", "symbol": symbol, "strategy_type": "cash_carry"}

            # Broadcast worker status: processing
            await self.ws_manager.broadcast_worker_status({
                "status": "processing",
                "symbol": symbol,
                "action": "validating",
                "strategy_type": "cash_carry",
            })

            # Calculate position size dynamically based on equity
            leverage = self.config.default_leverage
            try:
                equity = await self.capital_allocator.get_total_equity()
                max_pct = Decimal(str(self.config.max_single_position_pct))
                requested_amount = equity * max_pct
                if requested_amount <= 0:
                    requested_amount = Decimal("10000")
            except Exception:
                requested_amount = Decimal("10000")

            # Step 1: Allocate capital
            allocation = await self.capital_allocator.allocate_capital(
                "cash_carry",
                symbol,
                requested_amount,
                asset_class,
                0,
            )

            if not allocation.approved:
                await logger.info(f"资本分配被拒: {symbol}, reason={allocation.reason}")
                await self.ws_manager.broadcast_worker_status({
                    "status": "rejected",
                    "symbol": symbol,
                    "reason": "insufficient_capital",
                })
                return {"status": "rejected", "reason": "资本分配不足", "symbol": symbol, "strategy_type": "cash_carry"}

            position_size = allocation.allocated_amount

            # Step 2: Risk check
            risk_check = await self.risk_engine.validate_pre_trade(
                symbol,
                perp_symbol,
                position_size,
                asset_class,
                leverage,
                spot_price,
                strategy_type="cash_carry",
            )

            if not risk_check.passed:
                await logger.info(
                    f"风险引擎拒绝: {symbol}, checks={risk_check.failed_checks}"
                )
                await self.ws_manager.broadcast_worker_status({
                    "status": "rejected",
                    "symbol": symbol,
                    "reason": "risk_check_failed",
                    "failed_checks": risk_check.failed_checks,
                })
                failed = ", ".join(risk_check.failed_checks) if risk_check.failed_checks else "风控未通过"
                return {"status": "rejected", "reason": failed, "symbol": symbol, "strategy_type": "cash_carry"}

            # Step 3: Calculate quantity from allocated capital and price
            quantity = position_size / spot_price if spot_price > 0 else Decimal("0")

            # Step 4: Execute trade
            await logger.info(f"工蜂执行开仓: {symbol} (仓位={position_size} USDT)")
            await self.ws_manager.broadcast_worker_status({
                "status": "executing",
                "symbol": symbol,
                "action": "opening_position",
                "size": str(position_size),
            })

            result = await self.trade_executor.execute_open_position(
                spot_symbol,
                perp_symbol,
                quantity,
                leverage,
                spot_price,
                perp_price,
                asset_class,
                "cash_carry",
            )

            if result.success:
                # Persist position to Redis + DB
                await self._persist_position(result, sig, symbol, spot_symbol, perp_symbol, asset_class, position_size, quantity, leverage)

                await logger.info(
                    f"✓ 持仓开启成功: {result.position_id} ({symbol})"
                )
                await self.ws_manager.broadcast_worker_status({
                    "status": "success",
                    "symbol": symbol,
                    "position_id": result.position_id,
                    "spot_price": str(result.spot_fill_price),
                    "perp_price": str(result.perp_fill_price),
                })
                return {
                    "status": "opened",
                    "position_id": result.position_id,
                    "symbol": symbol,
                    "strategy_type": "cash_carry",
                    "position_size": str(position_size),
                    "spot_fill_price": str(result.spot_fill_price),
                    "perp_fill_price": str(result.perp_fill_price),
                    "is_shadow": self.config.is_simulation,
                }
            else:
                await logger.error(f"× 持仓执行失败: {symbol}, {result.error_message}")
                await self.ws_manager.broadcast_worker_status({
                    "status": "failed",
                    "symbol": symbol,
                    "error": result.error_message,
                })
                return {"status": "failed", "symbol": symbol, "strategy_type": "cash_carry", "error": result.error_message}

        except Exception as e:
            await logger.error(f"工蜂处理 {symbol} 信号出错: {e}")
            await self.ws_manager.broadcast_worker_status({
                "status": "error",
                "symbol": symbol,
                "error": str(e),
            })
            return {"status": "failed", "symbol": symbol, "strategy_type": "cash_carry", "error": str(e)}

    def set_funding_service(self, funding_service):
        """Inject funding service for monitoring."""
        self.funding_service = funding_service

    def set_notifier(self, notifier):
        """Inject notifier for close notifications."""
        self.notifier = notifier

    async def monitor_and_close(self, position_id: str, signal: BaseSignal) -> None:
        """
        Monitor cash-carry position and close when conditions met.

        Checks every 10 seconds:
        - Redis emergency flag / position status "closing" (risk engine trigger)
        - Funding rate turning negative via funding_service
        """
        sig: CashCarrySignal = signal  # type: ignore[assignment]
        symbol = sig.symbol
        perp_symbol = f"{symbol}:USDT" if symbol else sig.perp_symbol

        await logger.info(f"[自治] 现货合约监控启动: {position_id} ({symbol})")

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
                elif hasattr(self, "funding_service") and self.funding_service:
                    # Check if funding rate turned negative
                    try:
                        current_rate = await self.funding_service.get_current_funding_rate(perp_symbol)
                        if current_rate < 0:
                            should_close = True
                            close_reason = "funding_negative"
                    except Exception as e:
                        await logger.error(f"[自治] 获取资金费率失败: {perp_symbol}, {e}")

                if not should_close:
                    continue

                # --- Execute close ---
                await logger.info(
                    f"[自治] 现货合约平仓触发: {position_id}, 原因={close_reason}"
                )

                position = Position.from_dict(pos_data)
                result = await self.trade_executor.execute_close_position(position)

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
                            symbol=symbol,
                            strategy_type="cash_carry",
                            pnl=float(realized_pnl),
                            close_reason=close_reason,
                            holding_hours=holding_hours,
                        )

                    await logger.info(
                        f"[自治] 现货合约平仓完成: {position_id}, PnL={realized_pnl}"
                    )
                else:
                    await logger.error(
                        f"[自治] 现货合约平仓失败: {position_id}, {result.error_message}"
                    )
                return

        except asyncio.CancelledError:
            await logger.info(f"[自治] 监控任务已取消: {position_id}")
        except Exception as e:
            await logger.error(f"[自治] 监控异常: {position_id}, {e}")

    async def _persist_position(self, result, sig, symbol, spot_symbol, perp_symbol, asset_class, position_size, quantity, leverage):
        """Create a Position record and persist via PositionService."""
        if not self.position_service:
            await logger.warning(f"position_service 未注入，跳过持仓持久化: {symbol}")
            return

        await logger.info(
            f"准备创建持仓: symbol={symbol}, spot_symbol={spot_symbol}, perp_symbol={perp_symbol}"
        )

        position = Position(
            position_id=result.position_id,
            symbol=symbol,
            spot_symbol=spot_symbol,
            perp_symbol=perp_symbol,
            strategy_type="cash_carry",
            spot_entry_price=result.spot_fill_price or sig.spot_price,
            perp_entry_price=result.perp_fill_price or sig.perp_price,
            spot_quantity=result.spot_quantity or quantity,
            perp_quantity=result.perp_quantity or quantity,
            leverage=leverage,
            status=PositionStatus.OPEN,
            asset_class=asset_class,
            strategy_id="cash_carry",
            is_shadow=self.config.is_simulation,
        )
        
        # Debug: log the position dict before persisting
        position_dict = position.to_dict()
        await logger.info(
            f"持仓对象已创建，准备持久化: position_id={position.position_id}, "
            f"spot_symbol={position_dict.get('spot_symbol')}, "
            f"perp_symbol={position_dict.get('perp_symbol')}"
        )
        
        try:
            await self.position_service.create_position(position)
        except Exception as e:
            await logger.error(f"持仓持久化失败: {symbol} {result.position_id}: {e}")
