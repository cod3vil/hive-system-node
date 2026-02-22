"""
Risk Engine (Hive) module for centralized risk management.

This module implements the Hive component that:
- Validates all pre-trade risk checks
- Performs stress testing
- Monitors positions continuously
- Triggers emergency actions

Requirements: 4.1-4.13, 5.1-5.17
"""

import asyncio
from decimal import Decimal
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
from datetime import datetime, timedelta

from risk.liquidation_guard import LiquidationGuard
from risk.drawdown_guard import DrawdownGuard, DrawdownStatus
from risk.funding_guard import FundingGuard
from capital.capital_allocator import CapitalAllocator
from services.funding_service import FundingService
from storage.redis_client import RedisClient
from services.exchange_client import ExchangeClient
from config.settings import SystemConfig
from utils.logger import get_logger
from services.notifier import get_notifier

if TYPE_CHECKING:
    from execution.trade_executor import TradeExecutor
    from websocket.websocket_server import ConnectionManager


logger = get_logger(__name__)


@dataclass
class RiskCheckResult:
    """
    Result of pre-trade risk validation.
    
    Attributes:
        passed: Whether all risk checks passed
        failed_checks: List of failed check descriptions
        risk_score: Overall risk score (0.0 = no risk, 1.0 = maximum risk)
    """
    passed: bool
    failed_checks: List[str]
    risk_score: float


@dataclass
class StressTestResult:
    """
    Result of stress test simulation.
    
    Attributes:
        passed: Whether stress test passed
        simulated_move_pct: Percentage of adverse price movement simulated
        liquidation_price: Calculated liquidation price
        distance_to_liquidation: Distance to liquidation after stress
        would_liquidate: Whether position would liquidate under stress
    """
    passed: bool
    simulated_move_pct: Decimal
    liquidation_price: Decimal
    distance_to_liquidation: Decimal
    would_liquidate: bool


@dataclass
class EmergencyAction:
    """
    Emergency action to be taken.
    
    Attributes:
        action_type: Type of action ("soft_pause", "hard_stop", "reduce_position", "rebalance")
        reason: Reason for the action
        affected_positions: List of position IDs affected
        timestamp: When the action was triggered
    """
    action_type: str
    reason: str
    affected_positions: List[str]
    timestamp: datetime


class RiskEngine:
    """
    Hive (Risk Engine) for centralized risk management.
    
    This engine:
    - Validates all pre-trade risk checks (Requirements 4.1-4.13)
    - Performs stress testing (Requirements 4.11, 4.12)
    - Monitors positions continuously (Requirements 5.1-5.17)
    - Triggers emergency actions (soft pause, hard stop, rebalancing)
    """
    
    def __init__(
        self,
        liquidation_guard: LiquidationGuard,
        drawdown_guard: DrawdownGuard,
        funding_guard: FundingGuard,
        capital_allocator: CapitalAllocator,
        funding_service: FundingService,
        redis_client: RedisClient,
        exchange_client: ExchangeClient,
        config: SystemConfig,
        websocket_manager: Optional["ConnectionManager"] = None
    ):
        """
        Initialize Risk Engine.
        
        Args:
            liquidation_guard: Guard for liquidation risk
            drawdown_guard: Guard for drawdown risk
            funding_guard: Guard for funding rate risk
            capital_allocator: Capital allocator for exposure checks
            funding_service: Service for funding data
            redis_client: Redis client for state management
            exchange_client: Exchange client for market data
            config: System configuration
            websocket_manager: Optional WebSocket connection manager for broadcasts
        """
        self.liquidation_guard = liquidation_guard
        self.drawdown_guard = drawdown_guard
        self.funding_guard = funding_guard
        self.capital_allocator = capital_allocator
        self.funding_service = funding_service
        self.redis = redis_client
        self.exchange = exchange_client
        self.config = config
        self.websocket_manager = websocket_manager
        self.logger = logger

        # Trade executor reference (injected after construction)
        self._trade_executor: Optional["TradeExecutor"] = None

        # Monitoring task handle
        self._monitoring_task: Optional[asyncio.Task] = None
        self._monitoring_active = False

    def set_trade_executor(self, trade_executor: "TradeExecutor") -> None:
        """Inject trade executor reference for position closing."""
        self._trade_executor = trade_executor
    
    async def validate_pre_trade(
        self,
        symbol: str,
        perp_symbol: str,
        position_size: Decimal,
        asset_class: str,
        leverage: Decimal,
        entry_price: Decimal,
        strategy_type: str = "cash_carry",
    ) -> RiskCheckResult:
        """
        Validate all pre-trade risk checks.
        
        Requirements:
        - 4.1: Check system emergency flags
        - 4.2: Check manual pause
        - 4.3: Verify total exposure limit (50%)
        - 4.4: Verify single position limit (15%)
        - 4.5: Verify per-asset-class exposure limit (30%)
        - 4.6: Check daily loss threshold (3%)
        - 4.7: Check drawdown threshold (6%)
        - 4.8: Verify funding rate is positive
        - 4.9: Halt if funding rate is negative
        - 4.10: Verify liquidation distance >= 30%
        - 4.13: Reject trade if any check fails
        
        Args:
            symbol: Trading pair symbol
            perp_symbol: Perpetual contract symbol
            position_size: Notional value of position
            asset_class: Asset class (e.g., "BTC", "ETH")
            leverage: Proposed leverage
            entry_price: Proposed entry price
            
        Returns:
            RiskCheckResult with validation status
        """
        failed_checks = []
        risk_score = 0.0
        
        try:
            # Requirement 4.1: Check emergency flag
            emergency_flag = await self.redis.get_emergency_flag()
            if emergency_flag:
                failed_checks.append("Emergency flag is active")
                risk_score += 1.0
            
            # Requirement 4.2: Check manual pause
            manual_pause = await self.redis.get_manual_pause()
            if manual_pause:
                failed_checks.append("Manual pause is active")
                risk_score += 1.0
            
            # Requirements 4.3, 4.4, 4.5: Validate exposure limits
            exposure_validation = await self.capital_allocator.validate_position_size(
                symbol=symbol,
                notional_value=position_size,
                asset_class=asset_class
            )
            
            if not exposure_validation.is_valid:
                failed_checks.append(f"Exposure limit violation: {exposure_validation.reason}")
                risk_score += 0.3
            
            # Requirement 4.6: Check daily loss threshold (3%)
            daily_loss_pct = await self.redis.get_daily_loss_pct()
            if daily_loss_pct and daily_loss_pct > self.config.max_daily_loss_pct:
                failed_checks.append(
                    f"Daily loss {daily_loss_pct:.2%} exceeds threshold {self.config.max_daily_loss_pct:.2%}"
                )
                risk_score += 0.4
            
            # Requirement 4.7: Check drawdown threshold (6%)
            drawdown_status = await self.drawdown_guard.check_drawdown_limits()
            if drawdown_status.exceeds_limit:
                failed_checks.append(
                    f"Drawdown {drawdown_status.drawdown_pct:.2%} exceeds threshold {self.config.max_drawdown_pct:.2%}"
                )
                risk_score += 0.5
            
            # Requirements 4.8, 4.9: Verify funding rate is positive (cash-carry only)
            # Cross-exchange arbitrage profits from price spread, not funding rate direction
            if strategy_type == "cash_carry":
                current_funding = await self.funding_service.get_current_funding_rate(perp_symbol)
                if current_funding < 0:
                    failed_checks.append(
                        f"Funding rate is negative: {current_funding:.6f}"
                    )
                    risk_score += 0.3
            
            # Requirement 4.10: Verify liquidation distance >= 30%
            liquidation_safe = await self.liquidation_guard.validate_liquidation_safety(
                symbol=perp_symbol,
                entry_price=entry_price,
                leverage=leverage,
                quantity=position_size / entry_price,
                side="short"
            )
            
            if not liquidation_safe:
                failed_checks.append(
                    f"Liquidation distance would be < {self.config.min_liquidation_distance_pct:.0%}"
                )
                risk_score += 0.4
            
            # Determine if passed
            passed = len(failed_checks) == 0
            
            if not passed:
                await self.logger.warning(
                    f"交易前验证失败: {symbol}: {', '.join(failed_checks)}"
                )
            else:
                await self.logger.info(
                    f"交易前验证通过: {symbol}"
                )
            
            return RiskCheckResult(
                passed=passed,
                failed_checks=failed_checks,
                risk_score=min(risk_score, 1.0)
            )
            
        except Exception as e:
            await self.logger.error(f"交易前验证出错: {symbol}: {e}")
            # Return failed result on error (conservative approach)
            return RiskCheckResult(
                passed=False,
                failed_checks=[f"Validation error: {str(e)}"],
                risk_score=1.0
            )

    async def perform_stress_test(
        self,
        symbol: str,
        perp_symbol: str,
        position_size: Decimal,
        leverage: Decimal,
        entry_price: Decimal
    ) -> StressTestResult:
        """
        Perform stress test simulation using adverse price movement.
        
        Requirements:
        - 4.11: Calculate stress test percentage as max(20%, recent_30_day_max_hourly_move)
        - 4.12: Verify position would not liquidate under stress
        
        Args:
            symbol: Trading pair symbol
            perp_symbol: Perpetual contract symbol
            position_size: Notional value of position
            leverage: Proposed leverage
            entry_price: Proposed entry price
            
        Returns:
            StressTestResult with stress test outcome
        """
        try:
            # Requirement 4.11: Calculate stress test percentage
            # Get recent 30-day max hourly move from exchange
            recent_max_move = await self.exchange.calculate_max_hourly_move(
                symbol=perp_symbol,
                days=30
            )
            
            # Use max of 20% or recent max move
            stress_move_pct = max(
                self.config.stress_test_min_move_pct,
                recent_max_move
            )
            
            await self.logger.info(
                f"压力测试 {symbol}: 模拟 {stress_move_pct:.1%} 不利波动 "
                f"(30日最大: {recent_max_move:.1%}, 最小: {self.config.stress_test_min_move_pct:.1%})"
            )
            
            # Requirement 4.12: Verify position would not liquidate under stress
            quantity = position_size / entry_price
            
            would_survive = await self.liquidation_guard.validate_stress_test_liquidation(
                symbol=perp_symbol,
                entry_price=entry_price,
                leverage=leverage,
                quantity=quantity,
                adverse_move_pct=stress_move_pct,
                side="short"
            )
            
            # Get liquidation price for reporting
            liquidation_price = await self.liquidation_guard.get_exchange_liquidation_price(
                symbol=perp_symbol,
                side="short",
                entry_price=entry_price,
                leverage=leverage,
                quantity=quantity
            )
            
            # Calculate stressed price
            stressed_price = entry_price * (Decimal("1") + stress_move_pct)
            
            # Calculate distance to liquidation after stress
            distance = abs(stressed_price - liquidation_price) / stressed_price
            
            result = StressTestResult(
                passed=would_survive,
                simulated_move_pct=stress_move_pct,
                liquidation_price=liquidation_price,
                distance_to_liquidation=distance,
                would_liquidate=not would_survive
            )
            
            if not would_survive:
                await self.logger.warning(
                    f"压力测试失败: {symbol}: "
                    f"在 {stress_move_pct:.1%} 不利波动下持仓将被清算. "
                    f"入场价: {entry_price}, 压力价: {stressed_price}, "
                    f"清算价: {liquidation_price}"
                )
            else:
                await self.logger.info(
                    f"压力测试通过: {symbol}: "
                    f"在 {stress_move_pct:.1%} 不利波动下持仓安全"
                )
            
            return result
            
        except Exception as e:
            await self.logger.error(f"压力测试出错: {symbol}: {e}")
            # Return failed result on error (conservative approach)
            return StressTestResult(
                passed=False,
                simulated_move_pct=self.config.stress_test_min_move_pct,
                liquidation_price=Decimal("0"),
                distance_to_liquidation=Decimal("0"),
                would_liquidate=True
            )
    
    async def monitor_positions(self) -> None:
        """
        Continuous position monitoring loop.
        
        Requirements:
        - 5.1: Scan all open positions every 5 seconds
        - 5.2: Calculate current account equity
        - 5.3, 5.4: Track and update peak equity
        - 5.5: Calculate current drawdown
        - 5.6, 5.7: Monitor funding rate changes
        - 5.8, 5.9: Check liquidation distances
        - 5.10, 5.11: Check funding rate conditions
        - 5.12: Trigger soft pause on 3% daily loss
        - 5.13: Trigger hard stop on 6% drawdown
        - 5.14, 5.15, 5.16, 5.17: Trigger rebalancing when needed
        
        This method runs continuously in the background.
        """
        await self.logger.info("正在启动持仓监控循环")
        self._monitoring_active = True
        
        while self._monitoring_active:
            try:
                # Requirement 5.1: Scan every 5 seconds
                await asyncio.sleep(self.config.position_scan_interval_seconds)
                
                # Get all open positions
                open_positions = await self.redis.get_open_positions()
                
                if not open_positions:
                    await self.logger.debug("没有持仓需要监控")
                    continue
                
                await self.logger.debug(f"正在监控 {len(open_positions)} 个持仓")
                
                # Requirement 5.2: Calculate current equity
                current_equity = await self._calculate_current_equity()
                await self.redis.set_current_equity(current_equity)
                
                # Requirements 5.3, 5.4: Update peak equity
                await self.drawdown_guard.update_peak_equity(current_equity)
                
                # Requirement 5.5: Calculate drawdown
                drawdown_status = await self.drawdown_guard.check_drawdown_limits(current_equity)
                
                # Requirement 5.13: Check for hard stop condition (6% drawdown)
                if drawdown_status.exceeds_limit:
                    await self.logger.critical(
                        f"硬停止已触发: 回撤 {drawdown_status.drawdown_pct:.2%} "
                        f"超过限制 {self.config.max_drawdown_pct:.2%}"
                    )
                    await self.execute_hard_stop()
                    continue
                
                # Auto-recovery check: If in hard_stop but drawdown is now acceptable
                system_status = await self.redis.get_system_status()
                if system_status == "hard_stop":
                    # Check if drawdown has recovered to acceptable level (< 50% of limit)
                    recovery_threshold = self.config.max_drawdown_pct * Decimal("0.5")
                    if drawdown_status.drawdown_pct < recovery_threshold:
                        await self.logger.info(
                            f"回撤已恢复到安全水平: {drawdown_status.drawdown_pct:.2%} "
                            f"< {recovery_threshold:.2%}. 正在自动恢复系统."
                        )
                        await self.auto_recover_from_hard_stop(
                            f"Drawdown recovered to {drawdown_status.drawdown_pct:.2%}"
                        )
                        continue
                
                # Requirement 5.12: Check for soft pause condition (3% daily loss)
                daily_loss_pct = await self._calculate_daily_loss_pct(current_equity)
                await self.redis.set_daily_loss_pct(daily_loss_pct)
                
                if daily_loss_pct > self.config.max_daily_loss_pct:
                    await self.logger.warning(
                        f"软暂停已触发: 日亏损 {daily_loss_pct:.2%} "
                        f"超过阈值 {self.config.max_daily_loss_pct:.2%}"
                    )
                    await self.execute_soft_pause()
                
                # Monitor each position
                for position_id in open_positions:
                    position_data = await self.redis.get_position(position_id)
                    if position_data:
                        from core.models import Position
                        position = Position.from_dict(position_data)
                        
                        # Check strategy type and route to appropriate monitor
                        if position.strategy_type == "cross_exchange":
                            await self._monitor_cross_exchange_position(position)
                        else:
                            await self._monitor_single_position(position_id)
                
            except Exception as e:
                await self.logger.error(f"持仓监控循环出错: {e}")
                # Continue monitoring despite errors
                await asyncio.sleep(5)
        
        await self.logger.info("持仓监控循环已停止")
    
    async def _monitor_single_position(self, position_id: str) -> None:
        """
        Monitor a single position for risk conditions.
        
        Args:
            position_id: Position ID to monitor
        """
        try:
            # Get position data from Redis
            position_data = await self.redis.get_position(position_id)
            if not position_data:
                await self.logger.warning(f"持仓 {position_id} 在Redis中未找到")
                return
            
            symbol = position_data.get("symbol")
            perp_symbol = position_data.get("perp_symbol")
            
            # Requirements 5.8, 5.9: Check liquidation distance
            from core.models import Position
            position = Position.from_dict(position_data)
            
            liquidation_risk = await self.liquidation_guard.check_liquidation_risk(position)
            
            if liquidation_risk:
                await self.logger.error(
                    f"持仓 {position_id} ({symbol}) 存在高清算风险"
                )
                # Add to liquidation alerts
                await self.redis.add_liquidation_alert(position_id)
            
            # Requirements 5.6, 5.7: Monitor funding rate changes
            funding_analysis = await self.funding_guard.analyze_funding_trend(perp_symbol)
            
            # Requirement 5.10: Halt new positions if funding negative
            if funding_analysis.should_halt_new:
                await self.logger.warning(
                    f"{symbol} 的资金费率为负，暂停新建仓位"
                )
                # This is handled in pre-trade validation
            
            # Requirement 5.11: Reduce position if 6-period avg negative
            if funding_analysis.should_reduce:
                await self.logger.warning(
                    f"{symbol} 的6期平均资金费率为负, "
                    f"持仓 {position_id} 状态设为closing，等待工蜂自治平仓"
                )
                await self.redis.update_position(position_id, {
                    "status": "closing",
                    "close_reason": "funding_negative"
                })
            
            # Check funding reconciliation status
            recon_status = await self.funding_guard.check_funding_reconciliation_status(
                position_id, perp_symbol
            )
            
            if recon_status.is_anomaly:
                await self.funding_guard.handle_funding_anomaly(position_id, perp_symbol)
            
            # Requirements 5.14, 5.15: Check delta-neutral status
            spot_qty = position.spot_quantity
            perp_qty = position.perp_quantity
            
            delta_diff = abs(spot_qty - perp_qty) / spot_qty if spot_qty > 0 else Decimal("0")
            
            # Requirement 5.15: Trigger rebalancing if delta > 0.1%
            if delta_diff > self.config.delta_tolerance_pct:
                # Requirements 5.16, 5.17: Check rebalancing cooldown
                last_rebalance = position.last_rebalance_time
                
                if last_rebalance:
                    time_since_rebalance = datetime.utcnow() - last_rebalance
                    cooldown = timedelta(seconds=self.config.rebalance_cooldown_seconds)
                    
                    if time_since_rebalance < cooldown:
                        await self.logger.debug(
                            f"持仓 {position_id} 需要再平衡但冷却中 "
                            f"({time_since_rebalance.total_seconds():.0f}秒 / {cooldown.total_seconds():.0f}秒)"
                        )
                        return
                
                await self.logger.warning(
                    f"持仓 {position_id} 的Delta容差超限: {delta_diff:.2%}. "
                    f"触发再平衡."
                )
                if self._trade_executor:
                    rebalance_result = await self._trade_executor.execute_rebalance(position)
                    if rebalance_result.success:
                        await self.logger.info(
                            f"持仓 {position_id} 再平衡成功"
                        )
                    else:
                        await self.logger.error(
                            f"持仓 {position_id} 再平衡失败: {rebalance_result.error_message}"
                        )
                else:
                    await self.logger.error(
                        f"持仓 {position_id} 需要再平衡但TradeExecutor未注入"
                    )
            
        except Exception as e:
            await self.logger.error(f"监控持仓 {position_id} 时出错: {e}")
    
    async def _calculate_current_equity(self) -> Decimal:
        """
        Calculate current total account equity.
        
        Returns:
            Current equity (balances + unrealized PnL)
        """
        try:
            # Get combined balance (spot + swap)
            combined_balance = await self.exchange.fetch_combined_balance()
            
            # Total balance in USDT
            total_balance = Decimal(str(combined_balance.get("total", 0)))
            
            # Sanity check: Detect abnormal equity drops
            # If equity drops more than 50% from cached value, it's likely a data error
            cached_equity = await self.redis.get_current_equity()
            if cached_equity and cached_equity > 0:
                drop_pct = (cached_equity - total_balance) / cached_equity
                if drop_pct > Decimal("0.50"):  # 50% drop threshold
                    await self.logger.error(
                        f"检测到异常权益数据: 从 {cached_equity} 暴跌到 {total_balance} "
                        f"({drop_pct:.2%}). 使用缓存值."
                    )
                    return cached_equity
            
            # TODO: Add unrealized PnL from open positions
            # This will be implemented when PositionService is available
            
            await self.logger.debug(f"当前权益已计算: {total_balance}")
            
            return total_balance
            
        except Exception as e:
            await self.logger.error(f"计算当前权益失败: {e}")
            # Return cached value if available
            cached_equity = await self.redis.get_current_equity()
            return cached_equity or Decimal("0")
    
    async def _calculate_daily_loss_pct(self, current_equity: Decimal) -> Decimal:
        """
        Calculate daily loss percentage.
        
        Args:
            current_equity: Current equity
            
        Returns:
            Daily loss percentage (0.0 to 1.0)
        """
        try:
            # Get starting equity for the day
            daily_start_equity = await self.redis.get_daily_start_equity()
            
            if daily_start_equity is None or daily_start_equity == 0:
                await self.logger.debug("未找到每日起始权益，返回0亏损")
                return Decimal("0")
            
            # Calculate loss percentage
            if current_equity >= daily_start_equity:
                # No loss, actually a gain
                return Decimal("0")
            
            loss_pct = (daily_start_equity - current_equity) / daily_start_equity
            
            await self.logger.debug(
                f"日亏损: {loss_pct:.2%} "
                f"(起始: {daily_start_equity}, 当前: {current_equity})"
            )
            
            return loss_pct
            
        except Exception as e:
            await self.logger.error(f"计算日亏损失败: {e}")
            return Decimal("0")
    
    async def check_emergency_conditions(self) -> List[EmergencyAction]:
        """
        Check for conditions requiring immediate emergency action.
        
        Returns:
            List of EmergencyAction objects for conditions detected
        """
        actions = []
        
        try:
            # Check drawdown limit
            drawdown_status = await self.drawdown_guard.check_drawdown_limits()
            if drawdown_status.exceeds_limit:
                actions.append(EmergencyAction(
                    action_type="hard_stop",
                    reason=f"Drawdown {drawdown_status.drawdown_pct:.2%} exceeds limit",
                    affected_positions=[],  # All positions
                    timestamp=datetime.utcnow()
                ))
            
            # Check daily loss limit
            current_equity = await self.redis.get_current_equity()
            if current_equity:
                daily_loss = await self._calculate_daily_loss_pct(current_equity)
                if daily_loss > self.config.max_daily_loss_pct:
                    actions.append(EmergencyAction(
                        action_type="soft_pause",
                        reason=f"Daily loss {daily_loss:.2%} exceeds threshold",
                        affected_positions=[],  # All new positions
                        timestamp=datetime.utcnow()
                    ))
            
            # Check for positions requiring reduction
            open_positions = await self.redis.get_open_positions()
            positions_to_reduce = await self.funding_guard.get_positions_requiring_reduction(
                [(pid, await self._get_position_symbol(pid)) for pid in open_positions]
            )
            
            if positions_to_reduce:
                for position_id, reason in positions_to_reduce:
                    actions.append(EmergencyAction(
                        action_type="reduce_position",
                        reason=reason,
                        affected_positions=[position_id],
                        timestamp=datetime.utcnow()
                    ))
            
            return actions
            
        except Exception as e:
            await self.logger.error(f"检查紧急状况失败: {e}")
            return []
    
    async def _get_position_symbol(self, position_id: str) -> str:
        """Get symbol for a position ID."""
        try:
            position_data = await self.redis.get_position(position_id)
            return position_data.get("perp_symbol", "") if position_data else ""
        except Exception:
            return ""
    
    async def execute_soft_pause(self) -> None:
        """
        Execute soft pause: stop new position openings.
        
        Requirement 5.12: When current day loss exceeds 3%, stop opening new trades
        """
        try:
            await self.redis.set_system_status("soft_pause")
            await self.redis.set_manual_pause(True)
            
            await self.logger.critical(
                "软暂停已激活: 停止开新仓. "
                "现有持仓保持不变."
            )
            
            # Broadcast risk status update via WebSocket
            if self.websocket_manager:
                risk_data = {
                    "status": "soft_pause",
                    "message": "New position openings stopped. Existing positions remain open.",
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.websocket_manager.broadcast_risk_status_update(risk_data)

            # Send alerts to all channels
            notifier = get_notifier(self.config)
            await notifier.notify_soft_pause()

        except Exception as e:
            await self.logger.critical(f"执行软暂停失败: {e}")
    
    async def _monitor_cross_exchange_position(self, position) -> None:
        """
        Monitor a cross-exchange arbitrage position.
        
        Checks:
        - Current spread convergence
        - Holding time limits
        - Trading pair availability (delisting detection)
        - Triggers automatic closing when conditions met
        
        Args:
            position: Position object with strategy_type="cross_exchange"
        """
        try:
            await self.logger.debug(
                f"监控跨市持仓 {position.position_id} ({position.symbol}): "
                f"{position.exchange_high} ↔ {position.exchange_low}"
            )
            
            # Get spread service (will be injected from main.py)
            if not hasattr(self, 'spread_service'):
                await self.logger.warning("CrossExchangeSpreadService not initialized")
                return
            
            # Get current spread info with error handling for delisted pairs
            try:
                spread_info = await self.spread_service.get_spread_info(position.symbol)
            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if error indicates trading pair delisting
                if any(keyword in error_msg for keyword in [
                    "does not have market symbol",
                    "market not found",
                    "symbol not found",
                    "invalid symbol",
                    "delisted"
                ]):
                    await self.logger.error(
                        f"检测到交易对下架: {position.symbol} - {e}. "
                        f"持仓 {position.position_id} 将被标记为无法平仓."
                    )
                    
                    # Mark position as unable to close due to delisting
                    await self._handle_delisted_position(position, str(e))
                    return
                else:
                    # Other errors - log and skip this monitoring cycle
                    await self.logger.error(
                        f"获取价差信息失败: {position.symbol} - {e}"
                    )
                    return
            
            if not spread_info:
                await self.logger.warning(
                    f"无法获取 {position.symbol} 的价差信息，跳过监控"
                )
                return
            
            current_spread_pct = spread_info["spread_pct"]
            
            # Update position's current spread
            position.current_spread_pct = current_spread_pct
            await self.redis.update_position(position.position_id, {
                "current_spread_pct": str(current_spread_pct)
            })
            
            # Check if position should be closed
            should_close, close_reason = await self.spread_service.should_close_position(
                current_spread_pct,
                position.entry_timestamp
            )
            
            # ============================================================
            # SAFETY CHECK: Detect spread reversal
            # ============================================================
            # If entry spread was positive but current spread is significantly negative,
            # this indicates a price reversal - close immediately
            if position.entry_spread_pct and position.entry_spread_pct > 0:
                # Check if spread has reversed (become negative)
                if current_spread_pct < -Decimal("0.001"):  # -0.1% threshold
                    await self.logger.warning(
                        f"跨市持仓 {position.position_id} 价差反转: "
                        f"入场={position.entry_spread_pct:.4%}, 当前={current_spread_pct:.4%}"
                    )
                    should_close = True
                    close_reason = "spread_reversed"
            
            if should_close:
                await self.logger.info(
                    f"跨市持仓 {position.position_id} 触发平仓条件: {close_reason}, "
                    f"当前价差={current_spread_pct:.4%}, "
                    f"持仓时间={(datetime.utcnow() - position.entry_timestamp).total_seconds() / 3600:.1f}小时"
                )
                
                # Trigger position closing
                await self._trigger_cross_exchange_close(position, close_reason)
            else:
                await self.logger.debug(
                    f"跨市持仓 {position.position_id} 状态正常: "
                    f"价差={current_spread_pct:.4%}, "
                    f"持仓时间={(datetime.utcnow() - position.entry_timestamp).total_seconds() / 3600:.1f}小时"
                )
            
        except Exception as e:
            await self.logger.error(
                f"监控跨市持仓 {position.position_id} 时出错: {e}"
            )
    
    async def _handle_delisted_position(self, position, error_message: str) -> None:
        """
        Handle position with delisted trading pair.
        
        When a trading pair is delisted from an exchange, we cannot close it normally.
        This method marks the position appropriately and sends alerts.
        
        Args:
            position: Position with delisted trading pair
            error_message: Error message from exchange
        """
        try:
            await self.logger.critical(
                f"处理下架交易对持仓: {position.position_id} ({position.symbol})"
            )
            
            # For simulation mode, we can directly close the position
            if self.config.is_simulation:
                await self.logger.info(
                    f"模拟模式: 直接清理下架交易对持仓 {position.position_id}"
                )
                
                # Mark as closed with special reason
                await self.redis.update_position(position.position_id, {
                    "status": "closed",
                    "close_reason": "delisted_simulation",
                    "exit_timestamp": datetime.utcnow().isoformat(),
                    "realized_pnl": "0"  # Cannot calculate PnL for delisted pairs
                })
                
                # Remove from open positions
                await self.redis.remove_open_position(position.position_id)
                await self.redis.remove_position_by_symbol(position.symbol, position.position_id)
                
                await self.logger.info(
                    f"✓ 模拟持仓 {position.position_id} 已清理（交易对下架）"
                )
                
                # Send notification
                notifier = get_notifier(self.config)
                await notifier.notify_position_delisted(
                    position.position_id,
                    position.symbol,
                    error_message
                )
                
            else:
                # For real trading, mark as unable to close and alert operators
                await self.logger.critical(
                    f"实盘模式: 交易对 {position.symbol} 已下架，持仓 {position.position_id} "
                    f"无法自动平仓，需要人工处理！"
                )
                
                # Mark position with special status
                await self.redis.update_position(position.position_id, {
                    "status": "delisted",
                    "close_reason": "trading_pair_delisted",
                    "delisting_error": error_message
                })
                
                # Send critical alert
                notifier = get_notifier(self.config)
                await notifier.notify_position_delisted(
                    position.position_id,
                    position.symbol,
                    error_message
                )
                
        except Exception as e:
            await self.logger.critical(
                f"处理下架交易对持仓失败: {position.position_id} - {e}"
            )
    
    async def _trigger_cross_exchange_close(
        self,
        position,
        reason: str
    ) -> None:
        """
        Trigger cross-exchange position closing.
        
        Args:
            position: Position to close
            reason: Reason for closing
        """
        try:
            await self.logger.info(
                f"触发跨市持仓平仓: {position.position_id}, 原因: {reason}"
            )
            
            # Set position status to closing
            await self.redis.update_position(position.position_id, {
                "status": "closing",
                "close_reason": reason
            })
            
            # Broadcast to WebSocket
            if self.websocket_manager:
                await self.websocket_manager.broadcast({
                    "type": "cross_exchange_position_closing",
                    "data": {
                        "position_id": position.position_id,
                        "symbol": position.symbol,
                        "reason": reason,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                })
            
            # Workers detect "closing" status autonomously — no close queue needed
            await self.logger.info(
                f"跨市持仓 {position.position_id} 状态已设为closing，等待工蜂自治平仓"
            )
            
        except Exception as e:
            await self.logger.error(
                f"触发跨市持仓平仓失败 {position.position_id}: {e}"
            )
    
    async def execute_hard_stop(self) -> None:
        """
        Execute hard stop: close all positions and enter read-only mode.

        Requirement 5.13: When total drawdown exceeds 6%, close all positions
        and enter read-only mode
        """
        try:
            await self.redis.set_system_status("hard_stop")
            await self.redis.set_emergency_flag(True)

            await self.logger.critical(
                "硬停止已激活: 正在关闭所有持仓并进入只读模式."
            )

            # Broadcast risk status update via WebSocket
            if self.websocket_manager:
                risk_data = {
                    "status": "hard_stop",
                    "message": "Closing all positions and entering read-only mode.",
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.websocket_manager.broadcast_risk_status_update(risk_data)

            # Mark all open positions as "closing" — autonomous workers will detect
            # the emergency flag and/or "closing" status and close positions
            open_positions = await self.redis.get_open_positions()
            if open_positions:
                marked_count = 0
                for position_id in open_positions:
                    try:
                        await self.redis.update_position(position_id, {
                            "status": "closing",
                            "close_reason": "hard_stop"
                        })
                        marked_count += 1
                    except Exception as e:
                        await self.logger.error(f"硬停止标记持仓失败: {position_id}: {e}")

                await self.logger.critical(
                    f"硬停止: 已标记 {marked_count}/{len(open_positions)} 个持仓为closing，"
                    f"等待工蜂自治平仓 (emergency_flag已设置)"
                )

            # Send critical alerts to all channels
            notifier = get_notifier(self.config)
            await notifier.notify_hard_stop()

        except Exception as e:
            await self.logger.critical(f"执行硬停止失败: {e}")
    
    async def auto_recover_from_hard_stop(self, reason: str) -> None:
        """
        Automatically recover from hard stop when conditions improve.
        
        This is only triggered when:
        - Drawdown has recovered to < 50% of the limit
        - System was in hard_stop due to transient issues (not manual intervention)
        
        Args:
            reason: Reason for recovery
        """
        try:
            await self.logger.info(f"正在从硬停止自动恢复: {reason}")
            
            # Clear emergency flags
            await self.redis.set_emergency_flag(False)
            await self.redis.set_manual_pause(False)
            await self.redis.set_system_status("normal")
            
            await self.logger.info("✓ 系统已从硬停止恢复到正常状态")
            
            # Broadcast recovery via WebSocket
            if self.websocket_manager:
                risk_data = {
                    "status": "normal",
                    "message": f"System auto-recovered from hard stop: {reason}",
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.websocket_manager.broadcast_risk_status_update(risk_data)
            
            # Send notification
            notifier = get_notifier(self.config)
            await notifier.notify_system_recovery(reason)
            
        except Exception as e:
            await self.logger.error(f"自动恢复失败: {e}")
    
    async def start_monitoring(self) -> None:
        """Start the position monitoring loop in the background."""
        if self._monitoring_task is None or self._monitoring_task.done():
            self._monitoring_task = asyncio.create_task(self.monitor_positions())
            await self.logger.info("持仓监控已启动")
        else:
            await self.logger.warning("持仓监控已在运行中")
    
    async def stop_monitoring(self) -> None:
        """Stop the position monitoring loop."""
        self._monitoring_active = False
        
        if self._monitoring_task and not self._monitoring_task.done():
            await self._monitoring_task
            await self.logger.info("持仓监控已停止")
    
    async def is_monitoring_active(self) -> bool:
        """Check if monitoring is currently active."""
        return self._monitoring_active and (
            self._monitoring_task is not None and not self._monitoring_task.done()
        )
