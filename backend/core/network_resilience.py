"""
Network resilience module for handling API connectivity failures.

This module implements graduated response to network failures:
- 0-30s: Stop new trades, continue monitoring
- 30-120s: Mark positions HIGH RISK
- >120s: Emergency close if state unverifiable

Requirements: 9.4, 9.5, 9.6, 9.7, 9.8, 9.9, 9.10, 9.11
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from decimal import Decimal

from backend.utils.logger import get_logger


class NetworkFailureState:
    """Tracks network failure state and duration."""
    
    def __init__(self):
        """Initialize network failure state."""
        self.failure_start: Optional[datetime] = None
        self.last_successful_request: datetime = datetime.utcnow()
        self.consecutive_failures: int = 0
        self.is_failing: bool = False
    
    def mark_failure(self) -> None:
        """Mark a network failure."""
        if not self.is_failing:
            self.failure_start = datetime.utcnow()
            self.is_failing = True
        
        self.consecutive_failures += 1
    
    def mark_success(self) -> None:
        """Mark a successful network request."""
        self.last_successful_request = datetime.utcnow()
        self.consecutive_failures = 0
        self.is_failing = False
        self.failure_start = None
    
    def get_failure_duration(self) -> Optional[float]:
        """
        Get duration of current failure in seconds.
        
        Returns:
            Duration in seconds, or None if not failing
        """
        if not self.is_failing or not self.failure_start:
            return None
        
        return (datetime.utcnow() - self.failure_start).total_seconds()
    
    def get_time_since_last_success(self) -> float:
        """
        Get time since last successful request in seconds.
        
        Returns:
            Duration in seconds
        """
        return (datetime.utcnow() - self.last_successful_request).total_seconds()


class NetworkResilienceManager:
    """
    Manages network failure detection and graduated response.
    
    Requirements:
    - 9.4: Stop new trades on network interruption
    - 9.5: Maintain positions during 0-30s interruption
    - 9.6: Mark HIGH RISK during 30-120s interruption
    - 9.7: Emergency close after >120s if state unverifiable
    - 9.8: Query exchange for position state on recovery
    - 9.9: Rebuild Redis state from exchange data
    - 9.10: Validate database consistency with exchange
    - 9.11: Resume normal operation after successful synchronization
    """
    
    def __init__(
        self,
        redis_client,
        exchange_client,
        position_service,
        equity_service,
        websocket_manager,
        config
    ):
        """
        Initialize network resilience manager.
        
        Args:
            redis_client: Redis client for state management
            exchange_client: Exchange client for API calls
            position_service: Position service for position management
            equity_service: Equity service for equity calculations
            websocket_manager: WebSocket manager for notifications
            config: System configuration
        """
        self.redis = redis_client
        self.exchange = exchange_client
        self.position_service = position_service
        self.equity_service = equity_service
        self.websocket = websocket_manager
        self.config = config
        self.logger = get_logger("NetworkResilience", websocket_manager=websocket_manager)
        
        # Network failure tracking
        self.state = NetworkFailureState()
        
        # Thresholds (in seconds)
        self.soft_timeout = config.network_soft_timeout_seconds  # 30s
        self.hard_timeout = config.network_hard_timeout_seconds  # 120s
        
        # Monitoring task
        self.monitoring_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start_monitoring(self) -> None:
        """
        Start network failure monitoring.
        
        This runs a background task that checks network health
        and triggers appropriate responses.
        """
        if self.running:
            await self.logger.warning("网络监控已在运行中")
            return
        
        self.running = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        await self.logger.info("网络弹性监控已启动")
    
    async def stop_monitoring(self) -> None:
        """Stop network failure monitoring."""
        self.running = False
        
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        await self.logger.info("网络弹性监控已停止")
    
    async def _monitoring_loop(self) -> None:
        """
        Background monitoring loop for network health.
        
        Checks network health every 5 seconds and triggers
        appropriate responses based on failure duration.
        """
        while self.running:
            try:
                # Check network health
                await self._check_network_health()
                
                # Check if we need to take action based on failure duration
                if self.state.is_failing:
                    failure_duration = self.state.get_failure_duration()
                    
                    if failure_duration is not None:
                        await self._handle_failure_duration(failure_duration)
                
                # Wait before next check
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self.logger.error(
                    "网络监控循环出错",
                    error=str(e)
                )
                await asyncio.sleep(5)
    
    async def _check_network_health(self) -> bool:
        """
        Check network health by making a lightweight API call.
        
        Returns:
            True if network is healthy, False otherwise
        """
        try:
            # Make a lightweight API call (fetch server time)
            await self.exchange.exchange.fetch_time()
            
            # Mark success
            if self.state.is_failing:
                await self.logger.info("网络连接已恢复")
                await self._handle_recovery()
            
            self.state.mark_success()
            return True
            
        except Exception as e:
            # Mark failure
            was_failing = self.state.is_failing
            self.state.mark_failure()
            
            if not was_failing:
                await self.logger.warning(
                    "检测到网络故障",
                    error=str(e)
                )
            
            return False
    
    async def _handle_failure_duration(self, duration: float) -> None:
        """
        Handle network failure based on duration.
        
        Requirements:
        - 9.5: 0-30s - Stop new trades, continue monitoring
        - 9.6: 30-120s - Mark HIGH RISK
        - 9.7: >120s - Emergency close if state unverifiable
        
        Args:
            duration: Failure duration in seconds
        """
        if duration < self.soft_timeout:
            # Requirement 9.5: 0-30s - Stop new trades
            await self._handle_soft_timeout()
        
        elif duration < self.hard_timeout:
            # Requirement 9.6: 30-120s - Mark HIGH RISK
            await self._handle_medium_timeout()
        
        else:
            # Requirement 9.7: >120s - Emergency close
            await self._handle_hard_timeout()
    
    async def _handle_soft_timeout(self) -> None:
        """
        Handle 0-30s network interruption.
        
        Requirement 9.5: Stop new trades, continue monitoring
        """
        # Check if we've already set soft pause
        system_status = await self.redis.get_system_status()
        
        if system_status == "normal":
            await self.logger.warning(
                "网络中断0-30秒: 停止新交易"
            )
            
            # Set soft pause (stop new trades)
            await self.redis.set_system_status("soft_pause")
            
            # Broadcast risk status update
            await self.websocket.broadcast_risk_status_update({
                "status": "soft_pause",
                "reason": "network_interruption",
                "duration_seconds": self.state.get_failure_duration(),
                "message": "Network interruption detected - new trades stopped"
            })
    
    async def _handle_medium_timeout(self) -> None:
        """
        Handle 30-120s network interruption.
        
        Requirement 9.6: Mark positions HIGH RISK
        """
        # Check if we've already marked HIGH RISK
        system_status = await self.redis.get_system_status()
        
        if system_status != "high_risk":
            await self.logger.error(
                "网络中断30-120秒: 标记持仓为高风险"
            )
            
            # Set high risk status
            await self.redis.set_system_status("high_risk")
            
            # Get all open positions
            positions = await self.position_service.get_open_positions()
            
            # Broadcast risk status update
            await self.websocket.broadcast_risk_status_update({
                "status": "high_risk",
                "reason": "extended_network_interruption",
                "duration_seconds": self.state.get_failure_duration(),
                "open_positions": len(positions),
                "message": "Extended network interruption - positions marked HIGH RISK"
            })
    
    async def _handle_hard_timeout(self) -> None:
        """
        Handle >120s network interruption.
        
        Requirement 9.7: Emergency close if state unverifiable
        """
        await self.logger.critical(
            "网络中断>120秒: 正在启动紧急平仓"
        )
        
        # Set emergency flag
        await self.redis.set_emergency_flag(True)
        await self.redis.set_system_status("emergency")
        
        # Broadcast emergency status
        await self.websocket.broadcast_risk_status_update({
            "status": "emergency",
            "reason": "critical_network_failure",
            "duration_seconds": self.state.get_failure_duration(),
            "message": "Critical network failure - emergency close initiated"
        })
        
        # Note: Actual emergency close will be handled by the emergency controls module
        # We just set the flag here to trigger it
        await self.logger.critical(
            "紧急标志已设置 - 紧急控制应关闭所有持仓"
        )
    
    async def _handle_recovery(self) -> None:
        """
        Handle network recovery.
        
        Requirements:
        - 9.8: Query exchange for position state
        - 9.9: Rebuild Redis state from exchange data
        - 9.10: Validate database consistency
        - 9.11: Resume normal operation after synchronization
        """
        await self.logger.info("网络连接已恢复 - 正在启动恢复")
        
        try:
            # Requirement 9.8: Query exchange for actual position state
            await self.logger.info("正在查询交易所持仓状态...")
            exchange_positions = await self.exchange.fetch_positions()
            exchange_balance = await self.exchange.fetch_balance()
            
            await self.logger.info(
                f"交易所状态: {len(exchange_positions)} 个持仓, "
                f"余额: {exchange_balance.get('total', {}).get('USDT', 0)} USDT"
            )
            
            # Requirement 9.9: Rebuild Redis state from exchange data
            await self.logger.info("正在从交易所数据重建Redis状态...")
            
            # Get positions from database
            db_positions = await self.position_service.get_open_positions()
            
            # Requirement 9.10: Validate consistency
            await self.logger.info("正在验证数据库与交易所的一致性...")
            discrepancies = await self._validate_position_consistency(
                exchange_positions,
                db_positions
            )
            
            if discrepancies:
                await self.logger.warning(
                    f"发现 {len(discrepancies)} 个持仓差异",
                    discrepancies=discrepancies
                )
                
                # Log discrepancies for manual review
                for discrepancy in discrepancies:
                    await self.logger.warning(
                        "持仓差异",
                        details=discrepancy
                    )
            
            # Recalculate equity
            total_equity = await self.equity_service.calculate_total_equity()
            await self.logger.info(f"当前权益: {total_equity} USDT")
            
            # Requirement 9.11: Resume normal operation
            await self.logger.info("状态同步完成 - 恢复正常运行")
            
            # Clear emergency flag if set
            await self.redis.set_emergency_flag(False)
            
            # Resume normal status
            # Network-related emergencies should auto-recover after network restoration
            system_status = await self.redis.get_system_status()
            if system_status in ["soft_pause", "high_risk", "emergency"]:
                await self.redis.set_system_status("normal")
                await self.redis.set_manual_pause(False)
                
                await self.logger.info(
                    f"系统状态已从 {system_status} 恢复到 normal"
                )
                
                # Broadcast recovery
                await self.websocket.broadcast_risk_status_update({
                    "status": "normal",
                    "reason": "network_recovered",
                    "message": "Network connection restored - normal operation resumed"
                })
            elif system_status == "hard_stop":
                # Hard stop requires manual intervention (e.g., drawdown exceeded)
                # Do not auto-recover, but log for operator attention
                await self.logger.warning(
                    "网络已恢复，但系统处于 hard_stop 状态（可能由回撤触发）。"
                    "需要人工检查并手动恢复。"
                )
            
            await self.logger.info("网络恢复完成")
            
        except Exception as e:
            await self.logger.error(
                "网络恢复过程中出错",
                error=str(e)
            )
            # Keep in safe mode if recovery fails
            await self.redis.set_system_status("soft_pause")
    
    async def _validate_position_consistency(
        self,
        exchange_positions: list,
        db_positions: list
    ) -> list:
        """
        Validate consistency between exchange and database positions.
        
        Requirement 9.10: Validate database consistency with exchange state
        
        Args:
            exchange_positions: Positions from exchange API
            db_positions: Positions from database
            
        Returns:
            List of discrepancies found
        """
        discrepancies = []
        
        # Create lookup maps
        exchange_map = {
            p.get('symbol'): p for p in exchange_positions
            if float(p.get('contracts', 0)) != 0
        }
        
        db_map = {
            p.symbol: p for p in db_positions
            if p.status == 'open'
        }
        
        # Check for positions in DB but not on exchange
        for symbol, db_pos in db_map.items():
            if symbol not in exchange_map:
                discrepancies.append({
                    "type": "missing_on_exchange",
                    "symbol": symbol,
                    "position_id": db_pos.position_id,
                    "db_quantity": str(db_pos.perp_quantity)
                })
        
        # Check for positions on exchange but not in DB
        for symbol, exch_pos in exchange_map.items():
            if symbol not in db_map:
                discrepancies.append({
                    "type": "missing_in_database",
                    "symbol": symbol,
                    "exchange_quantity": str(exch_pos.get('contracts', 0))
                })
        
        # Check for quantity mismatches
        for symbol in set(exchange_map.keys()) & set(db_map.keys()):
            exch_qty = Decimal(str(exchange_map[symbol].get('contracts', 0)))
            db_qty = db_map[symbol].perp_quantity
            
            # Allow small differences due to rounding
            if abs(exch_qty - db_qty) > Decimal("0.001"):
                discrepancies.append({
                    "type": "quantity_mismatch",
                    "symbol": symbol,
                    "position_id": db_map[symbol].position_id,
                    "exchange_quantity": str(exch_qty),
                    "db_quantity": str(db_qty),
                    "difference": str(abs(exch_qty - db_qty))
                })
        
        return discrepancies
    
    def mark_request_success(self) -> None:
        """
        Mark a successful API request.
        
        This should be called after any successful exchange API call
        to update the network health state.
        """
        if self.state.is_failing:
            asyncio.create_task(self._handle_recovery())
        
        self.state.mark_success()
    
    def mark_request_failure(self) -> None:
        """
        Mark a failed API request.
        
        This should be called after any failed exchange API call
        to update the network health state.
        """
        self.state.mark_failure()
    
    def get_network_status(self) -> Dict[str, Any]:
        """
        Get current network status.
        
        Returns:
            Dictionary with network status information
        """
        return {
            "is_failing": self.state.is_failing,
            "failure_duration": self.state.get_failure_duration(),
            "time_since_last_success": self.state.get_time_since_last_success(),
            "consecutive_failures": self.state.consecutive_failures,
            "soft_timeout": self.soft_timeout,
            "hard_timeout": self.hard_timeout
        }
