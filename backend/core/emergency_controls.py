"""
Emergency control handlers for one-click emergency actions.

This module implements:
- One-click emergency close (close all positions)
- One-click pause (stop new trades)
- Logging and notifications for emergency actions

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6
"""

import asyncio
from typing import List, Optional, TYPE_CHECKING
from datetime import datetime
from dataclasses import dataclass

from storage.redis_client import RedisClient
from config.settings import get_config
from services.notifier import get_notifier
from utils.logger import get_logger

if TYPE_CHECKING:
    from execution.trade_executor import TradeExecutor
    from websocket.websocket_server import ConnectionManager


logger = get_logger(__name__)


@dataclass
class EmergencyActionResult:
    """
    Result of an emergency action.
    
    Attributes:
        success: Whether the action completed successfully
        action_type: Type of action ("emergency_close", "pause", "resume")
        message: Human-readable message
        positions_affected: List of position IDs affected
        timestamp: When the action was executed
        errors: List of errors encountered (if any)
    """
    success: bool
    action_type: str
    message: str
    positions_affected: List[str]
    timestamp: datetime
    errors: List[str]


class EmergencyControls:
    """
    Emergency control handlers for critical system actions.
    
    Provides one-click emergency controls:
    - Emergency close: Close all positions immediately
    - Pause: Stop new trade execution
    - Resume: Resume normal operations
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        trade_executor: Optional["TradeExecutor"] = None,
        websocket_manager: Optional["ConnectionManager"] = None,
        exchange_manager=None
    ):
        """
        Initialize Emergency Controls.

        Args:
            redis_client: Redis client for state management
            trade_executor: Trade executor for closing positions
            websocket_manager: WebSocket manager for broadcasting updates
            exchange_manager: Exchange manager for cross-exchange position closing
        """
        self.redis = redis_client
        self.trade_executor = trade_executor
        self.websocket_manager = websocket_manager
        self._exchange_manager = exchange_manager
        self.logger = logger
    
    async def emergency_close_all(self, reason: str = "Manual emergency close") -> EmergencyActionResult:
        """
        One-click emergency close: Close all open positions immediately.
        
        Requirements:
        - 7.1: Provide one-click emergency close button
        - 7.3: Close all positions using market orders
        - 7.5: Log emergency control activations
        - 7.6: Send notifications when emergency controls activated
        
        This method:
        1. Sets emergency flag in Redis
        2. Gets all open positions
        3. Closes each position using market orders
        4. Logs all actions
        5. Broadcasts updates via WebSocket
        
        Args:
            reason: Reason for emergency close
            
        Returns:
            EmergencyActionResult with details of the action
        """
        timestamp = datetime.utcnow()
        errors = []
        positions_closed = []
        
        await self.logger.critical(
            f"EMERGENCY CLOSE INITIATED: {reason}",
            extra={"timestamp": timestamp.isoformat()}
        )
        
        try:
            # Set emergency flag (Requirement 7.3)
            await self.redis.set_emergency_flag(True)
            await self.redis.set_system_status("emergency_close")
            
            # Get all open positions
            open_positions = await self.redis.get_open_positions()
            
            if not open_positions:
                message = "No open positions to close"
                await self.logger.warning(message)
                
                # Broadcast update
                if self.websocket_manager:
                    await self._broadcast_emergency_action(
                        action_type="emergency_close",
                        message=message,
                        positions_affected=[],
                        timestamp=timestamp
                    )
                
                return EmergencyActionResult(
                    success=True,
                    action_type="emergency_close",
                    message=message,
                    positions_affected=[],
                    timestamp=timestamp,
                    errors=[]
                )
            
            await self.logger.critical(
                f"Closing {len(open_positions)} open positions",
                extra={"position_count": len(open_positions)}
            )
            
            # Close each position in batches (Requirement 7.3)
            # Sort by position notional size (largest first) and batch to reduce market impact
            if self.trade_executor:
                from core.models import Position

                # Load all positions and sort by notional size descending
                position_objects = []
                for position_id in open_positions:
                    try:
                        position_data = await self.redis.get_position(position_id)
                        if not position_data:
                            error_msg = f"Position {position_id} not found in Redis"
                            await self.logger.error(error_msg)
                            errors.append(error_msg)
                            continue
                        position = Position.from_dict(position_data)
                        position_objects.append(position)
                    except Exception as e:
                        error_msg = f"Error loading position {position_id}: {str(e)}"
                        await self.logger.error(error_msg)
                        errors.append(error_msg)

                # Sort by notional value descending (close largest first)
                position_objects.sort(
                    key=lambda p: float(p.spot_quantity * (p.spot_entry_price or 0)),
                    reverse=True,
                )

                # Process in batches of 3 with 2-second sleep between batches
                batch_size = 3
                for batch_start in range(0, len(position_objects), batch_size):
                    batch = position_objects[batch_start:batch_start + batch_size]

                    if batch_start > 0:
                        await asyncio.sleep(2)

                    for position in batch:
                        try:
                            await self.logger.info(
                                f"Closing position {position.position_id} ({position.symbol})"
                            )

                            if position.strategy_type == "cross_exchange":
                                # Cross-exchange needs exchange_manager
                                result = await self.trade_executor.execute_cross_exchange_close(
                                    position, self._exchange_manager
                                ) if hasattr(self, '_exchange_manager') and self._exchange_manager else \
                                    await self.trade_executor.execute_close_position(position)
                            else:
                                result = await self.trade_executor.execute_close_position(position)

                            if result.success:
                                positions_closed.append(position.position_id)
                                await self.logger.info(
                                    f"Position {position.position_id} closed successfully"
                                )
                            else:
                                error_msg = f"Failed to close position {position.position_id}: {result.error_message}"
                                await self.logger.error(error_msg)
                                errors.append(error_msg)

                        except Exception as e:
                            error_msg = f"Error closing position {position.position_id}: {str(e)}"
                            await self.logger.error(error_msg)
                            errors.append(error_msg)
            else:
                error_msg = "Trade executor not available, cannot close positions"
                await self.logger.critical(error_msg)
                errors.append(error_msg)
            
            # Determine success
            success = len(errors) == 0
            
            if success:
                message = f"Emergency close completed: {len(positions_closed)} positions closed"
            else:
                message = (
                    f"Emergency close completed with errors: "
                    f"{len(positions_closed)}/{len(open_positions)} positions closed"
                )
            
            await self.logger.critical(
                message,
                extra={
                    "positions_closed": len(positions_closed),
                    "total_positions": len(open_positions),
                    "errors": len(errors)
                }
            )
            
            # Broadcast update (Requirement 7.6)
            if self.websocket_manager:
                await self._broadcast_emergency_action(
                    action_type="emergency_close",
                    message=message,
                    positions_affected=positions_closed,
                    timestamp=timestamp
                )

            # Send notification to all channels
            notifier = get_notifier(get_config())
            await notifier.notify_emergency_close(reason, len(open_positions))

            return EmergencyActionResult(
                success=success,
                action_type="emergency_close",
                message=message,
                positions_affected=positions_closed,
                timestamp=timestamp,
                errors=errors
            )
        
        except Exception as e:
            error_msg = f"Emergency close failed: {str(e)}"
            await self.logger.critical(error_msg)
            
            return EmergencyActionResult(
                success=False,
                action_type="emergency_close",
                message=error_msg,
                positions_affected=positions_closed,
                timestamp=timestamp,
                errors=[error_msg]
            )
    
    async def pause_trading(self, reason: str = "Manual pause") -> EmergencyActionResult:
        """
        One-click pause: Stop all new trade execution.
        
        Requirements:
        - 7.2: Provide one-click pause button
        - 7.4: Reject all new trade requests when paused
        - 7.5: Log emergency control activations
        - 7.6: Send notifications when emergency controls activated
        
        This method:
        1. Sets manual pause flag in Redis
        2. Sets system status to "soft_pause"
        3. Logs the action
        4. Broadcasts updates via WebSocket
        
        Existing positions remain open and continue to be monitored.
        
        Args:
            reason: Reason for pause
            
        Returns:
            EmergencyActionResult with details of the action
        """
        timestamp = datetime.utcnow()
        
        await self.logger.warning(
            f"TRADING PAUSED: {reason}",
            extra={"timestamp": timestamp.isoformat()}
        )
        
        try:
            # Set pause flags (Requirement 7.4)
            await self.redis.set_manual_pause(True)
            await self.redis.set_system_status("soft_pause")
            
            message = f"Trading paused: {reason}. New positions stopped, existing positions remain open."
            
            await self.logger.warning(
                message,
                extra={"reason": reason}
            )
            
            # Broadcast update (Requirement 7.6)
            if self.websocket_manager:
                await self._broadcast_emergency_action(
                    action_type="pause",
                    message=message,
                    positions_affected=[],
                    timestamp=timestamp
                )

            # Send notification to all channels
            notifier = get_notifier(get_config())
            await notifier.notify_trading_paused(reason)

            return EmergencyActionResult(
                success=True,
                action_type="pause",
                message=message,
                positions_affected=[],
                timestamp=timestamp,
                errors=[]
            )
        
        except Exception as e:
            error_msg = f"Failed to pause trading: {str(e)}"
            await self.logger.error(error_msg)
            
            return EmergencyActionResult(
                success=False,
                action_type="pause",
                message=error_msg,
                positions_affected=[],
                timestamp=timestamp,
                errors=[error_msg]
            )
    
    async def resume_trading(self, reason: str = "Manual resume") -> EmergencyActionResult:
        """
        Resume normal trading operations after pause.
        
        This method:
        1. Clears manual pause flag in Redis
        2. Clears emergency flag if set
        3. Sets system status to "normal"
        4. Logs the action
        5. Broadcasts updates via WebSocket
        
        Args:
            reason: Reason for resume
            
        Returns:
            EmergencyActionResult with details of the action
        """
        timestamp = datetime.utcnow()
        
        await self.logger.info(
            f"TRADING RESUMED: {reason}",
            extra={"timestamp": timestamp.isoformat()}
        )
        
        try:
            # Clear pause flags
            await self.redis.set_manual_pause(False)
            await self.redis.set_emergency_flag(False)
            await self.redis.set_system_status("normal")
            
            message = f"Trading resumed: {reason}. System returned to normal operations."
            
            await self.logger.info(
                message,
                extra={"reason": reason}
            )
            
            # Broadcast update
            if self.websocket_manager:
                await self._broadcast_emergency_action(
                    action_type="resume",
                    message=message,
                    positions_affected=[],
                    timestamp=timestamp
                )

            # Send notification to all channels
            notifier = get_notifier(get_config())
            await notifier.notify_trading_resumed(reason)

            return EmergencyActionResult(
                success=True,
                action_type="resume",
                message=message,
                positions_affected=[],
                timestamp=timestamp,
                errors=[]
            )
        
        except Exception as e:
            error_msg = f"Failed to resume trading: {str(e)}"
            await self.logger.error(error_msg)
            
            return EmergencyActionResult(
                success=False,
                action_type="resume",
                message=error_msg,
                positions_affected=[],
                timestamp=timestamp,
                errors=[error_msg]
            )
    
    async def get_system_status(self) -> dict:
        """
        Get current system status including emergency flags.
        
        Returns:
            Dictionary with system status information
        """
        try:
            status = await self.redis.get_system_status()
            emergency_flag = await self.redis.get_emergency_flag()
            manual_pause = await self.redis.get_manual_pause()
            open_positions = await self.redis.get_open_positions()
            
            return {
                "status": status,
                "emergency_flag": emergency_flag,
                "manual_pause": manual_pause,
                "open_positions_count": len(open_positions),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        except Exception as e:
            await self.logger.error(f"Failed to get system status: {e}")
            return {
                "status": "unknown",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def _broadcast_emergency_action(
        self,
        action_type: str,
        message: str,
        positions_affected: List[str],
        timestamp: datetime
    ) -> None:
        """
        Broadcast emergency action via WebSocket.
        
        Args:
            action_type: Type of emergency action
            message: Human-readable message
            positions_affected: List of position IDs affected
            timestamp: When the action occurred
        """
        if not self.websocket_manager:
            return
        
        try:
            data = {
                "type": "emergency_action",
                "action": action_type,
                "message": message,
                "positions_affected": positions_affected,
                "timestamp": timestamp.isoformat()
            }
            
            await self.websocket_manager.broadcast_risk_status_update(data)
            
        except Exception as e:
            await self.logger.error(f"Failed to broadcast emergency action: {e}")
