"""
Equity Service for calculating total equity, peak equity, and daily PnL.

This service manages equity calculations and tracking for risk management
and performance monitoring.

Requirements: 5.2, 5.3, 5.4
"""

from decimal import Decimal
from typing import Optional, TYPE_CHECKING
from datetime import datetime, date

from backend.storage.redis_client import RedisClient
from backend.services.exchange_client import ExchangeClient
from backend.utils.logger import get_logger

if TYPE_CHECKING:
    from backend.websocket.websocket_server import ConnectionManager


class EquityService:
    """
    Service for equity calculations and tracking.
    
    Responsibilities:
    - Calculate total account equity (balances + unrealized PnL)
    - Track peak equity with monotonicity guarantee (Requirement 5.3, 5.4)
    - Calculate daily PnL (Requirement 5.2)
    - Maintain equity state in Redis
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        exchange_client: ExchangeClient,
        position_service: Optional["PositionService"] = None,
        websocket_manager: Optional["ConnectionManager"] = None
    ):
        """
        Initialize EquityService.
        
        Args:
            redis_client: Redis client for state management
            exchange_client: Exchange client for balance queries
            position_service: Optional position service for PnL calculations
            websocket_manager: Optional WebSocket connection manager for broadcasts
        """
        self.redis = redis_client
        self.exchange = exchange_client
        self.position_service = position_service
        self.websocket_manager = websocket_manager
        self.logger = get_logger("EquityService")
    
    async def calculate_total_equity(self) -> Decimal:
        """
        Calculate total account equity from balances and unrealized PnL.
        
        Total equity = spot balance + futures balance + unrealized PnL
        
        Note: Unrealized PnL is already included in futures balance by most exchanges,
        so we fetch the total balance which includes both realized and unrealized PnL.
        
        Returns:
            Total equity in USD equivalent
            
        Raises:
            Exception: If calculation fails
        """
        try:
            # Fetch combined balance across spot + swap accounts
            combined = await self.exchange.fetch_combined_balance()

            # Total USDT across all accounts (includes unrealized PnL in swap)
            total_usdt = Decimal(str(combined.get('total', 0)))
            
            # Store in Redis
            await self.redis.set_current_equity(total_usdt)
            
            await self.logger.debug(
                f"Calculated total equity: {total_usdt} USDT"
            )
            
            # Broadcast equity update via WebSocket
            if self.websocket_manager:
                equity_data = {
                    "total_equity": str(total_usdt),
                    "timestamp": datetime.utcnow().isoformat()
                }
                await self.websocket_manager.broadcast_equity_update(equity_data)
            
            return total_usdt
            
        except Exception as e:
            await self.logger.error(
                "Failed to calculate total equity",
                error=str(e)
            )
            raise
    
    async def get_peak_equity(self) -> Decimal:
        """
        Get stored peak equity from Redis.
        
        Requirement 5.3: Track peak equity as maximum of all historical values
        
        Returns:
            Peak equity value, or current equity if no peak stored
        """
        try:
            peak = await self.redis.get_peak_equity()
            
            if peak is None:
                # Initialize peak with current equity
                current = await self.calculate_total_equity()
                await self.redis.set_peak_equity(current)
                await self.logger.info(
                    f"Initialized peak equity: {current} USDT"
                )
                return current
            
            return peak
            
        except Exception as e:
            await self.logger.error(
                "Failed to get peak equity",
                error=str(e)
            )
            # Return a safe default
            return Decimal("0")
    
    async def update_peak_equity(self, current_equity: Decimal) -> None:
        """
        Update peak equity with monotonicity check.
        
        Requirement 5.4: Store peak equity in Redis and update when current exceeds peak
        
        Peak equity is monotonically non-decreasing: only update if current > peak.
        
        Args:
            current_equity: Current equity value
        """
        try:
            peak = await self.redis.get_peak_equity()
            
            if peak is None:
                # Initialize peak
                await self.redis.set_peak_equity(current_equity)
                await self.logger.info(
                    f"Peak equity initialized: {current_equity} USDT"
                )
                return
            
            # Monotonicity check: only update if current exceeds peak
            if current_equity > peak:
                await self.redis.set_peak_equity(current_equity)
                await self.logger.info(
                    f"Peak equity updated: {peak} -> {current_equity} USDT"
                )
                
                # Broadcast equity update via WebSocket
                if self.websocket_manager:
                    equity_data = {
                        "peak_equity": str(current_equity),
                        "current_equity": str(current_equity),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    await self.websocket_manager.broadcast_equity_update(equity_data)
            else:
                await self.logger.debug(
                    f"Peak equity unchanged: {peak} USDT (current: {current_equity})"
                )
                
        except Exception as e:
            await self.logger.error(
                "Failed to update peak equity",
                error=str(e),
                current_equity=str(current_equity)
            )
    
    async def calculate_daily_pnl(self) -> Decimal:
        """
        Calculate PnL since start of day.
        
        Requirement 5.2: Calculate daily PnL for risk monitoring
        
        Daily PnL = current equity - equity at start of day
        
        Returns:
            Daily PnL in USD
        """
        try:
            # Get current equity
            current_equity = await self.redis.get_current_equity()
            if current_equity is None:
                current_equity = await self.calculate_total_equity()
            
            # Get equity at start of day
            daily_start = await self.redis.get_daily_start_equity()
            
            if daily_start is None:
                # Initialize daily start with current equity
                await self.redis.set_daily_start_equity(current_equity)
                await self.logger.info(
                    f"Initialized daily start equity: {current_equity} USDT"
                )
                return Decimal("0")
            
            # Calculate daily PnL
            daily_pnl = current_equity - daily_start
            
            await self.logger.debug(
                f"Daily PnL: {daily_pnl} USDT "
                f"(current: {current_equity}, start: {daily_start})"
            )
            
            return daily_pnl
            
        except Exception as e:
            await self.logger.error(
                "Failed to calculate daily PnL",
                error=str(e)
            )
            return Decimal("0")
    
    async def reset_daily_start_equity(self) -> None:
        """
        Reset daily start equity to current equity.
        
        This should be called at the start of each trading day.
        """
        try:
            current_equity = await self.calculate_total_equity()
            await self.redis.set_daily_start_equity(current_equity)
            
            await self.logger.info(
                f"Daily start equity reset: {current_equity} USDT"
            )
            
        except Exception as e:
            await self.logger.error(
                "Failed to reset daily start equity",
                error=str(e)
            )
    
    async def get_current_equity(self) -> Decimal:
        """
        Get current equity from Redis cache or calculate if not cached.
        
        Returns:
            Current equity in USD
        """
        try:
            equity = await self.redis.get_current_equity()
            
            if equity is None:
                equity = await self.calculate_total_equity()
            
            return equity
            
        except Exception as e:
            await self.logger.error(
                "Failed to get current equity",
                error=str(e)
            )
            return Decimal("0")
    
    async def calculate_drawdown(self) -> Decimal:
        """
        Calculate current drawdown percentage.
        
        Drawdown = (peak_equity - current_equity) / peak_equity
        
        Returns:
            Drawdown as decimal (e.g., 0.05 for 5%)
        """
        try:
            peak = await self.get_peak_equity()
            current = await self.get_current_equity()
            
            if peak == 0:
                return Decimal("0")
            
            drawdown = (peak - current) / peak
            
            # Ensure drawdown is non-negative
            if drawdown < 0:
                drawdown = Decimal("0")
            
            await self.logger.debug(
                f"Drawdown: {drawdown:.2%} (peak: {peak}, current: {current})"
            )
            
            return drawdown
            
        except Exception as e:
            await self.logger.error(
                "Failed to calculate drawdown",
                error=str(e)
            )
            return Decimal("0")
    
    async def calculate_daily_loss_pct(self) -> Decimal:
        """
        Calculate daily loss percentage.
        
        Daily loss % = daily_pnl / daily_start_equity (if negative)
        
        Returns:
            Daily loss percentage as positive decimal (e.g., 0.03 for 3% loss)
        """
        try:
            daily_pnl = await self.calculate_daily_pnl()
            daily_start = await self.redis.get_daily_start_equity()
            
            if daily_start is None or daily_start == 0:
                return Decimal("0")
            
            # Only calculate loss percentage if PnL is negative
            if daily_pnl >= 0:
                return Decimal("0")
            
            # Return as positive percentage
            loss_pct = abs(daily_pnl) / daily_start
            
            await self.logger.debug(
                f"Daily loss: {loss_pct:.2%} ({daily_pnl} / {daily_start})"
            )
            
            return loss_pct
            
        except Exception as e:
            await self.logger.error(
                "Failed to calculate daily loss percentage",
                error=str(e)
            )
            return Decimal("0")
