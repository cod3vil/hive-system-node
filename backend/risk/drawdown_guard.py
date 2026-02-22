"""
Drawdown Guard for monitoring drawdown risk.

This module provides functionality for:
- Tracking peak equity with Redis persistence
- Calculating drawdown percentage
- Checking drawdown limits against thresholds

Requirements: 5.2, 5.3, 5.4, 5.5, 5.13
"""

from decimal import Decimal
from typing import Optional
from dataclasses import dataclass

from backend.storage.redis_client import RedisClient
from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class DrawdownStatus:
    """
    Drawdown status information.
    
    Attributes:
        current_equity: Current account equity
        peak_equity: Peak equity value
        drawdown_pct: Current drawdown percentage (0.0 to 1.0)
        exceeds_limit: Whether drawdown exceeds configured limit
        limit_pct: Configured drawdown limit
    """
    current_equity: Decimal
    peak_equity: Decimal
    drawdown_pct: Decimal
    exceeds_limit: bool
    limit_pct: Decimal


class DrawdownGuard:
    """
    Guard for monitoring and preventing excessive drawdown.
    
    This guard:
    - Tracks peak equity with Redis persistence (Requirements 5.3, 5.4)
    - Calculates drawdown as (peak - current) / peak (Requirement 5.5)
    - Checks if drawdown exceeds 6% threshold (Requirement 5.13)
    - Ensures peak equity is monotonically non-decreasing (Requirement 5.3)
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        config: SystemConfig
    ):
        """
        Initialize DrawdownGuard.
        
        Args:
            redis_client: Redis client for persisting peak equity
            config: System configuration with risk thresholds
        """
        self.redis = redis_client
        self.config = config
        self.logger = logger
        self.max_drawdown = config.max_drawdown_pct
    
    async def get_peak_equity(self) -> Optional[Decimal]:
        """
        Get stored peak equity from Redis.
        
        Requirement 5.3: Track peak equity as the maximum of all historical real-time equity values
        
        Returns:
            Peak equity value or None if not set
        """
        try:
            peak = await self.redis.get_peak_equity()
            
            if peak is not None:
                await self.logger.debug(f"获取到峰值权益: {peak}")
            else:
                await self.logger.debug("Redis中未找到峰值权益")
            
            return peak
            
        except Exception as e:
            await self.logger.error(f"获取峰值权益失败: {e}")
            return None
    
    async def update_peak_equity(self, current_equity: Decimal) -> bool:
        """
        Update peak equity if current exceeds stored peak.
        
        Requirement 5.3: Track peak equity as the maximum of all historical real-time equity values
        Requirement 5.4: Store peak equity in Redis and update it whenever current equity exceeds the stored peak
        
        This ensures peak equity is monotonically non-decreasing.
        
        Args:
            current_equity: Current account equity
            
        Returns:
            True if peak was updated, False otherwise
        """
        try:
            peak = await self.get_peak_equity()
            
            # If no peak exists, set current as peak
            if peak is None:
                await self.redis.set_peak_equity(current_equity)
                await self.logger.info(
                    f"峰值权益已初始化: {current_equity}"
                )
                return True
            
            # Update peak if current exceeds it (monotonicity)
            if current_equity > peak:
                await self.redis.set_peak_equity(current_equity)
                await self.logger.info(
                    f"峰值权益已更新: {peak} -> {current_equity} "
                    f"(+{((current_equity - peak) / peak * 100):.2f}%)"
                )
                return True
            
            await self.logger.debug(
                f"峰值权益未变: {peak} (当前: {current_equity})"
            )
            return False
            
        except Exception as e:
            await self.logger.error(
                f"更新峰值权益失败: {e}"
            )
            return False
    
    async def calculate_drawdown(
        self,
        current_equity: Optional[Decimal] = None
    ) -> Decimal:
        """
        Calculate current drawdown percentage.
        
        Requirement 5.5: Calculate current drawdown as (peak_equity - current_equity) / peak_equity
        
        Formula: drawdown = (peak - current) / peak
        
        Args:
            current_equity: Current equity (if None, fetches from Redis)
            
        Returns:
            Drawdown percentage (0.0 to 1.0)
        """
        try:
            # Get current equity if not provided
            if current_equity is None:
                current_equity = await self.redis.get_current_equity()
                if current_equity is None:
                    await self.logger.warning(
                        "无法计算回撤: 当前权益不可用"
                    )
                    return Decimal("0")
            
            # Get peak equity
            peak = await self.get_peak_equity()
            if peak is None or peak == 0:
                await self.logger.warning(
                    "无法计算回撤: 峰值权益不可用"
                )
                return Decimal("0")
            
            # Calculate drawdown
            # Ensure current doesn't exceed peak (should be handled by update_peak_equity)
            if current_equity > peak:
                await self.logger.warning(
                    f"当前权益 ({current_equity}) 超过峰值 ({peak}). "
                    f"这不应该发生. 正在更新峰值."
                )
                await self.update_peak_equity(current_equity)
                return Decimal("0")
            
            drawdown = (peak - current_equity) / peak
            
            # Ensure drawdown is between 0 and 1
            drawdown = max(Decimal("0"), min(Decimal("1"), drawdown))
            
            await self.logger.debug(
                f"回撤: {drawdown:.2%} "
                f"(峰值: {peak}, 当前: {current_equity})"
            )
            
            return drawdown
            
        except Exception as e:
            await self.logger.error(f"计算回撤失败: {e}")
            return Decimal("0")
    
    async def check_drawdown_limits(
        self,
        current_equity: Optional[Decimal] = None
    ) -> DrawdownStatus:
        """
        Check if drawdown exceeds configured limits.
        
        Requirement 5.13: When total drawdown exceeds 6%, execute hard stop
        
        Args:
            current_equity: Current equity (if None, fetches from Redis)
            
        Returns:
            DrawdownStatus with current drawdown information
        """
        try:
            # Get current equity if not provided
            if current_equity is None:
                current_equity = await self.redis.get_current_equity()
                if current_equity is None:
                    await self.logger.error(
                        "无法检查回撤限制: 当前权益不可用"
                    )
                    # Return conservative status
                    return DrawdownStatus(
                        current_equity=Decimal("0"),
                        peak_equity=Decimal("0"),
                        drawdown_pct=Decimal("0"),
                        exceeds_limit=False,
                        limit_pct=self.max_drawdown
                    )
            
            # Get peak equity
            peak = await self.get_peak_equity()
            if peak is None:
                await self.logger.warning(
                    "无法检查回撤限制: 峰值权益不可用. "
                    "正在用当前权益初始化."
                )
                await self.update_peak_equity(current_equity)
                peak = current_equity
            
            # Calculate drawdown
            drawdown = await self.calculate_drawdown(current_equity)
            
            # Check if exceeds limit
            exceeds_limit = drawdown > self.max_drawdown
            
            status = DrawdownStatus(
                current_equity=current_equity,
                peak_equity=peak,
                drawdown_pct=drawdown,
                exceeds_limit=exceeds_limit,
                limit_pct=self.max_drawdown
            )
            
            if exceeds_limit:
                await self.logger.error(
                    f"回撤超限: {drawdown:.2%} > {self.max_drawdown:.2%}. "
                    f"峰值: {peak}, 当前: {current_equity}. "
                    f"需要硬停止."
                )
            else:
                await self.logger.debug(
                    f"回撤在限制范围内: {drawdown:.2%} <= {self.max_drawdown:.2%}"
                )
            
            # Update drawdown percentage in Redis for monitoring
            await self.redis.set_drawdown_pct(drawdown)
            
            return status
            
        except Exception as e:
            await self.logger.error(f"检查回撤限制失败: {e}")
            # Return conservative status on error
            return DrawdownStatus(
                current_equity=current_equity or Decimal("0"),
                peak_equity=Decimal("0"),
                drawdown_pct=Decimal("0"),
                exceeds_limit=False,
                limit_pct=self.max_drawdown
            )
    
    async def initialize_peak_equity(self, initial_equity: Decimal) -> None:
        """
        Initialize peak equity on system startup.
        
        Requirement 5.3: Track peak equity as the maximum of all historical real-time equity values
        
        Args:
            initial_equity: Initial equity value to set as peak
        """
        try:
            existing_peak = await self.get_peak_equity()
            
            if existing_peak is None:
                await self.redis.set_peak_equity(initial_equity)
                await self.logger.info(
                    f"峰值权益已初始化为: {initial_equity}"
                )
            else:
                await self.logger.info(
                    f"峰值权益已存在: {existing_peak}. "
                    f"不会用初始权益覆盖: {initial_equity}"
                )
                
                # Update if initial is higher (e.g., after system restart with gains)
                if initial_equity > existing_peak:
                    await self.update_peak_equity(initial_equity)
                    
        except Exception as e:
            await self.logger.error(f"初始化峰值权益失败: {e}")
    
    async def reset_peak_equity(self) -> None:
        """
        Reset peak equity (use only for testing or after hard stop recovery).
        
        WARNING: This should only be used in specific scenarios like:
        - Testing
        - After a hard stop when restarting with new capital
        - Manual reset by operator
        """
        try:
            await self.redis.set_peak_equity(Decimal("0"))
            await self.logger.warning("峰值权益已重置为0")
        except Exception as e:
            await self.logger.error(f"重置峰值权益失败: {e}")
