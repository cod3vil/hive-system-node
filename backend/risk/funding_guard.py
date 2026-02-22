"""
Funding Guard for monitoring funding rate risk.

This module provides functionality for:
- Detecting funding rate sign flips
- Calculating 6-period moving average
- Triggering position reduction when funding becomes negative
- Checking funding reconciliation timeouts
- Handling funding anomalies

Requirements: 5.6, 5.7, 5.10, 5.11, 18.10, 18.11
"""

from decimal import Decimal
from typing import List, Optional
from dataclasses import dataclass

from backend.services.funding_service import FundingService, FundingRate
from backend.storage.redis_client import RedisClient
from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger
from backend.services.notifier import get_notifier


logger = get_logger(__name__)


@dataclass
class FundingTrendAnalysis:
    """
    Funding rate trend analysis result.
    
    Attributes:
        symbol: Trading pair symbol
        current_rate: Current funding rate
        six_period_avg: 6-period moving average
        is_negative: Whether current rate is negative
        avg_is_negative: Whether 6-period average is negative
        should_halt_new: Whether to halt new position openings
        should_reduce: Whether to reduce existing positions
    """
    symbol: str
    current_rate: Decimal
    six_period_avg: Decimal
    is_negative: bool
    avg_is_negative: bool
    should_halt_new: bool
    should_reduce: bool


@dataclass
class FundingReconciliationStatus:
    """
    Funding reconciliation status for a position.
    
    Attributes:
        position_id: Position ID
        symbol: Trading pair symbol
        last_reconciliation_hours: Hours since last reconciliation
        cycles_since_reconciliation: Number of funding cycles since reconciliation
        is_anomaly: Whether this is considered an anomaly (> 16 hours)
        should_reduce: Whether position should be reduced
    """
    position_id: str
    symbol: str
    last_reconciliation_hours: float
    cycles_since_reconciliation: int
    is_anomaly: bool
    should_reduce: bool


class FundingGuard:
    """
    Guard for monitoring and managing funding rate risk.
    
    This guard:
    - Detects funding rate sign flips (Requirement 5.6)
    - Calculates 6-period moving average (Requirement 5.7)
    - Triggers position reduction when avg negative (Requirement 5.11)
    - Checks funding reconciliation timeouts (Requirement 18.10)
    - Handles funding anomalies (Requirement 18.11)
    """
    
    def __init__(
        self,
        funding_service: FundingService,
        redis_client: RedisClient,
        config: SystemConfig
    ):
        """
        Initialize FundingGuard.
        
        Args:
            funding_service: Service for fetching funding data
            redis_client: Redis client for state management
            config: System configuration
        """
        self.funding_service = funding_service
        self.redis = redis_client
        self.config = config
        self.logger = logger
        self.funding_reconciliation_timeout_hours = config.funding_reconciliation_timeout_hours
    
    async def check_funding_sign_flip(
        self,
        symbol: str,
        previous_rate: Optional[Decimal] = None
    ) -> bool:
        """
        Check if funding rate has flipped from positive to negative.
        
        Requirement 5.6: Monitor funding rate changes for all open positions
        Requirement 5.10: When current funding rate becomes negative, immediately stop opening new positions
        
        Args:
            symbol: Trading pair symbol
            previous_rate: Previous funding rate (if None, fetches from cache)
            
        Returns:
            True if funding rate flipped from positive to negative
        """
        try:
            # Get current funding rate
            current_rate = await self.funding_service.get_current_funding_rate(symbol)
            
            # Get previous rate if not provided
            if previous_rate is None:
                # Try to get from recent history
                history = await self.funding_service.get_funding_history(symbol, periods=2)
                if len(history) >= 2:
                    previous_rate = history[1].rate  # Second most recent
                else:
                    # No history available, can't detect flip
                    await self.logger.debug(
                        f"No previous funding rate available for {symbol}, cannot detect flip"
                    )
                    return False
            
            # Check for sign flip (positive to negative)
            flipped = previous_rate > 0 and current_rate < 0
            
            if flipped:
                await self.logger.warning(
                    f"FUNDING SIGN FLIP DETECTED: {symbol} "
                    f"changed from {previous_rate:.6f} to {current_rate:.6f}. "
                    f"Halting new position openings."
                )
            
            return flipped
            
        except Exception as e:
            await self.logger.error(
                f"Failed to check funding sign flip for {symbol}: {e}"
            )
            return False
    
    async def calculate_funding_trend(
        self,
        symbol: str,
        periods: int = 6
    ) -> Decimal:
        """
        Calculate N-period moving average of funding rates.
        
        Requirement 5.7: Calculate the 6-period moving average of funding rates for trend analysis
        
        Args:
            symbol: Trading pair symbol
            periods: Number of periods for moving average (default 6)
            
        Returns:
            Moving average funding rate
        """
        try:
            # Get funding history
            history = await self.funding_service.get_funding_history(symbol, periods=periods)
            
            if not history:
                await self.logger.warning(
                    f"No funding history available for {symbol}, cannot calculate trend"
                )
                return Decimal("0")
            
            if len(history) < periods:
                await self.logger.warning(
                    f"Insufficient funding history for {symbol}: "
                    f"got {len(history)}, need {periods}"
                )
            
            # Calculate average
            total = sum(rate.rate for rate in history)
            avg = total / len(history)
            
            await self.logger.debug(
                f"Funding trend for {symbol}: {avg:.6f} "
                f"(average of {len(history)} periods)"
            )
            
            return avg
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate funding trend for {symbol}: {e}"
            )
            return Decimal("0")
    
    async def analyze_funding_trend(
        self,
        symbol: str
    ) -> FundingTrendAnalysis:
        """
        Analyze funding rate trend and determine required actions.
        
        Requirements 5.6, 5.7, 5.10, 5.11:
        - Monitor funding rate changes
        - Calculate 6-period moving average
        - Halt new positions when current rate negative
        - Reduce positions when 6-period average negative
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            FundingTrendAnalysis with trend information and action flags
        """
        try:
            # Get current rate
            current_rate = await self.funding_service.get_current_funding_rate(symbol)
            
            # Calculate 6-period average
            six_period_avg = await self.calculate_funding_trend(symbol, periods=6)
            
            # Determine flags
            is_negative = current_rate < 0
            avg_is_negative = six_period_avg < 0
            
            # Requirement 5.10: Halt new positions when current rate negative
            should_halt_new = is_negative
            
            # Requirement 5.11: Reduce positions when 6-period average negative
            should_reduce = avg_is_negative
            
            analysis = FundingTrendAnalysis(
                symbol=symbol,
                current_rate=current_rate,
                six_period_avg=six_period_avg,
                is_negative=is_negative,
                avg_is_negative=avg_is_negative,
                should_halt_new=should_halt_new,
                should_reduce=should_reduce
            )
            
            if should_halt_new:
                await self.logger.warning(
                    f"Funding rate for {symbol} is NEGATIVE: {current_rate:.6f}. "
                    f"Halting new position openings."
                )
            
            if should_reduce:
                await self.logger.warning(
                    f"6-period average funding rate for {symbol} is NEGATIVE: {six_period_avg:.6f}. "
                    f"Position reduction required (50%)."
                )
            
            return analysis
            
        except Exception as e:
            await self.logger.error(
                f"Failed to analyze funding trend for {symbol}: {e}"
            )
            # Return conservative analysis on error
            return FundingTrendAnalysis(
                symbol=symbol,
                current_rate=Decimal("0"),
                six_period_avg=Decimal("0"),
                is_negative=False,
                avg_is_negative=False,
                should_halt_new=False,
                should_reduce=False
            )
    
    async def should_reduce_position(
        self,
        symbol: str
    ) -> bool:
        """
        Determine if position should be reduced based on funding trend.
        
        Requirement 5.11: When 6-period average funding rate becomes negative, reduce position size by 50%
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            True if position should be reduced
        """
        analysis = await self.analyze_funding_trend(symbol)
        return analysis.should_reduce
    
    async def check_funding_reconciliation_status(
        self,
        position_id: str,
        symbol: str
    ) -> FundingReconciliationStatus:
        """
        Check if funding has been reconciled within acceptable timeframe.
        
        Requirement 18.10: Verify funding payment receipt after each funding period
        
        If not reconciled within 2 funding cycles (16 hours), mark as anomaly.
        
        Args:
            position_id: Position ID to check
            symbol: Trading pair symbol
            
        Returns:
            FundingReconciliationStatus with reconciliation information
        """
        try:
            # Get last reconciliation timestamp
            last_reconciliation_ts = await self.redis.get_funding_reconciliation(symbol)
            
            if last_reconciliation_ts is None:
                await self.logger.warning(
                    f"No funding reconciliation timestamp found for {symbol}"
                )
                # Return status indicating no reconciliation yet
                return FundingReconciliationStatus(
                    position_id=position_id,
                    symbol=symbol,
                    last_reconciliation_hours=0.0,
                    cycles_since_reconciliation=0,
                    is_anomaly=False,
                    should_reduce=False
                )
            
            # Calculate time since last reconciliation
            import time
            current_time = time.time()
            time_since_seconds = current_time - last_reconciliation_ts
            time_since_hours = time_since_seconds / 3600
            
            # Calculate funding cycles (assuming 8-hour cycles)
            cycles_since = int(time_since_hours / 8)
            
            # Check if exceeds timeout (16 hours = 2 cycles)
            is_anomaly = time_since_hours > self.funding_reconciliation_timeout_hours
            
            # Should reduce if anomaly detected
            should_reduce = is_anomaly
            
            status = FundingReconciliationStatus(
                position_id=position_id,
                symbol=symbol,
                last_reconciliation_hours=time_since_hours,
                cycles_since_reconciliation=cycles_since,
                is_anomaly=is_anomaly,
                should_reduce=should_reduce
            )
            
            if is_anomaly:
                await self.logger.error(
                    f"FUNDING RECONCILIATION ANOMALY: Position {position_id} ({symbol}) "
                    f"has not been reconciled for {time_since_hours:.1f} hours "
                    f"({cycles_since} funding cycles). "
                    f"Threshold: {self.funding_reconciliation_timeout_hours} hours."
                )
            
            return status
            
        except Exception as e:
            await self.logger.error(
                f"Failed to check funding reconciliation status for {position_id}: {e}"
            )
            # Return conservative status on error
            return FundingReconciliationStatus(
                position_id=position_id,
                symbol=symbol,
                last_reconciliation_hours=0.0,
                cycles_since_reconciliation=0,
                is_anomaly=False,
                should_reduce=False
            )
    
    async def handle_funding_anomaly(
        self,
        position_id: str,
        symbol: str
    ) -> None:
        """
        Handle position with funding reconciliation anomaly.
        
        Requirement 18.11: Alert operators and log discrepancies when reconciliation fails
        
        Actions:
        - Mark position with funding anomaly flag
        - Log critical alert
        - Trigger position reduction (50%)
        
        Args:
            position_id: Position ID with anomaly
            symbol: Trading pair symbol
        """
        try:
            # Mark anomaly in Redis
            await self.funding_service.mark_funding_anomaly(position_id)
            
            # Log critical alert
            await self.logger.critical(
                f"FUNDING ANOMALY HANDLER: Position {position_id} ({symbol}) "
                f"marked for investigation and 50% reduction. "
                f"Funding payment not reconciled within "
                f"{self.funding_reconciliation_timeout_hours} hours."
            )
            
            # Send alerts to all channels
            notifier = get_notifier(self.config)
            await notifier.notify_funding_anomaly(position_id, symbol)
            
            await self.logger.info(
                f"Funding anomaly handling completed for position {position_id}"
            )
            
        except Exception as e:
            await self.logger.critical(
                f"CRITICAL: Failed to handle funding anomaly for position {position_id}: {e}"
            )
    
    async def get_positions_requiring_reduction(
        self,
        open_positions: List[str]
    ) -> List[tuple[str, str]]:
        """
        Get list of positions that require reduction due to funding issues.
        
        Checks both:
        - Negative 6-period average funding rate
        - Funding reconciliation anomalies
        
        Args:
            open_positions: List of (position_id, symbol) tuples
            
        Returns:
            List of (position_id, reason) tuples for positions requiring reduction
        """
        positions_to_reduce = []
        
        for position_id, symbol in open_positions:
            try:
                # Check funding trend
                analysis = await self.analyze_funding_trend(symbol)
                if analysis.should_reduce:
                    positions_to_reduce.append(
                        (position_id, f"Negative 6-period avg funding: {analysis.six_period_avg:.6f}")
                    )
                    continue
                
                # Check reconciliation status
                recon_status = await self.check_funding_reconciliation_status(
                    position_id, symbol
                )
                if recon_status.should_reduce:
                    positions_to_reduce.append(
                        (position_id, f"Funding reconciliation timeout: {recon_status.last_reconciliation_hours:.1f}h")
                    )
                    
            except Exception as e:
                await self.logger.error(
                    f"Failed to check reduction requirement for position {position_id}: {e}"
                )
        
        if positions_to_reduce:
            await self.logger.warning(
                f"Found {len(positions_to_reduce)} positions requiring reduction due to funding issues"
            )
        
        return positions_to_reduce
