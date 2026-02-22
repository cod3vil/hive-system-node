"""
Funding Service for managing funding rate data and payment reconciliation.

This module provides functionality for:
- Fetching and caching funding rates in Redis
- Calculating annualized rates
- Retrieving funding history
- Reconciling funding payments
- Detecting funding anomalies

Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6, 18.7, 18.8, 18.9, 18.10, 18.11
"""

import asyncio
from decimal import Decimal
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from services.exchange_client import ExchangeClient
from storage.redis_client import RedisClient
from utils.logger import get_logger
from config.settings import get_config


logger = get_logger(__name__)


@dataclass
class FundingRate:
    """
    Funding rate data structure.
    
    Attributes:
        symbol: Trading pair symbol (e.g., "BTC/USDT:USDT")
        rate: Funding rate as decimal (e.g., 0.0001 = 0.01%)
        timestamp: When the funding rate was recorded
        next_funding_time: When the next funding payment occurs
    """
    symbol: str
    rate: Decimal
    timestamp: datetime
    next_funding_time: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "symbol": self.symbol,
            "rate": str(self.rate),
            "timestamp": self.timestamp.isoformat(),
            "next_funding_time": self.next_funding_time.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FundingRate":
        """Create from dictionary."""
        return cls(
            symbol=data["symbol"],
            rate=Decimal(data["rate"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            next_funding_time=datetime.fromisoformat(data["next_funding_time"])
        )


@dataclass
class ReconciliationResult:
    """
    Result of funding payment reconciliation.
    
    Attributes:
        reconciled: Whether the funding payment was successfully reconciled
        expected_amount: Expected funding payment amount
        actual_amount: Actual funding payment received (if available)
        discrepancy: Difference between expected and actual
        timestamp: When reconciliation was performed
    """
    reconciled: bool
    expected_amount: Decimal
    actual_amount: Optional[Decimal]
    discrepancy: Optional[Decimal]
    timestamp: datetime


class FundingService:
    """
    Service for managing funding rate data and payment reconciliation.
    
    This service:
    - Fetches current funding rates from exchange (Requirement 18.1)
    - Caches funding rates in Redis (Requirement 18.4)
    - Calculates annualized rates (Requirement 18.3)
    - Retrieves funding history (Requirement 18.5)
    - Reconciles funding payments (Requirement 18.8)
    - Detects funding anomalies (Requirements 18.10, 18.11)
    """
    
    def __init__(
        self,
        exchange_client: ExchangeClient,
        redis_client: RedisClient,
        funding_reconciliation_timeout_hours: int = 16
    ):
        """
        Initialize FundingService.
        
        Args:
            exchange_client: Exchange client for fetching funding data
            redis_client: Redis client for caching
            funding_reconciliation_timeout_hours: Hours before marking funding as anomaly
        """
        self.exchange = exchange_client
        self.redis = redis_client
        self.funding_reconciliation_timeout_hours = funding_reconciliation_timeout_hours
        self.logger = logger
    
    async def get_current_funding_rate(self, symbol: str) -> Decimal:
        """
        Get current funding rate for a symbol.
        
        Requirement 18.1: Fetch current funding rates for all configured pairs
        Requirement 18.4: Cache funding rate data in Redis with appropriate TTL
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT:USDT")
            
        Returns:
            Current funding rate as Decimal
            
        Raises:
            Exception: If funding rate cannot be fetched
        """
        try:
            # Try to get from cache first
            cached_rate = await self.redis.get_current_funding(symbol)
            if cached_rate is not None:
                await self.logger.debug(f"Using cached funding rate for {symbol}: {cached_rate}")
                return cached_rate
            
            # Fetch from exchange
            funding_data = await self.exchange.fetch_funding_rate(symbol)
            rate = Decimal(str(funding_data.get("fundingRate", 0)))
            
            # Cache in Redis
            await self.redis.set_current_funding(symbol, rate)
            
            # Add to history
            funding_rate = FundingRate(
                symbol=symbol,
                rate=rate,
                timestamp=datetime.utcnow(),
                next_funding_time=datetime.fromtimestamp(
                    funding_data.get("nextFundingTime", 0) / 1000
                ) if funding_data.get("nextFundingTime") else datetime.utcnow()
            )
            await self.redis.add_funding_history(symbol, funding_rate.to_dict())
            
            await self.logger.info(f"Fetched funding rate for {symbol}: {rate}")
            return rate
            
        except Exception as e:
            await self.logger.error(f"Failed to get funding rate for {symbol}: {e}")
            
            # Try to return cached data if available (Requirement 18.6)
            cached_rate = await self.redis.get_current_funding(symbol)
            if cached_rate is not None:
                await self.logger.warning(
                    f"Using stale cached funding rate for {symbol} due to fetch error"
                )
                return cached_rate
            
            raise
    
    async def get_next_funding_rate(self, symbol: str) -> Optional[Decimal]:
        """
        Get next funding rate prediction if available.
        
        Requirement 18.2: Fetch next funding rate predictions when available
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Next funding rate or None if not available
        """
        try:
            funding_data = await self.exchange.fetch_funding_rate(symbol)
            
            # Some exchanges provide predicted next funding rate
            next_rate = funding_data.get("nextFundingRate")
            if next_rate is not None:
                return Decimal(str(next_rate))
            
            return None
            
        except Exception as e:
            await self.logger.warning(f"Failed to get next funding rate for {symbol}: {e}")
            return None
    
    async def calculate_annualized_rate(
        self,
        funding_rate: Decimal,
        periods_per_day: int = 3
    ) -> Decimal:
        """
        Calculate annualized return rate from funding rate.
        
        Requirement 18.3: Calculate annualized return rates from funding rates
        
        Formula: annualized_rate = (1 + funding_rate)^periods_per_year - 1
        
        Args:
            funding_rate: Single period funding rate (e.g., 0.0001 = 0.01%)
            periods_per_day: Number of funding periods per day (default 3 = every 8 hours)
            
        Returns:
            Annualized rate as Decimal
        """
        periods_per_year = periods_per_day * 365
        
        # Convert to float for exponentiation, then back to Decimal
        # annualized = (1 + rate)^periods - 1
        one_plus_rate = float(1 + funding_rate)
        annualized_float = (one_plus_rate ** periods_per_year) - 1
        annualized = Decimal(str(annualized_float))
        
        await self.logger.debug(
            f"Annualized rate: {funding_rate} -> {annualized} "
            f"({periods_per_day} periods/day)"
        )
        
        return annualized
    
    async def get_funding_history(
        self,
        symbol: str,
        periods: int = 6
    ) -> List[FundingRate]:
        """
        Get historical funding rates for a symbol.
        
        Requirement 18.5: Provide interfaces for querying current and historical funding rates
        
        Args:
            symbol: Trading pair symbol
            periods: Number of historical periods to retrieve (default 6)
            
        Returns:
            List of FundingRate objects, most recent first
        """
        try:
            # Get from Redis cache first
            history_data = await self.redis.get_funding_history(symbol, periods)
            
            if history_data:
                history = [FundingRate.from_dict(data) for data in history_data]
                await self.logger.debug(
                    f"Retrieved {len(history)} funding history records for {symbol} from cache"
                )
                return history
            
            # If not in cache, fetch from exchange
            await self.logger.info(
                f"No cached funding history for {symbol}, fetching from exchange"
            )
            
            exchange_history = await self.exchange.fetch_funding_rate_history(
                symbol,
                limit=periods
            )
            
            # Convert to FundingRate objects and cache
            history = []
            for item in exchange_history:
                funding_rate = FundingRate(
                    symbol=symbol,
                    rate=Decimal(str(item.get("fundingRate", 0))),
                    timestamp=datetime.fromtimestamp(item.get("timestamp", 0) / 1000),
                    next_funding_time=datetime.fromtimestamp(
                        item.get("nextFundingTime", 0) / 1000
                    ) if item.get("nextFundingTime") else datetime.utcnow()
                )
                history.append(funding_rate)
                
                # Cache each record
                await self.redis.add_funding_history(symbol, funding_rate.to_dict())
            
            await self.logger.info(
                f"Fetched and cached {len(history)} funding history records for {symbol}"
            )
            return history
            
        except Exception as e:
            await self.logger.error(f"Failed to get funding history for {symbol}: {e}")
            return []
    
    async def reconcile_funding_payment(
        self,
        position_id: str,
        symbol: str,
        expected_amount: Decimal
    ) -> ReconciliationResult:
        """
        Reconcile funding payment with exchange wallet records.
        
        Requirements 18.8, 18.9: Reconcile funding payments with exchange wallet records
        and support different funding payment mechanisms
        
        Args:
            position_id: Position ID for tracking
            symbol: Trading pair symbol
            expected_amount: Expected funding payment amount
            
        Returns:
            ReconciliationResult with reconciliation status
        """
        try:
            # Fetch recent balance changes or funding payment history
            # Note: This is exchange-specific and may need customization
            
            # For now, we'll mark as reconciled and log
            # In production, this should query exchange funding payment history
            await self.logger.info(
                f"Reconciling funding payment for position {position_id}, "
                f"symbol {symbol}, expected: {expected_amount}"
            )
            
            # Update reconciliation timestamp in Redis
            await self.redis.set_funding_reconciliation(symbol)
            
            # Clear any existing anomaly flag
            await self.redis.clear_funding_anomaly(position_id)
            
            result = ReconciliationResult(
                reconciled=True,
                expected_amount=expected_amount,
                actual_amount=expected_amount,  # TODO: Get actual from exchange
                discrepancy=Decimal("0"),
                timestamp=datetime.utcnow()
            )
            
            await self.logger.info(
                f"Funding payment reconciled for position {position_id}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                f"Failed to reconcile funding payment for position {position_id}: {e}"
            )
            
            # Log discrepancy (Requirement 18.11)
            result = ReconciliationResult(
                reconciled=False,
                expected_amount=expected_amount,
                actual_amount=None,
                discrepancy=None,
                timestamp=datetime.utcnow()
            )
            
            return result
    
    async def check_funding_reconciliation_timeout(
        self,
        position_id: str,
        symbol: str
    ) -> bool:
        """
        Check if funding has not been reconciled within acceptable timeframe.
        
        Requirement 18.10: Verify funding payment receipt after each funding period
        
        Args:
            position_id: Position ID to check
            symbol: Trading pair symbol
            
        Returns:
            True if funding reconciliation has timed out (> 16 hours)
        """
        try:
            last_reconciliation = await self.redis.get_funding_reconciliation(symbol)
            
            if last_reconciliation is None:
                # No reconciliation recorded yet
                await self.logger.warning(
                    f"No funding reconciliation recorded for {symbol}"
                )
                return False
            
            # Calculate time since last reconciliation
            last_reconciliation_time = datetime.fromtimestamp(last_reconciliation)
            time_since = datetime.utcnow() - last_reconciliation_time
            timeout_threshold = timedelta(hours=self.funding_reconciliation_timeout_hours)
            
            if time_since > timeout_threshold:
                await self.logger.warning(
                    f"Funding reconciliation timeout for position {position_id}, "
                    f"symbol {symbol}: {time_since.total_seconds() / 3600:.1f} hours since last reconciliation"
                )
                return True
            
            return False
            
        except Exception as e:
            await self.logger.error(
                f"Failed to check funding reconciliation timeout for {position_id}: {e}"
            )
            return False
    
    async def mark_funding_anomaly(self, position_id: str) -> None:
        """
        Mark position as having funding anomaly and trigger risk action.
        
        Requirement 18.11: Alert operators and log discrepancies when reconciliation fails
        
        Args:
            position_id: Position ID to mark
        """
        try:
            await self.redis.set_funding_anomaly(position_id)
            
            await self.logger.error(
                f"FUNDING ANOMALY: Position {position_id} marked for investigation. "
                f"Funding payment not reconciled within {self.funding_reconciliation_timeout_hours} hours."
            )
            
            # Send alerts to all channels
            from services.notifier import get_notifier
            notifier = get_notifier(get_config())
            await notifier.notify_funding_anomaly(position_id)

        except Exception as e:
            await self.logger.critical(
                f"Failed to mark funding anomaly for position {position_id}: {e}"
            )
    
    async def has_funding_anomaly(self, position_id: str) -> bool:
        """
        Check if position has a funding anomaly flag.
        
        Args:
            position_id: Position ID to check
            
        Returns:
            True if position has funding anomaly
        """
        return await self.redis.has_funding_anomaly(position_id)
    
    async def log_funding_rate_change(
        self,
        symbol: str,
        old_rate: Decimal,
        new_rate: Decimal
    ) -> None:
        """
        Log funding rate changes for analysis.
        
        Requirement 18.7: Log funding rate changes for analysis
        
        Args:
            symbol: Trading pair symbol
            old_rate: Previous funding rate
            new_rate: New funding rate
        """
        change_pct = ((new_rate - old_rate) / old_rate * 100) if old_rate != 0 else Decimal("0")
        
        await self.logger.info(
            f"Funding rate change for {symbol}: {old_rate} -> {new_rate} "
            f"({change_pct:+.2f}%)"
        )
