"""
Cross-Exchange Spread Service for calculating and monitoring price spreads.

This service handles price spread calculations between different exchanges
for cross-exchange arbitrage opportunities.
"""

from decimal import Decimal
from typing import Dict, Optional, Tuple
from datetime import datetime

from services.exchange_manager import ExchangeManager, ExchangeType
from config.settings import SystemConfig
from utils.logger import get_logger


logger = get_logger(__name__)


class CrossExchangeSpreadService:
    """
    Service for calculating and monitoring cross-exchange price spreads.
    
    Handles:
    - Spread calculation between exchanges
    - Profitability assessment
    - Price fetching from multiple exchanges
    """
    
    def __init__(
        self,
        exchange_manager: ExchangeManager,
        config: SystemConfig
    ):
        """
        Initialize CrossExchangeSpreadService.
        
        Args:
            exchange_manager: Manager for multiple exchange clients
            config: System configuration
        """
        self.exchange_manager = exchange_manager
        self.config = config
    
    def calculate_spread(
        self,
        price_high: Decimal,
        price_low: Decimal
    ) -> Decimal:
        """
        Calculate spread percentage between two prices.
        
        Formula: (price_high - price_low) / price_low
        
        Args:
            price_high: Higher price
            price_low: Lower price
            
        Returns:
            Spread as decimal (e.g., 0.003 = 0.3%)
        """
        if price_low == 0:
            return Decimal("0")
        
        spread = (price_high - price_low) / price_low
        return spread
    
    async def fetch_cross_exchange_prices(
        self,
        symbol: str
    ) -> Optional[Dict[str, Decimal]]:
        """
        Fetch prices from both exchanges for a symbol.

        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT")

        Returns:
            Dictionary with exchange prices or None if not available
            Format: {
                "binance": Decimal("50000.0"),
                "okx": Decimal("50100.0")
            }

        After calling, check self.last_missing_exchange for the exchange
        name if the symbol was not found (useful for caching).
        """
        self.last_missing_exchange = None
        try:
            prices = await self.exchange_manager.get_cross_exchange_prices(symbol)

            # Propagate missing-market info for upstream caching
            if hasattr(self.exchange_manager, "_last_missing_market"):
                self.last_missing_exchange = self.exchange_manager._last_missing_market

            if prices and len(prices) == 2:
                await logger.debug(
                    f"获取 {symbol} 跨市价格: {prices}"
                )
                return prices
            else:
                await logger.debug(
                    f"无法获取 {symbol} 的完整跨市价格数据"
                )
                return None

        except Exception as e:
            await logger.error(f"获取 {symbol} 跨市价格失败: {e}")
            return None
    
    async def get_spread_info(
        self,
        symbol: str
    ) -> Optional[Dict]:
        """
        Get comprehensive spread information for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dictionary with spread info or None if unavailable
            Format: {
                "symbol": "BTC/USDT",
                "exchange_high": "okx",
                "exchange_low": "binance",
                "price_high": Decimal("50100.0"),
                "price_low": Decimal("50000.0"),
                "spread_pct": Decimal("0.002"),
                "timestamp": datetime
            }
        """
        prices = await self.fetch_cross_exchange_prices(symbol)
        
        if not prices or len(prices) < 2:
            return None
        
        # Determine which exchange has higher price
        exchanges = list(prices.keys())
        price_1 = prices[exchanges[0]]
        price_2 = prices[exchanges[1]]
        
        if price_1 > price_2:
            exchange_high = exchanges[0]
            exchange_low = exchanges[1]
            price_high = price_1
            price_low = price_2
        else:
            exchange_high = exchanges[1]
            exchange_low = exchanges[0]
            price_high = price_2
            price_low = price_1
        
        spread_pct = self.calculate_spread(price_high, price_low)
        
        return {
            "symbol": symbol,
            "exchange_high": exchange_high,
            "exchange_low": exchange_low,
            "price_high": price_high,
            "price_low": price_low,
            "spread_pct": spread_pct,
            "timestamp": datetime.utcnow()
        }
    
    def is_spread_profitable(
        self,
        spread_pct: Decimal,
        min_spread_pct: Optional[Decimal] = None
    ) -> bool:
        """
        Check if spread is profitable after fees.
        
        Args:
            spread_pct: Spread percentage
            min_spread_pct: Minimum required spread (default: from config)
            
        Returns:
            True if spread exceeds minimum threshold
        """
        if min_spread_pct is None:
            min_spread_pct = self.config.cross_exchange_min_spread_pct
        
        # Calculate total fees for round trip
        # Entry: 2 taker fees (one on each exchange)
        # Exit: 2 taker fees (one on each exchange)
        total_fees = self.config.perp_taker_fee * 4
        
        # Spread must exceed fees + minimum profit margin
        required_spread = total_fees + min_spread_pct
        
        is_profitable = spread_pct >= required_spread
        
        return is_profitable
    
    def calculate_estimated_profit(
        self,
        spread_pct: Decimal,
        position_size_usd: Decimal
    ) -> Decimal:
        """
        Calculate estimated profit for a given spread and position size.
        
        Args:
            spread_pct: Spread percentage
            position_size_usd: Position size in USD
            
        Returns:
            Estimated profit in USD
        """
        # Calculate total fees
        total_fees_pct = self.config.perp_taker_fee * 4
        total_fees_usd = position_size_usd * total_fees_pct
        
        # Calculate gross profit
        gross_profit_usd = position_size_usd * spread_pct
        
        # Calculate net profit
        net_profit_usd = gross_profit_usd - total_fees_usd
        
        return net_profit_usd
    
    async def should_close_position(
        self,
        current_spread_pct: Decimal,
        entry_timestamp: datetime
    ) -> Tuple[bool, Optional[str]]:
        """
        Determine if a cross-exchange position should be closed.
        
        Closing conditions:
        1. Spread converged below target (< 0.1%)
        2. Holding time exceeded maximum (> 24 hours)
        
        Args:
            current_spread_pct: Current spread percentage
            entry_timestamp: Position entry time
            
        Returns:
            Tuple of (should_close, reason)
        """
        # Check spread convergence
        if current_spread_pct <= self.config.cross_exchange_close_spread_pct:
            return True, "spread_converged"
        
        # Check holding time
        holding_hours = (datetime.utcnow() - entry_timestamp).total_seconds() / 3600
        if holding_hours >= self.config.cross_exchange_max_holding_hours:
            return True, "max_holding_time"
        
        return False, None
