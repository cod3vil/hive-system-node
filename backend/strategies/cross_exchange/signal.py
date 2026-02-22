"""
Cross-Exchange signal - extends BaseSignal with cross-exchange-specific fields.
"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Dict, Any

from core.base_strategy import BaseSignal


@dataclass
class CrossExchangeSignal(BaseSignal):
    """Signal for cross-exchange arbitrage."""
    exchange_high: str = ""        # exchange with higher price (short side)
    exchange_low: str = ""         # exchange with lower price (long side)
    price_high: Decimal = Decimal("0")
    price_low: Decimal = Decimal("0")
    spread_pct: Decimal = Decimal("0")
    estimated_profit_pct: Decimal = Decimal("0")
    estimated_profit_usd: Decimal = Decimal("0")

    def is_valid(self) -> bool:
        return self.spread_pct >= Decimal("0.003")  # 0.3% min spread

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_type": self.strategy_type,
            "symbol": self.symbol,
            "exchange_high": self.exchange_high,
            "exchange_low": self.exchange_low,
            "price_high": str(self.price_high),
            "price_low": str(self.price_low),
            "spread_pct": str(self.spread_pct),
            "estimated_profit_pct": str(self.estimated_profit_pct),
            "estimated_profit_usd": str(self.estimated_profit_usd),
            "timestamp": datetime.fromtimestamp(self.timestamp).isoformat() if isinstance(self.timestamp, (int, float)) else str(self.timestamp),
            "is_valid": self.is_valid(),
        }
