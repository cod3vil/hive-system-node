"""
Cash & Carry signal - extends BaseSignal with funding-rate-specific fields.
"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Dict, Any

from backend.core.base_strategy import BaseSignal


@dataclass
class CashCarrySignal(BaseSignal):
    """Signal for cash-and-carry (funding rate) arbitrage."""
    spot_symbol: str = ""
    perp_symbol: str = ""
    current_funding_rate: Decimal = Decimal("0")
    annualized_rate: Decimal = Decimal("0")
    stability_score: float = 0.0
    crowding_indicator: float = 0.0
    estimated_slippage: Decimal = Decimal("0")
    expected_24h_funding: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    net_profit_24h: Decimal = Decimal("0")
    asset_class: str = "crypto"
    spot_price: Decimal = Decimal("0")
    perp_price: Decimal = Decimal("0")

    def is_valid(self) -> bool:
        """Check if signal meets all criteria for execution."""
        return (
            self.annualized_rate >= Decimal("0.15")
            and self.annualized_rate <= Decimal("0.80")
            and self.stability_score >= 0.7
            and self.estimated_slippage < Decimal("0.0005")
            and self.net_profit_24h > self.total_fees * 2
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_type": self.strategy_type,
            "symbol": self.symbol,
            "spot_symbol": self.spot_symbol,
            "perp_symbol": self.perp_symbol,
            "current_funding_rate": str(self.current_funding_rate),
            "annualized_rate": str(self.annualized_rate),
            "stability_score": self.stability_score,
            "crowding_indicator": self.crowding_indicator,
            "estimated_slippage": str(self.estimated_slippage),
            "expected_24h_funding": str(self.expected_24h_funding),
            "total_fees": str(self.total_fees),
            "net_profit_24h": str(self.net_profit_24h),
            "timestamp": datetime.fromtimestamp(self.timestamp).isoformat() if isinstance(self.timestamp, (int, float)) else str(self.timestamp),
            "is_valid": self.is_valid(),
        }
