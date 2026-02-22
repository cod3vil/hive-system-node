"""
Data models for the Stable Cash & Carry MVP system.

This module contains all core data structures used throughout the system,
including Position, CashCarrySignal, and related models.
"""

from dataclasses import dataclass, field, asdict
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum


class PositionStatus(str, Enum):
    """Position lifecycle status."""
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class Position:
    """
    Represents a delta-neutral cash and carry position or cross-exchange arbitrage position.
    
    A position can be either:
    1. Cash & Carry: spot long + perpetual short on same exchange
    2. Cross-Exchange: perpetual positions on two different exchanges
    
    Requirements: 19.1, 19.2, 19.3
    """
    
    # Identifiers
    position_id: str
    symbol: str  # e.g., "BTC/USDT"
    spot_symbol: str  # e.g., "BTC/USDT"
    perp_symbol: str  # e.g., "BTC/USDT:USDT"
    
    # Strategy type
    strategy_type: str = "cash_carry"  # "cash_carry" or "cross_exchange"
    
    # Cross-exchange specific fields
    exchange_high: Optional[str] = None  # e.g., "okx" (where we short)
    exchange_low: Optional[str] = None  # e.g., "binance" (where we long)
    entry_spread_pct: Optional[Decimal] = None  # Entry spread percentage
    current_spread_pct: Optional[Decimal] = None  # Current spread percentage
    target_close_spread_pct: Optional[Decimal] = None  # Target close spread
    
    # Entry data
    spot_entry_price: Decimal = Decimal("0")
    perp_entry_price: Decimal = Decimal("0")
    spot_quantity: Decimal = Decimal("0")
    perp_quantity: Decimal = Decimal("0")
    leverage: Decimal = Decimal("1")
    entry_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Current state
    status: str = PositionStatus.OPEN
    current_spot_price: Optional[Decimal] = None
    current_perp_price: Optional[Decimal] = None
    
    # PnL tracking
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Optional[Decimal] = None
    funding_collected: Decimal = Decimal("0")
    total_fees_paid: Decimal = Decimal("0")
    
    # Exit data (if closed)
    spot_exit_price: Optional[Decimal] = None
    perp_exit_price: Optional[Decimal] = None
    exit_timestamp: Optional[datetime] = None
    close_reason: Optional[str] = None
    
    # Risk metrics
    liquidation_price: Decimal = Decimal("0")
    liquidation_distance: Decimal = Decimal("0")
    last_rebalance_time: Optional[datetime] = None
    
    # Metadata
    asset_class: str = ""  # e.g., "BTC", "ETH"
    strategy_id: str = "cash_carry_v1"
    is_shadow: bool = False  # True when position was opened in shadow mode
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize Position to dictionary format.
        
        Converts all Decimal and datetime fields to JSON-serializable types.
        
        Returns:
            Dictionary representation of the Position
        """
        data = asdict(self)
        
        # Convert Decimal to string for JSON serialization
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Position":
        """
        Deserialize Position from dictionary format.
        
        Converts string representations back to Decimal and datetime types.
        
        Args:
            data: Dictionary containing position data
            
        Returns:
            Position instance
        """
        # Create a copy to avoid modifying the input
        data = data.copy()
        
        # Convert UUID to string if needed
        if "position_id" in data and data["position_id"] is not None:
            data["position_id"] = str(data["position_id"])
        
        # Convert string to Decimal for numeric fields
        decimal_fields = [
            "spot_entry_price", "perp_entry_price", "spot_quantity", 
            "perp_quantity", "leverage", "unrealized_pnl", "realized_pnl",
            "funding_collected", "total_fees_paid", "spot_exit_price",
            "perp_exit_price", "liquidation_price", "liquidation_distance",
            "current_spot_price", "current_perp_price",
            "entry_spread_pct", "current_spread_pct", "target_close_spread_pct"
        ]
        
        for field_name in decimal_fields:
            if field_name in data and data[field_name] is not None:
                data[field_name] = Decimal(str(data[field_name]))
        
        # Convert ISO string to datetime for timestamp fields
        datetime_fields = [
            "entry_timestamp", "exit_timestamp", "last_rebalance_time",
            "created_at", "updated_at"
        ]
        
        for field_name in datetime_fields:
            if field_name in data and data[field_name] is not None:
                if isinstance(data[field_name], str):
                    data[field_name] = datetime.fromisoformat(data[field_name])
        
        return cls(**data)
    
    def calculate_notional_value(self) -> Decimal:
        """
        Calculate the notional value of the position.
        
        Returns:
            Notional value in quote currency (USDT)
        """
        return self.spot_quantity * self.spot_entry_price
    
    def calculate_delta_difference(self) -> Decimal:
        """
        Calculate the delta difference between spot and perpetual quantities.
        
        Returns:
            Absolute percentage difference between spot and perp quantities
        """
        if self.spot_quantity == 0:
            return Decimal("0")
        
        diff = abs(self.spot_quantity - self.perp_quantity)
        return diff / self.spot_quantity
    
    def is_delta_neutral(self, tolerance: Decimal = Decimal("0.001")) -> bool:
        """
        Check if position is delta-neutral within tolerance.
        
        Args:
            tolerance: Maximum acceptable delta difference (default 0.1%)
            
        Returns:
            True if position is delta-neutral within tolerance
        """
        return self.calculate_delta_difference() <= tolerance
    
    def calculate_current_pnl(self) -> Decimal:
        """
        Calculate current unrealized PnL based on current prices.
        
        Returns:
            Unrealized PnL in quote currency
        """
        if self.current_spot_price is None or self.current_perp_price is None:
            return Decimal("0")
        
        # Spot PnL: (current_price - entry_price) * quantity
        spot_pnl = (self.current_spot_price - self.spot_entry_price) * self.spot_quantity
        
        # Perp PnL: (entry_price - current_price) * quantity (short position)
        perp_pnl = (self.perp_entry_price - self.current_perp_price) * self.perp_quantity
        
        # Total PnL includes spot, perp, funding collected, minus fees
        total_pnl = spot_pnl + perp_pnl + self.funding_collected - self.total_fees_paid
        
        return total_pnl
