"""
Abstract base classes for the Hive architecture.

Defines BaseSignal, BaseScout, and BaseWorker that all strategy
implementations must inherit from.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Coroutine, List, Optional


@dataclass
class BaseSignal:
    """Base class for all strategy signals."""
    strategy_type: str      # e.g. "cash_carry", "cross_exchange"
    symbol: str
    confidence: float       # 0.0 ~ 1.0
    timestamp: float

    def is_valid(self) -> bool:
        """Override in subclass to define validity criteria."""
        return self.confidence > 0

    def to_dict(self) -> dict:
        """Override in subclass for full serialization."""
        return {
            "strategy_type": self.strategy_type,
            "symbol": self.symbol,
            "confidence": self.confidence,
            "timestamp": self.timestamp,
        }


class BaseScout(ABC):
    """Scout base class - scans markets and produces signals."""
    strategy_type: str = ""

    def __init__(self, scout_id: int, config, exchange_manager, ws_manager):
        self.scout_id = scout_id
        self.config = config
        self.exchange_manager = exchange_manager
        self.ws_manager = ws_manager

    # Optional callback type: called immediately when a valid signal is found
    OnSignalCallback = Callable[["BaseSignal"], Coroutine]

    @abstractmethod
    async def scan(self, symbols: List[str], on_signal: Optional[OnSignalCallback] = None) -> List[BaseSignal]:
        """Scan given symbols and return signals.

        If on_signal is provided, call it immediately for each valid signal
        (enables real-time dispatch instead of batched processing).
        """
        ...

    @abstractmethod
    async def get_scannable_symbols(self) -> List[str]:
        """Return the list of symbols this strategy can scan."""
        ...


class BaseWorker(ABC):
    """Worker base class - executes trades based on signals."""
    strategy_type: str = ""

    def __init__(self, config, exchange_manager, trade_executor,
                 capital_allocator, risk_engine, ws_manager,
                 position_service=None, redis_client=None):
        self.config = config
        self.exchange_manager = exchange_manager
        self.trade_executor = trade_executor
        self.capital_allocator = capital_allocator
        self.risk_engine = risk_engine
        self.ws_manager = ws_manager
        self.position_service = position_service
        self.redis_client = redis_client

    @abstractmethod
    async def execute(self, signal: BaseSignal) -> Optional[dict]:
        """Execute a trade based on the signal. Returns result dict or None."""
        ...

    @abstractmethod
    async def monitor_and_close(self, position_id: str, signal: BaseSignal) -> None:
        """Monitor position and close when conditions met. Long-lived coroutine."""
        ...
