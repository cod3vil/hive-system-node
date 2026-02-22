"""
Strategy Registry - singleton that strategies self-register into.

Strategies register themselves in their package __init__.py:
    StrategyRegistry.register_scout("cash_carry", CashCarryScout)
    StrategyRegistry.register_worker("cash_carry", CashCarryWorker)
"""

from typing import Dict, List, Type


class StrategyRegistry:
    """Strategy registry singleton. Strategies self-register at import time."""

    _scouts: Dict[str, Type] = {}   # strategy_type -> ScoutClass
    _workers: Dict[str, Type] = {}  # strategy_type -> WorkerClass

    @classmethod
    def register_scout(cls, strategy_type: str, scout_cls: Type):
        cls._scouts[strategy_type] = scout_cls

    @classmethod
    def register_worker(cls, strategy_type: str, worker_cls: Type):
        cls._workers[strategy_type] = worker_cls

    @classmethod
    def get_scout_class(cls, strategy_type: str) -> Type:
        return cls._scouts[strategy_type]

    @classmethod
    def get_worker_class(cls, strategy_type: str) -> Type:
        return cls._workers[strategy_type]

    @classmethod
    def get_enabled_strategies(cls, config) -> List[str]:
        """Return list of strategy types that are both registered and enabled in config."""
        enabled = []
        # Map strategy_type to config enable flag
        toggle_map = {
            "cash_carry": "enable_cash_carry",
            "cross_exchange": "enable_cross_exchange",
        }
        for strategy_type in cls._scouts:
            flag_attr = toggle_map.get(strategy_type)
            if flag_attr and getattr(config, flag_attr, False):
                enabled.append(strategy_type)
        return enabled

    @classmethod
    def all_registered(cls) -> List[str]:
        """Return all registered strategy types."""
        return list(cls._scouts.keys())
