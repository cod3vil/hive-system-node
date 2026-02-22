"""Cross-Exchange strategy - self-registers with StrategyRegistry."""

from backend.core.strategy_registry import StrategyRegistry
from backend.strategies.cross_exchange.scout import CrossExchangeScout
from backend.strategies.cross_exchange.worker import CrossExchangeWorker

StrategyRegistry.register_scout("cross_exchange", CrossExchangeScout)
StrategyRegistry.register_worker("cross_exchange", CrossExchangeWorker)
