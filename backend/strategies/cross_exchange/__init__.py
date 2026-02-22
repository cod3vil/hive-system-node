"""Cross-Exchange strategy - self-registers with StrategyRegistry."""

from core.strategy_registry import StrategyRegistry
from strategies.cross_exchange.scout import CrossExchangeScout
from strategies.cross_exchange.worker import CrossExchangeWorker

StrategyRegistry.register_scout("cross_exchange", CrossExchangeScout)
StrategyRegistry.register_worker("cross_exchange", CrossExchangeWorker)
