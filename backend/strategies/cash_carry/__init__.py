"""Cash & Carry strategy - self-registers with StrategyRegistry."""

from core.strategy_registry import StrategyRegistry
from strategies.cash_carry.scout import CashCarryScout
from strategies.cash_carry.worker import CashCarryWorker

StrategyRegistry.register_scout("cash_carry", CashCarryScout)
StrategyRegistry.register_worker("cash_carry", CashCarryWorker)
