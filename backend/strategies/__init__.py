"""Trading strategy modules."""

# Import strategy packages to trigger self-registration with StrategyRegistry.
# New strategies only need to add a package directory with an __init__.py
# that calls StrategyRegistry.register_scout / register_worker.
import strategies.cash_carry  # noqa: F401
import strategies.cross_exchange  # noqa: F401
