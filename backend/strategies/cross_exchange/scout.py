"""
Cross-Exchange Scout - refactored from the original cross_exchange_scout.py.

Inherits BaseScout, implements scan() / get_scannable_symbols().
"""

from decimal import Decimal
from typing import List, Optional
from datetime import datetime

from core.base_strategy import BaseScout, BaseSignal
from strategies.cross_exchange.signal import CrossExchangeSignal
from utils.logger import get_logger

logger = get_logger(__name__)


class CrossExchangeScout(BaseScout):
    """Scout for cross-exchange price-spread arbitrage."""
    strategy_type = "cross_exchange"

    def __init__(self, scout_id, config, exchange_manager, ws_manager):
        super().__init__(scout_id, config, exchange_manager, ws_manager)
        # exchange_manager here is the ExchangeManager (multi-exchange)
        self.spread_service = None  # injected after construction
        self.market_pair_cache = None

    def set_spread_service(self, spread_service):
        """Inject the CrossExchangeSpreadService."""
        self.spread_service = spread_service

    def set_market_pair_cache(self, cache):
        """Inject the MarketPairCache."""
        self.market_pair_cache = cache

    async def get_scannable_symbols(self) -> List[str]:
        """Find common USDT perpetual symbols across all enabled exchanges."""
        # Use cache if available
        if self.market_pair_cache:
            try:
                pairs = await self.market_pair_cache.get_cross_exchange_pairs(
                    self.exchange_manager
                )
                if pairs:
                    return pairs
            except Exception as e:
                await logger.warning(f"缓存获取失败，回退到直接查询: {e}")

        # Fallback: original logic
        try:
            binance_client = self.exchange_manager.get_client("binance")
            okx_client = self.exchange_manager.get_client("okx")

            binance_markets = await binance_client.fetch_markets()
            okx_markets = await okx_client.fetch_markets()

            binance_set = {
                m['symbol'].replace(':USDT', '')
                for m in binance_markets
                if m.get('type') == 'swap' and m.get('quote') == 'USDT' and m.get('active', True)
            }
            okx_set = {
                m['symbol'].replace(':USDT', '').replace('-SWAP', '')
                for m in okx_markets
                if m.get('type') == 'swap' and m.get('quote') == 'USDT' and m.get('active', True)
            }
            return list(binance_set & okx_set)
        except Exception as e:
            await logger.error(f"获取跨市市场列表失败: {e}")
            return []

    async def scan(self, symbols: List[str], on_signal: Optional[BaseScout.OnSignalCallback] = None) -> List[BaseSignal]:
        """Scan symbols for cross-exchange price spread opportunities.

        If on_signal callback is provided, dispatches workers immediately
        upon discovering a valid signal (zero-delay).
        """
        if self.spread_service is None:
            await logger.error(f"Scout #{self.scout_id}: spread_service 未注入，跳过扫描")
            return []

        signals: List[BaseSignal] = []
        total = len(symbols)

        for i, symbol in enumerate(symbols, 1):
            try:
                # Skip symbols known to be unavailable on one exchange (24h cache)
                if self.market_pair_cache and await self.market_pair_cache.is_symbol_failed(symbol):
                    continue

                valid_count = sum(1 for s in signals if s.is_valid())
                await self._broadcast_progress(
                    f"正在扫描 {symbol}...", i, total, valid_count, symbol
                )

                spread_info = await self.spread_service.get_spread_info(symbol)
                if not spread_info:
                    # Cache symbols that don't exist on one exchange (24h skip)
                    missing_ex = getattr(self.spread_service, "last_missing_exchange", None)
                    if missing_ex and self.market_pair_cache:
                        await self.market_pair_cache.mark_symbol_failed(symbol, missing_ex)
                    continue

                spread_pct = spread_info["spread_pct"]

                # Sanity check: reject abnormally large spreads (data error)
                max_spread = getattr(self.config, "cross_exchange_max_spread_pct", Decimal("0.50"))
                if spread_pct > max_spread:
                    await logger.warning(
                        f"Scout #{self.scout_id} ✗ 价差异常丢弃 {symbol}: "
                        f"{spread_pct:.4%} > 上限 {max_spread:.4%} (疑似数据错误)"
                    )
                    continue

                if not self.spread_service.is_spread_profitable(spread_pct):
                    continue

                reference_size_usd = Decimal("10000")
                total_fees_pct = self.config.perp_taker_fee * 4
                estimated_profit_pct = spread_pct - total_fees_pct
                estimated_profit_usd = self.spread_service.calculate_estimated_profit(
                    spread_pct, reference_size_usd
                )

                sig = CrossExchangeSignal(
                    strategy_type="cross_exchange",
                    symbol=symbol,
                    confidence=float(min(spread_pct / Decimal("0.01"), Decimal("1"))),
                    timestamp=datetime.utcnow().timestamp(),
                    exchange_high=spread_info["exchange_high"],
                    exchange_low=spread_info["exchange_low"],
                    price_high=spread_info["price_high"],
                    price_low=spread_info["price_low"],
                    spread_pct=spread_pct,
                    estimated_profit_pct=estimated_profit_pct,
                    estimated_profit_usd=estimated_profit_usd,
                )
                signals.append(sig)

                if sig.is_valid():
                    await logger.info(
                        f"Scout #{self.scout_id} ✓ 跨市套利 {symbol}: "
                        f"{spread_info['exchange_high']}→{spread_info['exchange_low']}, "
                        f"价差={spread_pct:.4%}"
                    )
                    # Immediately dispatch worker (zero-delay)
                    if on_signal:
                        await on_signal(sig)
            except Exception as e:
                await logger.error(f"Scout #{self.scout_id} 跨市扫描 {symbol} 失败: {e}")
                continue

        valid_count = sum(1 for s in signals if s.is_valid())
        await self._broadcast_progress("扫描完成", total, total, valid_count, completed=True)
        return signals

    async def _broadcast_progress(
        self, status, scanned, total, valid, current_symbol=None, completed=False
    ):
        if self.ws_manager is None:
            return
        try:
            await self.ws_manager.broadcast({
                "type": "scout_progress",
                "data": {
                    "scout_id": self.scout_id,
                    "strategy_type": "cross_exchange",
                    "status": status,
                    "scanned": scanned,
                    "total": total,
                    "valid_opportunities": valid,
                    "progress_percent": int((scanned / total * 100) if total > 0 else 0),
                    "current_symbol": current_symbol,
                    "completed": completed,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            })
        except Exception:
            pass
