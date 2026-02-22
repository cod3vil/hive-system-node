"""
Cash & Carry Scout - refactored from the original cash_carry_scout.py.

Inherits BaseScout and implements scan() / get_scannable_symbols().
"""

import math
import statistics
from decimal import Decimal
from typing import List, Optional
from datetime import datetime

from core.base_strategy import BaseScout, BaseSignal
from strategies.cash_carry.signal import CashCarrySignal
from utils.logger import get_logger

logger = get_logger(__name__)


class CashCarryScout(BaseScout):
    """Scout for funding-rate based cash-and-carry arbitrage."""
    strategy_type = "cash_carry"

    def __init__(self, scout_id: int, config, exchange_manager, ws_manager):
        super().__init__(scout_id, config, exchange_manager, ws_manager)
        # For cash_carry, exchange_manager is actually the single ExchangeClient
        self.exchange = exchange_manager
        # FundingService will be set after construction
        self.funding_service = None
        self.market_pair_cache = None

    def set_funding_service(self, funding_service):
        """Inject the funding service (needed for funding rate queries)."""
        self.funding_service = funding_service

    def set_market_pair_cache(self, cache):
        """Inject the MarketPairCache."""
        self.market_pair_cache = cache

    async def get_scannable_symbols(self) -> List[str]:
        """Fetch validated cash-carry pairs (spot+perp) from cache or exchange."""
        # Use cache if available
        if self.market_pair_cache:
            try:
                pairs = await self.market_pair_cache.get_cash_carry_pairs(self.exchange)
                if pairs:
                    return pairs
            except Exception as e:
                await logger.warning(f"缓存获取失败，回退到直接查询: {e}")

        # Fallback: original logic (no spot validation)
        try:
            markets = await self.exchange.fetch_markets()
            symbols = [
                m['symbol'].replace(':USDT', '')
                for m in markets
                if m.get('type') == 'swap'
                and m.get('quote') == 'USDT'
                and m.get('active', True)
            ]
            return symbols
        except Exception as e:
            await logger.error(f"获取市场列表失败: {e}")
            symbols = self.config.enabled_symbols
            return symbols if isinstance(symbols, list) else []

    async def scan(self, symbols: List[str], on_signal: Optional[BaseScout.OnSignalCallback] = None) -> List[BaseSignal]:
        """Scan given symbols for cash-carry opportunities.

        If on_signal callback is provided, dispatches workers immediately
        upon discovering a valid signal (zero-delay).
        """
        if self.funding_service is None:
            await logger.error("Scout #{self.scout_id}: funding_service 未注入，跳过扫描")
            return []

        signals: List[BaseSignal] = []
        total = len(symbols)

        for i, symbol in enumerate(symbols, 1):
            try:
                spot_symbol = symbol
                perp_symbol = f"{symbol}:USDT"

                # Broadcast progress
                valid_count = sum(1 for s in signals if s.is_valid())
                await self._broadcast_progress(
                    f"正在扫描 {symbol}...", i, total, valid_count, symbol
                )

                # Volume filter
                try:
                    volume_24h = await self.exchange.fetch_24h_volume(perp_symbol)
                    if volume_24h < self.config.min_24h_volume_usd:
                        continue
                except Exception:
                    continue

                # Funding rate data
                current_funding = await self.funding_service.get_current_funding_rate(perp_symbol)
                annualized_rate = await self.funding_service.calculate_annualized_rate(
                    current_funding, periods_per_day=3
                )
                stability_score = await self._calculate_stability(perp_symbol)
                crowding = await self._calculate_crowding(perp_symbol)

                # Slippage estimation
                reference_size_usd = Decimal("10000")
                ticker = await self.exchange.fetch_ticker(spot_symbol)
                last_price = Decimal(str(ticker.get("last", 0)))
                if last_price == 0:
                    continue

                reference_qty = reference_size_usd / last_price
                spot_slippage = await self._estimate_slippage(spot_symbol, "buy", reference_qty)
                perp_slippage = await self._estimate_slippage(perp_symbol, "sell", reference_qty)

                # Expected funding
                expected_24h = await self._expected_net_funding(perp_symbol)
                total_fees = self._calculate_total_fees(reference_size_usd)
                funding_income = current_funding * 3 * reference_size_usd
                net_profit = funding_income - total_fees

                # Get perp price
                perp_ticker = await self.exchange.fetch_ticker(perp_symbol)
                perp_price = Decimal(str(perp_ticker.get("last", 0)))

                sig = CashCarrySignal(
                    strategy_type="cash_carry",
                    symbol=symbol,
                    confidence=float(stability_score),
                    timestamp=datetime.utcnow().timestamp(),
                    spot_symbol=spot_symbol,
                    perp_symbol=perp_symbol,
                    current_funding_rate=current_funding,
                    annualized_rate=annualized_rate,
                    stability_score=stability_score,
                    crowding_indicator=crowding,
                    estimated_slippage=spot_slippage + perp_slippage,
                    expected_24h_funding=expected_24h,
                    total_fees=total_fees,
                    net_profit_24h=net_profit,
                    asset_class="crypto",
                    spot_price=last_price,
                    perp_price=perp_price,
                )
                signals.append(sig)

                if sig.is_valid():
                    await logger.info(
                        f"Scout #{self.scout_id} ✓ 发现套利机会 {symbol}: "
                        f"年化={annualized_rate:.2%}, 稳定性={stability_score:.2f}"
                    )
                    # Immediately dispatch worker (zero-delay)
                    if on_signal:
                        await on_signal(sig)
            except Exception as e:
                await logger.error(f"Scout #{self.scout_id} 扫描 {symbol} 失败: {e}")
                continue

        # Final progress broadcast
        valid_count = sum(1 for s in signals if s.is_valid())
        await self._broadcast_progress("扫描完成", total, total, valid_count, completed=True)
        return signals

    # ── helpers (moved from original CashCarryScout) ──

    async def _calculate_stability(self, symbol: str) -> float:
        """Funding stability score from last 6 periods."""
        try:
            history = await self.funding_service.get_funding_history(symbol, periods=6)
            if len(history) < 6:
                return 0.0
            rates = [float(fr.rate) for fr in history]
            if not all(r > 0 for r in rates[:3]):
                return 0.0
            mean_rate = statistics.mean(rates)
            if mean_rate == 0:
                return 0.0
            cv = statistics.stdev(rates) / abs(mean_rate)
            return math.exp(-cv * 10)
        except Exception:
            return 0.0

    async def _calculate_crowding(self, symbol: str) -> float:
        """Crowding indicator = current / 30-day average."""
        try:
            current = await self.funding_service.get_current_funding_rate(symbol)
            history = await self.funding_service.get_funding_history(symbol, periods=90)
            if len(history) < 30:
                return 1.0
            avg = statistics.mean([float(fr.rate) for fr in history])
            return float(current) / avg if avg != 0 else 1.0
        except Exception:
            return 1.0

    async def _estimate_slippage(self, symbol: str, side: str, quantity: Decimal) -> Decimal:
        """Estimate slippage from order book depth (first 3 levels)."""
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=10)
            levels = ob.get("asks" if side == "buy" else "bids", [])[:3]
            if len(levels) < 3:
                return Decimal("0.01")
            best = Decimal(str(levels[0][0]))
            remaining = quantity
            total_cost = Decimal("0")
            total_filled = Decimal("0")
            for price_s, qty_s in levels:
                p, q = Decimal(str(price_s)), Decimal(str(qty_s))
                fill = min(remaining, q)
                total_cost += p * fill
                total_filled += fill
                remaining -= fill
                if remaining <= 0:
                    break
            if total_filled == 0:
                return Decimal("0.01")
            return abs(total_cost / total_filled - best) / best
        except Exception:
            return Decimal("0.01")

    async def _expected_net_funding(self, symbol: str, hours: int = 24) -> Decimal:
        try:
            rate = await self.funding_service.get_current_funding_rate(symbol)
            periods = (hours / 24) * 3
            return rate * Decimal(str(periods))
        except Exception:
            return Decimal("0")

    def _calculate_total_fees(self, size: Decimal) -> Decimal:
        return (
            size * self.config.spot_taker_fee * 2
            + size * self.config.perp_taker_fee * 2
        )

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
                    "strategy_type": "cash_carry",
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
