"""
MarketPairCache - Redis-backed 24h cache for validated trading pairs.

Caches:
- cash_carry pairs: symbols that exist in BOTH spot and perpetual markets
- cross_exchange pairs: perpetual symbols common across all enabled exchanges
"""

import json
from typing import List, Optional

from backend.utils.logger import get_logger

logger = get_logger("MarketPairCache")

# Redis keys
CASH_CARRY_KEY = "markets:cash_carry:pairs"
CROSS_EXCHANGE_KEY = "markets:cross_exchange:pairs"
FAILED_SYMBOLS_KEY = "markets:failed_symbols"  # hash: symbol -> exchange
TTL_SECONDS = 86400  # 24 hours


class MarketPairCache:
    """Redis-backed market pair cache with 24h TTL."""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def get_cash_carry_pairs(self, exchange_client) -> List[str]:
        """
        Get validated cash-carry pairs (exist in both spot and perpetual).

        On cache miss: fetches markets, filters, validates, and caches.
        """
        cached = await self._get_cached(CASH_CARRY_KEY)
        if cached is not None:
            await logger.info(
                f"MarketPairCache: cash_carry cache hit ({len(cached)} pairs)"
            )
            return cached

        try:
            markets = await exchange_client.fetch_markets()

            # Collect spot and swap symbols separately
            spot_symbols = set()
            swap_symbols = set()
            for m in markets:
                if not m.get("active", True) or m.get("quote") != "USDT":
                    continue
                # Normalize to base symbol like "BTC/USDT"
                base_symbol = m["symbol"].replace(":USDT", "")
                if m.get("type") == "spot":
                    spot_symbols.add(base_symbol)
                elif m.get("type") == "swap":
                    swap_symbols.add(base_symbol)

            # Only keep futures symbols that also have a spot market
            validated = sorted(spot_symbols & swap_symbols)
            total_futures = len(swap_symbols)

            await logger.info(
                f"MarketPairCache: cash_carry validated {len(validated)}/{total_futures} "
                f"futures symbols (filtered {total_futures - len(validated)} without spot market)"
            )

            await self._set_cached(CASH_CARRY_KEY, validated)
            return validated

        except Exception as e:
            await logger.error(f"MarketPairCache: failed to build cash_carry pairs: {e}")
            return []

    async def get_cross_exchange_pairs(self, exchange_manager) -> List[str]:
        """
        Get common perpetual symbols across all enabled exchanges.

        On cache miss: fetches markets from each exchange, computes intersection.
        """
        cached = await self._get_cached(CROSS_EXCHANGE_KEY)
        if cached is not None:
            await logger.info(
                f"MarketPairCache: cross_exchange cache hit ({len(cached)} pairs)"
            )
            return cached

        try:
            symbol_sets = []
            for exchange_type, client in exchange_manager._clients.items():
                markets = await client.fetch_markets()
                symbols = {
                    m["symbol"].replace(":USDT", "").replace("-SWAP", "")
                    for m in markets
                    if m.get("type") == "swap"
                    and m.get("quote") == "USDT"
                    and m.get("active", True)
                }
                symbol_sets.append(symbols)
                await logger.info(
                    f"MarketPairCache: {exchange_type.value} has {len(symbols)} USDT perps"
                )

            if not symbol_sets:
                return []

            common = sorted(symbol_sets[0].intersection(*symbol_sets[1:]))

            await logger.info(
                f"MarketPairCache: cross_exchange intersection = {len(common)} pairs"
            )

            await self._set_cached(CROSS_EXCHANGE_KEY, common)
            return common

        except Exception as e:
            await logger.error(
                f"MarketPairCache: failed to build cross_exchange pairs: {e}"
            )
            return []

    async def mark_symbol_failed(self, symbol: str, exchange: str) -> None:
        """Mark a symbol as unavailable on an exchange (cached for 24h)."""
        try:
            key = f"{FAILED_SYMBOLS_KEY}:{symbol}"
            await self.redis.client.set(key, exchange, ex=TTL_SECONDS)
            await logger.info(
                f"MarketPairCache: 标记 {symbol} 在 {exchange} 不可用 (24h缓存)"
            )
        except Exception as e:
            await logger.error(f"MarketPairCache: mark_symbol_failed error: {e}")

    async def is_symbol_failed(self, symbol: str) -> bool:
        """Check if a symbol has been marked as unavailable on any exchange."""
        try:
            key = f"{FAILED_SYMBOLS_KEY}:{symbol}"
            val = await self.redis.client.get(key)
            return val is not None
        except Exception:
            return False

    async def invalidate(self) -> None:
        """Manually clear all cached pairs."""
        try:
            await self.redis.client.delete(CASH_CARRY_KEY, CROSS_EXCHANGE_KEY)
            await logger.info("MarketPairCache: cache invalidated")
        except Exception as e:
            await logger.error(f"MarketPairCache: invalidation failed: {e}")

    # ── internal helpers ──

    async def _get_cached(self, key: str) -> Optional[List[str]]:
        """Read a JSON list from Redis, return None on miss."""
        try:
            raw = await self.redis.client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def _set_cached(self, key: str, pairs: List[str]) -> None:
        """Write a JSON list to Redis with TTL."""
        try:
            await self.redis.client.set(key, json.dumps(pairs), ex=TTL_SECONDS)
        except Exception as e:
            await logger.error(f"MarketPairCache: failed to cache {key}: {e}")
