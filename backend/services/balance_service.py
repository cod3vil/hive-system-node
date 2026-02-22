"""
Balance Service for querying and caching exchange account balances.

This service provides interfaces for fetching spot and futures balances
with caching to reduce API calls.

Requirements: 15.5
"""

from decimal import Decimal
from typing import Optional, Dict
import time

from services.exchange_client import ExchangeClient
from storage.redis_client import RedisClient
from utils.logger import get_logger


class BalanceService:
    """
    Service for querying and caching account balances.
    
    Responsibilities:
    - Fetch spot wallet balance (Requirement 15.5)
    - Fetch futures wallet balance (Requirement 15.5)
    - Calculate total balance in USD equivalent (Requirement 15.5)
    - Cache balance data with appropriate TTL
    """
    
    # Cache TTL in seconds (30 seconds for balance data)
    BALANCE_CACHE_TTL = 30
    
    def __init__(
        self,
        exchange_client: ExchangeClient,
        redis_client: RedisClient
    ):
        """
        Initialize BalanceService.
        
        Args:
            exchange_client: Exchange client for balance queries
            redis_client: Redis client for caching
        """
        self.exchange = exchange_client
        self.redis = redis_client
        self.logger = get_logger("BalanceService")
        
        # In-memory cache for balance data
        self._balance_cache: Optional[Dict] = None
        self._cache_timestamp: float = 0
    
    async def _fetch_and_cache_balance(self) -> Dict:
        """
        Fetch balance from exchange and cache it.
        
        Returns:
            Balance dictionary from exchange
        """
        try:
            balance = await self.exchange.fetch_balance()
            
            # Update in-memory cache
            self._balance_cache = balance
            self._cache_timestamp = time.time()
            
            await self.logger.debug("余额已获取并缓存")
            
            return balance
            
        except Exception as e:
            await self.logger.error(
                "获取余额失败",
                error=str(e)
            )
            raise
    
    def _is_cache_valid(self) -> bool:
        """
        Check if in-memory cache is still valid.
        
        Returns:
            True if cache is valid, False otherwise
        """
        if self._balance_cache is None:
            return False
        
        age = time.time() - self._cache_timestamp
        return age < self.BALANCE_CACHE_TTL
    
    async def _get_cached_balance(self) -> Dict:
        """
        Get balance from cache or fetch if cache is stale.
        
        Returns:
            Balance dictionary
        """
        if self._is_cache_valid():
            await self.logger.debug("使用缓存余额")
            return self._balance_cache
        
        return await self._fetch_and_cache_balance()
    
    async def get_spot_balance(self, currency: str = "USDT") -> Decimal:
        """
        Get spot wallet balance for a specific currency.
        
        Requirement 15.5: Support querying account balances
        
        Args:
            currency: Currency symbol (default: USDT)
            
        Returns:
            Spot balance for the currency
        """
        try:
            balance = await self._get_cached_balance()
            
            # Get free balance (available for trading)
            spot_balance = balance.get('free', {}).get(currency, 0)
            
            result = Decimal(str(spot_balance))
            
            await self.logger.debug(
                f"现货余额 {currency}: {result}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                "获取现货余额失败",
                error=str(e),
                currency=currency
            )
            return Decimal("0")
    
    async def get_futures_balance(self, currency: str = "USDT") -> Decimal:
        """
        Get futures wallet balance for a specific currency.
        
        Requirement 15.5: Support querying account balances
        
        Note: For unified margin accounts, this may be the same as spot balance.
        Check exchange-specific implementation.
        
        Args:
            currency: Currency symbol (default: USDT)
            
        Returns:
            Futures balance for the currency
        """
        try:
            balance = await self._get_cached_balance()
            
            # For most exchanges, futures balance is in the 'total' field
            # and includes unrealized PnL
            futures_balance = balance.get('total', {}).get(currency, 0)
            
            result = Decimal(str(futures_balance))
            
            await self.logger.debug(
                f"合约余额 {currency}: {result}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                "获取合约余额失败",
                error=str(e),
                currency=currency
            )
            return Decimal("0")
    
    async def get_total_balance_usd(self) -> Decimal:
        """
        Get total balance in USD equivalent.
        
        Requirement 15.5: Support querying account balances
        
        This includes both spot and futures balances, with unrealized PnL.
        
        Returns:
            Total balance in USD equivalent
        """
        try:
            balance = await self._get_cached_balance()
            
            # Get total USDT balance (includes all wallets and unrealized PnL)
            total_usdt = balance.get('total', {}).get('USDT', 0)
            
            result = Decimal(str(total_usdt))
            
            await self.logger.debug(
                f"总余额 (USD): {result}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                "获取总余额失败",
                error=str(e)
            )
            return Decimal("0")
    
    async def get_available_balance(self, currency: str = "USDT") -> Decimal:
        """
        Get available balance (free balance not in open orders).
        
        Args:
            currency: Currency symbol (default: USDT)
            
        Returns:
            Available balance for the currency
        """
        try:
            balance = await self._get_cached_balance()
            
            # Free balance is available for new orders
            available = balance.get('free', {}).get(currency, 0)
            
            result = Decimal(str(available))
            
            await self.logger.debug(
                f"可用余额 {currency}: {result}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                "获取可用余额失败",
                error=str(e),
                currency=currency
            )
            return Decimal("0")
    
    async def get_used_balance(self, currency: str = "USDT") -> Decimal:
        """
        Get used balance (locked in open orders).
        
        Args:
            currency: Currency symbol (default: USDT)
            
        Returns:
            Used balance for the currency
        """
        try:
            balance = await self._get_cached_balance()
            
            # Used balance is locked in open orders
            used = balance.get('used', {}).get(currency, 0)
            
            result = Decimal(str(used))
            
            await self.logger.debug(
                f"已用余额 {currency}: {result}"
            )
            
            return result
            
        except Exception as e:
            await self.logger.error(
                "获取已用余额失败",
                error=str(e),
                currency=currency
            )
            return Decimal("0")
    
    async def invalidate_cache(self) -> None:
        """
        Invalidate the balance cache to force a fresh fetch on next request.
        
        Use this after executing trades or when you need up-to-date balance data.
        """
        self._balance_cache = None
        self._cache_timestamp = 0
        await self.logger.debug("余额缓存已失效")
    
    async def refresh_balance(self) -> Dict:
        """
        Force refresh balance from exchange, bypassing cache.
        
        Returns:
            Fresh balance dictionary
        """
        return await self._fetch_and_cache_balance()
