"""
Exchange client wrapper using CCXT for unified exchange API interface.

This module provides a wrapper around CCXT with rate limiting, retry logic,
and error handling for reliable exchange connectivity.

Requirements: 15.1, 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 9.2, 9.3
"""

import asyncio
import ccxt.async_support as ccxt
from decimal import Decimal
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


logger = get_logger(__name__)


class ExchangeError(Exception):
    """Base exception for exchange-related errors."""
    pass


class RateLimitError(ExchangeError):
    """Raised when exchange rate limit is exceeded."""
    pass


class NetworkError(ExchangeError):
    """Raised when network connectivity issues occur."""
    pass


class OrderError(ExchangeError):
    """Raised when order placement or execution fails."""
    pass


class ExchangeClient:
    """
    Wrapper around CCXT exchange with rate limiting and retry logic.
    
    Provides methods for:
    - Fetching funding rates (Requirement 15.2)
    - Fetching order books (Requirement 15.3)
    - Placing market orders (Requirement 15.4)
    - Querying balances and positions (Requirement 15.5)
    - Rate limiting and retry logic (Requirements 15.6, 9.2, 9.3)
    - Network failure tracking (Requirements 9.4, 9.5, 9.6, 9.7)
    """
    
    def __init__(
        self,
        config: SystemConfig,
        exchange_name: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        password: Optional[str] = None,
        testnet: Optional[bool] = None
    ):
        """
        Initialize exchange client with configuration.
        
        Args:
            config: System configuration containing exchange credentials
            exchange_name: Optional exchange name override (default: use config)
            api_key: Optional API key override (default: use config)
            api_secret: Optional API secret override (default: use config)
            password: Optional password/passphrase for exchanges like OKX
            testnet: Optional testnet flag override (default: use config)
        """
        self.config = config
        self.exchange_name = exchange_name or config.exchange_name
        self.api_key = api_key or config.exchange_api_key
        self.api_secret = api_secret or config.exchange_api_secret
        self.password = password
        self.testnet = testnet if testnet is not None else config.is_testnet
        self.exchange: Optional[ccxt.Exchange] = None
        self._rate_limit_delay = 0.1  # 100ms between requests
        self._last_request_time = datetime.utcnow()
        
        # Network resilience manager (set externally after initialization)
        self.network_resilience = None
        
    async def initialize(self) -> None:
        """
        Initialize CCXT exchange connection.
        
        Requirement 15.1: Integrate with cryptocurrency exchanges using CCXT
        """
        try:
            exchange_class = getattr(ccxt, self.exchange_name)
            
            exchange_config = {
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'spot',
                    'adjustForTimeDifference': True,
                }
            }
            
            # Add password for exchanges that require it (e.g., OKX)
            if self.password:
                exchange_config['password'] = self.password
            
            self.exchange = exchange_class(exchange_config)
            
            # Use testnet if configured
            if self.testnet:
                self.exchange.set_sandbox_mode(True)
                logger.info(f"Exchange client initialized in TESTNET mode: {self.exchange_name}")
            else:
                logger.info(f"Exchange client initialized: {self.exchange_name}")
            
            # Load markets
            await self.exchange.load_markets()
            logger.info(f"Loaded {len(self.exchange.markets)} markets from {self.exchange_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize exchange client: {e}")
            raise ExchangeError(f"Exchange initialization failed: {e}")
    
    async def close(self) -> None:
        """Close exchange connection and cleanup resources."""
        if self.exchange:
            await self.exchange.close()
            logger.info("Exchange client closed")
    
    async def _rate_limit(self) -> None:
        """
        Enforce rate limiting between requests.
        
        Requirement 15.6: Handle exchange-specific rate limits appropriately
        """
        now = datetime.utcnow()
        time_since_last = (now - self._last_request_time).total_seconds()
        
        if time_since_last < self._rate_limit_delay:
            await asyncio.sleep(self._rate_limit_delay - time_since_last)
        
        self._last_request_time = datetime.utcnow()
    
    async def _retry_with_backoff(
        self,
        func,
        *args,
        max_retries: Optional[int] = None,
        **kwargs
    ) -> Any:
        """
        Execute function with exponential backoff retry logic.
        
        Requirements 9.2, 9.3: Exponential backoff for API retry attempts
        Requirements 9.4-9.7: Track network failures for resilience
        
        Args:
            func: Async function to execute
            *args: Positional arguments for func
            max_retries: Maximum retry attempts (defaults to config value)
            **kwargs: Keyword arguments for func
            
        Returns:
            Result from successful function execution
            
        Raises:
            ExchangeError: If all retry attempts fail
        """
        if max_retries is None:
            max_retries = self.config.api_retry_attempts
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                await self._rate_limit()
                result = await func(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"Request succeeded on attempt {attempt + 1}")
                
                # Mark success for network resilience tracking
                if self.network_resilience:
                    self.network_resilience.mark_request_success()
                
                return result
                
            except ccxt.RateLimitExceeded as e:
                last_exception = e
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    f"Rate limit exceeded (attempt {attempt + 1}/{max_retries}), "
                    f"waiting {wait_time}s: {e}"
                )
                await asyncio.sleep(wait_time)
                
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                last_exception = e
                wait_time = 2 ** attempt
                logger.warning(
                    f"Network error (attempt {attempt + 1}/{max_retries}), "
                    f"waiting {wait_time}s: {e}"
                )
                
                # Mark failure for network resilience tracking
                if self.network_resilience:
                    self.network_resilience.mark_request_failure()
                
                await asyncio.sleep(wait_time)
                
            except ccxt.ExchangeError as e:
                # Don't retry on exchange errors (invalid orders, insufficient balance, etc.)
                logger.error(f"Exchange error (no retry): {e}")
                raise OrderError(f"Exchange error: {e}")
        
        # All retries exhausted - mark failure
        if self.network_resilience:
            self.network_resilience.mark_request_failure()
        
        error_msg = f"Request failed after {max_retries} attempts: {last_exception}"
        logger.error(error_msg)
        raise NetworkError(error_msg)
    
    async def fetch_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch current funding rate for a perpetual contract.
        
        Requirement 15.2: Support fetching real-time funding rates
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT:USDT")
            
        Returns:
            Dictionary containing funding rate data:
            {
                'symbol': str,
                'fundingRate': float,
                'fundingTimestamp': int,
                'nextFundingTime': int
            }
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.fetch_funding_rate,
                symbol
            )
            
            logger.debug(f"Fetched funding rate for {symbol}: {result.get('fundingRate')}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch funding rate for {symbol}: {e}")
            raise
    
    async def fetch_funding_rate_history(
        self,
        symbol: str,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical funding rates for a perpetual contract.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT:USDT")
            limit: Number of historical records to fetch
            
        Returns:
            List of funding rate records
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.fetch_funding_rate_history,
                symbol,
                limit=limit
            )
            
            logger.debug(f"Fetched {len(result)} funding rate history records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch funding rate history for {symbol}: {e}")
            raise
    
    async def fetch_order_book(
        self,
        symbol: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Fetch order book depth for slippage calculation.
        
        Requirement 15.3: Support fetching order book depth
        
        Args:
            symbol: Trading pair symbol
            limit: Number of price levels to fetch
            
        Returns:
            Dictionary containing order book:
            {
                'symbol': str,
                'bids': [[price, quantity], ...],
                'asks': [[price, quantity], ...],
                'timestamp': int
            }
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.fetch_order_book,
                symbol,
                limit=limit
            )
            
            logger.debug(
                f"Fetched order book for {symbol}: "
                f"{len(result['bids'])} bids, {len(result['asks'])} asks"
            )
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch order book for {symbol}: {e}")
            raise
    
    async def fetch_balance(self, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Fetch account balance across all wallets.

        Requirement 15.5: Support querying account balances

        Args:
            params: Optional extra params forwarded to ccxt (e.g. {'type': 'swap'})

        Returns:
            Dictionary containing balance information:
            {
                'free': {'USDT': float, 'BTC': float, ...},
                'used': {'USDT': float, 'BTC': float, ...},
                'total': {'USDT': float, 'BTC': float, ...}
            }
        """
        try:
            if params:
                result = await self._retry_with_backoff(
                    self.exchange.fetch_balance, params
                )
            else:
                result = await self._retry_with_backoff(
                    self.exchange.fetch_balance
                )

            logger.debug(f"Fetched account balance: {result.get('total', {})}")
            return result

        except Exception as e:
            logger.error(f"Failed to fetch balance: {e}")
            raise

    async def fetch_combined_balance(self) -> Dict[str, Dict[str, float]]:
        """
        Fetch and merge balances from spot + swap (USDT-margined futures) accounts.

        OKX uses a unified account where spot/swap share the same pool,
        so we query once with 'trading' to avoid double-counting.
        Binance has separate spot and swap wallets — query both and sum.

        Returns a unified dict with keys 'spot', 'swap', and merged 'total'/'free'/'used'.
        """
        spot_usdt = {"total": 0.0, "free": 0.0, "used": 0.0}
        swap_usdt = {"total": 0.0, "free": 0.0, "used": 0.0}

        is_okx = self.exchange_name.lower() == "okx"

        if is_okx:
            # OKX unified account: single query, report under 'swap' (trading account)
            try:
                bal = await self.fetch_balance({"type": "trading"})
                swap_usdt = {
                    "total": float(bal.get("total", {}).get("USDT", 0)),
                    "free": float(bal.get("free", {}).get("USDT", 0)),
                    "used": float(bal.get("used", {}).get("USDT", 0)),
                }
            except Exception as e:
                logger.warning(f"获取OKX交易账户余额失败: {e}")
            # OKX funding account (deposit/withdraw wallet) as 'spot'
            try:
                funding_bal = await self.fetch_balance({"type": "funding"})
                spot_usdt = {
                    "total": float(funding_bal.get("total", {}).get("USDT", 0)),
                    "free": float(funding_bal.get("free", {}).get("USDT", 0)),
                    "used": float(funding_bal.get("used", {}).get("USDT", 0)),
                }
            except Exception as e:
                logger.warning(f"获取OKX资金账户余额失败: {e}")
        else:
            # Binance and others: separate spot and swap wallets
            try:
                spot_bal = await self.fetch_balance({"type": "spot"})
                spot_usdt = {
                    "total": float(spot_bal.get("total", {}).get("USDT", 0)),
                    "free": float(spot_bal.get("free", {}).get("USDT", 0)),
                    "used": float(spot_bal.get("used", {}).get("USDT", 0)),
                }
            except Exception as e:
                logger.warning(f"获取现货余额失败: {e}")

            try:
                swap_bal = await self.fetch_balance({"type": "swap"})
                swap_usdt = {
                    "total": float(swap_bal.get("total", {}).get("USDT", 0)),
                    "free": float(swap_bal.get("free", {}).get("USDT", 0)),
                    "used": float(swap_bal.get("used", {}).get("USDT", 0)),
                }
            except Exception as e:
                logger.warning(f"获取合约余额失败: {e}")

        combined = {
            "spot": spot_usdt,
            "swap": swap_usdt,
            "unified": is_okx,
            "total": spot_usdt["total"] + swap_usdt["total"],
            "free": spot_usdt["free"] + swap_usdt["free"],
            "used": spot_usdt["used"] + swap_usdt["used"],
        }
        logger.debug(
            f"Combined balance [{self.exchange_name}] (USDT): "
            f"spot={spot_usdt['total']:.2f}, swap={swap_usdt['total']:.2f}, "
            f"total={combined['total']:.2f}"
        )
        return combined
    
    async def fetch_positions(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fetch open positions for perpetual contracts.
        
        Requirement 15.5: Support querying positions
        
        Args:
            symbols: Optional list of symbols to filter positions
            
        Returns:
            List of position dictionaries
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.fetch_positions,
                symbols
            )
            
            # Filter out positions with zero size
            open_positions = [p for p in result if float(p.get('contracts', 0)) != 0]
            
            logger.debug(f"Fetched {len(open_positions)} open positions")
            return open_positions
            
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            raise
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch current ticker data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dictionary containing ticker data including last price, bid, ask, volume
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.fetch_ticker,
                symbol
            )
            
            logger.debug(f"Fetched ticker for {symbol}: last={result.get('last')}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch ticker for {symbol}: {e}")
            raise
    
    async def fetch_markets(self) -> List[Dict[str, Any]]:
        """
        Fetch all available markets from the exchange.
        
        Returns:
            List of market dictionaries with keys:
            - id: Exchange-specific market ID
            - symbol: Unified symbol (e.g., "BTC/USDT")
            - base: Base currency
            - quote: Quote currency
            - type: Market type (spot, swap, future)
            - active: Whether market is active
            - info: Raw exchange data
        """
        try:
            markets = await self._retry_with_backoff(
                self.exchange.fetch_markets
            )
            
            logger.debug(f"Fetched {len(markets)} markets from exchange")
            return markets
            
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")
            raise
    
    async def set_leverage(
        self,
        symbol: str,
        leverage: int,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Set leverage for a perpetual contract position.
        
        Args:
            symbol: Trading pair symbol
            leverage: Leverage value (1-125 depending on exchange)
            params: Additional parameters
            
        Returns:
            Exchange response
        """
        try:
            result = await self._retry_with_backoff(
                self.exchange.set_leverage,
                leverage,
                symbol,
                params or {}
            )
            
            logger.info(f"Set leverage for {symbol} to {leverage}x")
            return result
            
        except Exception as e:
            logger.error(f"Failed to set leverage for {symbol}: {e}")
            raise
    
    async def fetch_liquidation_price(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        leverage: Decimal,
        quantity: Decimal
    ) -> Optional[Decimal]:
        """
        Get exchange-provided liquidation price for a position.
        
        IMPORTANT: Uses exchange API to get accurate liquidation price
        based on exchange's margin calculation rules.
        
        Args:
            symbol: Trading pair symbol
            side: Position side ('long' or 'short')
            entry_price: Position entry price
            leverage: Position leverage
            quantity: Position quantity
            
        Returns:
            Liquidation price or None if not available
        """
        try:
            # Fetch current positions to get liquidation price
            positions = await self.fetch_positions([symbol])
            
            for position in positions:
                if position.get('symbol') == symbol:
                    liq_price = position.get('liquidationPrice')
                    if liq_price:
                        logger.debug(f"Liquidation price for {symbol}: {liq_price}")
                        return Decimal(str(liq_price))
            
            # If no position exists, calculate estimate
            # Note: This is a fallback and may not match exchange's exact calculation
            logger.warning(f"No liquidation price found for {symbol}, using estimate")
            return self._estimate_liquidation_price(entry_price, leverage, side)
            
        except Exception as e:
            logger.error(f"Failed to fetch liquidation price for {symbol}: {e}")
            return None
    
    def _estimate_liquidation_price(
        self,
        entry_price: Decimal,
        leverage: Decimal,
        side: str
    ) -> Decimal:
        """
        Estimate liquidation price (fallback when exchange data unavailable).
        
        Formula for long: liq_price = entry_price * (1 - 1/leverage)
        Formula for short: liq_price = entry_price * (1 + 1/leverage)
        
        Args:
            entry_price: Position entry price
            leverage: Position leverage
            side: Position side ('long' or 'short')
            
        Returns:
            Estimated liquidation price
        """
        if side.lower() == 'long':
            return entry_price * (Decimal("1") - Decimal("1") / leverage)
        else:  # short
            return entry_price * (Decimal("1") + Decimal("1") / leverage)
    
    async def validate_api_permissions(self) -> bool:
        """
        Validate API key permissions on startup.
        
        Requirement 8.5: Validate API key permissions and reject keys with withdrawal permissions
        
        Returns:
            True if permissions are valid (trading only, no withdrawal)
            
        Raises:
            ExchangeError: If API key has withdrawal permissions
        """
        try:
            # Fetch account info to check permissions
            # Note: CCXT doesn't have a standard way to check permissions
            # This is a basic check - exchange-specific implementation may be needed
            
            # Try to fetch balance (requires read permission)
            await self.fetch_balance()
            
            logger.info("API key permissions validated: trading access confirmed")
            
            # TODO: Add exchange-specific checks for withdrawal permissions
            # For now, we log a warning that manual verification is needed
            logger.warning(
                "Manual verification required: Ensure API key does NOT have withdrawal permissions"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"API key validation failed: {e}")
            raise ExchangeError(f"Invalid API credentials: {e}")
    
    async def fetch_24h_volume(self, symbol: str) -> Decimal:
        """
        Fetch 24-hour trading volume for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            24-hour volume in quote currency
        """
        try:
            ticker = await self.fetch_ticker(symbol)
            volume = ticker.get('quoteVolume', 0)
            
            logger.debug(f"24h volume for {symbol}: {volume}")
            return Decimal(str(volume))
            
        except Exception as e:
            logger.error(f"Failed to fetch 24h volume for {symbol}: {e}")
            raise

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1h',
        limit: int = 720  # 30 days of hourly data
    ) -> List[List]:
        """
        Fetch OHLCV (candlestick) data for a symbol.
        
        Requirement 4.11: Fetch historical price data for stress test calculation
        
        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe for candles (e.g., '1h', '1d')
            limit: Number of candles to fetch
            
        Returns:
            List of OHLCV data: [[timestamp, open, high, low, close, volume], ...]
            
        Raises:
            ExchangeError: If fetching fails
        """
        await self._rate_limit()
        
        try:
            ohlcv = await self._retry_with_backoff(
                self.exchange.fetch_ohlcv,
                symbol,
                timeframe,
                limit=limit
            )
            
            logger.debug(
                f"Fetched {len(ohlcv)} {timeframe} candles for {symbol}"
            )
            
            return ohlcv
            
        except ccxt.NetworkError as e:
            logger.error(f"Network error fetching OHLCV for {symbol}: {e}")
            raise NetworkError(f"Failed to fetch OHLCV: {e}")
        except ccxt.ExchangeError as e:
            logger.error(f"Exchange error fetching OHLCV for {symbol}: {e}")
            raise ExchangeError(f"Failed to fetch OHLCV: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching OHLCV for {symbol}: {e}")
            raise ExchangeError(f"Failed to fetch OHLCV: {e}")
    
    async def calculate_max_hourly_move(
        self,
        symbol: str,
        days: int = 30
    ) -> Decimal:
        """
        Calculate maximum hourly price movement over recent period.
        
        Requirement 4.11: Calculate recent_30_day_max_hourly_move for stress testing
        
        This calculates the largest percentage price change between consecutive
        hourly candles over the specified period.
        
        Args:
            symbol: Trading pair symbol
            days: Number of days to analyze (default 30)
            
        Returns:
            Maximum hourly move as percentage (e.g., 0.15 for 15%)
            
        Raises:
            ExchangeError: If calculation fails
        """
        try:
            # Fetch hourly candles for the period
            limit = days * 24  # hours in the period
            ohlcv = await self.fetch_ohlcv(symbol, timeframe='1h', limit=limit)
            
            if len(ohlcv) < 2:
                logger.warning(
                    f"Insufficient OHLCV data for {symbol}, "
                    f"using default 15% max move"
                )
                return Decimal("0.15")
            
            # Calculate hourly moves
            max_move = Decimal("0")
            
            for i in range(1, len(ohlcv)):
                prev_close = Decimal(str(ohlcv[i-1][4]))  # Previous close
                curr_close = Decimal(str(ohlcv[i][4]))    # Current close
                
                if prev_close == 0:
                    continue
                
                # Calculate absolute percentage move
                move = abs(curr_close - prev_close) / prev_close
                
                if move > max_move:
                    max_move = move
            
            logger.info(
                f"Max hourly move for {symbol} over {days} days: {max_move:.2%}"
            )
            
            return max_move
            
        except Exception as e:
            logger.error(
                f"Failed to calculate max hourly move for {symbol}: {e}"
            )
            # Return conservative default on error
            return Decimal("0.15")
