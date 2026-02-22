"""
Exchange Manager for managing multiple exchange clients.

Supports Binance and OKX for cross-exchange arbitrage.
"""

from enum import Enum
from typing import Dict, Optional
from decimal import Decimal

from services.exchange_client import ExchangeClient
from config.settings import SystemConfig
from utils.logger import get_logger


logger = get_logger(__name__)


class ExchangeType(str, Enum):
    """Supported exchange types."""
    BINANCE = "binance"
    OKX = "okx"


class ExchangeManager:
    """
    Manager for multiple exchange clients.
    
    Handles initialization and access to Binance and OKX exchange clients
    for cross-exchange arbitrage operations.
    """
    
    def __init__(self, config: SystemConfig):
        """
        Initialize ExchangeManager.
        
        Args:
            config: System configuration
        """
        self.config = config
        self._clients: Dict[ExchangeType, ExchangeClient] = {}
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Initialize all configured exchange clients.
        
        Raises:
            Exception: If initialization fails
        """
        await logger.info("正在初始化交易所管理器...")
        
        # Initialize all enabled exchanges
        for exchange_name in self.config.enabled_exchanges:
            await logger.info(f"正在初始化 {exchange_name.upper()} 客户端...")
            
            # Get exchange-specific configuration
            exchange_config = self.config.get_exchange_config(exchange_name)
            
            # Create exchange client
            client = ExchangeClient(
                self.config,
                exchange_name=exchange_name,
                api_key=exchange_config["api_key"],
                api_secret=exchange_config["api_secret"],
                password=exchange_config["password"],
                testnet=exchange_config["testnet"]
            )
            
            await client.initialize()
            
            # Store client by ExchangeType enum
            exchange_type = ExchangeType(exchange_name)
            self._clients[exchange_type] = client
            
            await logger.info(f"✓ {exchange_name.upper()} 客户端已初始化")
        
        self._initialized = True
        await logger.info(f"✓ 交易所管理器已初始化 ({len(self._clients)} 个交易所)")
    
    def get_client(self, exchange: ExchangeType) -> ExchangeClient:
        """
        Get exchange client by type.
        
        Args:
            exchange: Exchange type
            
        Returns:
            ExchangeClient instance
            
        Raises:
            ValueError: If exchange not initialized
        """
        if not self._initialized:
            raise ValueError("ExchangeManager not initialized")
        
        if exchange not in self._clients:
            raise ValueError(f"Exchange {exchange} not initialized")
        
        return self._clients[exchange]
    
    def has_exchange(self, exchange: ExchangeType) -> bool:
        """
        Check if exchange is initialized.
        
        Args:
            exchange: Exchange type
            
        Returns:
            True if exchange is initialized
        """
        return exchange in self._clients
    
    async def fetch_balance(self, exchange: ExchangeType) -> Dict:
        """
        Fetch balance from specific exchange.
        
        Args:
            exchange: Exchange type
            
        Returns:
            Balance dictionary
        """
        client = self.get_client(exchange)
        return await client.fetch_balance()
    
    async def fetch_ticker(self, symbol: str, exchange: ExchangeType) -> Dict:
        """
        Fetch ticker from specific exchange.
        
        Args:
            symbol: Trading pair symbol
            exchange: Exchange type
            
        Returns:
            Ticker dictionary
        """
        client = self.get_client(exchange)
        return await client.fetch_ticker(symbol)
    
    async def fetch_all_tickers(self, symbol: str) -> Dict[ExchangeType, Dict]:
        """
        Fetch ticker from all initialized exchanges.

        Args:
            symbol: Trading pair symbol

        Returns:
            Dictionary mapping exchange to ticker data.
            If a "does not have market symbol" error occurs, the exchange
            is stored in self._last_missing_market for upstream caching.
        """
        self._last_missing_market = None
        tickers = {}
        for exchange, client in self._clients.items():
            try:
                ticker = await client.fetch_ticker(symbol)
                tickers[exchange] = ticker
            except Exception as e:
                err_msg = str(e)
                if "does not have market symbol" in err_msg:
                    self._last_missing_market = exchange.value
                    await logger.debug(f"{exchange.value} 不支持 {symbol}，将缓存跳过")
                else:
                    await logger.warning(f"获取 {exchange} 的 {symbol} 行情失败: {e}")

        return tickers
    
    async def get_cross_exchange_prices(self, symbol: str) -> Optional[Dict[str, Decimal]]:
        """
        Get PERPETUAL prices from both exchanges for cross-exchange arbitrage.
        
        IMPORTANT: Cross-exchange arbitrage should compare perpetual prices,
        not spot prices. Both sides should be perpetual contracts.
        
        Args:
            symbol: Base trading pair symbol (e.g., "BTC/USDT")
            
        Returns:
            Dictionary with exchange perpetual prices or None if not available
            Format: {
                "binance": Decimal("50000.0"),
                "okx": Decimal("50100.0")
            }
        """
        if not self.has_exchange(ExchangeType.OKX):
            return None
        
        # Convert to perpetual symbol for cross-exchange arbitrage
        perp_symbol = f"{symbol}:USDT"
        
        tickers = await self.fetch_all_tickers(perp_symbol)
        
        if len(tickers) < 2:
            return None
        
        prices = {}
        for exchange, ticker in tickers.items():
            last_price = ticker.get("last")
            if last_price:
                prices[exchange.value] = Decimal(str(last_price))
        
        return prices if len(prices) == 2 else None
    
    async def fetch_all_combined_balances(self) -> dict:
        """
        Fetch combined (spot+swap) balances from ALL initialized exchanges.

        Returns:
            {
                "exchanges": {
                    "binance": {"spot": {...}, "swap": {...}, "total": ..., "free": ..., "used": ...},
                    "okx":     {"spot": {...}, "swap": {...}, "total": ..., "free": ..., "used": ...},
                },
                "total": ..., "free": ..., "used": ...,
            }
        """
        exchanges_data: dict = {}
        grand_total = 0.0
        grand_free = 0.0
        grand_used = 0.0

        for exchange_type, client in self._clients.items():
            try:
                combined = await client.fetch_combined_balance()
                exchanges_data[exchange_type.value] = combined
                grand_total += combined.get("total", 0)
                grand_free += combined.get("free", 0)
                grand_used += combined.get("used", 0)
            except Exception as e:
                await logger.warning(f"获取 {exchange_type.value} 余额失败: {e}")
                exchanges_data[exchange_type.value] = {
                    "spot": {"total": 0, "free": 0, "used": 0},
                    "swap": {"total": 0, "free": 0, "used": 0},
                    "total": 0, "free": 0, "used": 0,
                }

        return {
            "exchanges": exchanges_data,
            "total": grand_total,
            "free": grand_free,
            "used": grand_used,
        }

    async def close(self) -> None:
        """Close all exchange clients."""
        await logger.info("正在关闭交易所客户端...")
        for exchange, client in self._clients.items():
            try:
                await client.close()
                await logger.info(f"✓ {exchange} 客户端已关闭")
            except Exception as e:
                await logger.error(f"关闭 {exchange} 客户端失败: {e}")
        
        self._clients.clear()
        self._initialized = False
