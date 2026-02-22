"""
Cross-Exchange Scout module for discovering cross-exchange arbitrage opportunities.

This module monitors price spreads between OKX and Binance to identify
profitable cross-exchange arbitrage opportunities.
"""

import asyncio
from decimal import Decimal
from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass

from services.exchange_manager import ExchangeManager
from services.cross_exchange_spread_service import CrossExchangeSpreadService
from config.settings import SystemConfig
from utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class CrossExchangeSignal:
    """
    Cross-exchange arbitrage opportunity signal.
    
    Represents a profitable price spread between two exchanges.
    """
    symbol: str
    exchange_high: str  # Exchange with higher price (where we short)
    exchange_low: str  # Exchange with lower price (where we long)
    price_high: Decimal
    price_low: Decimal
    spread_pct: Decimal
    estimated_profit_pct: Decimal  # Net profit after fees
    estimated_profit_usd: Decimal  # For reference position size
    timestamp: datetime
    
    def is_valid(self, min_spread_pct: Decimal) -> bool:
        """
        Check if signal meets criteria for execution.
        
        Args:
            min_spread_pct: Minimum required spread percentage
            
        Returns:
            True if signal is valid
        """
        return self.spread_pct >= min_spread_pct
    
    def to_dict(self):
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "exchange_high": self.exchange_high,
            "exchange_low": self.exchange_low,
            "price_high": str(self.price_high),
            "price_low": str(self.price_low),
            "spread_pct": str(self.spread_pct),
            "estimated_profit_pct": str(self.estimated_profit_pct),
            "estimated_profit_usd": str(self.estimated_profit_usd),
            "timestamp": self.timestamp.isoformat(),
            "is_valid": self.is_valid(Decimal("0.003"))
        }


class CrossExchangeScout:
    """
    Scout module for discovering cross-exchange arbitrage opportunities.
    
    Monitors price spreads between OKX and Binance to identify profitable
    arbitrage opportunities when price differences exceed thresholds.
    """
    
    def __init__(
        self,
        exchange_manager: ExchangeManager,
        spread_service: CrossExchangeSpreadService,
        config: SystemConfig,
        scout_id: int = 0
    ):
        """
        Initialize CrossExchangeScout.
        
        Args:
            exchange_manager: Manager for multiple exchange clients
            spread_service: Service for spread calculations
            config: System configuration
            scout_id: Identifier for this scout instance
        """
        self.exchange_manager = exchange_manager
        self.spread_service = spread_service
        self.config = config
        self.scout_id = scout_id
        self.logger = logger
        
        # WebSocket manager for broadcasting progress
        self._ws_manager = None
    
    def set_websocket_manager(self, ws_manager):
        """Set WebSocket manager for broadcasting scan progress."""
        self._ws_manager = ws_manager
    
    async def _broadcast_scan_progress(
        self,
        status: str,
        scanned: int,
        total: int,
        valid_opportunities: int,
        current_symbol: Optional[str] = None,
        completed: bool = False
    ):
        """Broadcast current scan progress to frontend."""
        if self._ws_manager is None:
            return
        
        try:
            progress_data = {
                "scout_id": f"cross_exchange_{self.scout_id}",
                "scout_type": "cross_exchange",
                "status": status,
                "scanned": scanned,
                "total": total,
                "valid_opportunities": valid_opportunities,
                "progress_percent": int((scanned / total * 100) if total > 0 else 0),
                "current_symbol": current_symbol,
                "completed": completed,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await self._ws_manager.broadcast({
                "type": "cross_exchange_scout_progress",
                "data": progress_data
            })
        except Exception as e:
            await self.logger.debug(f"Failed to broadcast scan progress: {e}")
    
    async def scan_opportunities(
        self,
        symbols: Optional[List[str]] = None
    ) -> List[CrossExchangeSignal]:
        """
        Scan trading pairs for cross-exchange arbitrage opportunities.
        
        Args:
            symbols: Optional list of symbols to scan. If None, scans common markets.
            
        Returns:
            List of CrossExchangeSignal objects
        """
        signals = []
        scan_start_time = datetime.utcnow()
        
        # If no symbols provided, fetch common markets from both exchanges
        if symbols is None:
            try:
                await self.logger.info("跨市侦查蜂开始扫描...")
                await self._broadcast_scan_progress("正在获取市场列表...", 0, 0, 0)
                
                # Get markets from both exchanges
                binance_client = self.exchange_manager.get_client("binance")
                okx_client = self.exchange_manager.get_client("okx")
                
                binance_markets = await binance_client.fetch_markets()
                okx_markets = await okx_client.fetch_markets()
                
                # Find common USDT perpetual markets
                binance_symbols = {
                    m['symbol'].replace(':USDT', '')
                    for m in binance_markets
                    if m.get('type') == 'swap' and m.get('quote') == 'USDT' and m.get('active', True)
                }
                
                okx_symbols = {
                    m['symbol'].replace(':USDT', '').replace('-SWAP', '')
                    for m in okx_markets
                    if m.get('type') == 'swap' and m.get('quote') == 'USDT' and m.get('active', True)
                }
                
                # Get intersection of both exchanges
                symbols = list(binance_symbols & okx_symbols)
                
                await self.logger.info(
                    f"跨市侦查蜂发现 {len(symbols)} 个共同市场 "
                    f"(Binance: {len(binance_symbols)}, OKX: {len(okx_symbols)})"
                )
            except Exception as e:
                await self.logger.error(f"获取市场列表失败: {e}")
                return signals
        
        total_symbols = len(symbols)
        scanned_count = 0
        
        for symbol in symbols:
            scanned_count += 1
            try:
                await self.logger.debug(f"扫描 {symbol} 的跨市套利机会")
                
                # Broadcast current scanning progress
                valid_count = sum(1 for s in signals if s.is_valid(self.config.cross_exchange_min_spread_pct))
                await self._broadcast_scan_progress(
                    f"正在扫描 {symbol}...",
                    scanned_count,
                    total_symbols,
                    valid_count,
                    current_symbol=symbol
                )
                
                # Get spread info
                spread_info = await self.spread_service.get_spread_info(symbol)
                
                if not spread_info:
                    await self.logger.debug(f"× {symbol} 无法获取价差信息")
                    continue
                
                spread_pct = spread_info["spread_pct"]
                
                # Check if spread is profitable
                if not self.spread_service.is_spread_profitable(spread_pct):
                    await self.logger.debug(
                        f"× {symbol} 价差不足: {spread_pct:.4%}"
                    )
                    continue
                
                # Calculate estimated profit
                reference_size_usd = Decimal("10000")  # $10k reference
                total_fees_pct = self.config.perp_taker_fee * 4
                estimated_profit_pct = spread_pct - total_fees_pct
                estimated_profit_usd = self.spread_service.calculate_estimated_profit(
                    spread_pct,
                    reference_size_usd
                )
                
                # Create signal
                signal = CrossExchangeSignal(
                    symbol=symbol,
                    exchange_high=spread_info["exchange_high"],
                    exchange_low=spread_info["exchange_low"],
                    price_high=spread_info["price_high"],
                    price_low=spread_info["price_low"],
                    spread_pct=spread_pct,
                    estimated_profit_pct=estimated_profit_pct,
                    estimated_profit_usd=estimated_profit_usd,
                    timestamp=datetime.utcnow()
                )
                
                signals.append(signal)
                
                if signal.is_valid(self.config.cross_exchange_min_spread_pct):
                    await self.logger.info(
                        f"✓ 发现跨市套利机会 {symbol}: "
                        f"{spread_info['exchange_high']} ({spread_info['price_high']}) → "
                        f"{spread_info['exchange_low']} ({spread_info['price_low']}), "
                        f"价差={spread_pct:.4%}, "
                        f"净利润={estimated_profit_pct:.4%}"
                    )
                else:
                    await self.logger.debug(f"× {symbol} 不符合标准")
                
            except Exception as e:
                await self.logger.error(f"扫描 {symbol} 失败: {e}")
                continue
        
        scan_duration = (datetime.utcnow() - scan_start_time).total_seconds()
        valid_count = sum(1 for s in signals if s.is_valid(self.config.cross_exchange_min_spread_pct))
        
        await self.logger.info(
            f"跨市侦查蜂扫描完成: 扫描 {total_symbols} 个市场, "
            f"发现 {valid_count} 个有效机会, "
            f"耗时 {scan_duration:.1f}秒"
        )
        
        # Broadcast final scan completion
        await self._broadcast_scan_progress(
            "扫描完成",
            total_symbols,
            total_symbols,
            valid_count,
            completed=True
        )
        
        return signals
