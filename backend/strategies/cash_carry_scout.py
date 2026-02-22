"""
Opportunity Scout module for discovering profitable arbitrage opportunities.

This module monitors funding rates and market conditions to identify
delta-neutral cash and carry opportunities that meet profitability criteria.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.14, 2.15
"""

import asyncio
from decimal import Decimal
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass
import statistics

from backend.services.funding_service import FundingService
from backend.services.exchange_client import ExchangeClient
from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class CashCarrySignal:
    """
    Opportunity signal data structure.
    
    Represents a potential arbitrage opportunity with all relevant metrics
    for risk assessment and execution decision.
    
    Attributes:
        symbol: Base trading pair (e.g., "BTC/USDT")
        spot_symbol: Spot market symbol (e.g., "BTC/USDT")
        perp_symbol: Perpetual contract symbol (e.g., "BTC/USDT:USDT")
        current_funding_rate: Current funding rate as decimal
        annualized_rate: Annualized return rate
        stability_score: Funding stability score (0-1)
        crowding_indicator: Crowding indicator (current / 30d avg)
        estimated_slippage: Estimated execution slippage
        expected_24h_funding: Expected funding income over 24 hours
        total_fees: Total trading fees (spot + perpetual)
        net_profit_24h: Net profit after fees over 24 hours
        timestamp: When signal was generated
    """
    symbol: str
    spot_symbol: str
    perp_symbol: str
    current_funding_rate: Decimal
    annualized_rate: Decimal
    stability_score: float
    crowding_indicator: float
    estimated_slippage: Decimal
    expected_24h_funding: Decimal
    total_fees: Decimal
    net_profit_24h: Decimal
    timestamp: datetime
    
    def is_valid(self) -> bool:
        """
        Check if signal meets all criteria for execution.
        
        Requirement 2.12: Generate signal when ALL criteria are met
        Requirement 2.13: Reject opportunities with annualized rate > 80%
        
        Returns:
            True if signal meets all criteria
        """
        return (
            self.annualized_rate >= Decimal("0.15") and
            self.annualized_rate <= Decimal("0.80") and
            self.stability_score >= 0.7 and
            self.estimated_slippage < Decimal("0.0005") and
            self.net_profit_24h > self.total_fees * 2
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "symbol": self.symbol,
            "spot_symbol": self.spot_symbol,
            "perp_symbol": self.perp_symbol,
            "current_funding_rate": str(self.current_funding_rate),
            "annualized_rate": str(self.annualized_rate),
            "stability_score": self.stability_score,
            "crowding_indicator": self.crowding_indicator,
            "estimated_slippage": str(self.estimated_slippage),
            "expected_24h_funding": str(self.expected_24h_funding),
            "total_fees": str(self.total_fees),
            "net_profit_24h": str(self.net_profit_24h),
            "timestamp": self.timestamp.isoformat(),
            "is_valid": self.is_valid()
        }


class CashCarryScout:
    """
    Scout module for discovering arbitrage opportunities.
    
    Monitors funding rates and market conditions across configured trading pairs
    to identify profitable delta-neutral cash and carry opportunities.
    
    Requirements:
    - 2.1: Monitor funding rates in real-time
    - 2.2: Calculate annualized return rates
    - 2.3-2.5: Calculate funding stability score
    - 2.6-2.7: Calculate crowding indicator
    - 2.8-2.9: Estimate slippage from order book
    - 2.10-2.11: Calculate expected net funding
    - 2.12-2.13: Generate opportunity signals with criteria checks
    - 2.14: Include market depth and stability metrics
    - 2.15: Update signals at least every 10 seconds
    """
    
    def __init__(
        self,
        funding_service: FundingService,
        exchange_client: ExchangeClient,
        config: SystemConfig,
        scout_id: int = 0
    ):
        """
        Initialize CashCarryScout.

        Args:
            funding_service: Service for funding rate data
            exchange_client: Exchange client for market data
            config: System configuration
            scout_id: Identifier for this scout instance (1-based for workers, 0 for legacy)
        """
        self.funding_service = funding_service
        self.exchange = exchange_client
        self.config = config
        self.scout_id = scout_id
        self.logger = logger

        # Cache for market data
        self._last_scan_time: Optional[datetime] = None

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
                "scout_id": self.scout_id,
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
                "type": "scout_progress",
                "data": progress_data
            })
        except Exception as e:
            await self.logger.debug(f"Failed to broadcast scan progress: {e}")
    
    async def scan_opportunities(self, symbols: Optional[List[str]] = None) -> List[CashCarrySignal]:
        """
        Scan trading pairs for opportunities.
        
        Requirement 2.1: Monitor funding rates for trading pairs
        Requirement 2.15: Update opportunity signals at least every 10 seconds
        
        Args:
            symbols: Optional list of symbols to scan. If None, scans all available perpetual markets.
        
        Returns:
            List of CashCarrySignal objects (both valid and invalid)
        """
        signals = []
        scan_start_time = datetime.utcnow()
        
        # If no symbols provided, fetch all available perpetual markets
        if symbols is None:
            try:
                await self.logger.info("侦查蜂开始全市场扫描...")
                # Broadcast scanning status
                await self._broadcast_scan_progress("正在获取市场列表...", 0, 0, 0)
                
                markets = await self.exchange.fetch_markets()
                # Filter for USDT perpetual contracts with sufficient volume
                symbols = [
                    m['symbol'].replace(':USDT', '')  # Convert "BTC/USDT:USDT" to "BTC/USDT"
                    for m in markets
                    if m.get('type') == 'swap' 
                    and m.get('quote') == 'USDT'
                    and m.get('active', True)
                ]
                await self.logger.info(f"侦查蜂发现 {len(symbols)} 个可交易市场")
            except Exception as e:
                await self.logger.error(f"获取市场列表失败，使用配置的交易对: {e}")
                symbols = self.config.enabled_symbols
        
        total_symbols = len(symbols)
        scanned_count = 0
        
        for symbol in symbols:
            scanned_count += 1
            try:
                # Convert spot symbol to perpetual symbol
                # Format: "BTC/USDT" -> "BTC/USDT:USDT"
                spot_symbol = symbol
                perp_symbol = f"{symbol}:USDT"
                
                await self.logger.debug(f"扫描 {symbol} 的套利机会")
                
                # Broadcast current scanning progress
                valid_count = sum(1 for s in signals if s.is_valid())
                await self._broadcast_scan_progress(
                    f"正在扫描 {symbol}...",
                    scanned_count,
                    total_symbols,
                    valid_count,
                    current_symbol=symbol
                )
                
                # Requirement 1.8: Check 24h volume meets minimum threshold
                try:
                    volume_24h = await self.exchange.fetch_24h_volume(perp_symbol)
                    if volume_24h < self.config.min_24h_volume_usd:
                        await self.logger.debug(
                            f"× {symbol} 24小时交易量不足: ${volume_24h:,.0f} < ${self.config.min_24h_volume_usd:,.0f}"
                        )
                        continue
                except Exception as e:
                    await self.logger.debug(f"无法获取 {symbol} 的交易量，跳过: {e}")
                    continue
                
                # Fetch current funding rate
                current_funding = await self.funding_service.get_current_funding_rate(perp_symbol)
                
                # Calculate annualized rate
                annualized_rate = await self.funding_service.calculate_annualized_rate(
                    current_funding,
                    periods_per_day=3  # 8-hour funding periods
                )
                
                # Calculate funding stability score
                stability_score = await self.calculate_funding_stability_score(perp_symbol)
                
                # Calculate crowding indicator
                crowding_indicator = await self.calculate_crowding_indicator(perp_symbol)
                
                # Estimate slippage for both spot and perpetual
                # Use a reference size for estimation (e.g., $10,000)
                reference_size_usd = Decimal("10000")
                ticker = await self.exchange.fetch_ticker(spot_symbol)
                last_price = Decimal(str(ticker.get("last", 0)))
                
                if last_price == 0:
                    await self.logger.warning(f"Invalid price for {symbol}, skipping")
                    continue
                
                reference_qty = reference_size_usd / last_price
                
                spot_slippage = await self.estimate_slippage(spot_symbol, "buy", reference_qty)
                perp_slippage = await self.estimate_slippage(perp_symbol, "sell", reference_qty)
                total_slippage = spot_slippage + perp_slippage
                
                # Calculate expected net funding over 24 hours
                expected_24h_funding = await self.calculate_expected_net_funding(
                    perp_symbol,
                    holding_hours=24
                )
                
                # Calculate total fees
                total_fees = self._calculate_total_fees(reference_size_usd)
                
                # Calculate net profit
                funding_income_24h = current_funding * 3 * reference_size_usd  # 3 periods per day
                net_profit_24h = funding_income_24h - total_fees
                
                # Create signal
                signal = CashCarrySignal(
                    symbol=symbol,
                    spot_symbol=spot_symbol,
                    perp_symbol=perp_symbol,
                    current_funding_rate=current_funding,
                    annualized_rate=annualized_rate,
                    stability_score=stability_score,
                    crowding_indicator=crowding_indicator,
                    estimated_slippage=total_slippage,
                    expected_24h_funding=expected_24h_funding,
                    total_fees=total_fees,
                    net_profit_24h=net_profit_24h,
                    timestamp=datetime.utcnow()
                )
                
                signals.append(signal)
                
                if signal.is_valid():
                    await self.logger.info(
                        f"✓ 发现套利机会 {symbol}: "
                        f"年化={annualized_rate:.2%}, "
                        f"稳定性={stability_score:.2f}, "
                        f"拥挤度={crowding_indicator:.2f}"
                    )
                else:
                    await self.logger.debug(
                        f"× {symbol} 不符合标准"
                    )
                
            except Exception as e:
                await self.logger.error(f"Failed to scan opportunity for {symbol}: {e}")
                continue
        
        self._last_scan_time = datetime.utcnow()
        scan_duration = (datetime.utcnow() - scan_start_time).total_seconds()
        
        valid_count = sum(1 for s in signals if s.is_valid())
        total_scanned = len(symbols) if symbols else 0
        
        await self.logger.info(
            f"侦查蜂扫描完成: 扫描 {total_scanned} 个市场, "
            f"通过交易量过滤 {len(signals)} 个, "
            f"发现 {valid_count} 个有效机会, "
            f"耗时 {scan_duration:.1f}秒"
        )
        
        # Broadcast final scan completion
        await self._broadcast_scan_progress(
            "扫描完成",
            total_scanned,
            total_scanned,
            valid_count,
            completed=True
        )
        
        return signals
    
    async def calculate_funding_stability_score(self, symbol: str) -> float:
        """
        Calculate funding stability score from last 6 funding periods.
        
        Requirements:
        - 2.3: Calculate Funding Stability Score based on last 6 periods
        - 2.4: Calculate standard deviation of recent funding rates
        - 2.5: Verify funding rates continuously positive for minimum 3 periods
        
        The stability score is calculated as:
        1. Check if last 3 periods are positive (requirement 2.5)
        2. Calculate coefficient of variation (std_dev / mean)
        3. Convert to 0-1 score where higher = more stable
        
        Args:
            symbol: Perpetual contract symbol
            
        Returns:
            Stability score between 0 and 1 (1 = most stable)
        """
        try:
            # Get last 6 funding periods
            history = await self.funding_service.get_funding_history(symbol, periods=6)
            
            if len(history) < 6:
                await self.logger.warning(
                    f"Insufficient funding history for {symbol}: {len(history)} periods"
                )
                return 0.0
            
            rates = [float(fr.rate) for fr in history]
            
            # Requirement 2.5: Verify last 3 periods are positive
            last_3_rates = rates[:3]  # Most recent 3
            if not all(r > 0 for r in last_3_rates):
                await self.logger.debug(
                    f"Funding rates not continuously positive for {symbol}"
                )
                return 0.0
            
            # Requirement 2.4: Calculate standard deviation
            mean_rate = statistics.mean(rates)
            std_dev = statistics.stdev(rates) if len(rates) > 1 else 0.0
            
            # Calculate coefficient of variation
            if mean_rate == 0:
                return 0.0
            
            cv = std_dev / abs(mean_rate)
            
            # Convert to stability score (0-1)
            # Lower CV = higher stability
            # Use exponential decay: score = e^(-cv * k)
            # With k=10, cv=0.1 gives score ~0.37, cv=0.05 gives score ~0.61
            import math
            stability_score = math.exp(-cv * 10)
            
            await self.logger.debug(
                f"Funding stability for {symbol}: "
                f"mean={mean_rate:.6f}, std={std_dev:.6f}, cv={cv:.4f}, score={stability_score:.4f}"
            )
            
            return stability_score
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate funding stability score for {symbol}: {e}"
            )
            return 0.0
    
    async def calculate_crowding_indicator(self, symbol: str) -> float:
        """
        Calculate crowding indicator as current_funding / 30_day_average.
        
        Requirements:
        - 2.6: Calculate Funding Crowding Indicator as current / 30-day average
        - 2.7: Reduce opportunity score when crowding > 3.0
        
        Args:
            symbol: Perpetual contract symbol
            
        Returns:
            Crowding indicator (ratio of current to 30-day average)
        """
        try:
            # Get current funding rate
            current_funding = await self.funding_service.get_current_funding_rate(symbol)
            
            # Get 30-day history (30 periods at 8-hour intervals = ~10 days)
            # For 30 days, we need 30 * 3 = 90 periods
            history = await self.funding_service.get_funding_history(symbol, periods=90)
            
            if len(history) < 30:
                await self.logger.warning(
                    f"Insufficient funding history for crowding calculation: {len(history)} periods"
                )
                # Return 1.0 (neutral) if insufficient data
                return 1.0
            
            # Calculate 30-day average
            rates = [float(fr.rate) for fr in history]
            avg_30d = statistics.mean(rates)
            
            if avg_30d == 0:
                await self.logger.warning(f"30-day average funding is zero for {symbol}")
                return 1.0
            
            # Calculate crowding indicator
            crowding = float(current_funding) / avg_30d
            
            await self.logger.debug(
                f"Crowding indicator for {symbol}: "
                f"current={current_funding:.6f}, 30d_avg={avg_30d:.6f}, crowding={crowding:.2f}"
            )
            
            # Requirement 2.7: Log warning if crowding > 3.0
            if crowding > 3.0:
                await self.logger.warning(
                    f"High crowding detected for {symbol}: {crowding:.2f}x "
                    f"(current={current_funding:.6f}, 30d_avg={avg_30d:.6f})"
                )
            
            return crowding
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate crowding indicator for {symbol}: {e}"
            )
            return 1.0
    
    async def estimate_slippage(
        self,
        symbol: str,
        side: str,
        quantity: Decimal
    ) -> Decimal:
        """
        Estimate slippage from order book depth (first 3 levels).
        
        Requirements:
        - 2.8: Calculate spot-perpetual price spread
        - 2.9: Calculate expected slippage based on order book depth (first 3 levels)
        
        Slippage is calculated as:
        (weighted_average_price - best_price) / best_price
        
        Args:
            symbol: Trading pair symbol
            side: Order side ("buy" or "sell")
            quantity: Order quantity
            
        Returns:
            Estimated slippage as decimal (e.g., 0.0005 = 0.05%)
        """
        try:
            # Fetch order book with at least 3 levels
            order_book = await self.exchange.fetch_order_book(symbol, limit=10)
            
            # Select appropriate side of order book
            if side.lower() == "buy":
                levels = order_book.get("asks", [])  # Buy from asks
            else:
                levels = order_book.get("bids", [])  # Sell to bids
            
            if not levels or len(levels) < 3:
                await self.logger.warning(
                    f"Insufficient order book depth for {symbol}: {len(levels)} levels"
                )
                # Return high slippage estimate if insufficient depth
                return Decimal("0.01")  # 1%
            
            # Use first 3 levels for slippage calculation
            levels = levels[:3]
            
            best_price = Decimal(str(levels[0][0]))
            
            # Calculate weighted average price
            remaining_qty = quantity
            total_cost = Decimal("0")
            total_filled = Decimal("0")
            
            for price_str, qty_str in levels:
                price = Decimal(str(price_str))
                available_qty = Decimal(str(qty_str))
                
                fill_qty = min(remaining_qty, available_qty)
                total_cost += price * fill_qty
                total_filled += fill_qty
                remaining_qty -= fill_qty
                
                if remaining_qty <= 0:
                    break
            
            if total_filled == 0:
                await self.logger.warning(f"No liquidity available for {symbol}")
                return Decimal("0.01")
            
            weighted_avg_price = total_cost / total_filled
            
            # Calculate slippage percentage
            slippage = abs(weighted_avg_price - best_price) / best_price
            
            await self.logger.debug(
                f"Slippage estimate for {symbol} {side} {quantity}: "
                f"best={best_price}, avg={weighted_avg_price}, slippage={slippage:.4%}"
            )
            
            return slippage
            
        except Exception as e:
            await self.logger.error(
                f"Failed to estimate slippage for {symbol}: {e}"
            )
            # Return conservative estimate on error
            return Decimal("0.01")
    
    async def calculate_expected_net_funding(
        self,
        symbol: str,
        holding_hours: int = 24
    ) -> Decimal:
        """
        Calculate expected funding income over holding period.
        
        Requirements:
        - 2.10: Calculate expected net funding income over minimum 24-hour holding period
        - 2.11: Verify expected net funding exceeds total trading fees by at least 2x
        
        Args:
            symbol: Perpetual contract symbol
            holding_hours: Holding period in hours (default 24)
            
        Returns:
            Expected net funding income as decimal
        """
        try:
            # Get current funding rate
            current_funding = await self.funding_service.get_current_funding_rate(symbol)
            
            # Calculate number of funding periods in holding period
            # Funding occurs every 8 hours, so 3 periods per day
            periods_per_day = 3
            num_periods = (holding_hours / 24) * periods_per_day
            
            # Expected funding income = current_rate * num_periods * position_size
            # For calculation purposes, use normalized position size of 1.0
            expected_funding = current_funding * Decimal(str(num_periods))
            
            await self.logger.debug(
                f"Expected funding for {symbol} over {holding_hours}h: "
                f"{expected_funding:.6f} ({num_periods:.1f} periods)"
            )
            
            return expected_funding
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate expected net funding for {symbol}: {e}"
            )
            return Decimal("0")
    
    def _calculate_total_fees(self, position_size_usd: Decimal) -> Decimal:
        """
        Calculate total trading fees for opening and closing position.
        
        Fees include:
        - Spot market buy (entry)
        - Perpetual market short (entry)
        - Perpetual market close (exit)
        - Spot market sell (exit)
        
        Args:
            position_size_usd: Position size in USD
            
        Returns:
            Total fees as decimal
        """
        # Entry fees
        spot_entry_fee = position_size_usd * self.config.spot_taker_fee
        perp_entry_fee = position_size_usd * self.config.perp_taker_fee
        
        # Exit fees
        spot_exit_fee = position_size_usd * self.config.spot_taker_fee
        perp_exit_fee = position_size_usd * self.config.perp_taker_fee
        
        total_fees = spot_entry_fee + perp_entry_fee + spot_exit_fee + perp_exit_fee
        
        return total_fees
