"""
Capital Allocator (Queen) module for managing capital allocation and position sizing.

This module implements the Queen component that calculates available capital,
validates position sizes against risk limits, and allocates capital to strategies.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.10
"""

from decimal import Decimal
from typing import Optional, List
from dataclasses import dataclass

from config.settings import SystemConfig
from storage.redis_client import RedisClient
from services.equity_service import EquityService
from services.exchange_client import ExchangeClient
from utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of position size validation."""
    is_valid: bool
    reason: Optional[str] = None
    max_allowed: Optional[Decimal] = None


@dataclass
class AllocationResult:
    """Result of capital allocation."""
    approved: bool
    allocated_amount: Decimal
    remaining_capacity: Decimal
    reason: Optional[str] = None


class CapitalAllocator:
    """
    Queen module for capital allocation and position sizing.
    
    Responsibilities:
    - Calculate available capital (Requirement 1.1)
    - Enforce position sizing limits (Requirements 1.2, 1.3, 1.4)
    - Maintain safety reserve (Requirement 1.5)
    - Allocate capital with priority handling (Requirements 1.6, 1.7)
    - Validate minimum trade sizes and liquidity (Requirements 1.8, 1.9, 1.10)
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        equity_service: EquityService,
        exchange_client: ExchangeClient,
        config: SystemConfig
    ):
        """
        Initialize Capital Allocator.
        
        Args:
            redis_client: Redis client for state management
            equity_service: Equity service for equity calculations
            exchange_client: Exchange client for market data
            config: System configuration
        """
        self.redis = redis_client
        self.equity_service = equity_service
        self.exchange = exchange_client
        self.config = config
        self.logger = get_logger("CapitalAllocator")
    
    async def calculate_available_capital(self) -> Decimal:
        """
        Calculate capital available for new positions.
        
        Formula: available = total_equity - reserved_funds - current_exposure
        
        Requirement 1.1: Calculate available capital by subtracting reserved funds
        and current exposure from total equity
        
        Returns:
            Available capital (non-negative)
        """
        # Get current equity from Redis
        current_equity = await self.redis.get_current_equity()
        
        if current_equity is None:
            await self.logger.warning("Redis中未找到当前权益，返回0")
            return Decimal("0")
        
        # Get current exposure
        current_exposure = await self.redis.get_total_exposure()
        
        # Calculate reserved funds (safety reserve)
        # Requirement 1.5: Maintain minimum 50% safety reserve
        reserved_funds = current_equity * self.config.min_safety_reserve_pct
        
        # Calculate available capital
        available = current_equity - reserved_funds - current_exposure
        
        # Ensure non-negative (Property 1)
        if available < Decimal("0"):
            available = Decimal("0")
        
        await self.logger.debug(
            f"可用资金已计算: {available} "
            f"(权益={current_equity}, 预留={reserved_funds}, 敞口={current_exposure})"
        )
        
        return available
    
    async def calculate_position_size(
        self,
        symbol: str,
        expected_return: Decimal
    ) -> Decimal:
        """
        Calculate appropriate position size for a new opportunity.
        
        This method determines the optimal position size based on:
        - Available capital
        - Single position limit (15%)
        - Total exposure limit (50%)
        - Per-asset-class limit (30%)
        
        Args:
            symbol: Trading pair symbol
            expected_return: Expected return from the opportunity
            
        Returns:
            Position size in USDT (notional value)
        """
        # Get current equity
        current_equity = await self.redis.get_current_equity()
        
        if current_equity is None or current_equity == Decimal("0"):
            await self.logger.warning("没有可用权益进行仓位计算")
            return Decimal("0")
        
        # Calculate maximum position size based on single position limit
        # Requirement 1.2: Limit single positions to 15% of total capital
        max_single = current_equity * self.config.max_single_position_pct
        
        # Get available capital
        available = await self.calculate_available_capital()
        
        # Position size is the minimum of:
        # 1. Available capital
        # 2. Single position limit
        position_size = min(available, max_single)
        
        # Extract asset class from symbol
        asset_class = symbol.split('/')[0] if '/' in symbol else symbol
        
        # Check per-asset-class limit
        # Requirement 1.4: Enforce maximum per-asset-class exposure of 30%
        current_asset_exposure = await self.get_exposure_by_asset_class(asset_class)
        max_asset_exposure = current_equity * self.config.max_asset_class_exposure_pct
        remaining_asset_capacity = max_asset_exposure - current_asset_exposure
        
        if remaining_asset_capacity < position_size:
            position_size = remaining_asset_capacity
        
        # Ensure non-negative
        if position_size < Decimal("0"):
            position_size = Decimal("0")
        
        await self.logger.debug(
            f"已计算 {symbol} 的仓位大小: {position_size} USDT "
            f"(可用={available}, 单仓上限={max_single}, "
            f"资产容量={remaining_asset_capacity})"
        )
        
        return position_size
    
    async def validate_position_size(
        self,
        symbol: str,
        notional_value: Decimal,
        asset_class: str
    ) -> ValidationResult:
        """
        Validate position size against all limits.
        
        Validates:
        - Single position limit (15% of total capital) - Requirement 1.2
        - Total exposure limit (50% of total capital) - Requirement 1.3
        - Per-asset-class exposure limit (30% of total capital) - Requirement 1.4
        
        Args:
            symbol: Trading pair symbol
            notional_value: Notional value of the position
            asset_class: Asset class (e.g., "BTC", "ETH")
        
        Returns:
            ValidationResult with validation status and details
        """
        # Get current equity
        current_equity = await self.redis.get_current_equity()
        
        if current_equity is None or current_equity <= Decimal("0"):
            return ValidationResult(
                is_valid=False,
                reason="Current equity not available or zero"
            )
        
        # Check single position limit (Requirement 1.2)
        max_single_position = current_equity * self.config.max_single_position_pct
        
        if notional_value > max_single_position:
            return ValidationResult(
                is_valid=False,
                reason=f"Position size {notional_value} exceeds single position limit",
                max_allowed=max_single_position
            )
        
        # Check total exposure limit (Requirement 1.3)
        current_total_exposure = await self.redis.get_total_exposure()
        max_total_exposure = current_equity * self.config.max_total_exposure_pct
        new_total_exposure = current_total_exposure + notional_value
        
        if new_total_exposure > max_total_exposure:
            remaining_capacity = max_total_exposure - current_total_exposure
            return ValidationResult(
                is_valid=False,
                reason=f"New total exposure {new_total_exposure} would exceed limit {max_total_exposure}",
                max_allowed=remaining_capacity
            )
        
        # Check per-asset-class exposure limit (Requirement 1.4)
        current_asset_class_exposure = await self.redis.get_asset_class_exposure(asset_class)
        max_asset_class_exposure = current_equity * self.config.max_asset_class_exposure_pct
        new_asset_class_exposure = current_asset_class_exposure + notional_value
        
        if new_asset_class_exposure > max_asset_class_exposure:
            remaining_capacity = max_asset_class_exposure - current_asset_class_exposure
            return ValidationResult(
                is_valid=False,
                reason=f"New {asset_class} exposure {new_asset_class_exposure} would exceed limit {max_asset_class_exposure}",
                max_allowed=remaining_capacity
            )
        
        await self.logger.debug(
            f"仓位大小验证通过: {symbol}: {notional_value}"
        )
        
        return ValidationResult(is_valid=True)
    
    async def allocate_capital(
        self,
        strategy_id: str,
        symbol: str,
        requested_amount: Decimal,
        asset_class: str,
        priority: int = 0
    ) -> AllocationResult:
        """
        Allocate capital to a strategy.
        
        Requirements 1.6, 1.7: Allocate capital based on strategy priority
        and available funds, update calculations after every position change
        
        Args:
            strategy_id: Strategy identifier
            symbol: Trading pair symbol
            requested_amount: Requested capital amount
            asset_class: Asset class for the position
            priority: Strategy priority (higher = more important)
        
        Returns:
            AllocationResult with approval status and allocated amount
        """
        # Calculate available capital
        available = await self.calculate_available_capital()
        
        if available <= Decimal("0"):
            return AllocationResult(
                approved=False,
                allocated_amount=Decimal("0"),
                remaining_capacity=Decimal("0"),
                reason="No capital available"
            )
        
        # Validate position size against limits
        validation = await self.validate_position_size(
            symbol=symbol,
            notional_value=requested_amount,
            asset_class=asset_class
        )
        
        if not validation.is_valid:
            # If validation failed, try to allocate maximum allowed
            if validation.max_allowed and validation.max_allowed > Decimal("0"):
                allocated = min(validation.max_allowed, available)
                return AllocationResult(
                    approved=True,
                    allocated_amount=allocated,
                    remaining_capacity=available - allocated,
                    reason=f"Reduced from {requested_amount} to {allocated} due to limits"
                )
            else:
                return AllocationResult(
                    approved=False,
                    allocated_amount=Decimal("0"),
                    remaining_capacity=available,
                    reason=validation.reason
                )
        
        # Allocate requested amount (capped by available capital)
        allocated = min(requested_amount, available)
        
        await self.logger.info(
            f"已为策略 {strategy_id} 的 {symbol} 分配资金: {allocated} "
            f"(请求={requested_amount}, 可用={available})"
        )
        
        return AllocationResult(
            approved=True,
            allocated_amount=allocated,
            remaining_capacity=available - allocated
        )
    
    async def get_exposure_by_asset_class(self, asset_class: str) -> Decimal:
        """
        Get current exposure for an asset class.
        
        Args:
            asset_class: Asset class identifier (e.g., "BTC", "ETH")
        
        Returns:
            Current exposure for the asset class
        """
        exposure = await self.redis.get_asset_class_exposure(asset_class)
        return exposure
    
    async def validate_minimum_trade_size(
        self,
        symbol: str,
        quantity: Decimal
    ) -> ValidationResult:
        """
        Validate position meets exchange minimum trade size requirements.
        
        Requirement 1.8: Filter out positions below exchange minimum trade size
        
        Args:
            symbol: Trading pair symbol
            quantity: Position quantity
        
        Returns:
            ValidationResult indicating if trade size is valid
        """
        try:
            # Get market info from exchange
            market = self.exchange.exchange.market(symbol)
            
            # Check minimum quantity
            min_qty = market.get('limits', {}).get('amount', {}).get('min')
            if min_qty and quantity < Decimal(str(min_qty)):
                return ValidationResult(
                    is_valid=False,
                    reason=f"Quantity {quantity} below minimum {min_qty} for {symbol}",
                    max_allowed=Decimal(str(min_qty))
                )
            
            # Check minimum notional value (if available)
            min_cost = market.get('limits', {}).get('cost', {}).get('min')
            if min_cost:
                # Get current price to calculate notional
                ticker = await self.exchange.fetch_ticker(symbol)
                current_price = Decimal(str(ticker['last']))
                notional = quantity * current_price
                
                if notional < Decimal(str(min_cost)):
                    return ValidationResult(
                        is_valid=False,
                        reason=f"Notional value {notional} below minimum {min_cost} for {symbol}"
                    )
            
            return ValidationResult(is_valid=True)
            
        except Exception as e:
            await self.logger.error(f"验证 {symbol} 最小交易量失败: {e}")
            return ValidationResult(
                is_valid=False,
                reason=f"Failed to validate trade size: {e}"
            )
    
    async def validate_liquidity(
        self,
        symbol: str,
        position_size: Decimal
    ) -> ValidationResult:
        """
        Validate trading pair has sufficient liquidity.
        
        Requirements 1.9, 1.10:
        - Verify 24-hour volume above configured minimum threshold
        - Verify order book depth is at least 10x the intended position size
        
        Args:
            symbol: Trading pair symbol
            position_size: Intended position size in quote currency
        
        Returns:
            ValidationResult indicating if liquidity is sufficient
        """
        try:
            # Check 24-hour volume (Requirement 1.9)
            volume_24h = await self.exchange.fetch_24h_volume(symbol)
            
            if volume_24h < self.config.min_24h_volume_usd:
                return ValidationResult(
                    is_valid=False,
                    reason=f"24h volume {volume_24h} below minimum {self.config.min_24h_volume_usd}"
                )
            
            # Check order book depth (Requirement 1.10)
            order_book = await self.exchange.fetch_order_book(symbol, limit=20)
            
            # Calculate total liquidity in order book (both sides)
            total_bid_liquidity = Decimal("0")
            for bid in order_book['bids']:
                price = Decimal(str(bid[0]))
                quantity = Decimal(str(bid[1]))
                total_bid_liquidity += price * quantity
            
            total_ask_liquidity = Decimal("0")
            for ask in order_book['asks']:
                price = Decimal(str(ask[0]))
                quantity = Decimal(str(ask[1]))
                total_ask_liquidity += price * quantity
            
            # Use the smaller of bid/ask liquidity
            available_liquidity = min(total_bid_liquidity, total_ask_liquidity)
            
            required_liquidity = position_size * self.config.min_order_book_depth_multiplier
            
            if available_liquidity < required_liquidity:
                return ValidationResult(
                    is_valid=False,
                    reason=f"Order book liquidity {available_liquidity} below required {required_liquidity} "
                           f"({self.config.min_order_book_depth_multiplier}x position size)"
                )
            
            await self.logger.debug(
                f"流动性验证通过: {symbol}: "
                f"成交量={volume_24h}, 流动性={available_liquidity}"
            )
            
            return ValidationResult(is_valid=True)
            
        except Exception as e:
            await self.logger.error(f"验证 {symbol} 流动性失败: {e}")
            return ValidationResult(
                is_valid=False,
                reason=f"Failed to validate liquidity: {e}"
            )
    
    async def validate_full_position(
        self,
        symbol: str,
        notional_value: Decimal,
        quantity: Decimal,
        asset_class: str
    ) -> ValidationResult:
        """
        Perform full validation of a position including size, liquidity, and trade minimums.
        
        Args:
            symbol: Trading pair symbol
            notional_value: Notional value of the position
            quantity: Position quantity
            asset_class: Asset class
        
        Returns:
            ValidationResult with comprehensive validation status
        """
        # Validate position size limits
        size_validation = await self.validate_position_size(symbol, notional_value, asset_class)
        if not size_validation.is_valid:
            return size_validation
        
        # Validate minimum trade size
        min_size_validation = await self.validate_minimum_trade_size(symbol, quantity)
        if not min_size_validation.is_valid:
            return min_size_validation
        
        # Validate liquidity
        liquidity_validation = await self.validate_liquidity(symbol, notional_value)
        if not liquidity_validation.is_valid:
            return liquidity_validation
        
        return ValidationResult(is_valid=True)
