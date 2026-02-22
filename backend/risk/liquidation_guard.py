"""
Liquidation Guard for monitoring liquidation risk.

This module provides functionality for:
- Calculating liquidation distance using exchange API
- Getting exchange-provided liquidation prices
- Checking liquidation risk against thresholds

Requirements: 5.8, 5.9, 4.10, 4.12
"""

from decimal import Decimal
from typing import Optional

from core.models import Position
from services.exchange_client import ExchangeClient
from config.settings import SystemConfig
from utils.logger import get_logger


logger = get_logger(__name__)


class LiquidationGuard:
    """
    Guard for monitoring and preventing liquidation risk.
    
    This guard:
    - Calculates liquidation distance using exchange API (Requirement 5.8)
    - Gets exchange-provided liquidation prices (Requirement 5.9)
    - Checks if liquidation distance falls below threshold (Requirement 4.10)
    - Validates liquidation safety for stress tests (Requirement 4.12)
    """
    
    def __init__(
        self,
        exchange_client: ExchangeClient,
        config: SystemConfig
    ):
        """
        Initialize LiquidationGuard.
        
        Args:
            exchange_client: Exchange client for fetching liquidation data
            config: System configuration with risk thresholds
        """
        self.exchange = exchange_client
        self.config = config
        self.logger = logger
        self.min_liquidation_distance = config.min_liquidation_distance_pct
    
    async def get_exchange_liquidation_price(
        self,
        symbol: str,
        side: str,
        entry_price: Decimal,
        leverage: Decimal,
        quantity: Decimal
    ) -> Decimal:
        """
        Query exchange API for actual liquidation price.
        
        IMPORTANT: Use exchange-provided liquidation price formula.
        Do not calculate manually as each exchange has different margin rules.
        
        Requirement 5.9: Calculate liquidation distance for all perpetual positions
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT:USDT")
            side: Position side ("long" or "short")
            entry_price: Position entry price
            leverage: Position leverage
            quantity: Position quantity
            
        Returns:
            Liquidation price as Decimal
            
        Raises:
            Exception: If liquidation price cannot be fetched
        """
        try:
            # Fetch position data from exchange which includes liquidation price
            positions = await self.exchange.fetch_positions(symbol)
            
            if not positions:
                # If no position found, calculate estimated liquidation price
                # This is a fallback and should not be used for production decisions
                await self.logger.warning(
                    f"No position found on exchange for {symbol}, "
                    f"calculating estimated liquidation price"
                )
                return await self._estimate_liquidation_price(
                    entry_price, leverage, side
                )
            
            # Find the matching position
            for position_data in positions:
                if position_data.get("side", "").lower() == side.lower():
                    liq_price = position_data.get("liquidationPrice")
                    if liq_price:
                        exchange_liq_price = Decimal(str(liq_price))
                        # Cross-validate with local estimate
                        local_estimate = await self._estimate_liquidation_price(
                            entry_price, leverage, side
                        )
                        if local_estimate > 0:
                            deviation = abs(exchange_liq_price - local_estimate) / local_estimate
                            if deviation > Decimal("0.05"):
                                await self.logger.warning(
                                    f"清算价偏差过大: {symbol} 交易所={exchange_liq_price}, "
                                    f"本地估算={local_estimate}, 偏差={deviation:.2%}. "
                                    f"采用更保守的值"
                                )
                                # Use the more conservative value (closer to current price)
                                if side.lower() == "short":
                                    return min(exchange_liq_price, local_estimate)
                                else:
                                    return max(exchange_liq_price, local_estimate)

                        await self.logger.debug(
                            f"Exchange liquidation price for {symbol} {side}: {exchange_liq_price}"
                        )
                        return exchange_liq_price
            
            # Fallback to estimation if not found in position data
            await self.logger.warning(
                f"Liquidation price not found in exchange data for {symbol}, estimating"
            )
            return await self._estimate_liquidation_price(entry_price, leverage, side)
            
        except Exception as e:
            await self.logger.error(
                f"Failed to get exchange liquidation price for {symbol}: {e}"
            )
            # Fallback to estimation
            return await self._estimate_liquidation_price(entry_price, leverage, side)
    
    async def _estimate_liquidation_price(
        self,
        entry_price: Decimal,
        leverage: Decimal,
        side: str
    ) -> Decimal:
        """
        Estimate liquidation price (fallback only).
        
        This is a simplified calculation and should NOT be used for production
        risk decisions. Always prefer exchange-provided liquidation prices.
        
        Args:
            entry_price: Position entry price
            leverage: Position leverage
            side: Position side ("long" or "short")
            
        Returns:
            Estimated liquidation price
        """
        # Simplified formula: liquidation occurs when loss reaches (1 / leverage)
        # For long: liq_price = entry_price * (1 - 1/leverage)
        # For short: liq_price = entry_price * (1 + 1/leverage)
        
        liquidation_threshold = Decimal("1") / leverage
        
        if side.lower() == "long":
            liq_price = entry_price * (Decimal("1") - liquidation_threshold)
        else:  # short
            liq_price = entry_price * (Decimal("1") + liquidation_threshold)
        
        await self.logger.warning(
            f"Using estimated liquidation price: {liq_price} "
            f"(entry: {entry_price}, leverage: {leverage}, side: {side})"
        )
        
        return liq_price
    
    async def calculate_liquidation_distance(
        self,
        position: Position
    ) -> Decimal:
        """
        Calculate percentage distance to liquidation.
        
        Requirement 5.8: Calculate liquidation distance for all perpetual positions
        
        Distance = |current_price - liquidation_price| / current_price
        
        Args:
            position: Position to check
            
        Returns:
            Liquidation distance as percentage (0.0 to 1.0)
            
        Raises:
            ValueError: If position data is incomplete
        """
        try:
            if position.current_perp_price is None:
                raise ValueError(
                    f"Position {position.position_id} has no current perpetual price"
                )
            
            # Get liquidation price from exchange
            liquidation_price = await self.get_exchange_liquidation_price(
                symbol=position.perp_symbol,
                side="short",  # Cash & carry uses short perpetual
                entry_price=position.perp_entry_price,
                leverage=position.leverage,
                quantity=position.perp_quantity
            )
            
            # Calculate distance as percentage
            current_price = position.current_perp_price
            distance = abs(current_price - liquidation_price) / current_price
            
            await self.logger.debug(
                f"Liquidation distance for {position.position_id}: {distance:.2%} "
                f"(current: {current_price}, liquidation: {liquidation_price})"
            )
            
            return distance
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate liquidation distance for {position.position_id}: {e}"
            )
            raise
    
    async def check_liquidation_risk(
        self,
        position: Position
    ) -> bool:
        """
        Check if liquidation distance falls below threshold.
        
        Requirement 4.10: Verify liquidation distance would be >= 30%
        Requirement 5.9: Alert when liquidation distance falls below 30%
        
        Args:
            position: Position to check
            
        Returns:
            True if liquidation risk is HIGH (distance < 30%), False otherwise
        """
        try:
            distance = await self.calculate_liquidation_distance(position)
            
            is_high_risk = distance < self.min_liquidation_distance
            
            if is_high_risk:
                await self.logger.warning(
                    f"HIGH LIQUIDATION RISK: Position {position.position_id} "
                    f"has liquidation distance {distance:.2%} "
                    f"(threshold: {self.min_liquidation_distance:.2%})"
                )
            
            return is_high_risk
            
        except Exception as e:
            await self.logger.error(
                f"Failed to check liquidation risk for {position.position_id}: {e}"
            )
            # Return True (high risk) on error to be conservative
            return True
    
    async def validate_liquidation_safety(
        self,
        symbol: str,
        entry_price: Decimal,
        leverage: Decimal,
        quantity: Decimal,
        side: str = "short"
    ) -> bool:
        """
        Validate that a proposed position would have safe liquidation distance.
        
        Requirement 4.10: Calculate and verify liquidation distance would be >= 30%
        
        Args:
            symbol: Trading pair symbol
            entry_price: Proposed entry price
            leverage: Proposed leverage
            quantity: Proposed quantity
            side: Position side (default "short" for cash & carry)
            
        Returns:
            True if liquidation distance would be >= threshold, False otherwise
        """
        try:
            # Get liquidation price for proposed position
            liquidation_price = await self.get_exchange_liquidation_price(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                leverage=leverage,
                quantity=quantity
            )
            
            # Calculate distance
            distance = abs(entry_price - liquidation_price) / entry_price
            
            is_safe = distance >= self.min_liquidation_distance
            
            if not is_safe:
                await self.logger.warning(
                    f"Proposed position for {symbol} would have insufficient "
                    f"liquidation distance: {distance:.2%} "
                    f"(required: {self.min_liquidation_distance:.2%})"
                )
            else:
                await self.logger.debug(
                    f"Proposed position for {symbol} has safe liquidation distance: "
                    f"{distance:.2%}"
                )
            
            return is_safe
            
        except Exception as e:
            await self.logger.error(
                f"Failed to validate liquidation safety for {symbol}: {e}"
            )
            # Return False (unsafe) on error to be conservative
            return False
    
    async def validate_stress_test_liquidation(
        self,
        symbol: str,
        entry_price: Decimal,
        leverage: Decimal,
        quantity: Decimal,
        adverse_move_pct: Decimal,
        side: str = "short"
    ) -> bool:
        """
        Validate that position would not liquidate under stress test conditions.
        
        Requirement 4.12: Verify that simulated adverse movement would not trigger liquidation
        
        Args:
            symbol: Trading pair symbol
            entry_price: Proposed entry price
            leverage: Proposed leverage
            quantity: Proposed quantity
            adverse_move_pct: Adverse price movement percentage (e.g., 0.20 for 20%)
            side: Position side (default "short" for cash & carry)
            
        Returns:
            True if position would survive stress test, False otherwise
        """
        try:
            # Get liquidation price
            liquidation_price = await self.get_exchange_liquidation_price(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                leverage=leverage,
                quantity=quantity
            )
            
            # Calculate stressed price
            # For short positions, adverse move is price increase
            if side.lower() == "short":
                stressed_price = entry_price * (Decimal("1") + adverse_move_pct)
            else:  # long
                stressed_price = entry_price * (Decimal("1") - adverse_move_pct)
            
            # Check if stressed price would trigger liquidation
            would_liquidate = False
            if side.lower() == "short":
                # Short liquidates when price goes above liquidation price
                would_liquidate = stressed_price >= liquidation_price
            else:  # long
                # Long liquidates when price goes below liquidation price
                would_liquidate = stressed_price <= liquidation_price
            
            if would_liquidate:
                await self.logger.warning(
                    f"STRESS TEST FAILED: Position for {symbol} would liquidate "
                    f"under {adverse_move_pct:.1%} adverse move. "
                    f"Entry: {entry_price}, Stressed: {stressed_price}, "
                    f"Liquidation: {liquidation_price}"
                )
            else:
                await self.logger.debug(
                    f"Stress test passed for {symbol}: "
                    f"Entry: {entry_price}, Stressed: {stressed_price}, "
                    f"Liquidation: {liquidation_price}"
                )
            
            return not would_liquidate
            
        except Exception as e:
            await self.logger.error(
                f"Failed to validate stress test liquidation for {symbol}: {e}"
            )
            # Return False (would liquidate) on error to be conservative
            return False
