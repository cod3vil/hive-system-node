"""
Trade Executor (Worker) module for atomic position execution.

This module handles atomic trade execution with distributed locking,
rollback logic, and delta-neutral verification.

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 13.1, 13.2, 13.3, 13.4, 13.5, 13.6, 13.7
"""

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import uuid4

from execution.order_manager import OrderManager, OrderResult
from storage.redis_client import RedisClient
from core.models import Position, PositionStatus
from utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class ExecutionResult:
    """
    Result of a trade execution operation.
    
    Contains details about the execution including success status,
    position information, fill prices, and any error messages.
    """
    success: bool
    position_id: Optional[str] = None
    spot_fill_price: Optional[Decimal] = None
    perp_fill_price: Optional[Decimal] = None
    spot_quantity: Optional[Decimal] = None
    perp_quantity: Optional[Decimal] = None
    actual_slippage: Optional[Decimal] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "success": self.success,
            "position_id": self.position_id,
            "spot_fill_price": str(self.spot_fill_price) if self.spot_fill_price else None,
            "perp_fill_price": str(self.perp_fill_price) if self.perp_fill_price else None,
            "spot_quantity": str(self.spot_quantity) if self.spot_quantity else None,
            "perp_quantity": str(self.perp_quantity) if self.perp_quantity else None,
            "actual_slippage": str(self.actual_slippage) if self.actual_slippage else None,
            "error_message": self.error_message,
            "execution_time_ms": self.execution_time_ms
        }


class ExecutionError(Exception):
    """Exception raised during trade execution."""
    pass


class TradeExecutor:
    """
    Worker module for atomic trade execution.
    
    Handles:
    - Distributed lock acquisition (Requirement 3.1, 13.1)
    - Atomic position opening with spot-then-perpetual sequence (Requirement 3.2)
    - Delta-neutral verification (Requirement 3.4)
    - Rollback logic with retry attempts (Requirement 3.5, 3.6)
    - Atomic position closing with perpetual-then-spot sequence (Requirement 3.7)
    - Lock release with timeout protection (Requirement 3.8, 13.3)
    """
    
    def __init__(
        self,
        order_manager: OrderManager,
        redis_client: RedisClient,
        config: Any
    ):
        """
        Initialize Trade Executor.
        
        Args:
            order_manager: OrderManager instance for placing orders
            redis_client: RedisClient instance for distributed locking
            config: System configuration
        """
        self.order_manager = order_manager
        self.redis = redis_client
        self.config = config
        
        # Execution parameters
        self._lock_timeout = 30  # seconds
        self._max_rollback_attempts = 3
        self._delta_tolerance = Decimal("0.001")  # 0.1%
        self._max_directional_exposure_time = 10  # seconds (Requirement 3.6)
    
    async def execute_open_position(
        self,
        spot_symbol: str,
        perp_symbol: str,
        quantity: Decimal,
        leverage: Decimal,
        expected_spot_price: Optional[Decimal] = None,
        expected_perp_price: Optional[Decimal] = None,
        asset_class: str = "",
        strategy_id: str = "cash_carry_v1"
    ) -> ExecutionResult:
        """
        Execute atomic position opening with spot-then-perpetual sequence.
        
        Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.8, 13.1, 13.2
        
        Execution sequence:
        1. Acquire distributed lock
        2. Place spot market buy order
        3. Wait for spot fill
        4. Place perpetual short order
        5. Wait for perpetual fill
        6. Verify delta-neutral status
        7. Release lock
        
        If any step fails, execute rollback to close filled positions.
        
        Args:
            spot_symbol: Spot trading pair (e.g., "BTC/USDT")
            perp_symbol: Perpetual trading pair (e.g., "BTC/USDT:USDT")
            quantity: Position quantity in base currency
            leverage: Leverage for perpetual position
            expected_spot_price: Expected spot execution price
            expected_perp_price: Expected perpetual execution price
            asset_class: Asset class for the position (e.g., "BTC")
            strategy_id: Strategy identifier
            
        Returns:
            ExecutionResult with position details or error
        """
        start_time = time.time()
        lock_key = f"lock:execution:{spot_symbol}"
        spot_order: Optional[OrderResult] = None
        perp_order: Optional[OrderResult] = None
        position_id = uuid4().hex
        
        logger.info(
            f"Starting position opening execution: {spot_symbol} quantity={quantity} "
            f"leverage={leverage}x position_id={position_id}"
        )
        
        try:
            # Step 1: Acquire distributed lock (Requirement 3.1, 13.1)
            if not await self._acquire_lock(lock_key, self._lock_timeout):
                error_msg = f"Failed to acquire execution lock for {spot_symbol}"
                logger.warning(error_msg)
                return ExecutionResult(
                    success=False,
                    error_message=error_msg,
                    execution_time_ms=int((time.time() - start_time) * 1000)
                )
            
            logger.info(f"Acquired execution lock: {lock_key}")
            
            try:
                # Step 2: Place spot market buy order (Requirement 3.2)
                logger.info(f"Placing spot buy order: {spot_symbol} quantity={quantity}")
                spot_order = await self.order_manager.place_market_order(
                    symbol=spot_symbol,
                    side="buy",
                    quantity=quantity,
                    market_type="spot",
                    expected_price=expected_spot_price
                )
                
                if not spot_order.is_filled():
                    raise ExecutionError(
                        f"Spot order not fully filled: {spot_order.filled_quantity}/{spot_order.quantity}"
                    )
                
                logger.info(
                    f"Spot order filled: {spot_order.order_id} at {spot_order.average_fill_price}"
                )
                
                # Step 3: Place perpetual short order (Requirement 3.3)
                logger.info(f"Placing perpetual short order: {perp_symbol} quantity={quantity} leverage={leverage}x")
                perp_order = await self.order_manager.place_market_order(
                    symbol=perp_symbol,
                    side="sell",
                    quantity=quantity,
                    market_type="perpetual",
                    expected_price=expected_perp_price,
                    leverage=leverage
                )
                
                if not perp_order.is_filled():
                    raise ExecutionError(
                        f"Perpetual order not fully filled: {perp_order.filled_quantity}/{perp_order.quantity}"
                    )
                
                logger.info(
                    f"Perpetual order filled: {perp_order.order_id} at {perp_order.average_fill_price}"
                )
                
                # Step 4: Verify delta-neutral status (Requirement 3.4)
                if not self.verify_delta_neutral(spot_order.filled_quantity, perp_order.filled_quantity):
                    raise ExecutionError(
                        f"Delta-neutral verification failed: spot={spot_order.filled_quantity} "
                        f"perp={perp_order.filled_quantity}"
                    )
                
                logger.info("Delta-neutral verification passed")
                
                # Calculate actual slippage
                actual_slippage = None
                if expected_spot_price and expected_perp_price:
                    spot_slippage = self.order_manager.calculate_actual_slippage(
                        expected_spot_price, spot_order.average_fill_price
                    )
                    perp_slippage = self.order_manager.calculate_actual_slippage(
                        expected_perp_price, perp_order.average_fill_price
                    )
                    actual_slippage = abs(spot_slippage) + abs(perp_slippage)
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                logger.info(
                    f"Position opened successfully: {position_id} in {execution_time_ms}ms"
                )
                
                return ExecutionResult(
                    success=True,
                    position_id=position_id,
                    spot_fill_price=spot_order.average_fill_price,
                    perp_fill_price=perp_order.average_fill_price,
                    spot_quantity=spot_order.filled_quantity,
                    perp_quantity=perp_order.filled_quantity,
                    actual_slippage=actual_slippage,
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                # Execution failed - attempt rollback (Requirements 3.5, 3.6)
                logger.error(f"Execution failed, initiating rollback: {e}")
                
                rollback_success = await self._rollback_position(
                    spot_order=spot_order,
                    perp_order=perp_order,
                    spot_symbol=spot_symbol,
                    perp_symbol=perp_symbol
                )
                
                if not rollback_success:
                    # Critical: Rollback failed - manual intervention required
                    await logger.critical(
                        "CRITICAL: Rollback failed after execution error - MANUAL INTERVENTION REQUIRED",
                        extra={
                            "position_id": position_id,
                            "spot_order": spot_order.to_dict() if spot_order else None,
                            "perp_order": perp_order.to_dict() if perp_order else None,
                            "error": str(e)
                        }
                    )
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                return ExecutionResult(
                    success=False,
                    error_message=f"Execution failed: {e}",
                    execution_time_ms=execution_time_ms
                )
                
        finally:
            # Step 5: Release lock (Requirement 3.8, 13.3)
            await self._release_lock(lock_key)
            logger.info(f"Released execution lock: {lock_key}")
    
    async def execute_close_position(
        self,
        position: Position
    ) -> ExecutionResult:
        """
        Execute atomic position closing with perpetual-then-spot sequence.
        
        Requirements 3.7, 3.8, 13.1, 13.2
        
        Execution sequence:
        1. Acquire distributed lock
        2. Close perpetual short position (buy to close)
        3. Wait for perpetual fill
        4. Sell spot position
        5. Wait for spot fill
        6. Release lock
        
        Args:
            position: Position object to close
            
        Returns:
            ExecutionResult with close details or error
        """
        start_time = time.time()
        lock_key = f"lock:execution:{position.symbol}"
        perp_order: Optional[OrderResult] = None
        spot_order: Optional[OrderResult] = None
        
        logger.info(
            f"Starting position closing execution: {position.position_id} "
            f"symbol={position.symbol}"
        )
        
        try:
            # Step 1: Acquire distributed lock (Requirement 3.1, 13.1)
            if not await self._acquire_lock(lock_key, self._lock_timeout):
                error_msg = f"Failed to acquire execution lock for {position.symbol}"
                logger.warning(error_msg)
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=error_msg,
                    execution_time_ms=int((time.time() - start_time) * 1000)
                )
            
            logger.info(f"Acquired execution lock: {lock_key}")
            
            try:
                # Step 2: Close perpetual short position (buy to close) (Requirement 3.7)
                logger.info(
                    f"Closing perpetual short: {position.perp_symbol} "
                    f"quantity={position.perp_quantity}"
                )
                perp_order = await self.order_manager.place_market_order(
                    symbol=position.perp_symbol,
                    side="buy",  # Buy to close short
                    quantity=position.perp_quantity,
                    market_type="perpetual"
                )
                
                if not perp_order.is_filled():
                    raise ExecutionError(
                        f"Perpetual close order not fully filled: "
                        f"{perp_order.filled_quantity}/{perp_order.quantity}"
                    )
                
                logger.info(
                    f"Perpetual closed: {perp_order.order_id} at {perp_order.average_fill_price}"
                )
                
                # Step 3: Sell spot position
                logger.info(
                    f"Selling spot position: {position.spot_symbol} "
                    f"quantity={position.spot_quantity}"
                )
                spot_order = await self.order_manager.place_market_order(
                    symbol=position.spot_symbol,
                    side="sell",
                    quantity=position.spot_quantity,
                    market_type="spot"
                )
                
                if not spot_order.is_filled():
                    raise ExecutionError(
                        f"Spot sell order not fully filled: "
                        f"{spot_order.filled_quantity}/{spot_order.quantity}"
                    )
                
                logger.info(
                    f"Spot sold: {spot_order.order_id} at {spot_order.average_fill_price}"
                )
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                logger.info(
                    f"Position closed successfully: {position.position_id} in {execution_time_ms}ms"
                )
                
                return ExecutionResult(
                    success=True,
                    position_id=position.position_id,
                    spot_fill_price=spot_order.average_fill_price,
                    perp_fill_price=perp_order.average_fill_price,
                    spot_quantity=spot_order.filled_quantity,
                    perp_quantity=perp_order.filled_quantity,
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                # Close failed - attempt to restore position
                logger.error(f"Position close failed, attempting to restore: {e}")
                
                # If perpetual closed but spot failed, re-open perpetual short
                if perp_order and perp_order.is_filled() and (not spot_order or not spot_order.is_filled()):
                    logger.warning("Perpetual closed but spot failed, re-opening perpetual short")
                    try:
                        restore_order = await self.order_manager.place_market_order(
                            symbol=position.perp_symbol,
                            side="sell",
                            quantity=perp_order.filled_quantity,
                            market_type="perpetual"
                        )
                        logger.info(f"Perpetual short restored: {restore_order.order_id}")
                    except Exception as restore_error:
                        await logger.critical(
                            "CRITICAL: Failed to restore perpetual short - MANUAL INTERVENTION REQUIRED",
                            extra={
                                "position_id": position.position_id,
                                "perp_order": perp_order.to_dict(),
                                "error": str(restore_error)
                            }
                        )
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=f"Close failed: {e}",
                    execution_time_ms=execution_time_ms
                )
                
        finally:
            # Step 4: Release lock (Requirement 3.8, 13.3)
            await self._release_lock(lock_key)
            logger.info(f"Released execution lock: {lock_key}")
    
    def verify_delta_neutral(
        self,
        spot_qty: Decimal,
        perp_qty: Decimal
    ) -> bool:
        """
        Verify delta-neutral status by checking quantity difference.
        
        Requirement 3.4: Verify delta-neutral status after both orders complete
        
        Delta-neutral means the spot and perpetual quantities are equal within
        a small tolerance (0.1% by default).
        
        Args:
            spot_qty: Spot position quantity
            perp_qty: Perpetual position quantity
            
        Returns:
            True if position is delta-neutral within tolerance
        """
        if spot_qty == 0:
            logger.warning("Spot quantity is zero, cannot verify delta-neutral")
            return False
        
        # Calculate percentage difference
        diff = abs(spot_qty - perp_qty)
        diff_pct = diff / spot_qty
        
        is_neutral = diff_pct <= self._delta_tolerance
        
        logger.debug(
            f"Delta-neutral check: spot={spot_qty} perp={perp_qty} "
            f"diff={diff_pct:.4%} tolerance={self._delta_tolerance:.4%} "
            f"result={is_neutral}"
        )
        
        return is_neutral
    
    async def _acquire_lock(
        self,
        lock_key: str,
        timeout: int
    ) -> bool:
        """
        Acquire distributed lock via Redis SETNX.
        
        Requirements 3.1, 13.1, 13.2: Distributed lock acquisition
        
        Args:
            lock_key: Redis key for the lock
            timeout: Lock timeout in seconds
            
        Returns:
            True if lock acquired, False otherwise
        """
        acquired = await self.redis.acquire_lock(lock_key, timeout)
        
        if acquired:
            logger.debug(f"Lock acquired: {lock_key} (TTL: {timeout}s)")
        else:
            logger.warning(f"Failed to acquire lock: {lock_key}")
        
        return acquired
    
    async def _release_lock(self, lock_key: str) -> None:
        """
        Release distributed lock.
        
        Requirements 3.8, 13.3: Lock release with timeout protection
        
        Args:
            lock_key: Redis key for the lock
        """
        await self.redis.release_lock(lock_key)
        logger.debug(f"Lock released: {lock_key}")
    
    async def _rollback_position(
        self,
        spot_order: Optional[OrderResult],
        perp_order: Optional[OrderResult],
        spot_symbol: str,
        perp_symbol: str
    ) -> bool:
        """
        Rollback partially filled position by closing filled legs.
        
        Requirements 3.5, 3.6: Rollback logic with max 3 retry attempts
        
        This method attempts to close any filled orders to return to a flat position.
        It will retry up to max_rollback_attempts times with exponential backoff.
        
        Args:
            spot_order: Spot order result (may be None or partially filled)
            perp_order: Perpetual order result (may be None or partially filled)
            spot_symbol: Spot trading pair
            perp_symbol: Perpetual trading pair
            
        Returns:
            True if rollback successful, False otherwise
        """
        logger.warning("Initiating position rollback")
        
        for attempt in range(self._max_rollback_attempts):
            try:
                # Close spot position if filled
                if spot_order and spot_order.filled_quantity > 0:
                    logger.info(
                        f"Rolling back spot position: selling {spot_order.filled_quantity} {spot_symbol}"
                    )
                    await self.order_manager.place_market_order(
                        symbol=spot_symbol,
                        side="sell",
                        quantity=spot_order.filled_quantity,
                        market_type="spot"
                    )
                    logger.info("Spot position rolled back successfully")
                
                # Close perpetual position if filled
                if perp_order and perp_order.filled_quantity > 0:
                    logger.info(
                        f"Rolling back perpetual position: buying {perp_order.filled_quantity} {perp_symbol}"
                    )
                    await self.order_manager.place_market_order(
                        symbol=perp_symbol,
                        side="buy",  # Buy to close short
                        quantity=perp_order.filled_quantity,
                        market_type="perpetual"
                    )
                    logger.info("Perpetual position rolled back successfully")
                
                logger.info(f"Rollback completed successfully on attempt {attempt + 1}")
                return True
                
            except Exception as e:
                logger.error(f"Rollback attempt {attempt + 1} failed: {e}")
                
                if attempt < self._max_rollback_attempts - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.info(f"Retrying rollback in {wait_time}s...")
                    await asyncio.sleep(wait_time)
        
        # All rollback attempts failed
        logger.error(
            f"Rollback failed after {self._max_rollback_attempts} attempts - "
            "MANUAL INTERVENTION REQUIRED"
        )
        return False

    
    async def execute_rebalance(
        self,
        position: Position
    ) -> ExecutionResult:
        """
        Rebalance position to restore delta-neutral status.
        
        Requirements 5.14, 5.15, 5.16, 5.17: Rebalancing with cooldown enforcement
        
        This method adjusts position quantities to restore delta-neutral status
        when the spot-perpetual difference exceeds tolerance. It enforces a
        60-second cooldown between rebalancing operations.
        
        Args:
            position: Position object to rebalance
            
        Returns:
            ExecutionResult with rebalancing details or error
        """
        start_time = time.time()
        lock_key = f"lock:rebalance:{position.position_id}"
        
        logger.info(
            f"Starting rebalancing for position: {position.position_id} "
            f"spot={position.spot_quantity} perp={position.perp_quantity}"
        )
        
        try:
            # Check cooldown period (Requirement 5.16, 5.17)
            last_rebalance_time = await self.redis.get_rebalance_time(position.position_id)
            
            if last_rebalance_time is not None:
                time_since_last = int(time.time()) - last_rebalance_time
                cooldown_remaining = self.config.rebalance_cooldown_seconds - time_since_last
                
                if cooldown_remaining > 0:
                    error_msg = (
                        f"Rebalancing cooldown active: {cooldown_remaining}s remaining "
                        f"(last rebalance: {time_since_last}s ago)"
                    )
                    logger.warning(error_msg)
                    return ExecutionResult(
                        success=False,
                        position_id=position.position_id,
                        error_message=error_msg,
                        execution_time_ms=int((time.time() - start_time) * 1000)
                    )
            
            # Acquire rebalance lock
            if not await self._acquire_lock(lock_key, 60):
                error_msg = f"Failed to acquire rebalance lock for {position.position_id}"
                logger.warning(error_msg)
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=error_msg,
                    execution_time_ms=int((time.time() - start_time) * 1000)
                )
            
            logger.info(f"Acquired rebalance lock: {lock_key}")
            
            try:
                # Calculate hedge ratio and delta difference (Requirement 5.14)
                hedge_ratio = self._calculate_hedge_ratio(
                    position.spot_quantity,
                    position.perp_quantity
                )
                
                delta_diff = position.calculate_delta_difference()
                
                logger.info(
                    f"Current hedge ratio: {hedge_ratio:.6f} "
                    f"delta difference: {delta_diff:.4%}"
                )
                
                # Determine which side needs adjustment
                if position.spot_quantity > position.perp_quantity:
                    # Need to increase perpetual short
                    adjustment_qty = position.spot_quantity - position.perp_quantity
                    
                    logger.info(
                        f"Increasing perpetual short by {adjustment_qty} to match spot"
                    )
                    
                    perp_order = await self.order_manager.place_market_order(
                        symbol=position.perp_symbol,
                        side="sell",
                        quantity=adjustment_qty,
                        market_type="perpetual"
                    )
                    
                    if not perp_order.is_filled():
                        raise ExecutionError(
                            f"Rebalance perpetual order not filled: "
                            f"{perp_order.filled_quantity}/{perp_order.quantity}"
                        )
                    
                    logger.info(
                        f"Perpetual increased: {perp_order.order_id} at {perp_order.average_fill_price}"
                    )
                    
                elif position.perp_quantity > position.spot_quantity:
                    # Need to increase spot long
                    adjustment_qty = position.perp_quantity - position.spot_quantity
                    
                    logger.info(
                        f"Increasing spot long by {adjustment_qty} to match perpetual"
                    )
                    
                    spot_order = await self.order_manager.place_market_order(
                        symbol=position.spot_symbol,
                        side="buy",
                        quantity=adjustment_qty,
                        market_type="spot"
                    )
                    
                    if not spot_order.is_filled():
                        raise ExecutionError(
                            f"Rebalance spot order not filled: "
                            f"{spot_order.filled_quantity}/{spot_order.quantity}"
                        )
                    
                    logger.info(
                        f"Spot increased: {spot_order.order_id} at {spot_order.average_fill_price}"
                    )
                
                else:
                    # Already balanced
                    logger.info("Position already balanced, no adjustment needed")
                
                # Update last rebalance time (Requirement 5.16)
                await self.redis.set_rebalance_time(position.position_id)
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                logger.info(
                    f"Rebalancing completed successfully: {position.position_id} "
                    f"in {execution_time_ms}ms"
                )
                
                return ExecutionResult(
                    success=True,
                    position_id=position.position_id,
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                logger.error(f"Rebalancing failed: {e}")
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=f"Rebalancing failed: {e}",
                    execution_time_ms=execution_time_ms
                )
                
        finally:
            # Release rebalance lock
            await self._release_lock(lock_key)
            logger.info(f"Released rebalance lock: {lock_key}")
    
    def _calculate_hedge_ratio(
        self,
        spot_qty: Decimal,
        perp_qty: Decimal
    ) -> Decimal:
        """
        Calculate current hedge ratio.
        
        Requirement 5.14: Calculate hedge ratio
        
        Hedge ratio = perp_quantity / spot_quantity
        A perfect hedge has ratio = 1.0
        
        Args:
            spot_qty: Spot position quantity
            perp_qty: Perpetual position quantity
            
        Returns:
            Hedge ratio as decimal
        """
        if spot_qty == 0:
            logger.warning("Spot quantity is zero, cannot calculate hedge ratio")
            return Decimal("0")
        
        ratio = perp_qty / spot_qty
        
        logger.debug(
            f"Hedge ratio calculation: spot={spot_qty} perp={perp_qty} ratio={ratio:.6f}"
        )
        
        return ratio
    
    async def execute_cross_exchange_open(
        self,
        signal,
        quantity: Decimal,
        exchange_manager
    ) -> ExecutionResult:
        """
        Execute atomic cross-exchange position opening.
        
        Opens positions simultaneously on two exchanges:
        - Short on high-price exchange
        - Long on low-price exchange
        
        Args:
            signal: CrossExchangeSignal with opportunity details
            quantity: Position quantity in base currency
            exchange_manager: ExchangeManager instance
            
        Returns:
            ExecutionResult with position details or error
        """
        start_time = time.time()
        lock_key = f"lock:cross_exchange:{signal.symbol}"
        position_id = uuid4().hex
        
        high_order: Optional[OrderResult] = None
        low_order: Optional[OrderResult] = None
        
        logger.info(
            f"Starting cross-exchange position opening: {signal.symbol} "
            f"quantity={quantity} {signal.exchange_high} ↔ {signal.exchange_low} "
            f"position_id={position_id}"
        )
        
        try:
            # Acquire distributed lock
            if not await self._acquire_lock(lock_key, self._lock_timeout):
                error_msg = f"Failed to acquire lock for {signal.symbol}"
                logger.warning(error_msg)
                return ExecutionResult(
                    success=False,
                    error_message=error_msg,
                    execution_time_ms=int((time.time() - start_time) * 1000)
                )
            
            logger.info(f"Acquired execution lock: {lock_key}")
            
            try:
                # Get exchange clients
                high_client = exchange_manager.get_client(signal.exchange_high)
                low_client = exchange_manager.get_client(signal.exchange_low)
                
                # ============================================================
                # SAFETY CHECK 1: Re-fetch prices and validate spread direction
                # ============================================================
                logger.info(f"Re-fetching current prices for {signal.symbol} to validate spread...")
                
                try:
                    current_prices = await exchange_manager.get_cross_exchange_prices(signal.symbol)
                    
                    if not current_prices or len(current_prices) < 2:
                        error_msg = f"Cannot fetch current prices for {signal.symbol}"
                        logger.warning(error_msg)
                        return ExecutionResult(
                            success=False,
                            error_message=error_msg,
                            execution_time_ms=int((time.time() - start_time) * 1000)
                        )
                    
                    # Determine current high/low prices
                    exchanges = list(current_prices.keys())
                    price_1 = current_prices[exchanges[0]]
                    price_2 = current_prices[exchanges[1]]
                    
                    if price_1 > price_2:
                        current_high_price = price_1
                        current_low_price = price_2
                        current_high_exchange = exchanges[0]
                        current_low_exchange = exchanges[1]
                    else:
                        current_high_price = price_2
                        current_low_price = price_1
                        current_high_exchange = exchanges[1]
                        current_low_exchange = exchanges[0]
                    
                    # Calculate current spread
                    current_spread_pct = (current_high_price - current_low_price) / current_low_price
                    
                    logger.info(
                        f"Current prices: {current_high_exchange}={current_high_price}, "
                        f"{current_low_exchange}={current_low_price}, spread={current_spread_pct:.4%}"
                    )
                    
                    # Check 1: Spread must be positive
                    if current_spread_pct < self.config.cross_exchange_min_actual_spread_pct:
                        error_msg = (
                            f"Current spread is too low or negative ({current_spread_pct:.4%}), "
                            f"refusing to open position"
                        )
                        logger.warning(error_msg)
                        return ExecutionResult(
                            success=False,
                            error_message=error_msg,
                            execution_time_ms=int((time.time() - start_time) * 1000)
                        )
                    
                    # Check 2: Spread must still be profitable
                    min_spread = self.config.cross_exchange_min_spread_pct
                    if current_spread_pct < min_spread:
                        error_msg = (
                            f"Current spread ({current_spread_pct:.4%}) below minimum "
                            f"({min_spread:.4%}), refusing to open position"
                        )
                        logger.warning(error_msg)
                        return ExecutionResult(
                            success=False,
                            error_message=error_msg,
                            execution_time_ms=int((time.time() - start_time) * 1000)
                        )
                    
                    # Check 3: Spread change must not exceed threshold
                    spread_change_pct = abs(current_spread_pct - signal.spread_pct) / signal.spread_pct
                    max_change = self.config.cross_exchange_max_spread_change_pct
                    
                    if spread_change_pct > max_change:
                        error_msg = (
                            f"Spread changed too much ({spread_change_pct:.1%} > {max_change:.1%}), "
                            f"original={signal.spread_pct:.4%}, current={current_spread_pct:.4%}, "
                            f"possible price reversal"
                        )
                        logger.warning(error_msg)
                        return ExecutionResult(
                            success=False,
                            error_message=error_msg,
                            execution_time_ms=int((time.time() - start_time) * 1000)
                        )
                    
                    # Check 4: Exchange direction must match signal
                    if current_high_exchange != signal.exchange_high or current_low_exchange != signal.exchange_low:
                        error_msg = (
                            f"Price direction reversed! "
                            f"Original: {signal.exchange_high}(high) ↔ {signal.exchange_low}(low), "
                            f"Current: {current_high_exchange}(high) ↔ {current_low_exchange}(low)"
                        )
                        logger.warning(error_msg)
                        return ExecutionResult(
                            success=False,
                            error_message=error_msg,
                            execution_time_ms=int((time.time() - start_time) * 1000)
                        )
                    
                    logger.info(
                        f"✓ Price validation passed: spread={current_spread_pct:.4%}, "
                        f"change={spread_change_pct:.2%}"
                    )
                    
                except Exception as e:
                    error_msg = f"Price validation failed: {e}"
                    logger.error(error_msg)
                    return ExecutionResult(
                        success=False,
                        error_message=error_msg,
                        execution_time_ms=int((time.time() - start_time) * 1000)
                    )
                
                # Create order managers for each exchange
                high_order_manager = OrderManager(high_client, shadow_mode=self.order_manager.shadow_mode)
                low_order_manager = OrderManager(low_client, shadow_mode=self.order_manager.shadow_mode)
                
                perp_symbol = f"{signal.symbol}:USDT"
                
                # Place orders simultaneously on both exchanges
                logger.info(
                    f"Placing simultaneous orders: "
                    f"SHORT {perp_symbol} on {signal.exchange_high}, "
                    f"LONG {perp_symbol} on {signal.exchange_low}"
                )
                
                # Execute orders in parallel with retry logic
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        high_task = high_order_manager.place_market_order(
                            symbol=perp_symbol,
                            side="sell",  # Short on high-price exchange
                            quantity=quantity,
                            market_type="perpetual",
                            expected_price=signal.price_high
                        )
                        
                        low_task = low_order_manager.place_market_order(
                            symbol=perp_symbol,
                            side="buy",  # Long on low-price exchange
                            quantity=quantity,
                            market_type="perpetual",
                            expected_price=signal.price_low
                        )
                        
                        # Execute both orders in parallel
                        high_order, low_order = await asyncio.gather(high_task, low_task)
                        
                        # Check if both orders filled
                        if high_order.is_filled() and low_order.is_filled():
                            logger.info(
                                f"Both orders filled: "
                                f"{signal.exchange_high} @ {high_order.average_fill_price}, "
                                f"{signal.exchange_low} @ {low_order.average_fill_price}"
                            )
                            break
                        else:
                            raise ExecutionError(
                                f"Orders not fully filled: "
                                f"{signal.exchange_high}={high_order.filled_quantity}/{quantity}, "
                                f"{signal.exchange_low}={low_order.filled_quantity}/{quantity}"
                            )
                    
                    except Exception as e:
                        if attempt < max_attempts - 1:
                            logger.warning(f"Attempt {attempt + 1} failed, retrying: {e}")
                            await asyncio.sleep(1)
                        else:
                            raise ExecutionError(f"All {max_attempts} attempts failed: {e}")
                
                # Calculate actual spread
                actual_spread_pct = (high_order.average_fill_price - low_order.average_fill_price) / low_order.average_fill_price
                
                # ============================================================
                # SAFETY CHECK 2: Validate actual spread after execution
                # ============================================================
                if actual_spread_pct < self.config.cross_exchange_min_actual_spread_pct:
                    error_msg = (
                        f"Actual spread is negative or too low ({actual_spread_pct:.4%}), "
                        f"initiating rollback"
                    )
                    logger.error(error_msg)
                    
                    # Attempt rollback
                    rollback_success = await self._rollback_cross_exchange(
                        high_order=high_order,
                        low_order=low_order,
                        symbol=perp_symbol,
                        exchange_manager=exchange_manager,
                        exchange_high=signal.exchange_high,
                        exchange_low=signal.exchange_low
                    )
                    
                    if not rollback_success:
                        await logger.critical(
                            "CRITICAL: Negative spread rollback failed - MANUAL INTERVENTION REQUIRED",
                            extra={
                                "position_id": position_id,
                                "actual_spread_pct": str(actual_spread_pct),
                                "high_order": high_order.to_dict(),
                                "low_order": low_order.to_dict()
                            }
                        )
                    
                    return ExecutionResult(
                        success=False,
                        error_message=error_msg,
                        execution_time_ms=int((time.time() - start_time) * 1000)
                    )
                
                logger.info(f"✓ Actual spread validation passed: {actual_spread_pct:.4%}")
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                logger.info(
                    f"Cross-exchange position opened successfully: {position_id} "
                    f"in {execution_time_ms}ms, actual_spread={actual_spread_pct:.4%}"
                )
                
                return ExecutionResult(
                    success=True,
                    position_id=position_id,
                    spot_fill_price=low_order.average_fill_price,  # Store low price as "spot"
                    perp_fill_price=high_order.average_fill_price,  # Store high price as "perp"
                    spot_quantity=low_order.filled_quantity,
                    perp_quantity=high_order.filled_quantity,
                    actual_slippage=actual_spread_pct,
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                # Execution failed - attempt rollback
                logger.error(f"Cross-exchange execution failed, initiating rollback: {e}")
                
                rollback_success = await self._rollback_cross_exchange(
                    high_order=high_order,
                    low_order=low_order,
                    symbol=perp_symbol,
                    exchange_manager=exchange_manager,
                    exchange_high=signal.exchange_high,
                    exchange_low=signal.exchange_low
                )
                
                if not rollback_success:
                    await logger.critical(
                        "CRITICAL: Cross-exchange rollback failed - MANUAL INTERVENTION REQUIRED",
                        extra={
                            "position_id": position_id,
                            "high_order": high_order.to_dict() if high_order else None,
                            "low_order": low_order.to_dict() if low_order else None,
                            "error": str(e)
                        }
                    )
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                return ExecutionResult(
                    success=False,
                    error_message=f"Execution failed: {e}",
                    execution_time_ms=execution_time_ms
                )
                
        finally:
            await self._release_lock(lock_key)
            logger.info(f"Released execution lock: {lock_key}")
    
    async def execute_cross_exchange_close(
        self,
        position: Position,
        exchange_manager
    ) -> ExecutionResult:
        """
        Execute atomic cross-exchange position closing.
        
        Closes positions simultaneously on both exchanges.
        
        Args:
            position: Position object to close
            exchange_manager: ExchangeManager instance
            
        Returns:
            ExecutionResult with close details or error
        """
        start_time = time.time()
        lock_key = f"lock:cross_exchange:{position.symbol}"
        
        high_order: Optional[OrderResult] = None
        low_order: Optional[OrderResult] = None
        
        logger.info(
            f"Starting cross-exchange position closing: {position.position_id} "
            f"{position.exchange_high} ↔ {position.exchange_low}"
        )
        
        try:
            # Acquire distributed lock
            if not await self._acquire_lock(lock_key, self._lock_timeout):
                error_msg = f"Failed to acquire lock for {position.symbol}"
                logger.warning(error_msg)
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=error_msg,
                    execution_time_ms=int((time.time() - start_time) * 1000)
                )
            
            logger.info(f"Acquired execution lock: {lock_key}")
            
            try:
                # Get exchange clients
                high_client = exchange_manager.get_client(position.exchange_high)
                low_client = exchange_manager.get_client(position.exchange_low)
                
                # Create order managers
                high_order_manager = OrderManager(high_client, shadow_mode=self.order_manager.shadow_mode)
                low_order_manager = OrderManager(low_client, shadow_mode=self.order_manager.shadow_mode)
                
                perp_symbol = f"{position.symbol}:USDT"
                
                # Close positions simultaneously
                logger.info(
                    f"Closing positions: "
                    f"BUY {perp_symbol} on {position.exchange_high} (close short), "
                    f"SELL {perp_symbol} on {position.exchange_low} (close long)"
                )
                
                high_task = high_order_manager.place_market_order(
                    symbol=perp_symbol,
                    side="buy",  # Buy to close short
                    quantity=position.perp_quantity,
                    market_type="perpetual"
                )
                
                low_task = low_order_manager.place_market_order(
                    symbol=perp_symbol,
                    side="sell",  # Sell to close long
                    quantity=position.spot_quantity,
                    market_type="perpetual"
                )
                
                # Execute both orders in parallel
                high_order, low_order = await asyncio.gather(high_task, low_task)
                
                if not (high_order.is_filled() and low_order.is_filled()):
                    raise ExecutionError(
                        f"Close orders not fully filled: "
                        f"{position.exchange_high}={high_order.filled_quantity}/{position.perp_quantity}, "
                        f"{position.exchange_low}={low_order.filled_quantity}/{position.spot_quantity}"
                    )
                
                logger.info(
                    f"Both positions closed: "
                    f"{position.exchange_high} @ {high_order.average_fill_price}, "
                    f"{position.exchange_low} @ {low_order.average_fill_price}"
                )
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                logger.info(
                    f"Cross-exchange position closed successfully: {position.position_id} "
                    f"in {execution_time_ms}ms"
                )
                
                return ExecutionResult(
                    success=True,
                    position_id=position.position_id,
                    spot_fill_price=low_order.average_fill_price,
                    perp_fill_price=high_order.average_fill_price,
                    spot_quantity=low_order.filled_quantity,
                    perp_quantity=high_order.filled_quantity,
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                logger.error(f"Cross-exchange close failed: {e}")
                
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                return ExecutionResult(
                    success=False,
                    position_id=position.position_id,
                    error_message=f"Close failed: {e}",
                    execution_time_ms=execution_time_ms
                )
                
        finally:
            await self._release_lock(lock_key)
            logger.info(f"Released execution lock: {lock_key}")
    
    async def _rollback_cross_exchange(
        self,
        high_order: Optional[OrderResult],
        low_order: Optional[OrderResult],
        symbol: str,
        exchange_manager,
        exchange_high: str,
        exchange_low: str
    ) -> bool:
        """
        Rollback partially filled cross-exchange position.
        
        Args:
            high_order: Order result from high-price exchange
            low_order: Order result from low-price exchange
            symbol: Trading pair symbol
            exchange_manager: ExchangeManager instance
            exchange_high: High-price exchange name
            exchange_low: Low-price exchange name
            
        Returns:
            True if rollback successful
        """
        logger.warning("Initiating cross-exchange position rollback")
        
        for attempt in range(self._max_rollback_attempts):
            try:
                # Close high exchange position if filled (was short, so buy to close)
                if high_order and high_order.filled_quantity > 0:
                    logger.info(
                        f"Rolling back {exchange_high} position: "
                        f"buying {high_order.filled_quantity} {symbol}"
                    )
                    high_client = exchange_manager.get_client(exchange_high)
                    high_order_manager = OrderManager(high_client, shadow_mode=self.order_manager.shadow_mode)
                    await high_order_manager.place_market_order(
                        symbol=symbol,
                        side="buy",
                        quantity=high_order.filled_quantity,
                        market_type="perpetual"
                    )
                    logger.info(f"{exchange_high} position rolled back successfully")
                
                # Close low exchange position if filled (was long, so sell to close)
                if low_order and low_order.filled_quantity > 0:
                    logger.info(
                        f"Rolling back {exchange_low} position: "
                        f"selling {low_order.filled_quantity} {symbol}"
                    )
                    low_client = exchange_manager.get_client(exchange_low)
                    low_order_manager = OrderManager(low_client, shadow_mode=self.order_manager.shadow_mode)
                    await low_order_manager.place_market_order(
                        symbol=symbol,
                        side="sell",
                        quantity=low_order.filled_quantity,
                        market_type="perpetual"
                    )
                    logger.info(f"{exchange_low} position rolled back successfully")
                
                logger.info(f"Rollback completed successfully on attempt {attempt + 1}")
                return True
                
            except Exception as e:
                logger.error(f"Rollback attempt {attempt + 1} failed: {e}")
                
                if attempt < self._max_rollback_attempts - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying rollback in {wait_time}s...")
                    await asyncio.sleep(wait_time)
        
        logger.error(
            f"Rollback failed after {self._max_rollback_attempts} attempts - "
            "MANUAL INTERVENTION REQUIRED"
        )
        return False
