"""
Order Manager for placing and tracking market orders.

This module handles order placement, fill confirmation, and slippage calculation
for both spot and perpetual markets.

Requirements: 20.1, 20.2, 20.3, 20.4, 20.5, 20.6, 20.7, 20.8
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import uuid4
from services.exchange_client import ExchangeClient, OrderError
from utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class OrderResult:
    """
    Result of an order placement operation.
    
    Requirement 20.8: Log all order attempts and results
    """
    order_id: str
    symbol: str
    side: str
    quantity: Decimal
    filled_quantity: Decimal
    average_fill_price: Decimal
    status: str
    timestamp: datetime
    market_type: str = "spot"  # "spot" or "perpetual"
    
    def is_filled(self) -> bool:
        """Check if order is completely filled."""
        return self.status in ["closed", "filled"] and self.filled_quantity == self.quantity
    
    def is_partially_filled(self) -> bool:
        """Check if order is partially filled."""
        return self.filled_quantity > 0 and self.filled_quantity < self.quantity
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "filled_quantity": str(self.filled_quantity),
            "average_fill_price": str(self.average_fill_price),
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
            "market_type": self.market_type
        }


class OrderManager:
    """
    Manages order placement and tracking via exchange client.
    
    Provides methods for:
    - Placing market orders (Requirement 20.1)
    - Waiting for fill confirmation (Requirement 20.2)
    - Tracking partial fills (Requirement 20.3)
    - Retrying failed orders (Requirement 20.4)
    - Calculating actual slippage (Requirement 20.7)
    """
    
    SHADOW_SLIPPAGE = Decimal("0.0005")  # 0.05% simulated slippage

    def __init__(self, exchange_client: ExchangeClient, shadow_mode: bool = False, config=None):
        """
        Initialize Order Manager with exchange client.

        Args:
            exchange_client: Initialized ExchangeClient instance
            shadow_mode: If True, simulate orders instead of placing real ones
            config: System configuration for leverage settings
        """
        self.exchange = exchange_client
        self.shadow_mode = shadow_mode
        self.config = config
        self._fill_check_interval = 0.5  # Check order status every 500ms
        self._max_fill_wait_time = 30  # Wait up to 30 seconds for fill
        self._leverage_cache = {}  # Cache leverage settings per symbol
    
    async def place_market_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        market_type: str = "spot",
        expected_price: Optional[Decimal] = None,
        leverage: Optional[Decimal] = None
    ) -> OrderResult:
        """
        Place a market order and wait for fill confirmation.
        
        Requirements 20.1, 20.2: Place market orders and wait for fill confirmation
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDT")
            side: Order side ("buy" or "sell")
            quantity: Order quantity in base currency
            market_type: Market type ("spot" or "perpetual")
            expected_price: Expected execution price for slippage calculation
            leverage: Leverage for perpetual contracts (if None, uses config default)
            
        Returns:
            OrderResult with fill details
            
        Raises:
            OrderError: If order placement or fill fails
        """
        if self.shadow_mode:
            return await self._place_shadow_order(symbol, side, quantity, market_type, expected_price)

        logger.info(
            f"Placing {market_type} market {side} order: "
            f"{symbol} quantity={quantity}"
        )

        try:
            # Set leverage for perpetual contracts before placing order
            if market_type == "perpetual" and self.config:
                await self._ensure_leverage_set(symbol, leverage)
            
            # Prepare order parameters
            params = {}
            if market_type == "perpetual":
                params['type'] = 'future'
            
            # Place market order via exchange
            order = await self.exchange.exchange.create_market_order(
                symbol=symbol,
                side=side,
                amount=float(quantity),
                params=params
            )
            
            logger.info(f"Order placed: {order['id']} for {symbol}")
            
            # Wait for order to fill
            filled_order = await self._wait_for_fill(order['id'], symbol)
            
            # Extract fill details
            order_result = self._parse_order_result(filled_order, market_type)
            
            # Calculate and log slippage if expected price provided
            if expected_price:
                slippage = self.calculate_actual_slippage(
                    expected_price,
                    order_result.average_fill_price
                )
                logger.info(
                    f"Order filled: {order_result.order_id} at "
                    f"{order_result.average_fill_price}, slippage: {slippage:.4%}"
                )
            else:
                logger.info(
                    f"Order filled: {order_result.order_id} at "
                    f"{order_result.average_fill_price}"
                )
            
            return order_result
            
        except Exception as e:
            logger.error(
                f"Failed to place {market_type} {side} order for {symbol}: {e}",
                extra={"symbol": symbol, "side": side, "quantity": str(quantity)}
            )
            raise OrderError(f"Order placement failed: {e}")
    
    async def _place_shadow_order(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        market_type: str = "spot",
        expected_price: Optional[Decimal] = None
    ) -> OrderResult:
        """
        Simulate a market order using real market prices (shadow mode).

        Fetches the current ticker, applies simulated slippage, and returns
        a synthetic OrderResult without touching the exchange order API.
        """
        try:
            # Validate market exists before simulating order
            markets = self.exchange.exchange.markets
            if markets and symbol not in markets:
                raise OrderError(
                    f"交易对 {symbol} 在交易所中不存在，模拟订单终止"
                )

            ticker = await self.exchange.exchange.fetch_ticker(symbol)
            ask = Decimal(str(ticker.get("ask") or ticker.get("last", 0)))
            bid = Decimal(str(ticker.get("bid") or ticker.get("last", 0)))

            # Use ask for buys, bid for sells (pessimistic simulation)
            base_price = ask if side == "buy" else bid

            # Apply simulated slippage
            if side == "buy":
                fill_price = base_price * (1 + self.SHADOW_SLIPPAGE)
            else:
                fill_price = base_price * (1 - self.SHADOW_SLIPPAGE)

            order_id = f"shadow-{uuid4().hex[:12]}"

            logger.info(
                f"[SHADOW] {market_type} {side} {symbol} qty={quantity} "
                f"price={fill_price:.6f} (base={base_price:.6f}, "
                f"slippage={self.SHADOW_SLIPPAGE:.4%}) order_id={order_id}"
            )

            return OrderResult(
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                filled_quantity=quantity,
                average_fill_price=fill_price,
                status="closed",
                timestamp=datetime.utcnow(),
                market_type=market_type,
            )

        except Exception as e:
            logger.error(f"[SHADOW] Failed to simulate {market_type} {side} order for {symbol}: {e}")
            raise OrderError(f"Shadow order simulation failed: {e}")

    async def _get_dynamic_fill_timeout(self, symbol: str) -> float:
        """
        Calculate dynamic fill timeout based on 24h trading volume.

        Returns:
            Timeout in seconds: 15s for >$100M, 30s for >$10M, 60s for others
        """
        try:
            ticker = await self.exchange.exchange.fetch_ticker(symbol)
            quote_volume = ticker.get("quoteVolume", 0) or 0
            if quote_volume > 100_000_000:
                return 15.0
            elif quote_volume > 10_000_000:
                return 30.0
            else:
                return 60.0
        except Exception:
            return self._max_fill_wait_time

    async def _wait_for_fill(
        self,
        order_id: str,
        symbol: str,
        timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Wait for order to be filled.

        Requirement 20.2: Wait for order fill confirmation before returning

        Args:
            order_id: Exchange order ID
            symbol: Trading pair symbol
            timeout: Maximum wait time in seconds (defaults to dynamic calculation)

        Returns:
            Filled order details

        Raises:
            OrderError: If order doesn't fill within timeout
        """
        if timeout is None:
            timeout = await self._get_dynamic_fill_timeout(symbol)
        
        start_time = datetime.utcnow()
        
        while True:
            # Check if timeout exceeded
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout:
                raise OrderError(
                    f"Order {order_id} did not fill within {timeout}s timeout"
                )
            
            # Fetch order status
            try:
                order = await self.exchange.exchange.fetch_order(order_id, symbol)
                
                status = order.get('status', '').lower()
                filled = Decimal(str(order.get('filled', 0)))
                amount = Decimal(str(order.get('amount', 0)))
                
                # Check if order is filled
                if status in ['closed', 'filled'] and filled == amount:
                    logger.debug(f"Order {order_id} filled completely")
                    return order
                
                # Check if order is partially filled
                if filled > 0 and filled < amount:
                    logger.warning(
                        f"Order {order_id} partially filled: "
                        f"{filled}/{amount} ({filled/amount:.2%})"
                    )
                    # Continue waiting for complete fill
                
                # Check if order is cancelled or rejected
                if status in ['canceled', 'cancelled', 'rejected', 'expired']:
                    raise OrderError(f"Order {order_id} was {status}")
                
            except OrderError:
                raise
            except Exception as e:
                logger.warning(f"Error checking order status: {e}")
            
            # Wait before next check
            await asyncio.sleep(self._fill_check_interval)
    
    async def _ensure_leverage_set(self, symbol: str, leverage: Optional[Decimal] = None) -> None:
        """
        Ensure leverage is set for a perpetual contract symbol.
        Uses cache to avoid redundant API calls.
        
        Args:
            symbol: Trading pair symbol
            leverage: Desired leverage (if None, uses config default)
        """
        # Determine leverage to use
        if leverage is None:
            leverage = self.config.default_leverage if self.config else Decimal("1.0")
        
        leverage_int = int(leverage)
        
        # Check cache to avoid redundant API calls
        if self._leverage_cache.get(symbol) == leverage_int:
            logger.debug(f"Leverage already set for {symbol}: {leverage_int}x")
            return
        
        try:
            # Set leverage via exchange API
            await self.exchange.set_leverage(symbol, leverage_int)

            # Update cache
            self._leverage_cache[symbol] = leverage_int

            logger.info(f"✓ 已设置杠杆: {symbol} = {leverage_int}x")

        except Exception as e:
            logger.warning(f"设置杠杆失败 {symbol}: {e}, 正在查询当前杠杆...")
            # Verify current leverage matches desired leverage
            try:
                positions = await self.exchange.fetch_positions(symbol)
                if positions:
                    current_lev = positions[0].get("leverage")
                    if current_lev is not None and int(float(current_lev)) == leverage_int:
                        logger.info(f"杠杆已是目标值 {leverage_int}x, 继续交易")
                        self._leverage_cache[symbol] = leverage_int
                        return
                # Could not confirm leverage matches
                raise OrderError(
                    f"杠杆设置失败且无法确认当前杠杆匹配目标值 {leverage_int}x: {e}"
                )
            except OrderError:
                raise
            except Exception as verify_err:
                raise OrderError(
                    f"杠杆设置失败且验证异常: 设置错误={e}, 验证错误={verify_err}"
                )
    
    def _parse_order_result(
        self,
        order: Dict[str, Any],
        market_type: str
    ) -> OrderResult:
        """
        Parse exchange order response into OrderResult.
        
        Args:
            order: Exchange order response
            market_type: Market type ("spot" or "perpetual")
            
        Returns:
            OrderResult instance
        """
        return OrderResult(
            order_id=order['id'],
            symbol=order['symbol'],
            side=order['side'],
            quantity=Decimal(str(order['amount'])),
            filled_quantity=Decimal(str(order.get('filled', 0))),
            average_fill_price=Decimal(str(order.get('average', order.get('price', 0)))),
            status=order['status'],
            timestamp=datetime.fromtimestamp(order['timestamp'] / 1000) if order.get('timestamp') else datetime.utcnow(),
            market_type=market_type
        )
    
    async def get_order_status(
        self,
        order_id: str,
        symbol: str
    ) -> Dict[str, Any]:
        """
        Query order status from exchange.
        
        Requirement 20.2: Track order status
        
        Args:
            order_id: Exchange order ID
            symbol: Trading pair symbol
            
        Returns:
            Order status details
        """
        try:
            order = await self.exchange.exchange.fetch_order(order_id, symbol)
            logger.debug(f"Order status for {order_id}: {order.get('status')}")
            return order
            
        except Exception as e:
            logger.error(f"Failed to fetch order status for {order_id}: {e}")
            raise OrderError(f"Failed to fetch order status: {e}")
    
    def calculate_actual_slippage(
        self,
        expected_price: Decimal,
        fill_price: Decimal
    ) -> Decimal:
        """
        Calculate actual slippage from expected vs actual price.
        
        Requirement 20.7: Calculate actual slippage from expected vs actual price
        
        Slippage formula: (fill_price - expected_price) / expected_price
        - Positive slippage means worse execution (higher for buys, lower for sells)
        - Negative slippage means better execution (price improvement)
        
        Args:
            expected_price: Expected execution price
            fill_price: Actual fill price
            
        Returns:
            Slippage as decimal (e.g., 0.0005 = 0.05%)
        """
        if expected_price == 0:
            logger.warning("Expected price is zero, cannot calculate slippage")
            return Decimal("0")
        
        slippage = (fill_price - expected_price) / expected_price
        
        logger.debug(
            f"Slippage calculation: expected={expected_price}, "
            f"fill={fill_price}, slippage={slippage:.6f}"
        )
        
        return slippage
    
    async def place_order_with_retry(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        market_type: str = "spot",
        expected_price: Optional[Decimal] = None,
        max_retries: int = 3
    ) -> OrderResult:
        """
        Place order with retry logic for transient failures.
        
        Requirement 20.4: Retry failed orders according to retry policy
        
        Args:
            symbol: Trading pair symbol
            side: Order side ("buy" or "sell")
            quantity: Order quantity
            market_type: Market type ("spot" or "perpetual")
            expected_price: Expected execution price
            max_retries: Maximum retry attempts
            
        Returns:
            OrderResult with fill details
            
        Raises:
            OrderError: If all retry attempts fail
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                result = await self.place_market_order(
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    market_type=market_type,
                    expected_price=expected_price
                )
                
                if attempt > 0:
                    logger.info(f"Order succeeded on retry attempt {attempt + 1}")
                
                return result
                
            except OrderError as e:
                last_error = e
                
                # Don't retry on certain errors
                error_msg = str(e).lower()
                if any(keyword in error_msg for keyword in [
                    'insufficient', 'balance', 'invalid', 'rejected'
                ]):
                    logger.error(f"Non-retryable order error: {e}")
                    raise
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Order failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {wait_time}s: {e}"
                    )
                    await asyncio.sleep(wait_time)
        
        # All retries exhausted
        error_msg = f"Order failed after {max_retries} attempts: {last_error}"
        logger.error(error_msg)
        raise OrderError(error_msg)
    
    async def track_partial_fill(
        self,
        order_id: str,
        symbol: str,
        expected_quantity: Decimal
    ) -> Decimal:
        """
        Track remaining quantity for partially filled orders.
        
        Requirement 20.3: Handle partial fills by tracking remaining quantity
        
        Args:
            order_id: Exchange order ID
            symbol: Trading pair symbol
            expected_quantity: Expected total quantity
            
        Returns:
            Remaining unfilled quantity
        """
        try:
            order = await self.get_order_status(order_id, symbol)
            
            filled = Decimal(str(order.get('filled', 0)))
            remaining = expected_quantity - filled
            
            if remaining > 0:
                logger.warning(
                    f"Order {order_id} partially filled: "
                    f"{filled}/{expected_quantity}, remaining: {remaining}"
                )
            
            return remaining
            
        except Exception as e:
            logger.error(f"Failed to track partial fill for {order_id}: {e}")
            raise OrderError(f"Failed to track partial fill: {e}")
    
    async def cancel_order(
        self,
        order_id: str,
        symbol: str
    ) -> bool:
        """
        Cancel an open order.
        
        Args:
            order_id: Exchange order ID
            symbol: Trading pair symbol
            
        Returns:
            True if cancellation successful
        """
        try:
            await self.exchange.exchange.cancel_order(order_id, symbol)
            logger.info(f"Order cancelled: {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
