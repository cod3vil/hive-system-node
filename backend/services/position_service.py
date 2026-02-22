"""
Position Service for managing position lifecycle and PnL calculations.

This service handles position creation, updates, closure, and PnL calculations.
It maintains consistency between Redis cache and Supabase database.

Requirements: 19.1, 19.2, 19.3, 19.4, 19.5, 19.6, 19.7, 19.8
"""

from decimal import Decimal
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime
import uuid

from core.models import Position, PositionStatus
from storage.redis_client import RedisClient
from storage.postgres_client import PostgresClient
from services.exchange_client import ExchangeClient
from utils.logger import get_logger

if TYPE_CHECKING:
    from websocket.websocket_server import ConnectionManager


class PositionService:
    """
    Service for managing position lifecycle and PnL calculations.
    
    Responsibilities:
    - Create and persist new positions (Requirement 19.1)
    - Update position state (Requirement 19.2)
    - Close positions and calculate realized PnL (Requirement 19.3)
    - Query open positions (Requirement 19.4)
    - Calculate unrealized PnL (Requirement 19.5)
    - Query positions by asset class (Requirement 19.6)
    - Maintain consistency between Redis and database (Requirements 19.7, 19.8)
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        supabase_client: PostgresClient,
        exchange_client: ExchangeClient,
        websocket_manager: Optional["ConnectionManager"] = None
    ):
        """
        Initialize PositionService.
        
        Args:
            redis_client: Redis client for caching
            supabase_client: PostgreSQL client for persistence
            exchange_client: Exchange client for price data
            websocket_manager: Optional WebSocket connection manager for broadcasts
        """
        self.redis = redis_client
        self.db = supabase_client
        self.exchange = exchange_client
        self.websocket_manager = websocket_manager
        self.logger = get_logger("PositionService")
    
    async def create_position(self, position: Position) -> str:
        """
        Create new position record and persist to both DB and Redis.
        
        Requirement 19.1: Provide interfaces for creating new position records
        
        Args:
            position: Position object to create
            
        Returns:
            Position ID
            
        Raises:
            Exception: If creation fails
        """
        try:
            # Generate position ID if not provided
            if not position.position_id:
                position.position_id = str(uuid.uuid4())
            
            # Set timestamps
            now = datetime.utcnow()
            position.created_at = now
            position.updated_at = now
            
            # Persist to database first (source of truth)
            position_data = position.to_dict()
            await self.db.create_position(position_data)
            
            # Cache in Redis
            await self.redis.set_position_data(position.position_id, position_data)
            await self.redis.add_open_position(position.position_id)
            await self.redis.add_position_by_symbol(position.symbol, position.position_id)
            
            await self.logger.info(
                f"持仓已创建: {position.position_id}",
                position_id=position.position_id,
                symbol=position.symbol,
                notional=str(position.calculate_notional_value())
            )
            
            # Broadcast position update via WebSocket
            if self.websocket_manager:
                await self.websocket_manager.broadcast_position_update(position_data)
            
            return position.position_id
            
        except Exception as e:
            await self.logger.error(
                "创建持仓失败",
                error=str(e),
                symbol=position.symbol
            )
            raise
    
    async def update_position(
        self,
        position_id: str,
        updates: Dict[str, Any]
    ) -> None:
        """
        Update position fields in both DB and Redis.
        
        Requirement 19.2: Provide interfaces for updating position status
        
        Args:
            position_id: Position ID to update
            updates: Dictionary of fields to update
            
        Raises:
            Exception: If update fails
        """
        try:
            # Add updated timestamp (datetime object for DB)
            now = datetime.utcnow()
            db_updates = updates.copy()
            db_updates["updated_at"] = now
            
            # Update database (use datetime objects)
            await self.db.update_position(position_id, db_updates)
            
            # Update Redis cache (convert datetime to ISO string for JSON storage)
            redis_updates = updates.copy()
            redis_updates["updated_at"] = now.isoformat()
            
            cached_data = await self.redis.get_position_data(position_id)
            if cached_data:
                cached_data.update(redis_updates)
                await self.redis.set_position_data(position_id, cached_data)
            
            await self.logger.debug(
                f"持仓已更新: {position_id}",
                position_id=position_id,
                updates=updates
            )
            
            # Broadcast position update via WebSocket
            if self.websocket_manager and cached_data:
                await self.websocket_manager.broadcast_position_update(cached_data)
            
        except Exception as e:
            await self.logger.error(
                "更新持仓失败",
                error=str(e),
                position_id=position_id
            )
            raise

    async def close_position(
        self,
        position_id: str,
        spot_exit_price: Decimal,
        perp_exit_price: Decimal,
        close_reason: str
    ) -> Decimal:
        """
        Close position and calculate realized PnL.
        
        Requirement 19.3: Provide interfaces for querying open positions
        Requirement 19.5: Calculate realized PnL when positions close
        
        Args:
            position_id: Position ID to close
            spot_exit_price: Spot market exit price
            perp_exit_price: Perpetual market exit price
            close_reason: Reason for closing
            
        Returns:
            Realized PnL
            
        Raises:
            Exception: If closure fails
        """
        try:
            # Get position data
            position_data = await self.redis.get_position_data(position_id)
            if not position_data:
                # Fallback to database
                position_data = await self.db.get_position(position_id)
                if not position_data:
                    raise ValueError(f"Position not found: {position_id}")
            
            position = Position.from_dict(position_data)
            
            # Calculate realized PnL
            # Spot PnL: (exit_price - entry_price) * quantity
            spot_pnl = (spot_exit_price - position.spot_entry_price) * position.spot_quantity
            
            # Perp PnL: (entry_price - exit_price) * quantity (short position)
            perp_pnl = (position.perp_entry_price - perp_exit_price) * position.perp_quantity
            
            # Total realized PnL includes spot, perp, funding collected, minus fees
            realized_pnl = spot_pnl + perp_pnl + position.funding_collected - position.total_fees_paid
            
            # Update position with exit data
            now = datetime.utcnow()
            updates = {
                "status": PositionStatus.CLOSED,
                "spot_exit_price": str(spot_exit_price),
                "perp_exit_price": str(perp_exit_price),
                "exit_timestamp": now,  # Pass datetime object, not ISO string
                "close_reason": close_reason,
                "realized_pnl": str(realized_pnl),
                "updated_at": now  # Pass datetime object, not ISO string
            }
            
            # Update database (use datetime objects)
            await self.db.update_position(position_id, updates)
            
            # Update Redis (convert datetime to ISO string for JSON storage)
            redis_updates = {
                "status": PositionStatus.CLOSED,
                "spot_exit_price": str(spot_exit_price),
                "perp_exit_price": str(perp_exit_price),
                "exit_timestamp": now.isoformat(),
                "close_reason": close_reason,
                "realized_pnl": str(realized_pnl),
                "updated_at": now.isoformat()
            }
            position_data.update(redis_updates)
            await self.redis.set_position_data(position_id, position_data)
            
            # CRITICAL: Remove from open positions - use try/except to ensure this always runs
            try:
                await self.redis.remove_open_position(position_id)
                await self.redis.remove_position_by_symbol(position.symbol, position_id)
            except Exception as redis_error:
                await self.logger.error(
                    "Redis清理失败，但持仓已在数据库中标记为closed",
                    error=redis_error,
                    position_id=position_id
                )
                # Don't raise - database is source of truth
            
            # Add realized PnL to total
            await self.redis.add_realized_pnl(realized_pnl)
            
            await self.logger.info(
                f"持仓已关闭: {position_id}",
                position_id=position_id,
                symbol=position.symbol,
                realized_pnl=str(realized_pnl),
                close_reason=close_reason
            )
            
            # Broadcast position update via WebSocket
            if self.websocket_manager:
                await self.websocket_manager.broadcast_position_update(position_data)
            
            return realized_pnl
            
        except Exception as e:
            await self.logger.error(
                "关闭持仓失败",
                error=e,
                position_id=position_id
            )
            raise
    
    async def get_open_positions(self) -> List[Position]:
        """
        Get all open positions from Redis with DB fallback.
        
        Requirement 19.4: Calculate unrealized PnL for open positions
        
        Returns:
            List of open Position objects
        """
        try:
            # Try Redis first (fast path)
            position_ids = await self.redis.get_open_positions()
            
            if position_ids:
                positions = []
                for position_id in position_ids:
                    position_data = await self.redis.get_position_data(position_id)
                    if position_data:
                        positions.append(Position.from_dict(position_data))
                
                if positions:
                    await self.logger.debug(f"从Redis获取到 {len(positions)} 个持仓")
                    return positions
            
            # Fallback to database
            await self.logger.debug("Redis缓存未命中，从数据库获取")
            position_records = await self.db.get_open_positions()
            
            positions = [Position.from_dict(record) for record in position_records]
            
            # Rebuild Redis cache
            for position in positions:
                await self.redis.set_position_data(position.position_id, position.to_dict())
                await self.redis.add_open_position(position.position_id)
                await self.redis.add_position_by_symbol(position.symbol, position.position_id)
            
            await self.logger.info(f"从数据库获取到 {len(positions)} 个持仓")
            return positions
            
        except Exception as e:
            await self.logger.error("获取持仓失败", error=str(e))
            return []
    
    async def calculate_unrealized_pnl(self, position: Position) -> Decimal:
        """
        Calculate unrealized PnL for an open position.
        
        Requirement 19.5: Calculate unrealized PnL for open positions
        
        Args:
            position: Position object
            
        Returns:
            Unrealized PnL in quote currency
        """
        try:
            # Fetch current prices
            spot_ticker = await self.exchange.fetch_ticker(position.spot_symbol)
            perp_ticker = await self.exchange.fetch_ticker(position.perp_symbol)
            
            current_spot_price = Decimal(str(spot_ticker['last']))
            current_perp_price = Decimal(str(perp_ticker['last']))
            
            # Update position with current prices
            position.current_spot_price = current_spot_price
            position.current_perp_price = current_perp_price
            
            # Calculate PnL using position method
            unrealized_pnl = position.calculate_current_pnl()
            
            # Update position unrealized PnL
            position.unrealized_pnl = unrealized_pnl
            
            await self.logger.debug(
                f"已计算未实现盈亏: {position.position_id}",
                position_id=position.position_id,
                unrealized_pnl=str(unrealized_pnl)
            )
            
            return unrealized_pnl
            
        except Exception as e:
            await self.logger.error(
                "计算未实现盈亏失败",
                error=str(e),
                position_id=position.position_id
            )
            return Decimal("0")
    
    async def get_positions_by_asset_class(self, asset_class: str) -> List[Position]:
        """
        Get positions for a specific asset class.
        
        Requirement 19.6: Query positions by asset class
        
        Args:
            asset_class: Asset class identifier (e.g., "BTC", "ETH")
            
        Returns:
            List of Position objects for the asset class
        """
        try:
            # Get all open positions
            all_positions = await self.get_open_positions()
            
            # Filter by asset class
            filtered_positions = [
                p for p in all_positions
                if p.asset_class == asset_class
            ]
            
            await self.logger.debug(
                f"获取到资产类别 {asset_class} 的 {len(filtered_positions)} 个持仓"
            )
            
            return filtered_positions
            
        except Exception as e:
            await self.logger.error(
                "按资产类别获取持仓失败",
                error=str(e),
                asset_class=asset_class
            )
            return []
    
    async def get_position(self, position_id: str) -> Optional[Position]:
        """
        Get a single position by ID.
        
        Args:
            position_id: Position ID
            
        Returns:
            Position object or None if not found
        """
        try:
            # Try Redis first
            position_data = await self.redis.get_position_data(position_id)
            
            if not position_data:
                # Fallback to database
                position_data = await self.db.get_position(position_id)
            
            if position_data:
                return Position.from_dict(position_data)
            
            return None
            
        except Exception as e:
            await self.logger.error(
                "获取持仓失败",
                error=str(e),
                position_id=position_id
            )
            return None

    async def get_all_positions(self) -> List[Dict[str, Any]]:
        """
        Get all positions (open and closed) from database.
        
        This is used for historical analysis and daily PnL calculations.
        
        Returns:
            List of position dictionaries
        """
        try:
            # Query all positions from database
            all_positions = await self.db.get_all_positions()
            
            await self.logger.debug(
                f"从数据库获取到 {len(all_positions)} 个持仓"
            )
            
            return all_positions
            
        except Exception as e:
            await self.logger.error(
                "获取所有持仓失败",
                error=e
            )
            return []
    
    async def update_position_prices(self, position: Position) -> None:
        """
        Update position with current market prices and unrealized PnL.
        
        Args:
            position: Position object to update
        """
        try:
            # Calculate unrealized PnL (this also updates current prices)
            unrealized_pnl = await self.calculate_unrealized_pnl(position)
            
            # Update in database and Redis
            updates = {
                "current_spot_price": str(position.current_spot_price),
                "current_perp_price": str(position.current_perp_price),
                "unrealized_pnl": str(unrealized_pnl)
            }
            
            await self.update_position(position.position_id, updates)
            
        except Exception as e:
            await self.logger.error(
                "更新持仓价格失败",
                error=str(e),
                position_id=position.position_id
            )
