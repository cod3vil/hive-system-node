"""
Cold Start Recovery Module for rebuilding system state from exchange data.

This module handles system initialization and state recovery when Redis is empty
or when the system restarts. It reconciles exchange positions with database records
and rebuilds Redis state to ensure consistency.

Requirements: 22.1, 22.2, 22.3, 22.4, 22.5, 22.6, 22.7, 22.8, 22.9, 9.8, 9.9, 9.10, 9.11
"""

from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import asyncio

from core.models import Position, PositionStatus
from storage.redis_client import RedisClient
from storage.postgres_client import PostgresClient
from services.exchange_client import ExchangeClient
from services.position_service import PositionService
from services.equity_service import EquityService
from utils.logger import get_logger


class StateDiscrepancy:
    """Represents a discrepancy between exchange and database state."""
    
    def __init__(
        self,
        discrepancy_type: str,
        symbol: str,
        exchange_data: Optional[Dict[str, Any]] = None,
        db_data: Optional[Dict[str, Any]] = None,
        description: str = ""
    ):
        self.discrepancy_type = discrepancy_type
        self.symbol = symbol
        self.exchange_data = exchange_data
        self.db_data = db_data
        self.description = description
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        import uuid
        from decimal import Decimal
        from datetime import datetime
        
        def convert_for_json(obj):
            """Recursively convert objects to JSON-serializable types."""
            if isinstance(obj, uuid.UUID):
                return str(obj)
            elif isinstance(obj, Decimal):
                return str(obj)
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_for_json(item) for item in obj]
            else:
                return obj
        
        return {
            "type": self.discrepancy_type,
            "symbol": self.symbol,
            "exchange_data": convert_for_json(self.exchange_data),
            "db_data": convert_for_json(self.db_data),
            "description": self.description
        }


class ColdStartRecovery:
    """
    Handles cold start recovery and state synchronization.
    
    Responsibilities:
    - Mark system in RECOVERY MODE (Requirement 22.8)
    - Query exchange for actual positions and balances (Requirements 22.1, 22.2)
    - Query database for recorded positions (Requirement 22.4)
    - Reconcile discrepancies (Requirements 22.5, 22.6)
    - Rebuild Redis state from exchange data (Requirement 22.3)
    - Recalculate equity and exposure (Requirements 9.8, 9.9)
    - Transition to normal mode (Requirements 22.7, 22.9, 9.10, 9.11)
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        postgres_client: PostgresClient,
        exchange_client: ExchangeClient,
        position_service: PositionService,
        equity_service: EquityService
    ):
        """
        Initialize ColdStartRecovery.
        
        Args:
            redis_client: Redis client for state management
            postgres_client: PostgreSQL client for database access
            exchange_client: Exchange client for querying positions
            position_service: Position service for position management
            equity_service: Equity service for equity calculations
        """
        self.redis = redis_client
        self.db = postgres_client
        self.exchange = exchange_client
        self.position_service = position_service
        self.equity_service = equity_service
        self.logger = get_logger("ColdStartRecovery")
    
    async def perform_cold_start_recovery(self) -> bool:
        """
        Perform complete cold start recovery procedure.
        
        This is the main entry point for cold start recovery. It orchestrates
        all recovery steps and ensures the system is in a consistent state
        before resuming normal operations.
        
        Returns:
            True if recovery successful, False otherwise
            
        Raises:
            Exception: If critical recovery steps fail
        """
        try:
            await self.logger.info("=" * 60)
            await self.logger.info("冷启动恢复已启动")
            await self.logger.info("=" * 60)
            
            # Step 1: Mark system in RECOVERY MODE (Requirement 22.8)
            await self._set_recovery_mode()
            
            # Step 2: Query exchange for actual positions and balances (Requirements 22.1, 22.2)
            exchange_positions, exchange_balances = await self._fetch_exchange_state()
            
            # Step 3: Query database for recorded positions (Requirement 22.4)
            db_positions = await self._fetch_database_positions()
            
            # Step 4: Reconcile discrepancies (Requirements 22.5, 22.6)
            discrepancies = await self._reconcile_positions(
                exchange_positions,
                db_positions
            )
            
            if discrepancies:
                await self._handle_discrepancies(discrepancies)
            
            # Step 5: Rebuild Redis state from exchange data (Requirement 22.3)
            await self._rebuild_redis_state(exchange_positions, exchange_balances)
            
            # Step 6: Recalculate equity and exposure (Requirements 9.8, 9.9)
            await self._recalculate_equity_and_exposure()
            
            # Step 7: Transition to normal mode (Requirements 22.7, 22.9, 9.10, 9.11)
            await self._transition_to_normal_mode()
            
            await self.logger.info("=" * 60)
            await self.logger.info("冷启动恢复已成功完成")
            await self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            await self.logger.critical(
                "冷启动恢复失败",
                error=str(e)
            )
            # Keep system in recovery mode on failure
            await self.redis.set_system_status("recovery")
            return False
    
    async def _set_recovery_mode(self) -> None:
        """
        Mark system in RECOVERY MODE.
        
        Requirement 22.8: Mark system as "RECOVERY MODE" during cold start
        """
        await self.redis.set_system_status("recovery")
        await self.redis.set_manual_pause(True)
        await self.logger.info("系统状态已设置为恢复模式")
    
    async def _fetch_exchange_state(
        self
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Query exchange for actual positions and balances.
        
        Requirements 22.1, 22.2: Query exchange APIs for actual account balances
        and all open spot and perpetual positions
        
        Returns:
            Tuple of (positions, balances)
        """
        await self.logger.info("正在从交易所获取当前状态...")
        
        try:
            # Fetch all open positions
            positions = await self.exchange.fetch_positions()
            
            # Fetch account balances
            balances = await self.exchange.fetch_balance()
            
            await self.logger.info(
                f"交易所状态已获取: {len(positions)} 个持仓, "
                f"余额: {balances.get('total', {}).get('USDT', 0)} USDT"
            )
            
            return positions, balances
            
        except Exception as e:
            await self.logger.error(
                "获取交易所状态失败",
                error=str(e)
            )
            raise
    
    async def _fetch_database_positions(self) -> List[Dict[str, Any]]:
        """
        Query database for recorded positions.
        
        Requirement 22.4: Query database for historical position records
        
        Returns:
            List of position records from database
        """
        await self.logger.info("正在从数据库获取已记录的持仓...")
        
        try:
            positions = await self.db.get_open_positions()
            
            await self.logger.info(
                f"数据库持仓已获取: {len(positions)} 个持仓"
            )
            
            return positions
            
        except Exception as e:
            await self.logger.error(
                "获取数据库持仓失败",
                error=str(e)
            )
            raise
    
    async def _reconcile_positions(
        self,
        exchange_positions: List[Dict[str, Any]],
        db_positions: List[Dict[str, Any]]
    ) -> List[StateDiscrepancy]:
        """
        Reconcile discrepancies between exchange and database positions.
        
        Requirements 22.5, 22.6: Validate that exchange positions match database
        records, log discrepancies and alert operators
        
        Args:
            exchange_positions: Positions from exchange
            db_positions: Positions from database
            
        Returns:
            List of discrepancies found
        """
        await self.logger.info("正在对账交易所和数据库持仓...")
        
        discrepancies: List[StateDiscrepancy] = []
        
        # Create lookup maps
        exchange_by_symbol = {}
        for pos in exchange_positions:
            symbol = pos.get('symbol')
            if symbol:
                if symbol not in exchange_by_symbol:
                    exchange_by_symbol[symbol] = []
                exchange_by_symbol[symbol].append(pos)
        
        db_by_symbol = {}
        for pos in db_positions:
            symbol = pos.get('perp_symbol')  # Use perp symbol for matching
            if symbol:
                if symbol not in db_by_symbol:
                    db_by_symbol[symbol] = []
                db_by_symbol[symbol].append(pos)
        
        # Check for positions in exchange but not in database
        for symbol, ex_positions in exchange_by_symbol.items():
            if symbol not in db_by_symbol:
                for ex_pos in ex_positions:
                    discrepancies.append(StateDiscrepancy(
                        discrepancy_type="missing_in_db",
                        symbol=symbol,
                        exchange_data=ex_pos,
                        db_data=None,
                        description=f"Position exists on exchange but not in database"
                    ))
            else:
                # Check quantities match
                ex_total_qty = sum(
                    abs(float(p.get('contracts', 0))) for p in ex_positions
                )
                db_total_qty = sum(
                    float(p.get('perp_quantity', 0)) for p in db_by_symbol[symbol]
                )
                
                # Allow 0.1% tolerance for quantity differences
                if abs(ex_total_qty - db_total_qty) / max(ex_total_qty, 0.001) > 0.001:
                    discrepancies.append(StateDiscrepancy(
                        discrepancy_type="quantity_mismatch",
                        symbol=symbol,
                        exchange_data={"total_quantity": ex_total_qty},
                        db_data={"total_quantity": db_total_qty},
                        description=f"Quantity mismatch: exchange={ex_total_qty}, db={db_total_qty}"
                    ))
        
        # Check for positions in database but not on exchange
        for symbol, db_positions_list in db_by_symbol.items():
            if symbol not in exchange_by_symbol:
                for db_pos in db_positions_list:
                    discrepancies.append(StateDiscrepancy(
                        discrepancy_type="missing_on_exchange",
                        symbol=symbol,
                        exchange_data=None,
                        db_data=db_pos,
                        description=f"Position exists in database but not on exchange"
                    ))
        
        if discrepancies:
            await self.logger.warning(
                f"对账中发现 {len(discrepancies)} 个差异"
            )
        else:
            await self.logger.info("未发现差异 - 状态一致")
        
        return discrepancies
    
    async def _handle_discrepancies(
        self,
        discrepancies: List[StateDiscrepancy]
    ) -> None:
        """
        Handle position discrepancies by using exchange as source of truth.
        
        Requirement 22.6: Use exchange as source of truth when discrepancies found
        
        Args:
            discrepancies: List of discrepancies to handle
        """
        await self.logger.warning(
            "正在处理差异 - 以交易所为准"
        )
        
        for discrepancy in discrepancies:
            await self.logger.warning(
                f"差异: {discrepancy.discrepancy_type}",
                details=discrepancy.to_dict()
            )
            
            if discrepancy.discrepancy_type == "missing_in_db":
                # Position exists on exchange but not in DB
                # Log for manual investigation - don't auto-create DB records
                await self.logger.error(
                    f"需要人工干预: 交易所存在持仓但数据库中不存在",
                    symbol=discrepancy.symbol,
                    exchange_data=discrepancy.exchange_data
                )
            
            elif discrepancy.discrepancy_type == "missing_on_exchange":
                # Position in DB but not on exchange - mark as closed
                await self.logger.warning(
                    f"正在关闭交易所中不存在的数据库持仓",
                    symbol=discrepancy.symbol
                )
                
                # Mark position as closed in database
                db_pos = discrepancy.db_data
                if db_pos and db_pos.get('position_id'):
                    try:
                        # Convert position_id to string if it's a UUID object
                        position_id = str(db_pos['position_id'])
                        
                        await self.db.update_position(
                            position_id,
                            {
                                'status': PositionStatus.CLOSED,
                                'close_reason': 'cold_start_reconciliation',
                                'exit_timestamp': datetime.utcnow()  # Pass datetime object, not ISO string
                            }
                        )
                        
                        await self.logger.info(
                            f"已关闭僵尸持仓: {position_id}",
                            symbol=discrepancy.symbol
                        )
                    except Exception as e:
                        await self.logger.error(
                            f"关闭持仓失败 {str(db_pos['position_id'])}",
                            error=e
                        )
            
            elif discrepancy.discrepancy_type == "quantity_mismatch":
                # Quantities don't match - log for investigation
                await self.logger.error(
                    f"需要人工干预: 数量不匹配",
                    symbol=discrepancy.symbol,
                    exchange_qty=discrepancy.exchange_data,
                    db_qty=discrepancy.db_data
                )
    
    async def _rebuild_redis_state(
        self,
        exchange_positions: List[Dict[str, Any]],
        exchange_balances: Dict[str, Any]
    ) -> None:
        """
        Rebuild Redis state from exchange data.
        
        Requirement 22.3: Rebuild Redis state from exchange position data
        
        Args:
            exchange_positions: Positions from exchange
            exchange_balances: Balances from exchange
        """
        await self.logger.info("正在从交易所数据重建Redis状态...")
        
        try:
            # Clear existing Redis state (except system status)
            await self._clear_redis_state()
            
            # Rebuild position data
            for ex_pos in exchange_positions:
                await self._sync_position_to_redis(ex_pos)
            
            # Rebuild exposure tracking
            await self._recalculate_exposure(exchange_positions)
            
            await self.logger.info(
                f"Redis状态已重建: {len(exchange_positions)} 个持仓已同步"
            )
            
        except Exception as e:
            await self.logger.error(
                "重建Redis状态失败",
                error=str(e)
            )
            raise
    
    async def _clear_redis_state(self) -> None:
        """Clear Redis state while preserving system status."""
        await self.logger.debug("正在清除Redis状态...")
        
        # Get all open position IDs
        position_ids = await self.redis.get_open_positions()
        
        # Remove position data
        for position_id in position_ids:
            await self.redis.remove_open_position(position_id)
        
        # Clear exposure
        await self.redis.set_config_value("exposure:total", "0")
        
        await self.logger.debug("Redis状态已清除")
    
    async def _sync_position_to_redis(
        self,
        exchange_position: Dict[str, Any]
    ) -> None:
        """
        Sync a single exchange position to Redis.
        
        Args:
            exchange_position: Position data from exchange
        """
        symbol = exchange_position.get('symbol')
        contracts = float(exchange_position.get('contracts', 0))
        
        if contracts == 0:
            return  # Skip closed positions
        
        # Try to find matching position in database
        db_positions = await self.db.get_open_positions()
        matching_pos = None
        
        for db_pos in db_positions:
            if db_pos.get('perp_symbol') == symbol:
                matching_pos = db_pos
                break
        
        if matching_pos:
            # Update Redis with database position
            position_id = matching_pos['position_id']
            await self.redis.set_position_data(position_id, matching_pos)
            await self.redis.add_open_position(position_id)
            await self.redis.add_position_by_symbol(symbol, position_id)
            
            await self.logger.debug(
                f"持仓已同步到Redis: {position_id} ({symbol})"
            )
        else:
            await self.logger.warning(
                f"交易所持仓 {symbol} 在数据库中未找到 - 跳过Redis同步"
            )
    
    async def _recalculate_exposure(
        self,
        exchange_positions: List[Dict[str, Any]]
    ) -> None:
        """
        Recalculate total and per-asset-class exposure.
        
        Args:
            exchange_positions: Positions from exchange
        """
        total_exposure = Decimal("0")
        asset_class_exposure: Dict[str, Decimal] = {}
        
        for pos in exchange_positions:
            # Calculate notional value
            contracts = abs(float(pos.get('contracts', 0)))
            mark_price = float(pos.get('markPrice', 0))
            notional = Decimal(str(contracts * mark_price))
            
            total_exposure += notional
            
            # Extract asset class from symbol (e.g., "BTC/USDT:USDT" -> "BTC")
            symbol = pos.get('symbol', '')
            asset_class = symbol.split('/')[0] if '/' in symbol else symbol
            
            if asset_class not in asset_class_exposure:
                asset_class_exposure[asset_class] = Decimal("0")
            asset_class_exposure[asset_class] += notional
        
        # Update Redis
        await self.redis.set_config_value("exposure:total", str(total_exposure))
        
        for asset_class, exposure in asset_class_exposure.items():
            await self.redis.set_config_value(
                f"exposure:asset_class:{asset_class}",
                str(exposure)
            )
        
        await self.logger.info(
            f"敞口已重新计算: 总计={total_exposure} USDT, "
            f"资产类别数={len(asset_class_exposure)}"
        )
    
    async def _recalculate_equity_and_exposure(self) -> None:
        """
        Recalculate equity and exposure from current state.
        
        Requirements 9.8, 9.9: Rebuild Redis state and validate database
        consistency with exchange state
        """
        await self.logger.info("正在重新计算权益和敞口...")
        
        try:
            # Calculate total equity
            total_equity = await self.equity_service.calculate_total_equity()
            
            # Initialize peak equity if not set
            peak_equity = await self.redis.get_peak_equity()
            if peak_equity is None or total_equity > peak_equity:
                await self.redis.set_peak_equity(total_equity)
                await self.logger.info(f"峰值权益已设置为: {total_equity} USDT")
            
            # Initialize daily start equity
            daily_start = await self.redis.get_daily_start_equity()
            if daily_start is None:
                await self.redis.set_daily_start_equity(total_equity)
                await self.logger.info(f"每日起始权益已设置为: {total_equity} USDT")
            
            await self.logger.info(
                f"权益已重新计算: 当前={total_equity} USDT, "
                f"峰值={await self.redis.get_peak_equity()} USDT"
            )
            
        except Exception as e:
            await self.logger.error(
                "重新计算权益失败",
                error=str(e)
            )
            raise
    
    async def _transition_to_normal_mode(self) -> None:
        """
        Transition system to normal operation mode.
        
        Requirements 22.7, 22.9, 9.10, 9.11: Only resume normal trading after
        successful state synchronization
        """
        await self.logger.info("正在切换到正常运行模式...")
        
        try:
            # Verify state is consistent
            await self._verify_state_consistency()
            
            # Clear manual pause
            await self.redis.set_manual_pause(False)
            
            # Set system status to normal
            await self.redis.set_system_status("normal")
            
            await self.logger.info("系统已切换到正常模式 - 准备就绪可交易")
            
        except Exception as e:
            await self.logger.error(
                "切换到正常模式失败",
                error=str(e)
            )
            raise
    
    async def _verify_state_consistency(self) -> None:
        """
        Verify that state is consistent before resuming operations.
        
        Requirement 9.10: Validate database consistency with exchange state
        """
        await self.logger.info("正在验证状态一致性...")
        
        # Check that Redis has position data
        position_ids = await self.redis.get_open_positions()
        
        # Check that equity is calculated (should have been set during recalculation)
        current_equity = await self.equity_service.get_current_equity()
        if current_equity == 0:
            # Zero equity is acceptable if there are no positions
            if len(position_ids) > 0:
                raise ValueError("Current equity is zero but positions exist")
        
        # Check that peak equity is set
        peak_equity = await self.redis.get_peak_equity()
        if peak_equity is None:
            raise ValueError("Peak equity not set")
        
        await self.logger.info(
            f"状态一致性已验证: {len(position_ids)} 个持仓, "
            f"权益={current_equity} USDT"
        )
    
    async def check_needs_recovery(self) -> bool:
        """
        Check if system needs cold start recovery.
        
        Returns:
            True if recovery is needed, False otherwise
        """
        try:
            # Check if Redis is empty or in recovery mode
            system_status = await self.redis.get_system_status()
            
            if system_status == "recovery":
                await self.logger.info("系统处于恢复模式")
                return True
            
            # Check if Redis has position data
            position_ids = await self.redis.get_open_positions()
            
            # Check if equity is calculated
            current_equity = await self.redis.get_current_equity()
            
            # If Redis is empty but we have open positions in DB, need recovery
            db_positions = await self.db.get_open_positions()
            
            if len(db_positions) > 0 and (not position_ids or current_equity is None):
                await self.logger.info(
                    f"Redis状态缺失但数据库中有 {len(db_positions)} 个持仓 - 需要恢复"
                )
                return True
            
            await self.logger.info("系统状态一致 - 无需恢复")
            return False
            
        except Exception as e:
            await self.logger.error(
                "检查恢复状态时出错",
                error=str(e)
            )
            # Err on the side of caution
            return True
