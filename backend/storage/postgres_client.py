"""
PostgreSQL client wrapper for database operations.
Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.5
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncpg

from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


class PostgresClient:
    """
    PostgreSQL client wrapper for database operations.
    
    Tables:
    - positions: Position records with complete trade details
    - funding_logs: Funding rate payment logs
    - daily_pnl: Daily profit/loss summaries
    - system_logs: System event logs
    """
    
    def __init__(self, config: SystemConfig):
        """
        Initialize PostgreSQL client.
        
        Args:
            config: System configuration
        """
        self.config = config
        self.logger = get_logger("PostgresClient")
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self) -> None:
        """Establish PostgreSQL connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_database,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
                min_size=self.config.postgres_min_connections,
                max_size=self.config.postgres_max_connections,
                command_timeout=30
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            
            await self.logger.info("PostgreSQL连接池已建立")
        except Exception as e:
            await self.logger.critical(
                "连接PostgreSQL失败",
                error=e
            )
            raise
    
    async def disconnect(self) -> None:
        """Close PostgreSQL connection pool."""
        if self.pool:
            await self.pool.close()
            await self.logger.info("PostgreSQL连接池已关闭")
    
    # Position methods
    async def create_position(self, position_data: Dict[str, Any]) -> str:
        """
        Create a new position record (Requirement 19.1).
        
        Args:
            position_data: Position data dictionary
        
        Returns:
            Position ID
        """
        try:
            # Convert ISO datetime strings back to datetime objects for PostgreSQL
            def parse_datetime(value):
                if value is None:
                    return None
                if isinstance(value, str):
                    from datetime import datetime
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                return value
            
            # Ensure position_id is a string
            position_id_str = str(position_data.get("position_id"))
            
            async with self.pool.acquire() as conn:
                query = """
                    INSERT INTO positions (
                        position_id, symbol, spot_symbol, perp_symbol, exchange,
                        spot_entry_price, perp_entry_price, 
                        spot_quantity, perp_quantity, leverage, entry_timestamp,
                        status, current_spot_price, current_perp_price,
                        unrealized_pnl, realized_pnl, funding_collected, total_fees_paid,
                        liquidation_price, liquidation_distance,
                        asset_class, strategy_id, strategy_type,
                        exchange_high, exchange_low, 
                        entry_spread_pct, current_spread_pct, target_close_spread_pct,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30
                    ) RETURNING position_id
                """
                returned_id = await conn.fetchval(
                    query,
                    position_id_str,
                    position_data.get("symbol"),
                    position_data.get("spot_symbol"),
                    position_data.get("perp_symbol"),
                    position_data.get("exchange", "binance"),
                    position_data.get("spot_entry_price"),
                    position_data.get("perp_entry_price"),
                    position_data.get("spot_quantity"),
                    position_data.get("perp_quantity"),
                    position_data.get("leverage"),
                    parse_datetime(position_data.get("entry_timestamp")),
                    position_data.get("status"),
                    position_data.get("current_spot_price"),
                    position_data.get("current_perp_price"),
                    position_data.get("unrealized_pnl"),
                    position_data.get("realized_pnl"),
                    position_data.get("funding_collected"),
                    position_data.get("total_fees_paid"),
                    position_data.get("liquidation_price"),
                    position_data.get("liquidation_distance"),
                    position_data.get("asset_class"),
                    position_data.get("strategy_id"),
                    position_data.get("strategy_type", "cash_carry"),
                    position_data.get("exchange_high"),
                    position_data.get("exchange_low"),
                    position_data.get("entry_spread_pct"),
                    position_data.get("current_spread_pct"),
                    position_data.get("target_close_spread_pct"),
                    parse_datetime(position_data.get("created_at")),
                    parse_datetime(position_data.get("updated_at"))
                )
            
            await self.logger.info(
                "持仓已创建",
                position_id=str(returned_id),
                symbol=position_data.get("symbol"),
                spot_symbol=position_data.get("spot_symbol"),
                perp_symbol=position_data.get("perp_symbol")
            )
            
            return str(returned_id)
        except Exception as e:
            await self.logger.error(
                "创建持仓失败",
                error=e,
                symbol=position_data.get("symbol"),
                spot_symbol=position_data.get("spot_symbol"),
                perp_symbol=position_data.get("perp_symbol")
            )
            raise

    async def update_position(
        self,
        position_id: str,
        updates: Dict[str, Any]
    ) -> None:
        """
        Update position record (Requirement 19.2).
        
        Args:
            position_id: Position ID
            updates: Fields to update
        """
        try:
            if not updates:
                return
            
            # Build dynamic UPDATE query
            set_clauses = []
            values = []
            param_num = 1
            
            for key, value in updates.items():
                set_clauses.append(f"{key} = ${param_num}")
                values.append(value)
                param_num += 1
            
            values.append(position_id)
            query = f"""
                UPDATE positions
                SET {', '.join(set_clauses)}
                WHERE position_id = ${param_num}
            """
            
            async with self.pool.acquire() as conn:
                await conn.execute(query, *values)
            
            # Convert datetime objects to strings for logging
            log_updates = {}
            for k, v in updates.items():
                if isinstance(v, datetime):
                    log_updates[k] = v.isoformat()
                else:
                    log_updates[k] = v
            
            await self.logger.debug(
                "持仓已更新",
                position_id=position_id,
                details=log_updates
            )
        except Exception as e:
            await self.logger.error(
                "更新持仓失败",
                error=e,
                position_id=position_id
            )
            raise

    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """
        Get position by ID.
        
        Args:
            position_id: Position ID
        
        Returns:
            Position data or None
        """
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM positions WHERE position_id = $1"
                row = await conn.fetchrow(query, position_id)
                return dict(row) if row else None
        except Exception as e:
            await self.logger.error(
                "获取持仓失败",
                error=e,
                position_id=position_id
            )
            return None

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Get all open positions (Requirement 19.3).
        
        Returns:
            List of open position records
        """
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM positions WHERE status = $1"
                rows = await conn.fetch(query, "open")
                return [dict(row) for row in rows]
        except Exception as e:
            await self.logger.error("获取持仓失败", error=e)
            return []
    
    async def get_positions_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get positions for a specific symbol.
        
        Args:
            symbol: Trading pair symbol
        
        Returns:
            List of position records
        """
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM positions WHERE symbol = $1"
                rows = await conn.fetch(query, symbol)
                return [dict(row) for row in rows]
        except Exception as e:
            await self.logger.error(
                "按交易对获取持仓失败",
                error=e,
                symbol=symbol
            )
            return []
    
    async def get_all_positions(self) -> List[Dict[str, Any]]:
        """
        Get all positions (open and closed) from database.
        
        Returns:
            List of all position records
        """
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM positions"
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            await self.logger.error("获取所有持仓失败", error=e)
            return []
    
    # Funding log methods
    async def create_funding_log(self, funding_data: Dict[str, Any]) -> None:
        """
        Create funding log entry (Requirement 10.2).
        
        Args:
            funding_data: Funding log data
        """
        try:
            async with self.pool.acquire() as conn:
                query = """
                    INSERT INTO funding_logs (
                        position_id, symbol, funding_rate, payment_amount, timestamp
                    ) VALUES ($1, $2, $3, $4, $5)
                """
                await conn.execute(
                    query,
                    funding_data.get("position_id"),
                    funding_data.get("symbol"),
                    funding_data.get("funding_rate"),
                    funding_data.get("payment_amount"),
                    funding_data.get("timestamp")
                )
            
            await self.logger.debug(
                "资金费率日志已创建",
                position_id=funding_data.get("position_id"),
                symbol=funding_data.get("symbol")
            )
        except Exception as e:
            await self.logger.error(
                "创建资金费率日志失败",
                error=e,
                details=funding_data
            )
            raise
    
    async def get_funding_logs(
        self,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get funding logs with optional filters.
        
        Args:
            position_id: Filter by position ID
            symbol: Filter by symbol
        
        Returns:
            List of funding log records
        """
        try:
            async with self.pool.acquire() as conn:
                conditions = []
                params = []
                param_num = 1
                
                if position_id:
                    conditions.append(f"position_id = ${param_num}")
                    params.append(position_id)
                    param_num += 1
                
                if symbol:
                    conditions.append(f"symbol = ${param_num}")
                    params.append(symbol)
                    param_num += 1
                
                query = "SELECT * FROM funding_logs"
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except Exception as e:
            await self.logger.error("获取资金费率日志失败", error=e)
            return []
    
    # Daily PnL methods
    async def create_daily_pnl(self, pnl_data: Dict[str, Any]) -> None:
        """
        Create daily PnL record (Requirement 10.3).
        
        Args:
            pnl_data: Daily PnL data
        """
        try:
            async with self.pool.acquire() as conn:
                query = """
                    INSERT INTO daily_pnl (
                        date, total_pnl, realized_pnl, unrealized_pnl,
                        funding_pnl, fee_pnl, position_count, metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """
                await conn.execute(
                    query,
                    pnl_data.get("date"),
                    pnl_data.get("total_pnl"),
                    pnl_data.get("realized_pnl"),
                    pnl_data.get("unrealized_pnl"),
                    pnl_data.get("funding_pnl"),
                    pnl_data.get("fee_pnl"),
                    pnl_data.get("position_count"),
                    pnl_data.get("metadata")
                )
            
            await self.logger.info(
                "每日盈亏记录已创建",
                details=pnl_data
            )
        except Exception as e:
            await self.logger.error(
                "创建每日盈亏记录失败",
                error=e,
                details=pnl_data
            )
            raise
    
    async def get_daily_pnl(self, date: str) -> Optional[Dict[str, Any]]:
        """
        Get daily PnL for a specific date.
        
        Args:
            date: Date in YYYY-MM-DD format
        
        Returns:
            Daily PnL record or None
        """
        try:
            async with self.pool.acquire() as conn:
                query = "SELECT * FROM daily_pnl WHERE date = $1"
                row = await conn.fetchrow(query, date)
                return dict(row) if row else None
        except Exception as e:
            await self.logger.error("获取每日盈亏失败", error=e)
            return None
    
    # System log methods
    async def create_system_log(self, log_data: Dict[str, Any]) -> None:
        """
        Create system log entry (Requirement 10.4).
        
        Args:
            log_data: Log data
        """
        try:
            async with self.pool.acquire() as conn:
                query = """
                    INSERT INTO system_logs (
                        level, module, message, timestamp, metadata
                    ) VALUES ($1, $2, $3, $4, $5)
                """
                await conn.execute(
                    query,
                    log_data.get("level"),
                    log_data.get("module"),
                    log_data.get("message"),
                    log_data.get("timestamp"),
                    log_data.get("metadata")
                )
        except Exception as e:
            # Don't fail if logging fails
            print(f"Failed to create system log: {e}")
    
    async def get_system_logs(
        self,
        level: Optional[str] = None,
        module: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get system logs with optional filters.
        
        Args:
            level: Filter by log level
            module: Filter by module name
            limit: Maximum number of logs to return
        
        Returns:
            List of log records
        """
        try:
            async with self.pool.acquire() as conn:
                conditions = []
                params = []
                param_num = 1
                
                if level:
                    conditions.append(f"level = ${param_num}")
                    params.append(level)
                    param_num += 1
                
                if module:
                    conditions.append(f"module = ${param_num}")
                    params.append(module)
                    param_num += 1
                
                query = "SELECT * FROM system_logs"
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += f" ORDER BY timestamp DESC LIMIT ${param_num}"
                params.append(limit)
                
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Failed to get system logs: {e}")
            return []
