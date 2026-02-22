"""
Redis client wrapper with connection pooling and key management.
Validates: Requirements 12.1, 12.2, 12.3, 12.4, 12.5
"""

import json
from typing import Optional, List, Dict, Any
from decimal import Decimal
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


class RedisClient:
    """
    Redis client wrapper with connection pooling and key management.
    
    Key structure:
    - system:status -> "normal" | "soft_pause" | "hard_stop" | "recovery"
    - system:emergency_flag -> "true" | "false"
    - system:manual_pause -> "true" | "false"
    - equity:current -> Decimal (current total equity)
    - equity:peak -> Decimal (peak equity value)
    - equity:daily_start -> Decimal (equity at start of day)
    - positions:open -> Set[position_id]
    - positions:by_symbol:{symbol} -> Set[position_id]
    - position:{position_id}:data -> JSON (Position object)
    - position:{position_id}:last_update -> timestamp
    - exposure:total -> Decimal (total notional exposure)
    - exposure:asset_class:{asset_class} -> Decimal (exposure per asset class)
    - funding:{symbol}:current -> Decimal
    - funding:{symbol}:history -> List[JSON] (last 30 periods)
    - funding:{symbol}:last_update -> timestamp
    - funding:{symbol}:last_reconciliation -> timestamp
    - funding:anomaly:{position_id} -> "true"
    - lock:execution:{symbol} -> "locked" (TTL: 30 seconds)
    - lock:rebalance:{position_id} -> "locked" (TTL: 60 seconds)
    - rebalance:{position_id}:last_time -> timestamp
    - risk:daily_loss_pct -> Decimal
    - risk:drawdown_pct -> Decimal
    - risk:liquidation_alerts -> Set[position_id]
    - config:fee_version -> string
    - config:symbols -> List[string]
    - config:risk_limits -> JSON
    """
    
    def __init__(self, config: SystemConfig):
        """
        Initialize Redis client with connection pooling.
        
        Args:
            config: System configuration
        """
        self.config = config
        self.logger = get_logger("RedisClient")
        
        # Create connection pool (Requirement 12.4)
        self.pool = ConnectionPool(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password if config.redis_password else None,
            max_connections=config.redis_max_connections,
            decode_responses=True
        )
        
        self.client: Optional[redis.Redis] = None
        self._lua_scripts: Dict[str, Any] = {}
    
    async def connect(self) -> None:
        """Establish Redis connection."""
        try:
            self.client = redis.Redis(connection_pool=self.pool)
            await self.client.ping()
            await self.logger.info("Redis连接已建立")
            
            # Load Lua scripts
            await self._load_lua_scripts()
        except Exception as e:
            await self.logger.critical(
                "连接Redis失败",
                error=e
            )
            raise
    
    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            await self.pool.disconnect()
            await self.logger.info("Redis连接已关闭")
    
    async def _load_lua_scripts(self) -> None:
        """Load Lua scripts for atomic operations."""
        # Atomic exposure update script (Requirement 12.3)
        atomic_exposure_script = """
        local total_key = KEYS[1]
        local asset_class_key = KEYS[2]
        local delta = tonumber(ARGV[1])
        local max_total = tonumber(ARGV[2])
        local max_asset_class = tonumber(ARGV[3])
        
        local current_total = tonumber(redis.call('GET', total_key) or 0)
        local current_asset_class = tonumber(redis.call('GET', asset_class_key) or 0)
        
        local new_total = current_total + delta
        local new_asset_class = current_asset_class + delta
        
        -- Prevent negative exposure from rollback scenarios
        if new_total < 0 then
            new_total = 0
        end
        
        if new_asset_class < 0 then
            new_asset_class = 0
        end
        
        if new_total > max_total then
            return {0, "total_exposure_exceeded"}
        end
        
        if new_asset_class > max_asset_class then
            return {0, "asset_class_exposure_exceeded"}
        end
        
        redis.call('SET', total_key, new_total)
        redis.call('SET', asset_class_key, new_asset_class)
        
        return {1, "success"}
        """
        
        self._lua_scripts["atomic_update_exposure"] = await self.client.script_load(
            atomic_exposure_script
        )
        
        await self.logger.info("Lua脚本加载成功")
    
    # System state methods
    async def get_system_status(self) -> str:
        """Get current system status."""
        status = await self.client.get("system:status")
        return status or "normal"
    
    async def set_system_status(self, status: str) -> None:
        """Set system status."""
        await self.client.set("system:status", status)
        await self.logger.info(f"系统状态已设置为: {status}")
    
    async def get_emergency_flag(self) -> bool:
        """Get emergency flag status."""
        flag = await self.client.get("system:emergency_flag")
        return flag == "true"
    
    async def set_emergency_flag(self, value: bool) -> None:
        """Set emergency flag."""
        await self.client.set("system:emergency_flag", "true" if value else "false")
    
    async def get_manual_pause(self) -> bool:
        """Get manual pause status."""
        pause = await self.client.get("system:manual_pause")
        return pause == "true"
    
    async def set_manual_pause(self, value: bool) -> None:
        """Set manual pause."""
        await self.client.set("system:manual_pause", "true" if value else "false")
    
    # Equity methods
    async def get_current_equity(self) -> Optional[Decimal]:
        """Get current equity."""
        equity = await self.client.get("equity:current")
        return Decimal(equity) if equity else None
    
    async def set_current_equity(self, equity: Decimal) -> None:
        """Set current equity."""
        await self.client.set("equity:current", str(equity))
    
    async def get_peak_equity(self) -> Optional[Decimal]:
        """Get peak equity."""
        peak = await self.client.get("equity:peak")
        return Decimal(peak) if peak else None
    
    async def set_peak_equity(self, peak: Decimal) -> None:
        """Set peak equity (Requirement 5.4)."""
        await self.client.set("equity:peak", str(peak))
    
    async def get_daily_start_equity(self) -> Optional[Decimal]:
        """Get equity at start of day."""
        equity = await self.client.get("equity:daily_start")
        return Decimal(equity) if equity else None
    
    async def set_daily_start_equity(self, equity: Decimal) -> None:
        """Set equity at start of day."""
        await self.client.set("equity:daily_start", str(equity))
    
    # Total Realized PnL Tracking
    async def get_total_realized_pnl(self) -> Decimal:
        """Get cumulative realized PnL since system start."""
        val = await self.client.get("pnl:total_realized")
        return Decimal(val) if val else Decimal("0")
    
    async def add_realized_pnl(self, pnl: Decimal) -> None:
        """Add realized PnL from a closed position to the total."""
        await self.client.incrbyfloat("pnl:total_realized", float(pnl))
    
    async def reset_total_realized_pnl(self) -> None:
        """Reset total realized PnL to zero (called on system startup)."""
        await self.client.set("pnl:total_realized", "0")
    
    # Position methods
    async def add_open_position(self, position_id: str) -> None:
        """Add position to open positions set."""
        await self.client.sadd("positions:open", position_id)
    
    async def remove_open_position(self, position_id: str) -> None:
        """Remove position from open positions set."""
        await self.client.srem("positions:open", position_id)
    
    async def get_open_positions(self) -> List[str]:
        """Get all open position IDs."""
        positions = await self.client.smembers("positions:open")
        return list(positions) if positions else []
    
    async def add_position_by_symbol(self, symbol: str, position_id: str) -> None:
        """Add position to symbol index."""
        await self.client.sadd(f"positions:by_symbol:{symbol}", position_id)
    
    async def remove_position_by_symbol(self, symbol: str, position_id: str) -> None:
        """Remove position from symbol index."""
        await self.client.srem(f"positions:by_symbol:{symbol}", position_id)
    
    async def get_positions_by_symbol(self, symbol: str) -> List[str]:
        """Get position IDs for a symbol."""
        positions = await self.client.smembers(f"positions:by_symbol:{symbol}")
        return list(positions) if positions else []
    
    async def set_position_data(self, position_id: str, data: Dict[str, Any]) -> None:
        """Set position data."""
        import time
        await self.client.set(f"position:{position_id}:data", json.dumps(data))
        await self.client.set(f"position:{position_id}:last_update", str(int(time.time())))
    
    async def get_position_data(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position data."""
        data = await self.client.get(f"position:{position_id}:data")
        return json.loads(data) if data else None
    
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position data (alias for get_position_data)."""
        return await self.get_position_data(position_id)
    
    # Exposure methods
    async def atomic_update_exposure(
        self,
        delta: Decimal,
        asset_class: str,
        max_total: Decimal,
        max_asset_class: Decimal
    ) -> tuple[bool, str]:
        """
        Atomically update exposure with limit checks (Requirement 12.3).
        
        Args:
            delta: Change in exposure (positive or negative)
            asset_class: Asset class for the exposure
            max_total: Maximum total exposure allowed
            max_asset_class: Maximum asset class exposure allowed
        
        Returns:
            Tuple of (success, message)
        """
        result = await self.client.evalsha(
            self._lua_scripts["atomic_update_exposure"],
            2,  # Number of keys
            "exposure:total",
            f"exposure:asset_class:{asset_class}",
            str(delta),
            str(max_total),
            str(max_asset_class)
        )
        
        success = result[0] == 1
        message = result[1]
        
        return success, message
    
    async def get_total_exposure(self) -> Decimal:
        """Get total exposure."""
        exposure = await self.client.get("exposure:total")
        return Decimal(exposure) if exposure else Decimal("0")
    
    async def get_asset_class_exposure(self, asset_class: str) -> Decimal:
        """Get exposure for an asset class."""
        exposure = await self.client.get(f"exposure:asset_class:{asset_class}")
        return Decimal(exposure) if exposure else Decimal("0")
    
    # Lock methods (Requirements 13.1, 13.2, 13.3)
    async def acquire_lock(self, lock_key: str, ttl: int) -> bool:
        """
        Acquire a distributed lock using SETNX.
        
        Args:
            lock_key: Lock key (e.g., "lock:execution:BTC/USDT")
            ttl: Time to live in seconds
        
        Returns:
            True if lock acquired, False otherwise
        """
        acquired = await self.client.set(lock_key, "locked", nx=True, ex=ttl)
        return bool(acquired)
    
    async def release_lock(self, lock_key: str) -> None:
        """Release a distributed lock."""
        await self.client.delete(lock_key)
    
    # Funding methods
    async def set_current_funding(self, symbol: str, rate: Decimal) -> None:
        """Set current funding rate for symbol."""
        await self.client.set(f"funding:{symbol}:current", str(rate))
        await self.client.set(f"funding:{symbol}:last_update", str(int(self.client.time()[0])))
    
    async def get_current_funding(self, symbol: str) -> Optional[Decimal]:
        """Get current funding rate for symbol."""
        rate = await self.client.get(f"funding:{symbol}:current")
        return Decimal(rate) if rate else None
    
    async def add_funding_history(self, symbol: str, rate_data: Dict[str, Any]) -> None:
        """Add funding rate to history (keep last 30)."""
        key = f"funding:{symbol}:history"
        await self.client.lpush(key, json.dumps(rate_data))
        await self.client.ltrim(key, 0, 29)  # Keep only last 30
    
    async def get_funding_history(self, symbol: str, count: int = 30) -> List[Dict[str, Any]]:
        """Get funding rate history."""
        history = await self.client.lrange(f"funding:{symbol}:history", 0, count - 1)
        return [json.loads(item) for item in history] if history else []
    
    # Risk methods
    async def set_daily_loss_pct(self, loss_pct: Decimal) -> None:
        """Set daily loss percentage."""
        await self.client.set("risk:daily_loss_pct", str(loss_pct))
    
    async def get_daily_loss_pct(self) -> Decimal:
        """Get daily loss percentage."""
        loss = await self.client.get("risk:daily_loss_pct")
        return Decimal(loss) if loss else Decimal("0")
    
    async def set_drawdown_pct(self, drawdown_pct: Decimal) -> None:
        """Set drawdown percentage."""
        await self.client.set("risk:drawdown_pct", str(drawdown_pct))
    
    async def get_drawdown_pct(self) -> Decimal:
        """Get drawdown percentage."""
        drawdown = await self.client.get("risk:drawdown_pct")
        return Decimal(drawdown) if drawdown else Decimal("0")
    
    # Rebalance cooldown methods
    async def set_rebalance_time(self, position_id: str) -> None:
        """Set last rebalance time for position."""
        import time
        await self.client.set(f"rebalance:{position_id}:last_time", str(int(time.time())))
    
    async def get_rebalance_time(self, position_id: str) -> Optional[int]:
        """Get last rebalance time for position."""
        timestamp = await self.client.get(f"rebalance:{position_id}:last_time")
        return int(timestamp) if timestamp else None
    
    async def set_funding_reconciliation(self, symbol: str) -> None:
        """Set last funding reconciliation time."""
        import time
        await self.client.set(f"funding:{symbol}:last_reconciliation", str(int(time.time())))
    
    async def get_funding_reconciliation(self, symbol: str) -> Optional[int]:
        """Get last funding reconciliation time."""
        timestamp = await self.client.get(f"funding:{symbol}:last_reconciliation")
        return int(timestamp) if timestamp else None
    
    async def set_funding_anomaly(self, position_id: str) -> None:
        """Mark position as having funding anomaly."""
        await self.client.set(f"funding:anomaly:{position_id}", "true")
    
    async def has_funding_anomaly(self, position_id: str) -> bool:
        """Check if position has funding anomaly."""
        anomaly = await self.client.get(f"funding:anomaly:{position_id}")
        return anomaly == "true"
    
    async def clear_funding_anomaly(self, position_id: str) -> None:
        """Clear funding anomaly flag."""
        await self.client.delete(f"funding:anomaly:{position_id}")
    
    # Liquidation alert methods
    async def add_liquidation_alert(self, position_id: str) -> None:
        """Add position to liquidation alerts."""
        await self.client.sadd("risk:liquidation_alerts", position_id)
    
    async def remove_liquidation_alert(self, position_id: str) -> None:
        """Remove position from liquidation alerts."""
        await self.client.srem("risk:liquidation_alerts", position_id)
    
    async def get_liquidation_alerts(self) -> List[str]:
        """Get all positions with liquidation alerts."""
        alerts = await self.client.smembers("risk:liquidation_alerts")
        return list(alerts) if alerts else []
    
    # Config cache methods
    async def set_config_value(self, key: str, value: Any) -> None:
        """Set configuration value."""
        if isinstance(value, (list, dict)):
            await self.client.set(f"config:{key}", json.dumps(value))
        else:
            await self.client.set(f"config:{key}", str(value))
    
    async def get_config_value(self, key: str) -> Optional[Any]:
        """Get configuration value."""
        value = await self.client.get(f"config:{key}")
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return None
    
    async def add_to_close_queue(self, position_id: str) -> None:
        """
        Add position to close queue for processing.
        
        Args:
            position_id: Position ID to add to close queue
        """
        await self.client.sadd("close_queue", position_id)
    
    async def get_close_queue(self) -> List[str]:
        """
        Get all positions in close queue.
        
        Returns:
            List of position IDs in close queue
        """
        members = await self.client.smembers("close_queue")
        return [m.decode() if isinstance(m, bytes) else m for m in members]
    
    async def remove_from_close_queue(self, position_id: str) -> None:
        """
        Remove position from close queue.
        
        Args:
            position_id: Position ID to remove from close queue
        """
        await self.client.srem("close_queue", position_id)
    
    async def update_position(self, position_id: str, updates: Dict[str, Any]) -> None:
        """
        Update position data fields.
        
        Args:
            position_id: Position ID
            updates: Dictionary of fields to update
        """
        position_data = await self.get_position(position_id)
        if position_data:
            position_data.update(updates)
            await self.set_position_data(position_id, position_data)
    
    async def flush_all(self) -> None:
        """Flush all data (use only for testing)."""
        await self.client.flushdb()
