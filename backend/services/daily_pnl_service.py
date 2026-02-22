"""
Daily PnL Service for calculating and persisting daily performance metrics.

This service calculates comprehensive daily PnL including realized/unrealized PnL,
funding income, fees, and performance metrics.

Requirements: 10.3, 14.6
"""

from decimal import Decimal
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime, date, timezone, timedelta

from backend.storage.postgres_client import PostgresClient
from backend.storage.redis_client import RedisClient
from backend.utils.logger import get_logger

if TYPE_CHECKING:
    from backend.services.equity_service import EquityService
    from backend.services.position_service import PositionService


class DailyPnLService:
    """
    Service for calculating and persisting daily PnL metrics.
    
    Responsibilities:
    - Calculate daily starting/ending equity
    - Calculate realized/unrealized PnL
    - Calculate funding income and fees paid
    - Calculate return percentage and drawdown
    - Persist to daily_pnl table
    """
    
    def __init__(
        self,
        db_client: PostgresClient,
        redis_client: RedisClient,
        equity_service: "EquityService",
        position_service: "PositionService"
    ):
        """
        Initialize DailyPnLService.
        
        Args:
            db_client: Supabase database client
            redis_client: Redis client for state management
            equity_service: Equity service for equity calculations
            position_service: Position service for position data
        """
        self.db = db_client
        self.redis = redis_client
        self.equity_service = equity_service
        self.position_service = position_service
        self.logger = get_logger("DailyPnLService", db_client=db_client)
    
    async def calculate_and_persist_daily_pnl(
        self,
        target_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive daily PnL and persist to database.
        
        Requirements:
        - 10.3: Calculate and store daily profit/loss summaries
        - 14.6: Calculate return percentage and drawdown
        
        Args:
            target_date: Date to calculate PnL for (defaults to today)
        
        Returns:
            Dictionary containing daily PnL metrics
        """
        if target_date is None:
            target_date = date.today()
        
        try:
            await self.logger.info(
                f"Calculating daily PnL for {target_date}"
            )
            
            # Get starting and ending equity
            starting_equity = await self._get_starting_equity(target_date)
            ending_equity = await self.equity_service.get_current_equity()
            
            # Calculate realized PnL from closed positions
            realized_pnl = await self._calculate_realized_pnl(target_date)
            
            # Calculate unrealized PnL from open positions
            unrealized_pnl = await self._calculate_unrealized_pnl()
            
            # Calculate funding income
            funding_income = await self._calculate_funding_income(target_date)
            
            # Calculate fees paid
            fees_paid = await self._calculate_fees_paid(target_date)
            
            # Calculate net PnL
            net_pnl = realized_pnl + unrealized_pnl + funding_income - fees_paid
            
            # Calculate return percentage
            return_pct = self._calculate_return_pct(net_pnl, starting_equity)
            
            # Get peak equity and calculate drawdown
            peak_equity = await self.equity_service.get_peak_equity()
            drawdown_pct = await self._calculate_drawdown_pct(ending_equity, peak_equity)
            
            # Count positions opened and closed
            num_opened, num_closed = await self._count_positions(target_date)
            
            # Create PnL record
            pnl_data = {
                "date": target_date.isoformat(),
                "starting_equity": str(starting_equity),
                "ending_equity": str(ending_equity),
                "realized_pnl": str(realized_pnl),
                "unrealized_pnl": str(unrealized_pnl),
                "funding_income": str(funding_income),
                "fees_paid": str(fees_paid),
                "net_pnl": str(net_pnl),
                "return_pct": str(return_pct),
                "peak_equity": str(peak_equity),
                "drawdown_pct": str(drawdown_pct),
                "num_positions_opened": num_opened,
                "num_positions_closed": num_closed
            }
            
            # Persist to database
            await self.db.create_daily_pnl(pnl_data)
            
            await self.logger.info(
                f"Daily PnL persisted for {target_date}",
                details={
                    "net_pnl": str(net_pnl),
                    "return_pct": f"{return_pct:.2%}",
                    "drawdown_pct": f"{drawdown_pct:.2%}"
                }
            )
            
            return pnl_data
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate daily PnL for {target_date}",
                error=e
            )
            raise
    
    async def _get_starting_equity(self, target_date: date) -> Decimal:
        """
        Get starting equity for the target date.
        
        If target date is today, use daily_start_equity from Redis.
        Otherwise, get ending_equity from previous day's record.
        
        Args:
            target_date: Date to get starting equity for
        
        Returns:
            Starting equity for the date
        """
        try:
            if target_date == date.today():
                # Use Redis daily start equity
                equity = await self.redis.get_daily_start_equity()
                if equity is not None:
                    return equity
            
            # Get previous day's ending equity
            previous_date = target_date - timedelta(days=1)
            previous_pnl = await self.db.get_daily_pnl(previous_date.isoformat())
            
            if previous_pnl:
                return Decimal(previous_pnl["ending_equity"])
            
            # If no previous record, use current equity as starting point
            return await self.equity_service.get_current_equity()
            
        except Exception as e:
            await self.logger.error(
                f"Failed to get starting equity for {target_date}",
                error=e
            )
            return Decimal("0")
    
    async def _calculate_realized_pnl(self, target_date: date) -> Decimal:
        """
        Calculate realized PnL from positions closed on target date.
        
        Args:
            target_date: Date to calculate realized PnL for
        
        Returns:
            Total realized PnL for the date
        """
        try:
            # Get all positions from database
            positions = await self.position_service.get_all_positions()
            
            realized_pnl = Decimal("0")
            
            for position in positions:
                # Check if position was closed on target date
                if position.get("status") == "closed" and position.get("exit_timestamp"):
                    exit_date = datetime.fromisoformat(
                        position["exit_timestamp"].replace("Z", "+00:00")
                    ).date()
                    
                    if exit_date == target_date:
                        pnl = Decimal(str(position.get("realized_pnl", 0)))
                        realized_pnl += pnl
            
            await self.logger.debug(
                f"Realized PnL for {target_date}: {realized_pnl}"
            )
            
            return realized_pnl
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate realized PnL for {target_date}",
                error=e
            )
            return Decimal("0")
    
    async def _calculate_unrealized_pnl(self) -> Decimal:
        """
        Calculate total unrealized PnL from all open positions.
        
        Returns:
            Total unrealized PnL
        """
        try:
            positions = await self.position_service.get_open_positions()
            
            unrealized_pnl = Decimal("0")
            
            for position in positions:
                pnl = await self.position_service.calculate_unrealized_pnl(position)
                unrealized_pnl += pnl
            
            await self.logger.debug(
                f"Unrealized PnL: {unrealized_pnl}"
            )
            
            return unrealized_pnl
            
        except Exception as e:
            await self.logger.error(
                "Failed to calculate unrealized PnL",
                error=e
            )
            return Decimal("0")
    
    async def _calculate_funding_income(self, target_date: date) -> Decimal:
        """
        Calculate total funding income received on target date.
        
        Args:
            target_date: Date to calculate funding income for
        
        Returns:
            Total funding income for the date
        """
        try:
            # Get funding logs from database
            funding_logs = await self.db.get_funding_logs()
            
            funding_income = Decimal("0")
            
            for log in funding_logs:
                # Check if funding was received on target date
                funding_date = datetime.fromisoformat(
                    log["funding_timestamp"].replace("Z", "+00:00")
                ).date()
                
                if funding_date == target_date:
                    amount = Decimal(str(log.get("funding_amount", 0)))
                    funding_income += amount
            
            await self.logger.debug(
                f"Funding income for {target_date}: {funding_income}"
            )
            
            return funding_income
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate funding income for {target_date}",
                error=e
            )
            return Decimal("0")
    
    async def _calculate_fees_paid(self, target_date: date) -> Decimal:
        """
        Calculate total fees paid on target date.
        
        Args:
            target_date: Date to calculate fees for
        
        Returns:
            Total fees paid for the date
        """
        try:
            # Get all positions from database
            positions = await self.position_service.get_all_positions()
            
            fees_paid = Decimal("0")
            
            for position in positions:
                # Check if position was opened or closed on target date
                entry_date = datetime.fromisoformat(
                    position["entry_timestamp"].replace("Z", "+00:00")
                ).date()
                
                if entry_date == target_date:
                    # Add fees from position opening
                    fees = Decimal(str(position.get("total_fees_paid", 0)))
                    fees_paid += fees
                
                # If position was closed on target date, fees are already included
                # in total_fees_paid, so no need to add separately
            
            await self.logger.debug(
                f"Fees paid for {target_date}: {fees_paid}"
            )
            
            return fees_paid
            
        except Exception as e:
            await self.logger.error(
                f"Failed to calculate fees paid for {target_date}",
                error=e
            )
            return Decimal("0")
    
    def _calculate_return_pct(
        self,
        net_pnl: Decimal,
        starting_equity: Decimal
    ) -> Decimal:
        """
        Calculate return percentage.
        
        Return % = net_pnl / starting_equity
        
        Args:
            net_pnl: Net profit/loss
            starting_equity: Starting equity
        
        Returns:
            Return percentage as decimal (e.g., 0.05 for 5%)
        """
        if starting_equity == 0:
            return Decimal("0")
        
        return net_pnl / starting_equity
    
    async def _calculate_drawdown_pct(
        self,
        ending_equity: Decimal,
        peak_equity: Decimal
    ) -> Decimal:
        """
        Calculate drawdown percentage.
        
        Drawdown % = (peak_equity - ending_equity) / peak_equity
        
        Args:
            ending_equity: Ending equity for the day
            peak_equity: Peak equity value
        
        Returns:
            Drawdown percentage as decimal (e.g., 0.05 for 5%)
        """
        if peak_equity == 0:
            return Decimal("0")
        
        drawdown = (peak_equity - ending_equity) / peak_equity
        
        # Ensure drawdown is non-negative
        if drawdown < 0:
            return Decimal("0")
        
        return drawdown
    
    async def _count_positions(self, target_date: date) -> tuple[int, int]:
        """
        Count positions opened and closed on target date.
        
        Args:
            target_date: Date to count positions for
        
        Returns:
            Tuple of (num_opened, num_closed)
        """
        try:
            positions = await self.position_service.get_all_positions()
            
            num_opened = 0
            num_closed = 0
            
            for position in positions:
                # Count opened positions
                entry_date = datetime.fromisoformat(
                    position["entry_timestamp"].replace("Z", "+00:00")
                ).date()
                
                if entry_date == target_date:
                    num_opened += 1
                
                # Count closed positions
                if position.get("status") == "closed" and position.get("exit_timestamp"):
                    exit_date = datetime.fromisoformat(
                        position["exit_timestamp"].replace("Z", "+00:00")
                    ).date()
                    
                    if exit_date == target_date:
                        num_closed += 1
            
            return num_opened, num_closed
            
        except Exception as e:
            await self.logger.error(
                f"Failed to count positions for {target_date}",
                error=e
            )
            return 0, 0
    
    async def get_daily_pnl(self, target_date: date) -> Optional[Dict[str, Any]]:
        """
        Get daily PnL record for a specific date.
        
        Args:
            target_date: Date to get PnL for
        
        Returns:
            Daily PnL record or None if not found
        """
        try:
            return await self.db.get_daily_pnl(target_date.isoformat())
        except Exception as e:
            await self.logger.error(
                f"Failed to get daily PnL for {target_date}",
                error=e
            )
            return None
    
    async def get_daily_pnl_history(
        self,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get daily PnL history for the last N days.
        
        Args:
            days: Number of days to retrieve
        
        Returns:
            List of daily PnL records
        """
        try:
            # Query database for recent daily PnL records
            # This would need a method in SupabaseClient to query with date range
            # For now, we'll return empty list as placeholder
            await self.logger.debug(
                f"Retrieving daily PnL history for last {days} days"
            )
            
            # TODO: Implement date range query in PostgresClient
            return []
            
        except Exception as e:
            await self.logger.error(
                f"Failed to get daily PnL history",
                error=e
            )
            return []
