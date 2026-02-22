"""
Unified notification dispatcher.

Fans out notifications to all configured channels (Telegram, Feishu).
Each channel is independent — one failing won't block the others.
"""

import asyncio
from typing import Optional

from backend.config.settings import SystemConfig
from backend.services.telegram_notifier import TelegramNotifier, get_telegram_notifier
from backend.services.feishu_notifier import FeishuNotifier, get_feishu_notifier
from backend.utils.logger import get_logger


logger = get_logger(__name__)

_dispatcher: Optional["NotifyDispatcher"] = None


class NotifyDispatcher:
    """Dispatch notifications to all enabled channels."""

    # Running mode labels
    MODE_LABELS = {
        "simulation": "实盘模拟",
        "testnet": "测试网",
        "mainnet": "正式交易",
    }

    def __init__(self, config: SystemConfig):
        self.telegram = get_telegram_notifier(config)
        self.feishu = get_feishu_notifier(config)
        self.running_mode = config.running_mode
        self.mode_label = self.MODE_LABELS.get(config.running_mode, config.running_mode)
        # Propagate mode to underlying notifiers
        self.telegram.running_mode = self.running_mode
        self.telegram.mode_label = self.mode_label
        self.feishu.running_mode = self.running_mode
        self.feishu.mode_label = self.mode_label

    @property
    def enabled_channels(self) -> list[str]:
        channels = []
        if self.telegram.enabled:
            channels.append("Telegram")
        if self.feishu.enabled:
            channels.append("Feishu")
        return channels

    async def _fan_out(self, method_name: str, *args, **kwargs) -> None:
        """Call a method on all enabled notifiers concurrently."""
        tasks = []
        for notifier in (self.telegram, self.feishu):
            if notifier.enabled:
                fn = getattr(notifier, method_name, None)
                if fn:
                    tasks.append(fn(*args, **kwargs))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    await logger.error(f"通知发送失败 ({method_name}): {r}")

    async def notify_soft_pause(self) -> None:
        await self._fan_out("notify_soft_pause")

    async def notify_hard_stop(self) -> None:
        await self._fan_out("notify_hard_stop")

    async def notify_funding_anomaly(self, position_id: str, symbol: str = "") -> None:
        await self._fan_out("notify_funding_anomaly", position_id, symbol)

    async def notify_app_startup(self, environment: str, exchange: str, scout_count: int, balance: dict = None, config=None) -> None:
        channels = self.enabled_channels
        await self._fan_out("notify_app_startup", environment, exchange, scout_count, channels, balance, config)

    async def notify_app_startup_failed(self, error: str) -> None:
        await self._fan_out("notify_app_startup_failed", error)

    async def notify_app_shutdown(self) -> None:
        await self._fan_out("notify_app_shutdown")

    async def notify_emergency_close(self, reason: str, positions_count: int) -> None:
        await self._fan_out("notify_emergency_close", reason, positions_count)

    async def notify_trading_paused(self, reason: str) -> None:
        await self._fan_out("notify_trading_paused", reason)

    async def notify_trading_resumed(self, reason: str) -> None:
        await self._fan_out("notify_trading_resumed", reason)
    
    async def notify_system_recovery(self, reason: str) -> None:
        """Notify that system has auto-recovered from emergency state."""
        await self._fan_out("notify_system_recovery", reason)
    
    async def notify_position_delisted(self, position_id: str, symbol: str, error_message: str) -> None:
        """Notify that a position's trading pair has been delisted."""
        await self._fan_out("notify_position_delisted", position_id, symbol, error_message)

    async def notify_worker_opened(
        self, symbol: str, strategy_type: str, position_size: str, position_id: str = "",
        exchange_high: str = "", exchange_low: str = "", 
        price_high: float = 0, price_low: float = 0, spread_pct: float = 0,
        spot_price: float = 0, perp_price: float = 0,
    ) -> None:
        await self._fan_out(
            "notify_worker_opened", symbol, strategy_type, position_size, position_id,
            exchange_high, exchange_low, price_high, price_low, spread_pct,
            spot_price, perp_price
        )

    async def notify_worker_rejected(
        self, symbol: str, strategy_type: str, reason: str,
    ) -> None:
        await self._fan_out("notify_worker_rejected", symbol, strategy_type, reason)
    
    async def notify_position_closed(
        self, position_id: str, symbol: str, strategy_type: str, 
        pnl: float, close_reason: str = "manual",
        holding_hours: float = 0,
        exchange_high: str = "", exchange_low: str = "",
    ) -> None:
        """Notify that a position has been closed."""
        await self._fan_out(
            "notify_position_closed", position_id, symbol, strategy_type, pnl, close_reason,
            holding_hours, exchange_high, exchange_low
        )

    async def notify_hive_report(
        self, scan_duration: float, total_markets: int, valid_count: int,
        dispatched: int, opened: int, rejected: int, failed: int,
        all_positions: list = None, opened_positions: list = None,
        exchange_balances: dict = None, total_realized_pnl: float = 0,
        daily_realized_pnl: float = 0, monthly_realized_pnl: float = 0,
    ) -> None:
        await self._fan_out(
            "notify_hive_report", scan_duration, total_markets, valid_count,
            dispatched, opened, rejected, failed, all_positions or [], opened_positions or [],
            exchange_balances or {}, total_realized_pnl,
            daily_realized_pnl, monthly_realized_pnl,
        )


def get_notifier(config: SystemConfig) -> NotifyDispatcher:
    """Get or create the singleton NotifyDispatcher."""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = NotifyDispatcher(config)
    return _dispatcher
