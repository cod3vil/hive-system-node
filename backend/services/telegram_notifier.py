"""
Telegram notification service for sending alerts to operators.

Supports critical risk alerts, funding anomalies, and scout scan reports.
Sends messages asynchronously with rate limiting to avoid Telegram API limits.
"""

import asyncio
from typing import Optional
from datetime import datetime

from config.settings import SystemConfig
from utils.logger import get_logger


logger = get_logger(__name__)

# Singleton instance
_notifier: Optional["TelegramNotifier"] = None


class TelegramNotifier:
    """Async Telegram notification sender using python-telegram-bot."""

    def __init__(self, config: SystemConfig):
        self.bot_token = config.telegram_bot_token
        self.chat_id = config.telegram_chat_id
        self.enabled = bool(self.bot_token and self.chat_id)
        self.running_mode: str = ""   # Set by NotifyDispatcher
        self.mode_label: str = ""     # Set by NotifyDispatcher
        self._bot = None
        self._lock = asyncio.Lock()

    async def _get_bot(self):
        """Lazy-init the bot instance."""
        if self._bot is None:
            try:
                from telegram import Bot
                self._bot = Bot(token=self.bot_token)
            except ImportError:
                await logger.error("python-telegram-bot 未安装，Telegram 通知不可用")
                self.enabled = False
        return self._bot

    async def send(self, text: str, parse_mode: str = "HTML") -> bool:
        """
        Send a message to the configured Telegram chat.

        Args:
            text: Message text (supports HTML formatting)
            parse_mode: Telegram parse mode (HTML or Markdown)

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False

        async with self._lock:
            try:
                bot = await self._get_bot()
                if bot is None:
                    return False
                # Prepend mode line to message body
                full_text = text
                if self.mode_label:
                    full_text = f"模式: <b>{self.mode_label}</b>\n{text}"
                await bot.send_message(
                    chat_id=self.chat_id,
                    text=full_text,
                    parse_mode=parse_mode,
                )
                return True
            except Exception as e:
                await logger.error(f"Telegram 发送失败: {e}")
                return False

    # ── Convenience methods for common alert types ──

    async def notify_soft_pause(self) -> None:
        """Notify: soft pause triggered (daily loss > 3%)."""
        await self.send(
            "⚠️ <b>软暂停已激活</b>\n"
            "日亏损超过 3%，已停止开新仓。\n"
            "现有持仓保持不变。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_hard_stop(self) -> None:
        """Notify: hard stop triggered (drawdown > 6%)."""
        await self.send(
            "🚨 <b>硬停止已激活</b>\n"
            "总回撤超过 6%，正在关闭所有持仓并进入只读模式。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_funding_anomaly(self, position_id: str, symbol: str = "") -> None:
        """Notify: funding payment reconciliation anomaly."""
        symbol_info = f" ({symbol})" if symbol else ""
        await self.send(
            f"⚠️ <b>资金费异常</b>\n"
            f"持仓 <code>{position_id}</code>{symbol_info} 资金费对账超时，\n"
            f"已标记为异常并触发仓位缩减。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_app_startup(self, environment: str, exchange: str, scout_count: int, channels: list, balance: dict = None, config=None) -> None:
        """Notify: application started successfully with full config summary."""
        lines = [
            f"🟢 <b>蜂巢套利系统已启动</b>",
            f"环境: <b>{environment}</b>",
            f"交易所: <b>{exchange}</b>",
            f"侦查蜂: 🐝 x<b>{scout_count}</b>",
            f"通知渠道: {', '.join(channels) or '无'}",
        ]
        if balance:
            lines.append(f"💰 余额(USDT): 总计 <b>{balance.get('total', 0):.2f}</b> | 可用 {balance.get('free', 0):.2f} | 冻结 {balance.get('used', 0):.2f}")
            exchanges = balance.get("exchanges")
            if exchanges:
                for ex_name, ex_bal in exchanges.items():
                    if ex_bal.get("unified"):
                        # OKX unified account — single trading pool
                        lines.append(
                            f"  {ex_name.upper()}: 交易 {ex_bal.get('swap', {}).get('total', 0):.2f} | "
                            f"资金 {ex_bal.get('spot', {}).get('total', 0):.2f} | "
                            f"小计 <b>{ex_bal.get('total', 0):.2f}</b>"
                        )
                    else:
                        lines.append(
                            f"  {ex_name.upper()}: 现货 {ex_bal.get('spot', {}).get('total', 0):.2f} | "
                            f"合约 {ex_bal.get('swap', {}).get('total', 0):.2f} | "
                            f"小计 <b>{ex_bal.get('total', 0):.2f}</b>"
                        )
            else:
                spot = balance.get("spot", {})
                swap = balance.get("swap", {})
                if spot or swap:
                    lines.append(f"  现货: {spot.get('total', 0):.2f} | 合约: {swap.get('total', 0):.2f}")
        if config:
            # Strategy switches
            strategies = []
            if getattr(config, 'enable_cash_carry', False):
                strategies.append("期现套利")
            if getattr(config, 'enable_cross_exchange', False):
                strategies.append("跨市套利")
            lines.append(f"\n📐 <b>套利参数</b>")
            lines.append(f"启用策略: {', '.join(strategies) or '无'}")
            lines.append(f"杠杆: {config.default_leverage}x (上限 {config.max_leverage}x)")
            lines.append(f"年化费率门槛: {float(config.min_annualized_funding_pct)*100:.0f}%~{float(config.max_annualized_funding_pct)*100:.0f}%")
            lines.append(f"稳定性门槛: {config.min_funding_stability_score} | 拥挤度上限: {config.max_crowding_indicator}")
            lines.append(f"最大滑点: {float(config.max_slippage_pct)*100:.3f}%")
            if getattr(config, 'enable_cross_exchange', False):
                lines.append(f"跨市最小价差: {float(config.cross_exchange_min_spread_pct)*100:.2f}% | 平仓价差: {float(config.cross_exchange_close_spread_pct)*100:.2f}%")
            lines.append(f"\n🛡️ <b>风控参数</b>")
            lines.append(f"单仓上限: {float(config.max_single_position_pct)*100:.0f}% | 总敞口: {float(config.max_total_exposure_pct)*100:.0f}%")
            lines.append(f"安全储备: {float(config.min_safety_reserve_pct)*100:.0f}%")
            lines.append(f"日亏损阈值: {float(config.max_daily_loss_pct)*100:.0f}% (软暂停)")
            lines.append(f"最大回撤: {float(config.max_drawdown_pct)*100:.0f}% (硬停止)")
            lines.append(f"强平距离下限: {float(config.min_liquidation_distance_pct)*100:.0f}%")
            lines.append(f"并发工蜂: {config.max_concurrent_workers} | 扫描间隔: {config.funding_update_interval_seconds}s")
        lines.append(f"\n时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        await self.send("\n".join(lines))

    async def notify_app_startup_failed(self, error: str) -> None:
        """Notify: application startup failed."""
        await self.send(
            f"❌ <b>蜂巢套利系统启动失败</b>\n"
            f"错误: <code>{error[:200]}</code>\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_app_shutdown(self) -> None:
        """Notify: application shutting down."""
        await self.send(
            f"🔴 <b>蜂巢套利系统正在关闭</b>\n"
            f"系统正在执行优雅关闭流程。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_emergency_close(self, reason: str, positions_count: int) -> None:
        """Notify: emergency close all positions."""
        await self.send(
            f"🚨 <b>紧急平仓</b>\n"
            f"原因: {reason}\n"
            f"受影响持仓: <b>{positions_count}</b> 个\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_trading_paused(self, reason: str) -> None:
        """Notify: trading paused."""
        await self.send(
            f"⏸️ <b>交易已暂停</b>\n"
            f"原因: {reason}\n"
            f"新开仓已停止，现有持仓保持监控。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_trading_resumed(self, reason: str) -> None:
        """Notify: trading resumed."""
        await self.send(
            f"▶️ <b>交易已恢复</b>\n"
            f"原因: {reason}\n"
            f"系统恢复正常运行。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
    
    async def notify_system_recovery(self, reason: str) -> None:
        """Notify: system auto-recovered from emergency state."""
        await self.send(
            f"✅ <b>系统自动恢复</b>\n"
            f"原因: {reason}\n"
            f"系统已从紧急状态自动恢复到正常运行。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
    
    async def notify_position_delisted(self, position_id: str, symbol: str, error_message: str) -> None:
        """Notify: trading pair delisted, position cannot be closed normally."""
        await self.send(
            f"⚠️ <b>交易对下架警告</b>\n"
            f"持仓ID: <code>{position_id}</code>\n"
            f"交易对: <code>{symbol}</code>\n"
            f"错误: {error_message}\n\n"
            f"该交易对已从交易所下架，持仓已自动清理（模拟模式）或需要人工处理（实盘模式）。\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    async def notify_worker_opened(
        self, symbol: str, strategy_type: str, position_size: str, position_id: str = "",
        exchange_high: str = "", exchange_low: str = "", 
        price_high: float = 0, price_low: float = 0, spread_pct: float = 0,
        spot_price: float = 0, perp_price: float = 0,
    ) -> None:
        """Notify: worker successfully opened a position."""
        strategy_label = {"cash_carry": "期现套利", "cross_exchange": "跨市套利"}.get(strategy_type, strategy_type)
        
        msg = f"🐝 <b>工蜂开仓成功</b> [{strategy_label}]\n"
        msg += f"交易对: <code>{symbol}</code>\n"
        msg += f"仓位: <b>{position_size} USDT</b>\n"
        
        # 根据策略类型显示不同的价格信息
        if strategy_type == "cross_exchange" and exchange_high and exchange_low:
            msg += f"\n📊 <b>跨市信息</b>\n"
            msg += f"买入: {exchange_low} @ {price_low:.8f}\n"
            msg += f"卖出: {exchange_high} @ {price_high:.8f}\n"
            msg += f"价差: <b>{spread_pct:.4f}%</b>\n"
        elif strategy_type == "cash_carry" and spot_price > 0 and perp_price > 0:
            msg += f"\n📊 <b>期现信息</b>\n"
            msg += f"现货价格: {spot_price:.8f}\n"
            msg += f"合约价格: {perp_price:.8f}\n"
            if spot_price > 0:
                spread = ((perp_price - spot_price) / spot_price) * 100
                msg += f"价差: <b>{spread:.4f}%</b>\n"
        
        msg += f"\n时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self.send(msg)

    async def notify_worker_rejected(
        self, symbol: str, strategy_type: str, reason: str,
    ) -> None:
        """Notify: worker was rejected (capital or risk check)."""
        strategy_label = {"cash_carry": "期现套利", "cross_exchange": "跨市套利"}.get(strategy_type, strategy_type)
        await self.send(
            f"🚫 <b>工蜂被拒绝</b> [{strategy_label}]\n"
            f"交易对: <code>{symbol}</code>\n"
            f"原因: {reason}\n"
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )
    
    async def notify_position_closed(
        self, position_id: str, symbol: str, strategy_type: str, 
        pnl: float, close_reason: str = "manual",
        holding_hours: float = 0,
        exchange_high: str = "", exchange_low: str = "",
    ) -> None:
        """Notify: position has been closed."""
        strategy_label = {"cash_carry": "期现套利", "cross_exchange": "跨市套利"}.get(strategy_type, strategy_type)
        
        # Format PnL with emoji
        pnl_emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➖"
        pnl_sign = "+" if pnl > 0 else ""
        
        # Format close reason
        reason_labels = {
            "spread_converged": "价差收敛",
            "max_holding_time": "持仓超时",
            "manual": "手动平仓",
            "emergency": "紧急平仓",
            "risk_limit": "风控触发"
        }
        reason_text = reason_labels.get(close_reason, close_reason)
        
        msg = f"✅ <b>持仓已平仓</b> [{strategy_label}]\n"
        msg += f"交易对: <code>{symbol}</code>\n"
        
        if exchange_high and exchange_low:
            msg += f"交易所: {exchange_low} ↔ {exchange_high}\n"
        
        msg += f"盈亏: {pnl_emoji} <b>{pnl_sign}{pnl:.2f} USDT</b>\n"
        holding_minutes = holding_hours * 60
        msg += f"持有时间: <b>{holding_minutes:.0f}</b> 分钟\n"
        msg += f"平仓原因: {reason_text}\n"
        msg += f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self.send(msg)


    async def notify_hive_report(
        self, scan_duration: float, total_markets: int, valid_count: int,
        dispatched: int, opened: int, rejected: int, failed: int,
        all_positions: list = None, opened_positions: list = None,
        exchange_balances: dict = None, total_realized_pnl: float = 0,
        daily_realized_pnl: float = 0, monthly_realized_pnl: float = 0,
    ) -> None:
        """Notify: hive scan round summary report with current positions."""
        all_positions = all_positions or []
        opened_positions = opened_positions or []
        exchange_balances = exchange_balances or {}
        
        msg = (
            f"📋 <b>蜂巢扫描报告</b>\n"
            f"扫描耗时: <b>{scan_duration:.1f}s</b> | 市场: <b>{total_markets}</b> | 有效机会: <b>{valid_count}</b>\n"
            f"工蜂: 派遣 {dispatched} | ✅新开 {opened} | 🚫拒绝 {rejected} | ❌失败 {failed}\n"
        )
        
        # 添加交易所资金信息
        if exchange_balances:
            msg += f"\n💰 <b>交易所资金 (USDT)</b>:\n"
            exchanges = exchange_balances.get("exchanges", {})
            if exchanges:
                for ex_name, ex_bal in exchanges.items():
                    total = ex_bal.get("total", 0)
                    msg += f"  {ex_name.upper()}: ${total:.2f}\n"
            else:
                # Single exchange mode
                total = exchange_balances.get("total", 0)
                msg += f"  总计: ${total:.2f}\n"
        
        # 添加当前持仓信息
        if all_positions:
            # msg += f"\n📊 <b>当前持仓 ({len(all_positions)}个)</b>:\n"
            
            # 按策略类型分组
            cash_carry_positions = [p for p in all_positions if p.strategy_type == "cash_carry"]
            cross_exchange_positions = [p for p in all_positions if p.strategy_type == "cross_exchange"]
            
            if cash_carry_positions:
                msg += f"\n💰 <b>现货-合约套利 ({len(cash_carry_positions)}个)</b>:\n"
                for pos in cash_carry_positions:
                    symbol = pos.symbol
                    notional = float(pos.calculate_notional_value())
                    pnl = float(pos.unrealized_pnl)
                    pnl_pct = (pnl / notional * 100) if notional > 0 else 0
                    pnl_emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➖"
                    msg += f"  {symbol}: 持仓 ${notional:.0f} 收益 {pnl:+.2f}\n"
                # if len(cash_carry_positions) > 5:
                #     msg += f"  ... 还有 {len(cash_carry_positions) - 5} 个持仓\n"
            
            if cross_exchange_positions:
                msg += f"\n🔄 <b>跨市套利 ({len(cross_exchange_positions)}个)</b>:\n"
                for pos in cross_exchange_positions:
                    symbol = pos.symbol
                    notional = float(pos.calculate_notional_value())
                    pnl = float(pos.unrealized_pnl)
                    pnl_pct = (pnl / notional * 100) if notional > 0 else 0
                    pnl_emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➖"
                    spread = f"{float(pos.current_spread_pct or 0):.2f}%" if pos.current_spread_pct else "N/A"
                    msg += f"  {symbol}: 持仓 ${notional:.0f} | 收益 {pnl:+.2f}\n"
                # if len(cross_exchange_positions) > 5:
                #     msg += f"  ... 还有 {len(cross_exchange_positions) - 5} 个持仓\n"
            
            # 计算未实现收益
            total_unrealized_pnl = sum(float(p.unrealized_pnl) for p in all_positions)
            total_notional = sum(float(p.calculate_notional_value()) for p in all_positions)
            total_pnl_pct = (total_unrealized_pnl / total_notional * 100) if total_notional > 0 else 0
            pnl_emoji = "📈" if total_unrealized_pnl > 0 else "📉" if total_unrealized_pnl < 0 else "➖"
            msg += f"\n💵 <b>未实现收益</b>: ${total_unrealized_pnl:+.2f} ({total_pnl_pct:+.2f}%)\n"
        else:
            msg += "\n📊 <b>当前持仓</b>: 无\n"
        
        # 添加已实现收益统计
        def _pnl_emoji(v): return "📈" if v > 0 else "📉" if v < 0 else "➖"
        msg += f"\n💰 <b>已实现收益</b>:\n"
        msg += f"  今日: {_pnl_emoji(daily_realized_pnl)} ${daily_realized_pnl:+.2f}\n"
        msg += f"  本月: {_pnl_emoji(monthly_realized_pnl)} ${monthly_realized_pnl:+.2f}\n"
        msg += f"  累计: {_pnl_emoji(total_realized_pnl)} ${total_realized_pnl:+.2f}\n"
        
        # 添加新开仓信息
        if opened_positions:
            msg += f"\n🆕 <b>本轮新开仓</b>:\n"
            for pos in opened_positions:
                strategy_emoji = "💰" if pos["strategy_type"] == "cash_carry" else "🔄"
                msg += f"  {strategy_emoji} {pos['symbol']}\n"
        
        msg += f"\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        
        await self.send(msg)


def get_telegram_notifier(config: SystemConfig) -> TelegramNotifier:
    """Get or create the singleton TelegramNotifier."""
    global _notifier
    if _notifier is None:
        _notifier = TelegramNotifier(config)
    return _notifier
