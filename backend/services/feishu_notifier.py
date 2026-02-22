"""
Feishu (飞书) webhook notification service.

Uses Feishu custom bot webhook — no SDK dependency required, just aiohttp POST.
Docs: https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot

Configuration:
  FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx
  FEISHU_WEBHOOK_SECRET= (optional, for signed verification)
"""

import asyncio
import hashlib
import hmac
import base64
import time
from typing import Optional
from datetime import datetime

import aiohttp

from backend.config.settings import SystemConfig
from backend.utils.logger import get_logger


logger = get_logger(__name__)

_notifier: Optional["FeishuNotifier"] = None


class FeishuNotifier:
    """Async Feishu webhook notification sender."""

    def __init__(self, config: SystemConfig):
        self.webhook_url = config.feishu_webhook_url
        self.secret = config.feishu_webhook_secret
        self.enabled = bool(self.webhook_url)
        self.running_mode: str = ""   # Set by NotifyDispatcher
        self.mode_label: str = ""     # Set by NotifyDispatcher
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _gen_sign(self, timestamp: str) -> str:
        """Generate HMAC-SHA256 signature for Feishu signed verification."""
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"), b"", hashlib.sha256
        ).digest()
        return base64.b64encode(hmac_code).decode("utf-8")

    async def send(self, title: str, content_lines: list[str]) -> bool:
        """
        Send a rich-text message to Feishu via webhook.

        Args:
            title: Card title
            content_lines: List of text lines (plain text, no HTML)

        Returns:
            True if sent successfully
        """
        if not self.enabled:
            return False

        async with self._lock:
            try:
                session = await self._get_session()

                # Build rich text content
                elements = []
                for line in content_lines:
                    elements.append([{"tag": "text", "text": line}])

                # Prepend mode line to content
                if self.mode_label:
                    elements.insert(0, [{"tag": "text", "text": f"模式: {self.mode_label}"}])

                payload: dict = {
                    "msg_type": "post",
                    "content": {
                        "post": {
                            "zh_cn": {
                                "title": title,
                                "content": elements,
                            }
                        }
                    },
                }

                # Signed verification (optional)
                if self.secret:
                    timestamp = str(int(time.time()))
                    payload["timestamp"] = timestamp
                    payload["sign"] = self._gen_sign(timestamp)

                async with session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    body = await resp.json()
                    if body.get("code") != 0:
                        await logger.error(f"飞书 webhook 返回错误: {body}")
                        return False
                    return True

            except Exception as e:
                await logger.error(f"飞书发送失败: {e}")
                return False

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ── Convenience methods (same interface as TelegramNotifier) ──

    async def notify_soft_pause(self) -> None:
        await self.send("⚠️ 软暂停已激活", [
            "日亏损超过 3%，已停止开新仓。",
            "现有持仓保持不变。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_hard_stop(self) -> None:
        await self.send("🚨 硬停止已激活", [
            "总回撤超过 6%，正在关闭所有持仓并进入只读模式。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_funding_anomaly(self, position_id: str, symbol: str = "") -> None:
        symbol_info = f" ({symbol})" if symbol else ""
        await self.send("⚠️ 资金费异常", [
            f"持仓 {position_id}{symbol_info} 资金费对账超时，",
            "已标记为异常并触发仓位缩减。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_app_startup(self, environment: str, exchange: str, scout_count: int, channels: list, balance: dict = None, config=None) -> None:
        lines = [
            f"环境: {environment}",
            f"交易所: {exchange}",
            f"侦查蜂: 🐝 x{scout_count}",
            f"通知渠道: {', '.join(channels) or '无'}",
        ]
        if balance:
            lines.append(f"💰 余额(USDT): 总计 {balance.get('total', 0):.2f} | 可用 {balance.get('free', 0):.2f} | 冻结 {balance.get('used', 0):.2f}")
            exchanges = balance.get("exchanges")
            if exchanges:
                for ex_name, ex_bal in exchanges.items():
                    if ex_bal.get("unified"):
                        lines.append(
                            f"  {ex_name.upper()}: 交易 {ex_bal.get('swap', {}).get('total', 0):.2f} | "
                            f"资金 {ex_bal.get('spot', {}).get('total', 0):.2f} | "
                            f"小计 {ex_bal.get('total', 0):.2f}"
                        )
                    else:
                        lines.append(
                            f"  {ex_name.upper()}: 现货 {ex_bal.get('spot', {}).get('total', 0):.2f} | "
                            f"合约 {ex_bal.get('swap', {}).get('total', 0):.2f} | "
                            f"小计 {ex_bal.get('total', 0):.2f}"
                        )
            else:
                spot = balance.get("spot", {})
                swap = balance.get("swap", {})
                if spot or swap:
                    lines.append(f"  现货: {spot.get('total', 0):.2f} | 合约: {swap.get('total', 0):.2f}")
        if config:
            strategies = []
            if getattr(config, 'enable_cash_carry', False):
                strategies.append("期现套利")
            if getattr(config, 'enable_cross_exchange', False):
                strategies.append("跨市套利")
            lines.append("")
            lines.append("📐 套利参数")
            lines.append(f"启用策略: {', '.join(strategies) or '无'}")
            lines.append(f"杠杆: {config.default_leverage}x (上限 {config.max_leverage}x)")
            lines.append(f"年化费率门槛: {float(config.min_annualized_funding_pct)*100:.0f}%~{float(config.max_annualized_funding_pct)*100:.0f}%")
            lines.append(f"稳定性门槛: {config.min_funding_stability_score} | 拥挤度上限: {config.max_crowding_indicator}")
            lines.append(f"最大滑点: {float(config.max_slippage_pct)*100:.3f}%")
            if getattr(config, 'enable_cross_exchange', False):
                lines.append(f"跨市最小价差: {float(config.cross_exchange_min_spread_pct)*100:.2f}% | 平仓价差: {float(config.cross_exchange_close_spread_pct)*100:.2f}%")
            lines.append("")
            lines.append("🛡️ 风控参数")
            lines.append(f"单仓上限: {float(config.max_single_position_pct)*100:.0f}% | 总敞口: {float(config.max_total_exposure_pct)*100:.0f}%")
            lines.append(f"安全储备: {float(config.min_safety_reserve_pct)*100:.0f}%")
            lines.append(f"日亏损阈值: {float(config.max_daily_loss_pct)*100:.0f}% (软暂停)")
            lines.append(f"最大回撤: {float(config.max_drawdown_pct)*100:.0f}% (硬停止)")
            lines.append(f"强平距离下限: {float(config.min_liquidation_distance_pct)*100:.0f}%")
            lines.append(f"并发工蜂: {config.max_concurrent_workers} | 扫描间隔: {config.funding_update_interval_seconds}s")
        lines.append(f"\n时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        await self.send("🟢 蜂巢套利系统已启动", lines)

    async def notify_app_startup_failed(self, error: str) -> None:
        await self.send("❌ 蜂巢套利系统启动失败", [
            f"错误: {error[:200]}",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_app_shutdown(self) -> None:
        await self.send("🔴 蜂巢套利系统正在关闭", [
            "系统正在执行优雅关闭流程。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_emergency_close(self, reason: str, positions_count: int) -> None:
        await self.send("🚨 紧急平仓", [
            f"原因: {reason}",
            f"受影响持仓: {positions_count} 个",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_trading_paused(self, reason: str) -> None:
        await self.send("⏸️ 交易已暂停", [
            f"原因: {reason}",
            "新开仓已停止，现有持仓保持监控。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_trading_resumed(self, reason: str) -> None:
        await self.send("▶️ 交易已恢复", [
            f"原因: {reason}",
            "系统恢复正常运行。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])
    
    async def notify_system_recovery(self, reason: str) -> None:
        """Notify: system auto-recovered from emergency state."""
        await self.send("✅ 系统自动恢复", [
            f"原因: {reason}",
            "系统已从紧急状态自动恢复到正常运行。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])
    
    async def notify_position_delisted(self, position_id: str, symbol: str, error_message: str) -> None:
        """Notify: trading pair delisted, position cannot be closed normally."""
        await self.send("⚠️ 交易对下架警告", [
            f"持仓ID: {position_id}",
            f"交易对: {symbol}",
            f"错误: {error_message}",
            "该交易对已从交易所下架，持仓已自动清理（模拟模式）或需要人工处理（实盘模式）。",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])

    async def notify_worker_opened(
        self, symbol: str, strategy_type: str, position_size: str, position_id: str = "",
        exchange_high: str = "", exchange_low: str = "", 
        price_high: float = 0, price_low: float = 0, spread_pct: float = 0,
        spot_price: float = 0, perp_price: float = 0,
    ) -> None:
        strategy_label = {"cash_carry": "期现套利", "cross_exchange": "跨市套利"}.get(strategy_type, strategy_type)
        lines = [
            f"交易对: {symbol}",
            f"仓位: {position_size} USDT",
        ]
        
        # 根据策略类型显示不同的价格信息
        if strategy_type == "cross_exchange" and exchange_high and exchange_low:
            lines.append(f"买入: {exchange_low.upper()} @ {price_low:.4f}U")
            lines.append(f"卖出: {exchange_high.upper()} @ {price_high:.4f}U")
            lines.append(f"价差: {spread_pct:.2f}%")
        elif strategy_type == "cash_carry" and spot_price > 0 and perp_price > 0:
            lines.append(f"现货价格: {spot_price:.4f}U")
            lines.append(f"合约价格: {perp_price:.4f}U")
            spread = ((perp_price - spot_price) / spot_price * 100) if spot_price > 0 else 0
            lines.append(f"价差: {spread:.2f}%")
        
        lines.append(f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        await self.send(f"🐝 工蜂开仓成功 [{strategy_label}]", lines)

    async def notify_worker_rejected(
        self, symbol: str, strategy_type: str, reason: str,
    ) -> None:
        strategy_label = {"cash_carry": "期现套利", "cross_exchange": "跨市套利"}.get(strategy_type, strategy_type)
        await self.send(f"🚫 工蜂被拒绝 [{strategy_label}]", [
            f"交易对: {symbol}",
            f"原因: {reason}",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])
    
    async def notify_position_closed(
        self, position_id: str, symbol: str, strategy_type: str, 
        pnl: float, close_reason: str = "manual",
        holding_hours: float = 0, exchange_high: str = "", exchange_low: str = "",
    ) -> None:
        """通知持仓已平仓"""
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
        
        lines = [
            f"交易对: {symbol}",
        ]
        
        # 显示交易所信息（如果是跨市套利）
        if strategy_type == "cross_exchange" and exchange_high and exchange_low:
            lines.append(f"交易所: {exchange_low.upper()} ↔ {exchange_high.upper()}")
        
        lines.extend([
            f"盈亏: {pnl_sign}{pnl:.2f}U",
            f"持有时间: {holding_hours * 60:.0f}分钟",
            f"平仓原因: {reason_text}",
            f"时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ])
        
        await self.send(f"✅ 持仓已平仓 [{strategy_label}]", lines)

    async def notify_hive_report(
        self, scan_duration: float, total_markets: int, valid_count: int,
        dispatched: int, opened: int, rejected: int, failed: int,
        all_positions: list = None, opened_positions: list = None,
        exchange_balances: dict = None, total_realized_pnl: float = 0,
        daily_realized_pnl: float = 0, monthly_realized_pnl: float = 0,
    ) -> None:
        """发送蜂巢扫描报告，包含当前持仓信息"""
        all_positions = all_positions or []
        opened_positions = opened_positions or []
        exchange_balances = exchange_balances or {}

        lines = [
            f"耗时: {scan_duration:.1f}s | 市场: {total_markets} | 机会: {valid_count}",
            f"🐝: 新开 {opened} | 拒绝 {rejected} | 失败 {failed}",
        ]

        # 添加交易所资金信息
        if exchange_balances:
            lines.append(f"\n💰 交易所资金:")
            exchanges = exchange_balances.get("exchanges", {})
            if exchanges:
                for ex_name, ex_bal in exchanges.items():
                    total = ex_bal.get("total", 0)
                    lines.append(f"  {ex_name.upper()}: {total:.2f}U")
            else:
                # Single exchange mode
                total = exchange_balances.get("total", 0)
                lines.append(f"  总计: {total:.2f}U")

        # 添加当前持仓信息
        if all_positions:
            lines.append(f"\n📊 当前持仓 ({len(all_positions)}个):")

            # 按策略类型分组
            cash_carry_positions = [p for p in all_positions if p.strategy_type == "cash_carry"]
            cross_exchange_positions = [p for p in all_positions if p.strategy_type == "cross_exchange"]

            if cash_carry_positions:
                lines.append(f"\n💰 现货-合约套利 ({len(cash_carry_positions)}个):")
                for pos in cash_carry_positions:  # 最多显示5个
                    symbol = pos.symbol
                    notional = float(pos.calculate_notional_value())
                    pnl = float(pos.unrealized_pnl)
                    pnl_pct = (pnl / notional * 100) if notional > 0 else 0
                    pnl_emoji = "+" if pnl > 0 else "-" if pnl < 0 else ""
                    lines.append(f"  {symbol}: {notional:.0f}U | 收益：{pnl_emoji}{pnl:.2f}")
                

            if cross_exchange_positions:
                lines.append(f"\n🔄 跨市套利 ({len(cross_exchange_positions)}个):")
                for pos in cross_exchange_positions:  # 最多显示5个
                    symbol = pos.symbol
                    notional = float(pos.calculate_notional_value())
                    pnl = float(pos.unrealized_pnl)
                    pnl_pct = (pnl / notional * 100) if notional > 0 else 0
                    pnl_emoji = "+" if pnl > 0 else "-" if pnl < 0 else ""
                    spread = f"{float(pos.current_spread_pct or 0):.2f}%" if pos.current_spread_pct else "N/A"
                    lines.append(f"  {symbol}: {notional:.0f}U | 收益：{pnl_emoji}{pnl:.2f}U")

            # 计算未实现收益
            total_unrealized_pnl = sum(float(p.unrealized_pnl) for p in all_positions)
            total_notional = sum(float(p.calculate_notional_value()) for p in all_positions)
            total_pnl_pct = (total_unrealized_pnl / total_notional * 100) if total_notional > 0 else 0
            pnl_emoji = "+" if total_unrealized_pnl > 0 else "-" if total_unrealized_pnl < 0 else ""
            lines.append(f"\n💵 未实现收益: {pnl_emoji}{total_unrealized_pnl:.2f}U ({total_pnl_pct:+.2f}%)")
        else:
            lines.append("\n📊 当前持仓: 无")

        # 添加已实现收益统计
        def _e(v): return "+" if v > 0 else "-" if v < 0 else ""
        lines.append(f"\n💰 已实现收益:")
        lines.append(f"  今日: {_e(daily_realized_pnl)}{daily_realized_pnl:.2f}U")
        lines.append(f"  本月: {_e(monthly_realized_pnl)}{monthly_realized_pnl:.2f}U")
        lines.append(f"  累计: {_e(total_realized_pnl)}{total_realized_pnl:.2f}U")

        # 添加新开仓信息
        if opened_positions:
            lines.append(f"\n🆕 本轮新开仓:")
            for pos in opened_positions:
                strategy_emoji = "💰" if pos["strategy_type"] == "cash_carry" else "🔄"
                lines.append(f"  {strategy_emoji} {pos['symbol']}")

        lines.append(f"\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

        await self.send("📋 蜂巢扫描报告", lines)


def get_feishu_notifier(config: SystemConfig) -> FeishuNotifier:
    """Get or create the singleton FeishuNotifier."""
    global _notifier
    if _notifier is None:
        _notifier = FeishuNotifier(config)
    return _notifier
