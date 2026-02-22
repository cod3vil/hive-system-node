"""
Communication Bee — WebSocket client that connects to the Hive Central Server.

Runs as an independent asyncio task. On connection:
1. Sends a register message with node identity
2. Listens for server commands and responds with collected data
3. Auto-reconnects on disconnection

Commands supported:
- ping           → pong
- get_status     → full node status snapshot
- get_balances   → exchange balances
- get_positions  → open positions details
- get_pnl        → realized/unrealized PnL
- emergency_stop → trigger hard stop
- pause_trading  → set manual pause
- resume_trading → clear manual pause
- close_position → close a specific position by ID
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import websockets

from config.settings import SystemConfig
from utils.logger import get_logger


class CommunicationBee:
    """WebSocket client: connect to central server, respond to commands."""

    def __init__(
        self,
        config: SystemConfig,
        redis_client,
        position_service,
        equity_service,
        exchange_client,
        exchange_manager=None,
        queen=None,
    ):
        self.config = config
        self.redis_client = redis_client
        self.position_service = position_service
        self.equity_service = equity_service
        self.exchange_client = exchange_client
        self.exchange_manager = exchange_manager
        self.queen = queen

        self._running = False
        self._start_time: Optional[datetime] = None
        self._ws_url = config.hive_server_url
        self.logger = get_logger("CommunicationBee")

    async def run(self):
        """Main loop: connect → register → listen → reconnect on failure."""
        self._running = True
        self._start_time = datetime.now(timezone.utc)

        await self.logger.info(f"通讯蜂启动 — 连接目标: {self._ws_url}")

        while self._running:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    # Register
                    await self._send_register(ws)
                    await self.logger.info("通讯蜂已连接中央服务器")

                    # Wait for registration acknowledgement
                    try:
                        ack_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                        ack = json.loads(ack_raw)
                        if ack.get("type") == "registered":
                            await self.logger.info(f"通讯蜂注册成功: {ack.get('node_id')}")
                    except asyncio.TimeoutError:
                        await self.logger.warning("等待注册确认超时，继续监听")

                    # Listen for commands
                    await self._listen_loop(ws)

            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    await self.logger.warning(f"通讯蜂连接断开: {e}，5秒后重连")
                    await asyncio.sleep(5)

        await self.logger.info("通讯蜂已停止")

    async def shutdown(self):
        self._running = False

    # ── Internal ──

    async def _send_register(self, ws):
        """Send the initial registration message."""
        await ws.send(json.dumps({
            "type": "register",
            "node_id": self.config.hive_node_name,
            "name": self.config.hive_node_name,
            "ip": self.config.hive_node_ip,
            "remark": self.config.hive_node_remark,
        }))

    async def _listen_loop(self, ws):
        """Listen for server commands and respond."""
        async for raw in ws:
            if not self._running:
                break
            try:
                msg = json.loads(raw)
                cmd = msg.get("cmd", "")
                request_id = msg.get("request_id", "")
                params = msg.get("params", {})
                response = await self._handle_command(cmd, request_id, params)
                await ws.send(json.dumps(response, default=str))
            except Exception as e:
                await self.logger.error(f"通讯蜂处理命令异常: {e}")

    async def _handle_command(self, cmd: str, request_id: str, params: dict) -> dict:
        """Route command to the appropriate handler."""
        try:
            match cmd:
                case "ping":
                    return {"type": "pong"}
                case "get_status":
                    data = await self._collect_full_status()
                    return {"type": "status", "request_id": request_id, "data": data}
                case "get_balances":
                    data = await self._collect_balances()
                    return {"type": "balances", "request_id": request_id, "data": data}
                case "get_positions":
                    data = await self._collect_positions()
                    return {"type": "positions", "request_id": request_id, "data": data}
                case "get_pnl":
                    data = await self._collect_pnl()
                    return {"type": "pnl", "request_id": request_id, "data": data}
                case "emergency_stop":
                    data = await self._execute_emergency_stop()
                    return {"type": "ack", "request_id": request_id, "cmd": cmd, **data}
                case "pause_trading":
                    data = await self._execute_pause()
                    return {"type": "ack", "request_id": request_id, "cmd": cmd, **data}
                case "resume_trading":
                    data = await self._execute_resume()
                    return {"type": "ack", "request_id": request_id, "cmd": cmd, **data}
                case "close_position":
                    position_id = params.get("position_id", "")
                    data = await self._execute_close_position(position_id)
                    return {"type": "ack", "request_id": request_id, "cmd": cmd, **data}
                case _:
                    return {"type": "error", "request_id": request_id, "error": f"unknown command: {cmd}"}
        except Exception as e:
            return {"type": "error", "request_id": request_id, "error": str(e)}

    # ── Data collectors ──

    async def _collect_full_status(self) -> dict:
        """Collect complete node status snapshot."""
        balances = await self._collect_balances()
        positions = await self._collect_positions()
        pnl = await self._collect_pnl()
        risk = await self._collect_risk()

        uptime = 0
        if self._start_time:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        return {
            "node_id": self.config.hive_node_name,
            "name": self.config.hive_node_name,
            "ip": self.config.hive_node_ip,
            "remark": self.config.hive_node_remark,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": await self.redis_client.get_system_status(),
            "running_mode": self.config.running_mode,
            "uptime_seconds": int(uptime),
            "exchanges": list(self.config.enabled_exchanges),
            "balances": balances,
            "positions": positions,
            "pnl": pnl,
            "risk": risk,
        }

    async def _collect_balances(self) -> dict:
        """Collect exchange balances."""
        try:
            if self.exchange_manager:
                all_bal = await self.exchange_manager.fetch_all_combined_balances()
                return {
                    "total": all_bal.get("total", 0),
                    "free": all_bal.get("free", 0),
                    "used": all_bal.get("used", 0),
                    "by_exchange": {
                        ex_name: {
                            "total": ex_bal.get("total", 0),
                            "free": ex_bal.get("free", 0),
                            "used": ex_bal.get("used", 0),
                        }
                        for ex_name, ex_bal in all_bal.get("exchanges", {}).items()
                    },
                }
            else:
                combined = await self.exchange_client.fetch_combined_balance()
                return {
                    "total": combined.get("total", 0),
                    "free": combined.get("free", 0),
                    "used": combined.get("used", 0),
                    "by_exchange": {
                        self.config.exchange_name: {
                            "total": combined.get("total", 0),
                            "free": combined.get("free", 0),
                            "used": combined.get("used", 0),
                        }
                    },
                }
        except Exception as e:
            return {"error": str(e), "total": 0}

    async def _collect_positions(self) -> dict:
        """Collect open positions summary and details."""
        try:
            position_ids = await self.redis_client.get_open_positions()
            positions = []
            strategies = {}

            total_unrealized = 0.0

            for pid in position_ids:
                pos_data = await self.redis_client.get_position_data(pid)
                if not pos_data:
                    continue

                strategy = pos_data.get("strategy_type", "cash_carry")
                strategies[strategy] = strategies.get(strategy, 0) + 1

                unrealized = float(pos_data.get("unrealized_pnl", "0"))
                total_unrealized += unrealized

                positions.append({
                    "position_id": pid,
                    "symbol": pos_data.get("symbol", ""),
                    "strategy_type": strategy,
                    "status": pos_data.get("status", "open"),
                    "unrealized_pnl": unrealized,
                    "funding_collected": float(pos_data.get("funding_collected", "0")),
                    "entry_timestamp": pos_data.get("entry_timestamp"),
                    "exchange_high": pos_data.get("exchange_high"),
                    "exchange_low": pos_data.get("exchange_low"),
                })

            return {
                "open_count": len(positions),
                "strategies": strategies,
                "total_unrealized_pnl": round(total_unrealized, 4),
                "details": positions,
            }
        except Exception as e:
            return {"error": str(e), "open_count": 0}

    async def _collect_pnl(self) -> dict:
        """Collect PnL data."""
        try:
            total_realized = float(await self.redis_client.get_total_realized_pnl())

            # Daily PnL from equity service
            daily_pnl = 0.0
            try:
                daily_pnl = float(await self.equity_service.calculate_daily_pnl())
            except Exception:
                pass

            return {
                "total_realized": round(total_realized, 4),
                "daily_realized": round(daily_pnl, 4),
            }
        except Exception as e:
            return {"error": str(e), "total_realized": 0, "daily_realized": 0}

    async def _collect_risk(self) -> dict:
        """Collect risk metrics."""
        try:
            daily_loss_pct = float(await self.redis_client.get_daily_loss_pct())
            drawdown = 0.0
            try:
                drawdown = float(await self.equity_service.calculate_drawdown())
            except Exception:
                pass

            return {
                "daily_loss_pct": round(daily_loss_pct, 6),
                "drawdown_pct": round(drawdown, 6),
                "emergency_flag": await self.redis_client.get_emergency_flag(),
                "manual_pause": await self.redis_client.get_manual_pause(),
            }
        except Exception as e:
            return {"error": str(e)}

    # ── Command executors ──

    async def _execute_emergency_stop(self) -> dict:
        """Trigger emergency hard stop."""
        try:
            await self.redis_client.set_emergency_flag(True)
            await self.redis_client.set_system_status("hard_stop")
            await self.logger.warning("收到远程紧急停运命令")
            return {"success": True, "message": "Emergency stop triggered"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _execute_pause(self) -> dict:
        """Pause trading (manual pause)."""
        try:
            await self.redis_client.set_manual_pause(True)
            await self.logger.info("收到远程暂停交易命令")
            return {"success": True, "message": "Trading paused"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _execute_resume(self) -> dict:
        """Resume trading."""
        try:
            await self.redis_client.set_manual_pause(False)
            await self.redis_client.set_emergency_flag(False)
            await self.redis_client.set_system_status("normal")
            await self.logger.info("收到远程恢复交易命令")
            return {"success": True, "message": "Trading resumed"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    async def _execute_close_position(self, position_id: str) -> dict:
        """Close a specific position by setting its status to 'closing'."""
        if not position_id:
            return {"success": False, "message": "position_id is required"}

        try:
            pos_data = await self.redis_client.get_position_data(position_id)
            if not pos_data:
                return {"success": False, "message": f"Position {position_id} not found"}

            # Set position status to "closing" — autonomous workers will pick it up
            pos_data["status"] = "closing"
            pos_data["close_reason"] = "remote_command"
            await self.redis_client.set_position_data(position_id, pos_data)

            symbol = pos_data.get("symbol", "unknown")
            await self.logger.info(f"收到远程平仓命令: {symbol} ({position_id[:8]}...)")
            return {"success": True, "message": f"Position {position_id} marked for closing", "symbol": symbol}
        except Exception as e:
            return {"success": False, "message": str(e)}
