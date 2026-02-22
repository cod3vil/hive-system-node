"""
Configuration management system with environment variable loading.
Validates: Requirements 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7
"""

from decimal import Decimal
from typing import List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SystemConfig(BaseSettings):
    """System configuration loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        # 配置文件加载优先级（从右到左，右边优先级更高）：
        # 1. 环境变量（最高优先级）
        # 2. .env（如果存在）
        # 3. .env.sim（默认回退）
        # 
        # 使用方式：
        # - 开发/测试：使用 .env.sim（模拟模式）
        # - 实盘：创建 .env 文件（会覆盖 .env.sim）
        env_file=[".env.sim", ".env"],  # 使用列表更清晰
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Application
    app_name: str = "stable-cash-carry-mvp"
    environment: str = Field(default="development", pattern="^(development|staging|production)$")
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    
    # Capital limits (Requirements 1.2, 1.3, 1.4, 1.5)
    max_single_position_pct: Decimal = Decimal("0.15")  # 15%
    max_total_exposure_pct: Decimal = Decimal("0.50")   # 50%
    max_asset_class_exposure_pct: Decimal = Decimal("0.30")  # 30%
    min_safety_reserve_pct: Decimal = Decimal("0.50")   # 50%
    
    # Risk limits (Requirements 4.6, 4.7, 4.10)
    max_daily_loss_pct: Decimal = Decimal("0.03")  # 3%
    max_drawdown_pct: Decimal = Decimal("0.06")    # 6%
    min_liquidation_distance_pct: Decimal = Decimal("0.30")  # 30%
    
    # Leverage (Requirements 6.2, 6.3)
    default_leverage: Decimal = Field(default=Decimal("1.5"), validation_alias="DEFAULT_LEVERAGE")
    max_leverage: Decimal = Field(default=Decimal("2.0"), validation_alias="MAX_LEVERAGE")
    
    # Funding criteria (Requirements 2.12, 2.13)
    min_annualized_funding_pct: Decimal = Decimal("0.15")  # 15%
    max_annualized_funding_pct: Decimal = Decimal("0.80")  # 80%
    min_funding_stability_score: float = 0.7
    max_crowding_indicator: float = 3.0
    min_positive_funding_periods: int = 3
    
    # Execution (Requirements 21.7, 5.14)
    max_slippage_pct: Decimal = Decimal("0.0005")  # 0.05%
    delta_tolerance_pct: Decimal = Decimal("0.001")  # 0.1%
    rebalance_cooldown_seconds: int = 60
    execution_lock_timeout_seconds: int = 30
    
    # Monitoring (Requirements 5.1, 2.15)
    position_scan_interval_seconds: int = 5
    funding_update_interval_seconds: int = 10
    
    # Network resilience (Requirements 9.2, 9.3, 9.5, 9.6, 9.7)
    api_retry_attempts: int = 3
    network_soft_timeout_seconds: int = 30
    network_hard_timeout_seconds: int = 120
    
    # Trading pairs (Requirement 16.2)
    enabled_symbols: str = Field(default="BTC/USDT,ETH/USDT")
    
    # Enabled exchanges (comma-separated list)
    enabled_exchanges: str = Field(default="binance")
    
    # Running mode: testnet | simulation | mainnet
    running_mode: str = Field(default="testnet", pattern="^(testnet|mainnet|simulation)$")

    # Strategy switches
    enable_cash_carry: bool = Field(default=True)  # 现货-合约套利
    enable_cross_exchange: bool = Field(default=False)  # 跨市套利
    
    # Binance Exchange settings
    binance_api_key: str = Field(default="")
    binance_api_secret: str = Field(default="")

    # OKX Exchange settings
    okx_api_key: str = Field(default="")
    okx_api_secret: str = Field(default="")
    okx_passphrase: str = Field(default="")

    # Legacy exchange settings (auto-filled from primary exchange in model_post_init)
    exchange_name: str = Field(default="binance")
    exchange_api_key: str = Field(default="")
    exchange_api_secret: str = Field(default="")

    @property
    def is_testnet(self) -> bool:
        return self.running_mode == "testnet"

    @property
    def is_simulation(self) -> bool:
        return self.running_mode == "simulation"

    @property
    def shadow_mode(self) -> bool:
        """Backward-compatible alias for simulation mode."""
        return self.is_simulation

    def model_post_init(self, __context) -> None:
        """Back-fill legacy exchange_api_* from the primary exchange credentials."""
        if not self.exchange_api_key:
            cfg = self.get_exchange_config(self.exchange_name)
            object.__setattr__(self, "exchange_api_key", cfg["api_key"])
            object.__setattr__(self, "exchange_api_secret", cfg["api_secret"])
    
    # Fee configuration (Requirement 23.1, 23.2, 23.3)
    fee_version: str = "v1"
    spot_maker_fee: Decimal = Decimal("0.001")   # 0.1%
    spot_taker_fee: Decimal = Decimal("0.001")   # 0.1%
    perp_maker_fee: Decimal = Decimal("0.0002")  # 0.02%
    perp_taker_fee: Decimal = Decimal("0.0005")  # 0.05%
    
    # Redis (Requirements 12.1, 12.2)
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_db: int = Field(default=0)
    redis_password: str = Field(default="")
    redis_max_connections: int = Field(default=10)
    
    # PostgreSQL (Requirements 10.1, 10.2)
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_database: str = Field(default="stable_cash_carry")
    postgres_user: str = Field(default="postgres")
    postgres_password: str = Field(default="")
    postgres_min_connections: int = Field(default=2)
    postgres_max_connections: int = Field(default=10)
    
    # WebSocket (Requirement 17.1)
    websocket_host: str = Field(default="0.0.0.0")
    websocket_port: int = Field(default=8000)
    
    # Minimum trade requirements (Requirements 1.8, 1.9, 1.10)
    min_24h_volume_usd: Decimal = Decimal("10000000")  # $10M
    min_order_book_depth_multiplier: int = 10
    
    # Stress test (Requirement 4.11)
    stress_test_min_move_pct: Decimal = Decimal("0.20")  # 20%
    
    # Scout configuration
    scout_max_workers: int = 3

    # Cross-exchange arbitrage configuration
    cross_exchange_min_spread_pct: Decimal = Decimal("0.003")  # 0.3%
    cross_exchange_max_spread_pct: Decimal = Decimal("1.00")   # above error
    cross_exchange_close_spread_pct: Decimal = Decimal("0.001")  # 0.1%
    cross_exchange_max_holding_hours: int = 24
    cross_exchange_max_exposure_pct: Decimal = Decimal("0.30")  # 30%
    
    # Cross-exchange safety parameters
    cross_exchange_price_validity_seconds: int = 3  # Price validity period
    cross_exchange_max_spread_change_pct: Decimal = Decimal("0.30")  # Max 30% spread change
    cross_exchange_min_actual_spread_pct: Decimal = Decimal("0.0001")  # Min 0.01% actual spread

    # Hive: max concurrent workers (simultaneous trade executions)
    max_concurrent_workers: int = 10

    # Telegram notifications
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")

    # Feishu (飞书) notifications
    feishu_webhook_url: str = Field(default="")
    feishu_webhook_secret: str = Field(default="")

    # Funding reconciliation (Requirements 18.10, 18.11)
    funding_reconciliation_timeout_hours: int = 16  # 2 funding cycles

    # Hive node identity (for central server registration)
    hive_node_name: str = Field(default="hive-node-1")
    hive_node_ip: str = Field(default="")
    hive_node_remark: str = Field(default="")

    # Central server WebSocket URL (empty = communication bee disabled)
    hive_server_url: str = Field(default="")
    
    @field_validator("enabled_symbols")
    @classmethod
    def parse_symbols(cls, v: str) -> List[str]:
        """Parse comma-separated symbols into list."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v
    
    @field_validator("enabled_exchanges")
    @classmethod
    def parse_exchanges(cls, v: str) -> List[str]:
        """Parse comma-separated exchanges into list."""
        if isinstance(v, str):
            exchanges = [s.strip().lower() for s in v.split(",") if s.strip()]
            # Validate exchange names
            valid_exchanges = {"binance", "okx"}
            for exchange in exchanges:
                if exchange not in valid_exchanges:
                    raise ValueError(f"Invalid exchange: {exchange}. Supported: {valid_exchanges}")
            return exchanges
        return v
    
    @field_validator("max_leverage")
    @classmethod
    def validate_max_leverage(cls, v: Decimal) -> Decimal:
        """Validate max leverage does not exceed 2x (Requirement 6.1)."""
        if v > Decimal("2.0"):
            raise ValueError("max_leverage cannot exceed 2.0x")
        return v
    
    @field_validator("default_leverage")
    @classmethod
    def validate_default_leverage(cls, v: Decimal, info) -> Decimal:
        """Validate default leverage is within acceptable range (Requirement 6.2)."""
        if v < Decimal("1.2") or v > Decimal("1.5"):
            raise ValueError("default_leverage must be between 1.2x and 1.5x")
        return v
    
    def validate_all(self) -> None:
        """
        Validate all configuration values on startup (Requirement 16.6).
        Raises ValueError if any configuration is invalid.
        """
        errors = []
        
        # Validate enabled exchanges
        if not self.enabled_exchanges:
            errors.append("enabled_exchanges is required (at least one exchange)")
        
        # Validate exchange credentials for enabled exchanges
        for exchange in self.enabled_exchanges:
            if exchange == "binance":
                if not self.binance_api_key:
                    errors.append("binance_api_key is required when binance is enabled")
                if not self.binance_api_secret:
                    errors.append("binance_api_secret is required when binance is enabled")
            elif exchange == "okx":
                if not self.okx_api_key:
                    errors.append("okx_api_key is required when okx is enabled")
                if not self.okx_api_secret:
                    errors.append("okx_api_secret is required when okx is enabled")
                if not self.okx_passphrase:
                    errors.append("okx_passphrase is required when okx is enabled")
        
        # Validate strategy switches
        if not self.enable_cash_carry and not self.enable_cross_exchange:
            errors.append("At least one strategy must be enabled (enable_cash_carry or enable_cross_exchange)")
        
        # Validate cross-exchange requirements
        if self.enable_cross_exchange and len(self.enabled_exchanges) < 2:
            errors.append("Cross-exchange arbitrage requires at least 2 exchanges enabled")
        
        # Validate database credentials
        if not self.postgres_host:
            errors.append("postgres_host is required")
        if not self.postgres_database:
            errors.append("postgres_database is required")
        if not self.postgres_user:
            errors.append("postgres_user is required")
        
        # Validate Redis connection
        if not self.redis_host:
            errors.append("redis_host is required")
        
        # Validate percentage limits are within 0-1 range
        if not (0 < self.max_single_position_pct <= 1):
            errors.append("max_single_position_pct must be between 0 and 1")
        if not (0 < self.max_total_exposure_pct <= 1):
            errors.append("max_total_exposure_pct must be between 0 and 1")
        if not (0 < self.max_asset_class_exposure_pct <= 1):
            errors.append("max_asset_class_exposure_pct must be between 0 and 1")
        
        # Validate risk limits
        if self.max_daily_loss_pct >= self.max_drawdown_pct:
            errors.append("max_daily_loss_pct should be less than max_drawdown_pct")
        
        if errors:
            raise ValueError(
                f"Configuration validation failed (Requirement 16.7):\n" + 
                "\n".join(f"  - {error}" for error in errors)
            )
    
    def get_exchange_config(self, exchange_name: str) -> dict:
        """
        Get configuration for a specific exchange.
        
        Args:
            exchange_name: Exchange name (e.g., "binance", "okx")
            
        Returns:
            Dictionary with exchange configuration
        """
        exchange_name = exchange_name.lower()
        
        if exchange_name == "binance":
            return {
                "api_key": self.binance_api_key,
                "api_secret": self.binance_api_secret,
                "testnet": self.is_testnet,
                "password": None
            }
        elif exchange_name == "okx":
            return {
                "api_key": self.okx_api_key,
                "api_secret": self.okx_api_secret,
                "testnet": self.is_testnet,
                "password": self.okx_passphrase
            }
        else:
            raise ValueError(f"Unknown exchange: {exchange_name}")


# Global configuration instance
config: SystemConfig | None = None


def get_config() -> SystemConfig:
    """Get the global configuration instance."""
    global config
    if config is None:
        config = SystemConfig()
        config.validate_all()
    return config


def reload_config() -> SystemConfig:
    """Reload configuration from environment."""
    global config
    config = SystemConfig()
    config.validate_all()
    return config
