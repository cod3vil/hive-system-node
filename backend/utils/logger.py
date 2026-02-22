"""
Structured logging infrastructure with file and database output.
Validates: Requirements 10.4, 10.6
"""

import logging
import json
import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
from enum import Enum
from logging.handlers import RotatingFileHandler


# Patterns for sensitive data redaction
_SENSITIVE_PATTERNS = [
    (re.compile(r'(?i)(api[_-]?key|apikey|api[_-]?secret|secret[_-]?key)\s*[=:]\s*\S+'), r'\1=***REDACTED***'),
    (re.compile(r'(?i)(password|passphrase|passwd)\s*[=:]\s*\S+'), r'\1=***REDACTED***'),
    (re.compile(r'(?i)(token|bearer)\s*[=:]\s*\S+'), r'\1=***REDACTED***'),
    (re.compile(r'(?i)(webhook[_-]?url|webhook[_-]?secret)\s*[=:]\s*\S+'), r'\1=***REDACTED***'),
]


def _redact_sensitive(text: str) -> str:
    """Redact sensitive information from log messages."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


class LogLevel(str, Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StructuredLogger:
    """
    Structured logger that writes to both file and database.
    
    Log format includes:
    - timestamp: ISO 8601 format with timezone
    - level: Log level
    - module: Source module name
    - message: Human-readable message
    - details: JSON object with additional context
    - position_id: If related to a position
    - symbol: If related to a trading pair
    """
    
    def __init__(
        self,
        module_name: str,
        log_dir: str = "logs",
        db_client=None,
        websocket_manager=None
    ):
        """
        Initialize structured logger.
        
        Args:
            module_name: Name of the module using this logger
            log_dir: Directory for log files
            db_client: Database client for persisting logs (optional)
            websocket_manager: WebSocket connection manager for broadcasts (optional)
        """
        self.module_name = module_name
        self.db_client = db_client
        self.websocket_manager = websocket_manager
        
        # Create log directory if it doesn't exist
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up file logger
        self.file_logger = logging.getLogger(f"{module_name}_file")
        self.file_logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers to avoid duplicates
        self.file_logger.handlers.clear()
        
        # Create file handler with rotation (10MB max, keep 5 backup files)
        log_file = self.log_dir / f"{module_name}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        self.file_logger.addHandler(file_handler)
        
        # Also log to console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.file_logger.addHandler(console_handler)
    
    def _create_log_entry(
        self,
        level: LogLevel,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create structured log entry with sensitive data redaction."""
        entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": level.value,
            "module": self.module_name,
            "message": _redact_sensitive(message),
            "details": details or {},
            "position_id": position_id,
            "symbol": symbol
        }
        
        # Add any additional kwargs to details
        if kwargs:
            entry["details"].update(kwargs)
        
        return entry
    
    async def _persist_to_db(self, entry: Dict[str, Any]) -> None:
        """Persist log entry to database."""
        if self.db_client is None:
            return
        
        try:
            await self.db_client.table("system_logs").insert({
                "level": entry["level"],
                "module": entry["module"],
                "message": entry["message"],
                "details": entry["details"],
                "timestamp": entry["timestamp"]
            }).execute()
        except Exception as e:
            # Don't fail if database logging fails
            self.file_logger.error(f"Failed to persist log to database: {e}")
    
    def _log_to_file(self, level: LogLevel, entry: Dict[str, Any]) -> None:
        """Log entry to file."""
        log_message = json.dumps(entry)
        
        if level == LogLevel.DEBUG:
            self.file_logger.debug(log_message)
        elif level == LogLevel.INFO:
            self.file_logger.info(log_message)
        elif level == LogLevel.WARNING:
            self.file_logger.warning(log_message)
        elif level == LogLevel.ERROR:
            self.file_logger.error(log_message)
        elif level == LogLevel.CRITICAL:
            self.file_logger.critical(log_message)
    
    async def log(
        self,
        level: LogLevel,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Log a message with structured data.
        
        Args:
            level: Log level
            message: Human-readable message
            details: Additional context as dictionary
            position_id: Position ID if related to a position
            symbol: Trading pair if related to a symbol
            **kwargs: Additional fields to include in details
        """
        entry = self._create_log_entry(
            level, message, details, position_id, symbol, **kwargs
        )
        
        # Log to file
        self._log_to_file(level, entry)
        
        # Persist to database asynchronously
        await self._persist_to_db(entry)
        
        # Broadcast to WebSocket clients (only INFO and above)
        if self.websocket_manager and level.value in ["INFO", "WARNING", "ERROR", "CRITICAL"]:
            await self.websocket_manager.broadcast_system_log({
                "level": entry["level"],
                "module": entry["module"],
                "message": entry["message"],
                "timestamp": entry["timestamp"]
            })
    
    async def debug(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        """Log debug message."""
        await self.log(LogLevel.DEBUG, message, details, **kwargs)
    
    async def info(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log info message."""
        await self.log(LogLevel.INFO, message, details, position_id, symbol, **kwargs)
    
    async def warning(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log warning message."""
        await self.log(LogLevel.WARNING, message, details, position_id, symbol, **kwargs)
    
    async def error(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        error: Optional[Exception] = None,
        **kwargs
    ) -> None:
        """Log error message."""
        if error:
            if details is None:
                details = {}
            details["error_type"] = type(error).__name__
            details["error_message"] = str(error)
        
        await self.log(LogLevel.ERROR, message, details, position_id, symbol, **kwargs)
    
    async def critical(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        error: Optional[Exception] = None,
        **kwargs
    ) -> None:
        """Log critical message."""
        if error:
            if details is None:
                details = {}
            details["error_type"] = type(error).__name__
            details["error_message"] = str(error)
        
        await self.log(LogLevel.CRITICAL, message, details, position_id, symbol, **kwargs)


def get_logger(module_name: str, db_client=None, log_dir: str = "logs", websocket_manager=None) -> StructuredLogger:
    """
    Get a structured logger instance for a module.
    
    Args:
        module_name: Name of the module
        db_client: Database client for persisting logs
        log_dir: Directory for log files
        websocket_manager: WebSocket connection manager for broadcasts
    
    Returns:
        StructuredLogger instance
    """
    return StructuredLogger(module_name, log_dir=log_dir, db_client=db_client, websocket_manager=websocket_manager)
