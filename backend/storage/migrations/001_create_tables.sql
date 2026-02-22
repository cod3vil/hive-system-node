-- Migration: Create database tables for Stable Cash & Carry MVP
-- Requirements: 10.1, 10.2, 10.3, 10.4, 10.5

-- Table: positions
-- Stores all position records with complete trade details
CREATE TABLE IF NOT EXISTS positions (
    position_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(20) NOT NULL,
    spot_symbol VARCHAR(20) NOT NULL,
    perp_symbol VARCHAR(30) NOT NULL,
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
    
    -- Entry data
    spot_entry_price DECIMAL(20, 8) NOT NULL,
    perp_entry_price DECIMAL(20, 8) NOT NULL,
    spot_quantity DECIMAL(20, 8) NOT NULL,
    perp_quantity DECIMAL(20, 8) NOT NULL,
    leverage DECIMAL(5, 2) NOT NULL,
    entry_timestamp TIMESTAMPTZ NOT NULL,
    
    -- Current state
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    current_spot_price DECIMAL(20, 8),
    current_perp_price DECIMAL(20, 8),
    
    -- PnL tracking
    unrealized_pnl DECIMAL(20, 8) DEFAULT 0,
    realized_pnl DECIMAL(20, 8),
    funding_collected DECIMAL(20, 8) DEFAULT 0,
    total_fees_paid DECIMAL(20, 8) DEFAULT 0,
    
    -- Exit data
    spot_exit_price DECIMAL(20, 8),
    perp_exit_price DECIMAL(20, 8),
    exit_timestamp TIMESTAMPTZ,
    close_reason VARCHAR(100),
    
    -- Risk metrics
    liquidation_price DECIMAL(20, 8) NOT NULL,
    liquidation_distance DECIMAL(10, 4),
    last_rebalance_time TIMESTAMPTZ,
    
    -- Funding reconciliation
    last_funding_reconciliation TIMESTAMPTZ,
    funding_anomaly BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    asset_class VARCHAR(10) NOT NULL,
    strategy_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for positions table
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);
CREATE INDEX IF NOT EXISTS idx_positions_asset_class ON positions(asset_class);
CREATE INDEX IF NOT EXISTS idx_positions_entry_timestamp ON positions(entry_timestamp);
CREATE INDEX IF NOT EXISTS idx_positions_exchange ON positions(exchange);
CREATE INDEX IF NOT EXISTS idx_positions_funding_anomaly ON positions(funding_anomaly) WHERE funding_anomaly = TRUE;

-- Table: funding_logs
-- Stores funding rate payment logs with position reference
CREATE TABLE IF NOT EXISTS funding_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id UUID REFERENCES positions(position_id),
    symbol VARCHAR(20) NOT NULL,
    funding_rate DECIMAL(10, 8) NOT NULL,
    funding_amount DECIMAL(20, 8) NOT NULL,
    funding_timestamp TIMESTAMPTZ NOT NULL,
    reconciled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for funding_logs table
CREATE INDEX IF NOT EXISTS idx_funding_logs_position ON funding_logs(position_id);
CREATE INDEX IF NOT EXISTS idx_funding_logs_timestamp ON funding_logs(funding_timestamp);

-- Table: daily_pnl
-- Stores daily profit/loss summaries with date unique constraint
CREATE TABLE IF NOT EXISTS daily_pnl (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL UNIQUE,
    starting_equity DECIMAL(20, 8) NOT NULL,
    ending_equity DECIMAL(20, 8) NOT NULL,
    realized_pnl DECIMAL(20, 8) NOT NULL,
    unrealized_pnl DECIMAL(20, 8) NOT NULL,
    funding_income DECIMAL(20, 8) NOT NULL,
    fees_paid DECIMAL(20, 8) NOT NULL,
    net_pnl DECIMAL(20, 8) NOT NULL,
    return_pct DECIMAL(10, 4) NOT NULL,
    peak_equity DECIMAL(20, 8) NOT NULL,
    drawdown_pct DECIMAL(10, 4) NOT NULL,
    num_positions_opened INT DEFAULT 0,
    num_positions_closed INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for daily_pnl table
CREATE INDEX IF NOT EXISTS idx_daily_pnl_date ON daily_pnl(date);

-- Table: system_logs
-- Stores system event logs with level and timestamp indexes
CREATE TABLE IF NOT EXISTS system_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    level VARCHAR(10) NOT NULL,
    module VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for system_logs table
CREATE INDEX IF NOT EXISTS idx_system_logs_level ON system_logs(level);
CREATE INDEX IF NOT EXISTS idx_system_logs_timestamp ON system_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_system_logs_module ON system_logs(module);
