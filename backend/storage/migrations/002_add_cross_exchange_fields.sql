--
-- Database migration: Add cross-exchange arbitrage support to positions table.

-- Migration: 002_add_cross_exchange_fields
-- Date: 2026-02-15

-- Add strategy_type column to distinguish between cash_carry and cross_exchange
ALTER TABLE positions 
ADD COLUMN IF NOT EXISTS strategy_type VARCHAR(50) DEFAULT 'cash_carry';

-- Add cross-exchange specific columns
ALTER TABLE positions 
ADD COLUMN IF NOT EXISTS exchange_high VARCHAR(50),
ADD COLUMN IF NOT EXISTS exchange_low VARCHAR(50),
ADD COLUMN IF NOT EXISTS entry_spread_pct DECIMAL(20, 10),
ADD COLUMN IF NOT EXISTS current_spread_pct DECIMAL(20, 10),
ADD COLUMN IF NOT EXISTS target_close_spread_pct DECIMAL(20, 10);

-- Create index on strategy_type for faster queries
CREATE INDEX IF NOT EXISTS idx_positions_strategy_type ON positions(strategy_type);

-- Create index on exchange fields for cross-exchange queries
CREATE INDEX IF NOT EXISTS idx_positions_exchanges ON positions(exchange_high, exchange_low) 
WHERE strategy_type = 'cross_exchange';

-- Add comment to table
COMMENT ON COLUMN positions.strategy_type IS 'Arbitrage strategy type: cash_carry or cross_exchange';
COMMENT ON COLUMN positions.exchange_high IS 'High price exchange for cross-exchange arbitrage (where we short)';
COMMENT ON COLUMN positions.exchange_low IS 'Low price exchange for cross-exchange arbitrage (where we long)';
COMMENT ON COLUMN positions.entry_spread_pct IS 'Entry spread percentage for cross-exchange arbitrage';
COMMENT ON COLUMN positions.current_spread_pct IS 'Current spread percentage for cross-exchange arbitrage';
COMMENT ON COLUMN positions.target_close_spread_pct IS 'Target close spread percentage for cross-exchange arbitrage';
