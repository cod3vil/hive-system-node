--
-- Database migration: Fix position fields to match code expectations
-- Migration: 003_fix_position_fields
-- Date: 2026-02-20
--
-- This migration adds missing fields that the code expects but are not in the original schema.
-- The original schema used spot_quantity/perp_quantity, but the code uses spot_size/perp_size.

-- Add missing fields that the code expects
ALTER TABLE positions 
ADD COLUMN IF NOT EXISTS spot_size DECIMAL(20, 8),
ADD COLUMN IF NOT EXISTS perp_size DECIMAL(20, 8),
ADD COLUMN IF NOT EXISTS spot_current_price DECIMAL(20, 8),
ADD COLUMN IF NOT EXISTS perp_current_price DECIMAL(20, 8),
ADD COLUMN IF NOT EXISTS entry_funding_rate DECIMAL(10, 8),
ADD COLUMN IF NOT EXISTS current_funding_rate DECIMAL(10, 8),
ADD COLUMN IF NOT EXISTS total_funding_paid DECIMAL(20, 8) DEFAULT 0,
ADD COLUMN IF NOT EXISTS entry_time TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_update_time TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS spot_order_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS perp_order_id VARCHAR(100),
ADD COLUMN IF NOT EXISTS metadata JSONB;

-- Copy data from old fields to new fields if they exist and new fields are null
UPDATE positions 
SET 
    spot_size = COALESCE(spot_size, spot_quantity),
    perp_size = COALESCE(perp_size, perp_quantity),
    spot_current_price = COALESCE(spot_current_price, current_spot_price),
    perp_current_price = COALESCE(perp_current_price, current_perp_price),
    entry_time = COALESCE(entry_time, entry_timestamp),
    last_update_time = COALESCE(last_update_time, updated_at)
WHERE spot_size IS NULL OR perp_size IS NULL;

-- Create indexes for new fields
CREATE INDEX IF NOT EXISTS idx_positions_entry_time ON positions(entry_time);
CREATE INDEX IF NOT EXISTS idx_positions_last_update_time ON positions(last_update_time);

-- Add comments
COMMENT ON COLUMN positions.spot_size IS 'Spot position size (quantity)';
COMMENT ON COLUMN positions.perp_size IS 'Perpetual position size (quantity)';
COMMENT ON COLUMN positions.spot_current_price IS 'Current spot price';
COMMENT ON COLUMN positions.perp_current_price IS 'Current perpetual price';
COMMENT ON COLUMN positions.entry_funding_rate IS 'Funding rate at entry';
COMMENT ON COLUMN positions.current_funding_rate IS 'Current funding rate';
COMMENT ON COLUMN positions.total_funding_paid IS 'Total funding fees paid/received';
COMMENT ON COLUMN positions.entry_time IS 'Position entry timestamp';
COMMENT ON COLUMN positions.last_update_time IS 'Last update timestamp';
COMMENT ON COLUMN positions.spot_order_id IS 'Spot order ID';
COMMENT ON COLUMN positions.perp_order_id IS 'Perpetual order ID';
COMMENT ON COLUMN positions.metadata IS 'Additional metadata in JSON format';
