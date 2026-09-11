-- ============================================================
-- Popula dim_coins a partir de staging.coingecko_list
-- ============================================================
INSERT INTO dim.dim_coins (coin_id, symbol, name, _extracted_at)
SELECT DISTINCT ON (coin_id)
    coin_id,
    symbol,
    name,
    _extracted_at
FROM staging.coingecko_list
ON CONFLICT (coin_id) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    _extracted_at = EXCLUDED._extracted_at,
    _loaded_at = NOW();

-- ============================================================
-- Popula fact_coin_daily a partir de staging.coingecko_coins
-- ============================================================
INSERT INTO fact.fact_coin_daily (
    coin_id, date_key, current_price, market_cap, market_cap_rank,
    total_volume, high_24h, low_24h, price_change_24h,
    price_change_percentage_24h, _extracted_at
)
SELECT
    dc.id AS coin_id,
    TO_CHAR(cc.last_updated AT TIME ZONE 'UTC', 'YYYYMMDD')::INTEGER AS date_key,
    cc.current_price,
    cc.market_cap,
    cc.market_cap_rank,
    cc.total_volume,
    cc.high_24h,
    cc.low_24h,
    cc.price_change_24h,
    cc.price_change_percentage_24h,
    cc._extracted_at
FROM staging.coingecko_coins cc
JOIN dim.dim_coins dc ON dc.coin_id = cc.coin_id
WHERE cc.last_updated IS NOT NULL
ON CONFLICT DO NOTHING;
