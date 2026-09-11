-- ============================================================
-- Views de Agregação - Schema analytics
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- View: Resumo diário de moedas com preço e volume
CREATE OR REPLACE VIEW analytics.v_coin_daily_summary AS
SELECT
    dc.coin_id,
    dc.symbol,
    dc.name,
    dd.full_date,
    dd.day_of_week,
    f.current_price,
    f.market_cap,
    f.market_cap_rank,
    f.total_volume,
    f.high_24h,
    f.low_24h,
    f.price_change_24h,
    f.price_change_percentage_24h
FROM fact.fact_coin_daily f
JOIN dim.dim_coins dc ON dc.id = f.coin_id
JOIN dim.dim_date dd ON dd.date_key = f.date_key
ORDER BY dd.full_date DESC, f.market_cap_rank;

-- View: Top 10 moedas por market cap
CREATE OR REPLACE VIEW analytics.v_top_coins_by_market_cap AS
SELECT
    dc.coin_id,
    dc.symbol,
    dc.name,
    f.current_price,
    f.market_cap,
    f.market_cap_rank,
    f.total_volume,
    f.price_change_percentage_24h,
    dd.full_date
FROM fact.fact_coin_daily f
JOIN dim.dim_coins dc ON dc.id = f.coin_id
JOIN dim.dim_date dd ON dd.date_key = f.date_key
WHERE f.market_cap_rank <= 10
  AND dd.full_date = (SELECT MAX(full_date) FROM dim.dim_date WHERE date_key = f.date_key)
ORDER BY f.market_cap_rank;

-- View: Performance semanal de moedas
CREATE OR REPLACE VIEW analytics.v_weekly_performance AS
SELECT
    dc.coin_id,
    dc.symbol,
    dc.name,
    dd.year,
    dd.month,
    dd.day,
    AVG(f.current_price) AS avg_price,
    MAX(f.high_24h) AS max_high,
    MIN(f.low_24h) AS min_low,
    SUM(f.total_volume) AS total_volume,
    COUNT(*) AS data_points
FROM fact.fact_coin_daily f
JOIN dim.dim_coins dc ON dc.id = f.coin_id
JOIN dim.dim_date dd ON dd.date_key = f.date_key
GROUP BY dc.coin_id, dc.symbol, dc.name, dd.year, dd.month, dd.day
ORDER BY dd.year DESC, dd.month DESC, dd.day DESC;

-- View: Livros Open Library por assunto
CREATE OR REPLACE VIEW analytics.v_openlibrary_books_by_subject AS
SELECT
    subject,
    COUNT(*) AS total_books,
    MIN(publish_date) AS earliest_published,
    MAX(publish_date) AS latest_published
FROM staging.openlibrary_books
WHERE subject IS NOT NULL
GROUP BY subject
ORDER BY total_books DESC;

-- View: Moedas trending
CREATE OR REPLACE VIEW analytics.v_trending_coins AS
SELECT
    coin_id,
    name,
    symbol,
    market_cap_rank,
    score,
    price_btc,
    _extracted_at
FROM staging.coingecko_trending
ORDER BY score DESC;

-- View: Resumo global do mercado
CREATE OR REPLACE VIEW analytics.v_global_market_summary AS
SELECT
    active_cryptocurrencies,
    markets,
    total_market_cap_usd,
    total_volume_usd,
    bitcoin_dominance_pct,
    ethereum_dominance_pct,
    market_cap_change_pct_24h,
    updated_at,
    _extracted_at
FROM staging.coingecko_global
ORDER BY _extracted_at DESC
LIMIT 1;
