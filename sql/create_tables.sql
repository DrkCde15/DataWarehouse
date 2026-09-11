-- ============================================================
-- DDL para Data Warehouse - API Pipeline
-- PostgreSQL
-- ============================================================

-- Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ============================================================
-- TABELAS DE STAGING (dados brutos da API)
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.api_raw_data (
    id SERIAL PRIMARY KEY,
    source_api VARCHAR(255) NOT NULL,
    endpoint VARCHAR(500) NOT NULL,
    raw_payload JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staging_raw_source ON staging.api_raw_data(source_api);
CREATE INDEX IF NOT EXISTS idx_staging_raw_extracted ON staging.api_raw_data(extracted_at);

-- ============================================================
-- TABELAS DE STAGING - COINGECKO
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.coingecko_coins (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    image TEXT,
    current_price NUMERIC(18, 8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    high_24h NUMERIC(18, 8),
    low_24h NUMERIC(18, 8),
    price_change_24h NUMERIC(18, 8),
    price_change_percentage_24h NUMERIC(10, 4),
    market_cap_change_24h NUMERIC(18, 2),
    market_cap_change_percentage_24h NUMERIC(10, 4),
    circulating_supply NUMERIC(18, 2),
    total_supply NUMERIC(18, 2),
    max_supply NUMERIC(18, 2),
    ath NUMERIC(18, 8),
    ath_change_percentage NUMERIC(12, 4),
    ath_date TIMESTAMP WITH TIME ZONE,
    atl NUMERIC(18, 8),
    atl_change_percentage NUMERIC(12, 4),
    atl_date TIMESTAMP WITH TIME ZONE,
    roi TEXT,
    last_updated TIMESTAMP WITH TIME ZONE,
    _extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staging_coingecko_coin_id ON staging.coingecko_coins(coin_id);
CREATE INDEX IF NOT EXISTS idx_staging_coingecko_symbol ON staging.coingecko_coins(symbol);
CREATE INDEX IF NOT EXISTS idx_staging_coingecko_rank ON staging.coingecko_coins(market_cap_rank);

-- CoinGecko: /coins/list
CREATE TABLE IF NOT EXISTS staging.coingecko_list (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(200) NOT NULL,
    symbol VARCHAR(100) NOT NULL,
    name VARCHAR(500) NOT NULL,
    _extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_coingecko_list_coin_id ON staging.coingecko_list(coin_id);

-- CoinGecko: /global
CREATE TABLE IF NOT EXISTS staging.coingecko_global (
    id SERIAL PRIMARY KEY,
    active_cryptocurrencies INTEGER,
    markets INTEGER,
    total_market_cap_usd NUMERIC(18, 2),
    total_volume_usd NUMERIC(18, 2),
    bitcoin_dominance_pct NUMERIC(8, 4),
    ethereum_dominance_pct NUMERIC(8, 4),
    market_cap_change_pct_24h NUMERIC(10, 4),
    updated_at TIMESTAMP WITH TIME ZONE,
    _extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- CoinGecko: /trending
CREATE TABLE IF NOT EXISTS staging.coingecko_trending (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(200) NOT NULL,
    name VARCHAR(500) NOT NULL,
    symbol VARCHAR(100) NOT NULL,
    market_cap_rank INTEGER,
    thumb VARCHAR(500),
    small VARCHAR(500),
    large VARCHAR(500),
    slug VARCHAR(200),
    score INTEGER,
    price_btc NUMERIC(18, 8),
    data JSONB,
    _extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_coingecko_trending_coin_id ON staging.coingecko_trending(coin_id);

-- ============================================================
-- TABELAS DE DIMENSÃO
-- ============================================================

-- Dimensão: Exemplo genérico de dimensão (substituir conforme API)
CREATE TABLE IF NOT EXISTS dim.dim_entities (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(500) NOT NULL,
    description TEXT,
    category VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    _extracted_at TIMESTAMP WITH TIME ZONE,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_entities_ext_id ON dim.dim_entities(external_id);
CREATE INDEX IF NOT EXISTS idx_dim_entities_status ON dim.dim_entities(status);

-- Dimensão: Categorias
CREATE TABLE IF NOT EXISTS dim.dim_categories (
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(500) NOT NULL,
    parent_category_id INTEGER REFERENCES dim.dim_categories(id),
    level INTEGER DEFAULT 1,
    _extracted_at TIMESTAMP WITH TIME ZONE,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Dimensão: Moedas (CoinGecko)
CREATE TABLE IF NOT EXISTS dim.dim_coins (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) UNIQUE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(255) NOT NULL,
    image_url TEXT,
    _extracted_at TIMESTAMP WITH TIME ZONE,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_coins_coin_id ON dim.dim_coins(coin_id);
CREATE INDEX IF NOT EXISTS idx_dim_coins_symbol ON dim.dim_coins(symbol);

-- Dimensão: Tempo (conforme TABLESPACE temporal)
CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_key INTEGER PRIMARY KEY,  -- formato YYYYMMDD
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day INTEGER NOT NULL,
    day_of_week VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Popula dim_date para 5 anos
INSERT INTO dim.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'FMMonth') AS month_name,
    EXTRACT(DAY FROM d)::INTEGER AS day,
    TO_CHAR(d, 'FMDay') AS day_of_week,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend
FROM generate_series('2022-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- ============================================================
-- TABELAS DE FATO
-- ============================================================

-- Fato: Exemplo genérico de métricas/eventos
CREATE TABLE IF NOT EXISTS fact.fact_events (
    id SERIAL PRIMARY KEY,
    entity_id INTEGER NOT NULL REFERENCES dim.dim_entities(id),
    date_key INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    event_type VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15, 4),
    metric_count INTEGER DEFAULT 1,
    source VARCHAR(255),
    _extracted_at TIMESTAMP WITH TIME ZONE,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_events_entity ON fact.fact_events(entity_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_date ON fact.fact_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_events_type ON fact.fact_events(event_type);

-- Fato: Métricas diárias de moedas (CoinGecko)
CREATE TABLE IF NOT EXISTS fact.fact_coin_daily (
    id SERIAL PRIMARY KEY,
    coin_id INTEGER NOT NULL REFERENCES dim.dim_coins(id),
    date_key INTEGER NOT NULL REFERENCES dim.dim_date(date_key),
    current_price NUMERIC(18, 8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    total_volume BIGINT,
    high_24h NUMERIC(18, 8),
    low_24h NUMERIC(18, 8),
    price_change_24h NUMERIC(18, 8),
    price_change_percentage_24h NUMERIC(8, 4),
    _extracted_at TIMESTAMP WITH TIME ZONE,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_coin_daily_coin ON fact.fact_coin_daily(coin_id);
CREATE INDEX IF NOT EXISTS idx_fact_coin_daily_date ON fact.fact_coin_daily(date_key);

-- ============================================================
-- CONTROLE DE PIPELINE (metadata)
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.etl_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    source_api VARCHAR(255),
    endpoint VARCHAR(500),
    status VARCHAR(50) NOT NULL DEFAULT 'running',  -- running, success, failed
    records_extracted INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_seconds NUMERIC(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_etl_jobs_status ON metadata.etl_jobs(status);
CREATE INDEX IF NOT EXISTS idx_etl_jobs_started ON metadata.etl_jobs(started_at);

-- Função para registrar início de job
CREATE OR REPLACE FUNCTION metadata.start_job(
    p_job_name VARCHAR,
    p_source_api VARCHAR DEFAULT NULL,
    p_endpoint VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_job_id INTEGER;
BEGIN
    INSERT INTO metadata.etl_jobs (job_name, source_api, endpoint, status)
    VALUES (p_job_name, p_source_api, p_endpoint, 'running')
    RETURNING id INTO v_job_id;

    RETURN v_job_id;
END;
$$ LANGUAGE plpgsql;

-- Função para finalizar job
CREATE OR REPLACE FUNCTION metadata.finish_job(
    p_job_id INTEGER,
    p_status VARCHAR,
    p_records_extracted INTEGER DEFAULT 0,
    p_records_loaded INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE metadata.etl_jobs
    SET
        status = p_status,
        records_extracted = p_records_extracted,
        records_loaded = p_records_loaded,
        error_message = p_error_message,
        finished_at = NOW(),
        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
    WHERE id = p_job_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- VIEWS ÚTEIS
-- ============================================================

-- View: Último status dos jobs de ETL
CREATE OR REPLACE VIEW metadata.v_latest_jobs AS
SELECT DISTINCT ON (job_name)
    id,
    job_name,
    source_api,
    endpoint,
    status,
    records_extracted,
    records_loaded,
    error_message,
    started_at,
    finished_at,
    duration_seconds
FROM metadata.etl_jobs
ORDER BY job_name, started_at DESC;

-- View: Resumo de dados carregados
CREATE OR REPLACE VIEW metadata.v_load_summary AS
SELECT
    source_api,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_jobs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_jobs,
    SUM(records_extracted) AS total_extracted,
    SUM(records_loaded) AS total_loaded,
    MAX(finished_at) AS last_run
FROM metadata.etl_jobs
WHERE status IN ('success', 'failed')
GROUP BY source_api;
