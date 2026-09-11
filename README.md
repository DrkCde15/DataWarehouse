# API → Data Warehouse Pipeline

Pipeline ETL completo para ingestão de dados de APIs externas (CoinGecko, Open Library) e armazenamento em Data Warehouse PostgreSQL, orquestrado por Apache Airflow em containers Podman.

## Visão Geral

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│  CoinGecko   │────▶│   Extract    │────▶│   Transform  │────▶│  PostgreSQL   │
│  Open Library│     │  (Pull)      │     │  (Clean)     │     │  (Warehouse)  │
└──────────────┘     └──────────────┘     └──────────────┘     └───────────────┘
         │                                                         │
         └──────── Airflow DAG (Podman Containers) ───────────────┘
```

## Estrutura do Projeto

```
02-api-data-warehouse/
├── src/
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── api_client.py              # Cliente API genérico
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── cleaner.py                 # Limpeza e transformação
│   └── loaders/
│       ├── __init__.py
│       └── postgres_loader.py         # Inserção/upsert em PostgreSQL
├── sql/
│   ├── create_tables.sql              # DDL para staging, dimensões e fatos
│   ├── populate_dim_fact.sql          # Popula dim e fact
│   ├── analytics_views.sql            # Views de agregação
│   └── init_airflow_db.sql            # Cria banco do Airflow
├── dags/
│   ├── pipeline_dag.py                # DAG principal (coins/markets, coins/list)
│   ├── pipeline_coingecko_global.py   # DAG para /global
│   ├── pipeline_coingecko_trending.py # DAG para /search/trending
│   └── pipeline_openlibrary.py        # DAG para Open Library
├── notebooks/
│   ├── eda_coingecko.ipynb            # EDA CoinGecko
│   └── eda_open_library.ipynb         # EDA Open Library
├── tests/
│   ├── test_api_client.py             # Testes do cliente API
│   ├── test_cleaner.py                # Testes do limpeza
│   └── test_postgres_loader.py        # Testes do loader
├── scripts/
│   └── init_airflow.sh                # Setup inicial do Airflow
├── .github/workflows/tests.yml        # CI/CD GitHub Actions
├── Containerfile                      # Build da imagem Airflow
├── docker-compose.yml                 # Serviços: postgres, airflow, scheduler
├── .env                               # Variáveis de ambiente
├── requirements.txt
└── README.md
```

## Fontes de Dados

### CoinGecko API

- **Base URL:** `https://api.coingecko.com/api/v3`
- **Autenticação:** Demo API (sem chave)

| Endpoint | Tabela | Descrição |
|---|---|---|
| `/coins/markets` | `staging.coingecko_coins` | Preço, market cap, volume, ATH, ATL |
| `/coins/list` | `staging.coingecko_list` | Lista completa de moedas |
| `/global` | `staging.coingecko_global` | Estatísticas globais do mercado |
| `/search/trending` | `staging.coingecko_trending` | Moedas em alta |

### Open Library API

- **Base URL:** `https://openlibrary.org`
- **Autenticação:** Não precisa (com User-Agent)

| Endpoint | Tabela | Descrição |
|---|---|---|
| `/subjects/{subject}.json` | `staging.openlibrary_books` | Livros por assunto |

## DAGs

### pipeline_etl_api

Para CoinGecko. Execute via UI ou CLI:

```bash
# /coins/markets
podman exec 02-api-data-warehouse_airflow-scheduler_1 airflow dags trigger pipeline_etl_api \
  --conf '{
    "endpoint": "/coins/markets",
    "pagination_type": "page",
    "page_size": 250,
    "target_table": "staging.coingecko_coins",
    "conflict_columns": ["coin_id"],
    "vs_currency": "usd"
  }'

# /coins/list
podman exec 02-api-data-warehouse_airflow-scheduler_1 airflow dags trigger pipeline_etl_api \
  --conf '{
    "endpoint": "/coins/list",
    "pagination_type": "none",
    "target_table": "staging.coingecko_list",
    "conflict_columns": ["coin_id"]
  }'
```

### pipeline_coingecko_global

Execute via **Airflow UI** → Trigger DAG.

### pipeline_coingecko_trending

Execute via **Airflow UI** → Trigger DAG.

### pipeline_openlibrary

Execute via **Airflow UI** → Trigger DAG. Extrai livros por subjects (fiction, science, history, technology, philosophy).

## Parâmetros do DAG

| Param | Default | Descrição |
|---|---|---|
| `endpoint` | `/coins/markets` | Path da API |
| `pagination_type` | `"offset"` | `"none"`, `"offset"`, `"page"`, `"cursor"`, `"link"` |
| `page_size` | `250` | Itens por página |
| `target_table` | `staging.coingecko_coins` | Tabela destino |
| `conflict_columns` | `null` | Colunas para upsert |
| `vs_currency` | `"usd"` | Moeda de referência |
| `results_key` | `"results"` | Chave JSON com os registros |
| `record_path` | `null` | Caminho para sub-registros |

## Variáveis de Ambiente (.env)

```
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
OPENLIBRARY_BASE_URL=https://openlibrary.org
DB_CONNECTION_STRING=postgresql+psycopg2://postgres:postgres@postgres:5432/data_warehouse
```

## Schema do Data Warehouse

```
staging/          → Dados brutos das APIs
  ├── coingecko_coins
  ├── coingecko_list
  ├── coingecko_global
  ├── coingecko_trending
  └── openlibrary_books

dim/              → Dimensões
  ├── dim_coins
  ├── dim_date (2022-2027)
  └── dim_entities

fact/             → Fatos
  └── fact_coin_daily

analytics/        → Views de agregação
  ├── v_coin_daily_summary
  ├── v_top_coins_by_market_cap
  ├── v_weekly_performance
  ├── v_openlibrary_books_by_subject
  ├── v_trending_coins
  └── v_global_market_summary

metadata/         → Controle ETL
  ├── etl_jobs
  ├── v_latest_jobs
  └── v_load_summary
```

## Testes

```bash
# Rodar todos os testes
python -m pytest tests/ -v

# Rodar com cobertura
python -m pytest tests/ --cov=src --cov-report=term-missing
```

42 testes cobrindo: api_client, cleaner, postgres_loader.

## CI/CD

GitHub Actions roda testes automaticamente em push/PR para `main`/`master`.

## Setup com Podman

```bash
# 1. Subir os containers
podman compose up -d

# 2. Verificar status
podman ps

# 3. Acessar Airflow
# Webserver: http://localhost:8080 (admin/admin)
# Postgres:  localhost:5432
```

## Licença

Projeto para fins de estudo e portfólio.
