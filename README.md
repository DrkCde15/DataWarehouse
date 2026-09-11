# API → Data Warehouse Pipeline

Pipeline ETL completo para ingestão de dados de APIs externas (CoinGecko) e armazenamento em Data Warehouse PostgreSQL, orquestrado por Apache Airflow em containers Podman.

## Visão Geral

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────────┐
│  CoinGecko   │────▶│   Extract    │────▶│   Transform  │────▶│  PostgreSQL   │
│     API      │     │  (Pull)      │     │  (Clean)     │     │  (Warehouse)  │
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
│   │   ├── api_client.py              # Cliente API genérico (Airflow importa)
│   │   └── api_client.ipynb           # Notebook de exploração
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── cleaner.py                 # Limpeza e transformação (Airflow importa)
│   │   └── cleaner.ipynb              # Notebook de exploração
│   └── loaders/
│       ├── __init__.py
│       ├── postgres_loader.py         # Inserção/upsert em PostgreSQL (Airflow importa)
│       └── postgres_loader.ipynb      # Notebook de exploração
├── sql/
│   ├── create_tables.sql              # DDL para staging, dimensões e fatos
│   └── init_airflow_db.sql            # Cria banco do Airflow
├── dags/
│   ├── pipeline_dag.py                # DAG principal (coins/markets, coins/list)
│   ├── pipeline_coingecko_global.py   # DAG para /global
│   └── pipeline_coingecko_trending.py # DAG para /search/trending
├── notebooks/
│   └── 02_eda_coingecko.ipynb         # Análise exploratória das 4 tabelas
├── scripts/
│   └── init_airflow.sh                # Setup inicial do Airflow
├── Containerfile                      # Build da imagem Airflow
├── docker-compose.yml                 # Serviços: postgres, airflow, scheduler
├── .dockerignore
├── .gitignore
├── requirements.txt
└── README.md
```

> **Importante:** Arquivos `.py` e `.ipynb` coexistem em `src/`. O Airflow importa os `.py`; os `.ipynb` são para documentação/exploração local.

## Fonte de Dados: CoinGecko API

- **Base URL:** `https://api.coingecko.com/api/v3`
- **Autenticação:** Demo API (sem chave)
- **Endpoints utilizados:**

| Endpoint | Tabela | Descrição |
|---|---|---|
| `/coins/markets` | `staging.coingecko_coins` | Preço, market cap, volume, ATH, ATL |
| `/coins/list` | `staging.coingecko_list` | Lista completa de moedas (id, symbol, name) |
| `/global` | `staging.coingecko_global` | Estatísticas globais do mercado |
| `/search/trending` | `staging.coingecko_trending` | Moedas em alta no momento |

## Tabelas de Staging

| Tabela | Colunas | Exemplo de dado |
|---|---|---|
| `coingecko_coins` | 27 | Bitcoin: price, market_cap, volume, ath, atl, etc. |
| `coingecko_list` | 3 | {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"} |
| `coingecko_global` | 11 | Market cap total, dominancia BTC/ETH, JSONB |
| `coingecko_trending` | 13 | Moedas trending com score, price_btc, data (JSONB) |

## DAGs

### pipeline_etl_api (principal)

Para `/coins/markets` e `/coins/list`. Execute via UI ou CLI:

```bash
# /coins/markets (100 moedas com market data)
podman exec 02-api-data-warehouse_airflow-scheduler_1 airflow dags trigger pipeline_etl_api \
  --conf '{
    "endpoint": "/coins/markets",
    "pagination_type": "offset",
    "page_size": 250,
    "target_table": "staging.coingecko_coins",
    "conflict_columns": null,
    "source_api": "coingecko",
    "query_params": {
      "vs_currency": "usd",
      "order": "market_cap_desc",
      "per_page": 100,
      "page": 1,
      "sparkline": false,
      "price_change_percentage": "24h,7d,30d"
    }
  }'

# /coins/list (lista completa de ~19k moedas)
podman exec 02-api-data-warehouse_airflow-scheduler_1 airflow dags trigger pipeline_etl_api \
  --conf '{
    "endpoint": "/coins/list",
    "pagination_type": "none",
    "page_size": 250,
    "target_table": "staging.coingecko_list",
    "conflict_columns": null,
    "source_api": "coingecko",
    "query_params": {}
  }'
```

### pipeline_coingecko_global

Execute via **Airflow UI** → Trigger DAG. Defaults já configurados.

### pipeline_coingecko_trending

Execute via **Airflow UI** → Trigger DAG. Defaults já configurados.

## Parâmetros do DAG

| Param | Default | Descrição |
|---|---|---|
| `endpoint` | `/coins/markets` | Path da API |
| `pagination_type` | `"offset"` | `"offset"` ou `"none"` |
| `page_size` | `250` | Itens por página |
| `target_table` | `staging.coingecko_coins` | Tabela destino |
| `conflict_columns` | `null` | Colunas para upsert (ou `null` para insert) |
| `source_api` | `"coingecko"` | Nome da fonte |
| `query_params` | `null` | Params da query string da API |
| `results_key` | `"results"` | Chave JSON com os registros |
| `record_path` | `null` | Caminho para extrair sub-registros |

## Fluxo de Execução

```
start → register_start → extract → transform → load → register_success → end
                                                  ↓
                                            register_failure
```

| Task | Função |
|---|---|
| `register_start` | Registra início no `metadata.job_log` |
| `extract` | Chama API, serializa JSON no XCom |
| `transform` | Limpa dados com `DataCleaner`, serializa para JSON |
| `load` | Insere no PostgreSQL (bulk_insert ou upsert) |
| `register_success` | Registra conclusão no `metadata.job_log` |
| `register_failure` | Registra falha no `metadata.job_log` |

## Setup com Podman

```bash
# 1. Subir os containers
podman compose up -d

# 2. Verificar status
podman compose ps

# 3. Acessar Airflow
# Webserver: http://localhost:8080 (admin/admin)
# Postgres:  localhost:5432
```

## Setup Local (Desenvolvimento)

```bash
# 1. Criar ambiente virtual
python -m venv .venv
source .venv/bin/activate

# 2. Instalar dependências
pip install -r requirements.txt
```

## Conceitos-Chave

### Extração (Extract)
- Cliente HTTP genérico com paginação (offset, cursor, link, none)
- Rate limiting para evitar bloqueio pela API
- Retry automático com backoff exponencial
- Suporte a `query_params`, `results_key`, `record_path`

### Transformação (Transform)
- Padronização de nomes de colunas (snake_case)
- Limpeza de dados nulos e duplicados
- Inferência e conversão automática de tipos
- Tratamento de dicts/lists com `json.dumps()` (para JSONB)
- Conversão de unix timestamps para ISO string
- Serialização de dados via XCom (JSON strings)

### Carga (Load)
- Bulk insert para cargas completas
- Upsert (ON CONFLICT DO UPDATE) para cargas incrementais
- Filtragem automática de colunas que existem na tabela
- Tratamento de erros com logging

### Análise (EDA)

O notebook `notebooks/eda_coingecko.ipynb` cobre:
1. Conexão com PostgreSQL
2. Visão geral das 4 tabelas
3. Análise de `coingecko_coins` (preço, market cap, volume, correlações)
4. Análise de `coingecko_list` (distribuição de símbolos)
5. Análise de `coingecko_global` (market cap total, dominância)
6. Análise de `coingecko_trending` (moedas em alta)
7. Histórico de jobs ETL

## Licença

Projeto para fins de estudo e portfólio.
