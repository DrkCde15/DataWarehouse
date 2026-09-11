#!/bin/bash
set -e

echo "=== Inicializando Airflow ==="

airflow db migrate

echo "=== Criando usuário admin ==="
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

echo "=== Configurando Variáveis do Airflow ==="
airflow variables set coingecko_base_url "${COINGECKO_BASE_URL:-https://api.coingecko.com/api/v3}"
airflow variables set coingecko_api_key "${COINGECKO_API_KEY:-}"
airflow variables set openlibrary_base_url "${OPENLIBRARY_BASE_URL:-https://openlibrary.org}"
airflow variables set openlibrary_user_agent "${OPENLIBRARY_USER_AGENT:-DataWarehouseBot/1.0 (data@example.com)}"
airflow variables set db_connection_string "${DB_CONNECTION_STRING:-postgresql+psycopg2://postgres:postgres@postgres:5432/data_warehouse}"

echo "=== Init concluído ==="
