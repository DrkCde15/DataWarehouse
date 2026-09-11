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
airflow variables set api_base_url "https://api.coingecko.com/api/v3"
airflow variables set api_key ""
airflow variables set db_connection_string "${DB_CONNECTION_STRING:-postgresql+psycopg2://postgres:postgres@postgres:5432/data_warehouse}"

echo "=== Init concluído ==="
