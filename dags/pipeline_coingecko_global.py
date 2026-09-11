"""DAG para extrair dados globais do mercado crypto (/global)."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-team@exemplo.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

API_RATE_LIMIT = 10
API_PAGE_SIZE = 100


def extract_data(**context) -> None:
    from src.extractors.api_client import APIClient
    import json

    client = APIClient(
        base_url=Variable.get("api_base_url"),
        api_key=Variable.get("api_key"),
        rate_limit=API_RATE_LIMIT,
    )

    params = context["params"]
    endpoint = params.get("endpoint", "/global")
    pagination_type = params.get("pagination_type", "none")
    page_size = params.get("page_size", API_PAGE_SIZE)

    query_params = params.get("query_params")
    if isinstance(query_params, str):
        query_params = json.loads(query_params)
    if query_params is None:
        query_params = {}

    logger.info(f"Extraindo dados de {endpoint} | params={query_params}")

    records = client.get_all_pages(
        endpoint=endpoint,
        pagination_type=pagination_type,
        page_size=page_size,
        params=query_params,
        results_key=params.get("results_key", "data"),
    )

    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    context["ti"].xcom_push(key="raw_data", value=json.dumps(records))
    context["ti"].xcom_push(key="record_count", value=len(records))
    context["ti"].xcom_push(key="api_stats", value=client.stats)

    logger.info(f"Extração concluída: {len(records)} registros")


def transform_data(**context) -> None:
    from src.transformers.cleaner import DataCleaner
    from datetime import datetime, timezone
    import pandas as pd
    import json

    ti = context["ti"]
    raw_json = ti.xcom_pull(key="raw_data", task_ids="extract")
    records = json.loads(raw_json)
    df = pd.DataFrame(records)

    cleaner = DataCleaner()
    df_clean = cleaner.clean_dataframe(df)

    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].astype(str)

    for col in df_clean.columns:
        if df_clean[col].dtype in ("int64", "float64"):
            sample = df_clean[col].dropna().head(10)
            if len(sample) > 0 and all(isinstance(v, (int, float)) and v > 1e9 and v < 1e11 for v in sample):
                df_clean[col] = df_clean[col].apply(
                    lambda x: datetime.fromtimestamp(int(x), tz=timezone.utc).isoformat()
                    if pd.notna(x) and isinstance(x, (int, float)) and x > 1e9 and x < 1e11
                    else x
                )

    for col in df_clean.columns:
        if df_clean[col].dtype == "object":
            df_clean[col] = df_clean[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

    ti.xcom_push(key="clean_data", value=json.dumps(df_clean.to_dict(orient="records")))
    ti.xcom_push(key="clean_count", value=len(df_clean))

    logger.info(f"Transformação concluída: {len(df_clean)} registros limpos")


def load_data(**context) -> None:
    from src.loaders.postgres_loader import PostgresLoader
    import pandas as pd
    import json

    ti = context["ti"]
    clean_json = ti.xcom_pull(key="clean_data", task_ids="transform")
    records = json.loads(clean_json)
    df = pd.DataFrame(records)

    if "id" in df.columns:
        df = df.rename(columns={"id": "coin_id"})

    target_table = context["params"].get("target_table", "staging.coingecko_global")
    schema, table = target_table.split(".")

    loader = PostgresLoader(connection_string=Variable.get("db_connection_string"))

    try:
        cols_in_table = loader.execute_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table",
            {"schema": schema, "table": table},
        )
        valid_cols = {row[0] for row in cols_in_table}
        cols_to_keep = [c for c in df.columns if c in valid_cols]
        if set(df.columns) != valid_cols:
            logger.warning(f"Colunas ignoradas: {set(df.columns) - valid_cols}")
        if cols_to_keep:
            df = df[cols_to_keep]

        conflict_cols = context["params"].get("conflict_columns")
        if conflict_cols and isinstance(conflict_cols, list) and len(conflict_cols) > 0:
            rows_loaded = loader.upsert(
                df=df, table=table, schema=schema, conflict_columns=conflict_cols,
            )
        else:
            rows_loaded = loader.bulk_insert(
                df=df, table=table, schema=schema,
            )

        ti.xcom_push(key="rows_loaded", value=rows_loaded)
        logger.info(f"Carga concluída: {rows_loaded} registros em {target_table}")

    finally:
        loader.close()


def register_job_start(**context) -> None:
    from src.loaders.postgres_loader import PostgresLoader

    loader = PostgresLoader(connection_string=Variable.get("db_connection_string"))
    job_id = loader.execute_sql(
        "SELECT metadata.start_job(:name, :source, :endpoint)",
        {
            "name": context["task"].dag_id,
            "source": context["params"].get("source_api", "unknown"),
            "endpoint": context["params"].get("endpoint", "unknown"),
        },
    )
    ti = context["ti"]
    ti.xcom_push(key="job_id", value=job_id[0][0] if job_id else None)
    loader.close()


def register_job_completion(**context) -> None:
    from src.loaders.postgres_loader import PostgresLoader

    ti = context["ti"]
    job_id = ti.xcom_pull(key="job_id", task_ids="register_start")
    records_extracted = ti.xcom_pull(key="record_count", task_ids="extract")
    records_loaded = ti.xcom_pull(key="rows_loaded", task_ids="load")

    loader = PostgresLoader(connection_string=Variable.get("db_connection_string"))
    loader.execute_sql(
        "SELECT metadata.finish_job(:job_id, :status, :extracted, :loaded)",
        {
            "job_id": job_id,
            "status": "success",
            "extracted": records_extracted or 0,
            "loaded": records_loaded or 0,
        },
    )
    loader.close()


def register_job_failure(**context) -> None:
    from src.loaders.postgres_loader import PostgresLoader

    ti = context["ti"]
    job_id = ti.xcom_pull(key="job_id", task_ids="register_start")

    if job_id:
        loader = PostgresLoader(connection_string=Variable.get("db_connection_string"))
        loader.execute_sql(
            "SELECT metadata.finish_job(:job_id, :status, 0, 0, :error)",
            {
                "job_id": job_id,
                "status": "failed",
                "error": str(context.get("exception", "Erro desconhecido")),
            },
        )
        loader.close()


with DAG(
    dag_id="pipeline_coingecko_global",
    default_args=DEFAULT_ARGS,
    description="Pipeline ETL: CoinGecko /global -> staging.coingecko_global",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "api", "coingecko", "global"],
    params={
        "endpoint": "/global",
        "pagination_type": "none",
        "page_size": 250,
        "target_table": "staging.coingecko_global",
        "conflict_columns": None,
        "source_api": "coingecko",
        "query_params": {},
        "results_key": "data",
    },
) as dag:

    start = EmptyOperator(task_id="start")

    register_start = PythonOperator(
        task_id="register_start",
        python_callable=register_job_start,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    register_success = PythonOperator(
        task_id="register_success",
        python_callable=register_job_completion,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    register_failure = PythonOperator(
        task_id="register_failure",
        python_callable=register_job_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> register_start >> extract >> transform >> load >> register_success >> end
    [extract, transform, load] >> register_failure
