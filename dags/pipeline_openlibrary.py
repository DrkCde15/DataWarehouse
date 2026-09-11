"""DAG para extrair dados do Open Library API."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def extract_openlibrary_books(**context) -> None:
    """Busca livros do Open Library por subjects."""
    from src.extractors.api_client import APIClient
    from airflow.models import Variable
    import json

    client = APIClient(
        base_url=Variable.get("openlibrary_base_url"),
        rate_limit=1,
        headers={"User-Agent": Variable.get("openlibrary_user_agent")},
    )

    subjects = ["fiction", "science", "history", "technology", "philosophy"]
    all_books = []

    for subject in subjects:
        endpoint = f"/subjects/{subject}.json"
        records = client.get_all_pages(
            endpoint=endpoint,
            pagination_type="none",
            params={"limit": 50},
            results_key="works",
        )
        for book in records:
            book["subject"] = subject
        all_books.extend(records)

    context["ti"].xcom_push(key="raw_data", value=json.dumps(all_books))
    context["ti"].xcom_push(key="record_count", value=len(all_books))


def transform_openlibrary_books(**context) -> None:
    """Transforma dados brutos do Open Library."""
    import pandas as pd
    import json

    ti = context["ti"]
    raw_json = ti.xcom_pull(key="raw_data", task_ids="extract")
    records = json.loads(raw_json)

    df = pd.DataFrame(records)

    # Mapear key para olid
    if "key" in df.columns:
        df = df.rename(columns={"key": "olid"})

    # Converter authors para JSON string
    if "authors" in df.columns:
        df["authors"] = df["authors"].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    # Converter ia_collection para JSON string
    if "ia_collection" in df.columns:
        df["ia_collection"] = df["ia_collection"].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    # Converter availability para JSON string
    if "availability" in df.columns:
        df["availability"] = df["availability"].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict)) else x
        )

    # Gerar cover_url a partir de cover_id
    if "cover_id" in df.columns:
        df["cover_url"] = df["cover_id"].apply(
            lambda x: f"https://covers.openlibrary.org/b/id/{int(x)}-L.jpg" if pd.notna(x) else None
        )

    ti.xcom_push(key="clean_data", value=json.dumps(df.to_dict(orient="records")))
    ti.xcom_push(key="clean_count", value=len(df))


def load_openlibrary_books(**context) -> None:
    """Insere dados no PostgreSQL."""
    from src.loaders.postgres_loader import PostgresLoader
    from airflow.models import Variable
    import pandas as pd
    import json

    ti = context["ti"]
    clean_json = ti.xcom_pull(key="clean_data", task_ids="transform")
    records = json.loads(clean_json)
    df = pd.DataFrame(records)

    loader = PostgresLoader(connection_string=Variable.get("db_connection_string"))

    try:
        # Filtrar colunas existentes na tabela
        cols_in_table = loader.execute_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'staging' AND table_name = 'openlibrary_books'"
        )
        valid_cols = {row[0] for row in cols_in_table}
        cols_to_keep = [c for c in df.columns if c in valid_cols]
        if cols_to_keep:
            df = df[cols_to_keep]

        rows_loaded = loader.bulk_insert(
            df=df, table="openlibrary_books", schema="staging"
        )
        ti.xcom_push(key="rows_loaded", value=rows_loaded)
    finally:
        loader.close()


with DAG(
    dag_id="pipeline_openlibrary",
    default_args=DEFAULT_ARGS,
    description="Pipeline ETL: Open Library → Staging",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "openlibrary", "books"],
) as dag:

    start = EmptyOperator(task_id="start")
    extract = PythonOperator(task_id="extract", python_callable=extract_openlibrary_books)
    transform = PythonOperator(task_id="transform", python_callable=transform_openlibrary_books)
    load = PythonOperator(task_id="load", python_callable=load_openlibrary_books)
    end = EmptyOperator(task_id="end")

    start >> extract >> transform >> load >> end
