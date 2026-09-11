"""Módulo de carga para PostgreSQL com suporte a bulk insert e upsert."""

import logging
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Loader para inserção e atualização de dados em PostgreSQL."""

    def __init__(
        self,
        connection_string: str | None = None,
        host: str = "localhost",
        port: int = 5432,
        database: str = "data_warehouse",
        user: str = "postgres",
        password: str = "",
    ) -> None:
        if connection_string:
            self.engine = create_engine(connection_string)
        else:
            self.engine = create_engine(
                f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            )
        self._connection = None

    @property
    def engine_ref(self) -> Engine:
        """Retorna a instância do engine SQLAlchemy."""
        return self.engine

    def bulk_insert(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "staging",
        chunk_size: int = 5000,
        if_exists: str = "append",
    ) -> int:
        """
        Insere dados em massa na tabela.

        Args:
            df: DataFrame com os dados
            table: Nome da tabela destino
            schema: Schema do banco
            chunk_size: Tamanho do chunk para inserção em lote
            if_exists: Comportamento se tabela existir ('append', 'replace', 'fail')

        Returns:
            Número de linhas inseridas
        """
        if df.empty:
            logger.warning("DataFrame vazio, nada para inserir")
            return 0

        logger.info(f"Inserindo {len(df)} registros em {schema}.{table}")

        df.to_sql(
            name=table,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
            method="multi",
        )

        logger.info(f"Inserção concluída: {len(df)} registros em {schema}.{table}")
        return len(df)

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "staging",
        conflict_columns: list[str] | None = None,
        update_columns: list[str] | None = None,
        chunk_size: int = 5000,
    ) -> int:
        """
        Insere ou atualiza registros (ON CONFLICT DO UPDATE).

        Args:
            df: DataFrame com os dados
            table: Nome da tabela destino
            schema: Schema do banco
            conflict_columns: Colunas que definem o conflito (PRIMARY KEY ou UNIQUE)
            update_columns: Colunas para atualizar no conflito (None = todas exceto conflito)
            chunk_size: Tamanho do chunk

        Returns:
            Número de registros processados
        """
        if df.empty:
            logger.warning("DataFrame vazio, nada para processar")
            return 0

        if conflict_columns is None:
            conflict_columns = ["id"]

        total_processed = 0

        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            records = chunk.to_dict(orient="records")

            stmt = pg_insert(
                self._get_table_ref(schema, table)
            ).values(records)

            if update_columns is None:
                update_columns = [c for c in chunk.columns if c not in conflict_columns]

            update_dict = {col: stmt.excluded[col] for col in update_columns}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_dict,
            )

            with self.engine.begin() as conn:
                conn.execute(stmt)

            total_processed += len(records)
            logger.debug(f"Upsert chunk {start}-{start + chunk_size}: {len(records)} registros")

        logger.info(f"Upsert concluído: {total_processed} registros em {schema}.{table}")
        return total_processed

    def execute_sql(self, query: str, params: dict[str, Any] | None = None) -> Any:
        """Executa SQL bruto e retorna o resultado."""
        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            if result.returns_rows:
                return result.fetchall()
            return result.rowcount

    def table_exists(self, table: str, schema: str = "staging") -> bool:
        """Verifica se uma tabela existe no banco."""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """
        result = self.execute_sql(query, {"schema": schema, "table": table})
        return result[0][0] if result else False

    def get_row_count(self, table: str, schema: str = "staging") -> int:
        """Retorna a contagem de linhas de uma tabela."""
        result = self.execute_sql(f"SELECT COUNT(*) FROM {schema}.{table}")
        return result[0][0] if result else 0

    def truncate_table(self, table: str, schema: str = "staging") -> None:
        """Trunca uma tabela (remove todos os dados)."""
        self.execute_sql(f"TRUNCATE TABLE {schema}.{table} CASCADE")
        logger.info(f"Tabela {schema}.{table} truncada")

    def drop_table(self, table: str, schema: str = "staging") -> None:
        """Remove uma tabela."""
        self.execute_sql(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
        logger.info(f"Tabela {schema}.{table} removida")

    def _get_table_ref(self, schema: str, table: str) -> Any:
        """Retorna referência da tabela SQLAlchemy."""
        from sqlalchemy import MetaData, Table

        metadata = MetaData(schema=schema)
        return Table(table, metadata, autoload_with=self.engine)

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        if self._connection:
            self._connection.close()
        self.engine.dispose()
        logger.info("Conexão com PostgreSQL fechada")
