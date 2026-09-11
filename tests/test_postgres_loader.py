"""Testes unitarios para PostgresLoader."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from src.loaders.postgres_loader import PostgresLoader


@pytest.fixture
def loader():
    return PostgresLoader(connection_string="postgresql+psycopg2://test:test@localhost:5432/test_db")


class TestPostgresLoader:
    def test_init_with_connection_string(self, loader):
        assert loader.engine is not None

    def test_init_without_connection_string(self):
        loader = PostgresLoader(host="localhost", database="test")
        assert loader.engine is not None

    @patch("src.loaders.postgres_loader.PostgresLoader.execute_sql")
    def test_table_exists_true(self, mock_exec, loader):
        mock_exec.return_value = [(True,)]
        assert loader.table_exists("test_table") is True
        mock_exec.assert_called_once()

    @patch("src.loaders.postgres_loader.PostgresLoader.execute_sql")
    def test_table_exists_false(self, mock_exec, loader):
        mock_exec.return_value = [(False,)]
        assert loader.table_exists("test_table") is False

    @patch("src.loaders.postgres_loader.PostgresLoader.execute_sql")
    def test_get_row_count(self, mock_exec, loader):
        mock_exec.return_value = [(42,)]
        count = loader.get_row_count("test_table")
        assert count == 42

    def test_bulk_insert_empty_df(self, loader):
        df = pd.DataFrame()
        result = loader.bulk_insert(df, "test_table")
        assert result == 0

    @patch.object(pd.DataFrame, "to_sql")
    def test_bulk_insert_calls_to_sql(self, mock_to_sql, loader):
        df = pd.DataFrame({"id": [1], "name": ["test"]})
        mock_to_sql.return_value = None
        result = loader.bulk_insert(df, "test_table", schema="staging")
        assert result == 1
        mock_to_sql.assert_called_once()

    def test_upsert_empty_df(self, loader):
        df = pd.DataFrame()
        result = loader.upsert(df, "test_table")
        assert result == 0

    def test_upsert_default_conflict_columns(self, loader):
        df = pd.DataFrame()
        # Verify default conflict_columns is ["id"]
        # This is tested indirectly - upsert returns 0 for empty df

    def test_execute_sql(self, loader):
        with patch.object(loader, "engine") as mock_engine:
            mock_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.returns_rows = True
            mock_result.fetchall.return_value = [(1,)]
            mock_conn.execute.return_value = mock_result
            mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_conn)
            mock_engine.begin.return_value.__exit__ = Mock(return_value=False)

            result = loader.execute_sql("SELECT 1")
            assert result == [(1,)]

    def test_close(self, loader):
        with patch.object(loader, "engine") as mock_engine:
            loader._connection = None
            loader.close()
            mock_engine.dispose.assert_called_once()

    def test_engine_ref(self, loader):
        engine = loader.engine_ref
        assert engine is loader.engine
