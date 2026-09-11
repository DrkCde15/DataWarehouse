"""Testes unitarios para DataCleaner."""

import json
import pytest
import pandas as pd
from src.transformers.cleaner import DataCleaner


@pytest.fixture
def cleaner():
    return DataCleaner(timestamp_col="_extracted_at")


class TestCleaner:
    def test_clean_list_of_dicts(self, cleaner):
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        result = cleaner.clean_dataframe(data, drop_duplicates=False)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "_extracted_at" in result.columns

    def test_clean_empty_list(self, cleaner):
        result = cleaner.clean_dataframe([])
        assert result.empty

    def test_clean_dataframe_input(self, cleaner):
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        result = cleaner.clean_dataframe(df, drop_duplicates=False)
        assert len(result) == 2

    def test_standardize_column_names(self, cleaner):
        df = pd.DataFrame({"CamelCase": [1], "with-dash": [2], "UPPER": [3]})
        result = cleaner.standardize_column_names(df)
        assert list(result.columns) == ["camelcase", "with_dash", "upper"]

    def test_remove_high_null_columns(self, cleaner):
        df = pd.DataFrame(
            {
                "good": [1, 2, 3],
                "bad": [None, None, None],
            }
        )
        result = cleaner.remove_high_null_columns(df, threshold=0.5)
        assert "good" in result.columns
        assert "bad" not in result.columns

    def test_handle_null_values(self, cleaner):
        df = pd.DataFrame(
            {
                "nums": pd.array([1.0, None, 3.0], dtype="float64"),
                "strs": pd.array(["a", None, "c"], dtype="object"),
            }
        )
        result = cleaner.handle_null_values(df)
        assert result["nums"].iloc[1] == 0
        assert result["strs"].iloc[1] == ""

    def test_remove_duplicates(self, cleaner):
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        result = cleaner.clean_dataframe(data)
        assert len(result) == 2

    def test_remove_duplicates_with_dict_values(self, cleaner):
        data = [
            {"id": 1, "meta": {"key": "val"}},
            {"id": 1, "meta": {"key": "val"}},
        ]
        result = cleaner.clean_dataframe(data)
        assert len(result) == 1

    def test_flatten_nested_dict(self):
        data = [{"id": 1, "address": {"city": "SP", "zip": "01000"}}]
        result = DataCleaner.flatten_nested_dict(data)
        assert result[0]["address_city"] == "SP"
        assert result[0]["address_zip"] == "01000"
        assert "address" not in result[0]

    def test_flatten_nested_dict_list_of_dicts(self):
        data = [{"id": 1, "tags": [{"name": "a"}]}]
        result = DataCleaner.flatten_nested_dict(data)
        assert isinstance(result[0]["tags"], str)

    def test_validate_schema_valid(self, cleaner):
        df = pd.DataFrame({"id": [1], "name": ["test"]})
        valid, errors = cleaner.validate_schema(df, ["id", "name"])
        assert valid is True
        assert errors == []

    def test_validate_schema_missing_columns(self, cleaner):
        df = pd.DataFrame({"id": [1]})
        valid, errors = cleaner.validate_schema(df, ["id", "name"])
        assert valid is False
        assert len(errors) == 1

    def test_validate_schema_null_required(self, cleaner):
        df = pd.DataFrame({"id": [1, None], "name": ["a", "b"]})
        valid, errors = cleaner.validate_schema(
            df, ["id", "name"], required_columns=["id"]
        )
        assert valid is False

    def test_normalize_dataframe(self, cleaner):
        df = pd.DataFrame(
            {
                "user": ["alice", "bob"],
                "skills": [[{"name": "python"}, {"name": "sql"}], [{"name": "java"}]],
            }
        )
        result = cleaner.normalize_dataframe(df, "user", "skills")
        assert len(result) == 3
        assert "name" in result.columns

    def test_infer_and_convert_numeric(self, cleaner):
        df = pd.DataFrame({"val": pd.array(["1", "2", "3"], dtype="object")})
        result = cleaner.infer_and_convert_types(df)
        assert pd.api.types.is_numeric_dtype(result["val"])

    def test_infer_and_convert_datetime(self, cleaner):
        df = pd.DataFrame({"date": pd.array(["2024-01-01", "2024-01-02"], dtype="object")})
        result = cleaner.infer_and_convert_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["date"])
