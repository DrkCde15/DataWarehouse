"""Funções de limpeza e transformação de dados."""

import re
from datetime import datetime, timezone
from typing import Any
import pandas as pd


class DataCleaner:
    """Classe para limpeza e transformação de DataFrames extraídos de APIs."""

    def __init__(self, timestamp_col: str = "_extracted_at") -> None:
        self.timestamp_col = timestamp_col

    def clean_dataframe(
        self,
        data: list[dict[str, Any]] | pd.DataFrame,
        drop_duplicates: bool = True,
        null_threshold: float = 0.95,
    ) -> pd.DataFrame:
        """
        Pipeline completo de limpeza.

        Args:
            data: Lista de dicts ou DataFrame
            drop_duplicates: Remove linhas duplicadas
            null_threshold: Remove colunas com mais de X% de nulos (0.0 a 1.0)

        Returns:
            DataFrame limpo
        """
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()

        if df.empty:
            return df

        df = self.standardize_column_names(df)
        df = self.remove_high_null_columns(df, threshold=null_threshold)
        df = self.handle_null_values(df)

        if drop_duplicates:
            df = self.remove_duplicates(df)

        df = self.infer_and_convert_types(df)
        df = self.add_metadata_columns(df)

        return df

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza nomes das colunas: snake_case, sem caracteres especiais."""
        new_columns: list[str] = []
        for col in df.columns:
            name = col.lower().strip()
            name = re.sub(r"[^a-z0-9_]", "_", name)
            name = re.sub(r"_+", "_", name)
            name = name.strip("_")
            new_columns.append(name)

        df.columns = new_columns
        return df

    def remove_high_null_columns(self, df: pd.DataFrame, threshold: float = 0.95) -> pd.DataFrame:
        """Remove colunas com proporção de nulos acima do threshold."""
        null_ratio = df.isnull().mean()
        cols_to_drop = null_ratio[null_ratio > threshold].index.tolist()

        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        return df

    def handle_null_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Trata valores nulos conforme o tipo de dados."""
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            elif pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].fillna("")
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].fillna(pd.NaT)
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove linhas completamente duplicadas."""
        import json as _json

        before = len(df)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: _json.dumps(x) if isinstance(x, (dict, list)) else x)
        df = df.drop_duplicates()
        removed = before - len(df)
        if removed > 0:
            print(f"Removidas {removed} linhas duplicadas")
        return df

    def infer_and_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tenta inferir e converter tipos de dados para formatos mais adequados."""
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_numeric_dtype(df[col]):
                has_complex = df[col].apply(lambda x: isinstance(x, (dict, list))).any()
                if has_complex:
                    continue

                converted = pd.to_datetime(df[col], errors="coerce", utc=True)
                not_null_mask = df[col].notna()
                if not_null_mask.sum() > 0 and converted.notna().sum() / not_null_mask.sum() > 0.8:
                    df[col] = converted
                    continue

                try:
                    numeric = pd.to_numeric(df[col], errors="coerce")
                    if numeric.notna().sum() / max(df[col].notna().sum(), 1) > 0.8:
                        df[col] = numeric
                        continue
                except (ValueError, TypeError):
                    pass

        return df

    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas de metadados de ingestão."""
        df[self.timestamp_col] = datetime.now(timezone.utc)
        return df

    @staticmethod
    def flatten_nested_dict(
        data: list[dict[str, Any]],
        parent_key: str = "",
        separator: str = "_",
    ) -> list[dict[str, Any]]:
        """
        Achata dicionários aninhados em um nível.
        Ex: {"endereco": {"cidade": "SP"}} -> {"endereco_cidade": "SP"}
        """
        flat_list: list[dict[str, Any]] = []

        for record in data:
            flat_record: dict[str, Any] = {}
            _flatten(record, flat_record, parent_key, separator)
            flat_list.append(flat_record)

        return flat_list

    @staticmethod
    def normalize_dataframe(
        df: pd.DataFrame,
        key_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Normaliza coluna de listas/dict para múltiplas linhas."""
        exploded = df.explode(value_col)
        normalized = pd.json_normalize(exploded[value_col])
        result = pd.concat(
            [exploded[[key_col]].reset_index(drop=True), normalized.reset_index(drop=True)],
            axis=1,
        )
        return result

    @staticmethod
    def validate_schema(
        df: pd.DataFrame,
        expected_columns: list[str],
        required_columns: list[str] | None = None,
    ) -> tuple[bool, list[str]]:
        """
        Valida se o DataFrame possui as colunas esperadas.

        Returns:
            Tupla (é_válido, lista_de_erros)
        """
        errors: list[str] = []
        missing = set(expected_columns) - set(df.columns)

        if missing:
            errors.append(f"Colunas ausentes: {missing}")

        if required_columns:
            for col in required_columns:
                if col in df.columns and df[col].isnull().any():
                    null_count = df[col].isnull().sum()
                    errors.append(f"Coluna obrigatória '{col}' possui {null_count} valores nulos")

        return len(errors) == 0, errors


def _flatten(
    d: dict[str, Any],
    result: dict[str, Any],
    parent_key: str,
    separator: str,
) -> None:
    """Função auxiliar recursiva para achatar dicionários."""
    for key, value in d.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            _flatten(value, result, new_key, separator)
        elif isinstance(value, list) and all(isinstance(i, dict) for i in value):
            result[new_key] = str(value)
        else:
            result[new_key] = value
