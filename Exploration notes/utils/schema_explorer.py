import os
import re
import uuid

import duckdb


class SchemaExplorer:
    _conn: duckdb.DuckDBPyConnection | None = None

    @classmethod
    def _get_connection(cls) -> duckdb.DuckDBPyConnection:
        if cls._conn is None:
            cls._conn = duckdb.connect()
            cpu_count = os.cpu_count() or 2
            threads = max(1, cpu_count - 1)
            cls._conn.execute(f"PRAGMA threads={threads}")
        return cls._conn

    @staticmethod
    def _quote_ident(value: str) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _safe_column_name(value: str) -> str:
        clean = re.sub(r"[^0-9A-Za-z_]", "_", value).strip("_")
        if not clean:
            clean = "dataset"
        if clean[0].isdigit():
            clean = f"_{clean}"
        return clean

    @classmethod
    def _rows_to_relation(
        cls,
        rows: list[tuple],
        columns: list[tuple[str, str]],
        order_by: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        conn = cls._get_connection()
        table_name = f"tmp_schema_{uuid.uuid4().hex}"
        col_defs = ", ".join(
            f"{cls._quote_ident(name)} {dtype}" for name, dtype in columns
        )
        conn.execute(f"CREATE TEMP TABLE {cls._quote_ident(table_name)} ({col_defs})")
        if rows:
            placeholders = ", ".join(["?"] * len(columns))
            conn.executemany(
                f"INSERT INTO {cls._quote_ident(table_name)} VALUES ({placeholders})",
                rows,
            )

        query = f"SELECT * FROM {cls._quote_ident(table_name)}"
        if order_by:
            query += f" ORDER BY {order_by}"
        return conn.sql(query)

    @staticmethod
    def compare_columns(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        all_columns = sorted(set().union(*[set(df.columns) for df in dfs.values()]))

        dataset_cols: list[tuple[str, str, set[str]]] = []
        used_names: set[str] = set()
        for original_name, df in dfs.items():
            safe_name = SchemaExplorer._safe_column_name(original_name)
            candidate = safe_name
            suffix = 1
            while candidate in used_names:
                suffix += 1
                candidate = f"{safe_name}_{suffix}"
            used_names.add(candidate)
            dataset_cols.append((original_name, candidate, set(df.columns)))

        rows: list[tuple] = []
        for col in all_columns:
            row = [col]
            for _, _, cols in dataset_cols:
                row.append(col in cols)
            rows.append(tuple(row))

        columns = [("column", "VARCHAR")] + [
            (safe_name, "BOOLEAN") for _, safe_name, _ in dataset_cols
        ]
        return SchemaExplorer._rows_to_relation(rows, columns, order_by='"column"')

    @staticmethod
    def compare_dtypes(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        all_columns = sorted(set().union(*[set(df.columns) for df in dfs.values()]))

        dataset_cols: list[tuple[str, str, dict[str, str]]] = []
        used_names: set[str] = set()
        for original_name, df in dfs.items():
            safe_name = SchemaExplorer._safe_column_name(original_name)
            candidate = safe_name
            suffix = 1
            while candidate in used_names:
                suffix += 1
                candidate = f"{safe_name}_{suffix}"
            used_names.add(candidate)
            dtype_map = dict(zip(df.columns, map(str, df.types)))
            dataset_cols.append((original_name, candidate, dtype_map))

        rows: list[tuple] = []
        for col in all_columns:
            row = [col]
            for _, _, dtype_map in dataset_cols:
                row.append(dtype_map.get(col))
            rows.append(tuple(row))

        columns = [("column", "VARCHAR")] + [
            (safe_name, "VARCHAR") for _, safe_name, _ in dataset_cols
        ]
        return SchemaExplorer._rows_to_relation(rows, columns, order_by='"column"')

    @staticmethod
    def show_schema_differences(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        all_columns = sorted(set().union(*[set(df.columns) for df in dfs.values()]))

        dataset_cols: list[tuple[str, str, dict[str, str]]] = []
        used_names: set[str] = set()
        for original_name, df in dfs.items():
            safe_name = SchemaExplorer._safe_column_name(original_name)
            candidate = safe_name
            suffix = 1
            while candidate in used_names:
                suffix += 1
                candidate = f"{safe_name}_{suffix}"
            used_names.add(candidate)
            dtype_map = dict(zip(df.columns, map(str, df.types)))
            dataset_cols.append((original_name, candidate, dtype_map))

        rows: list[tuple] = []
        for col in all_columns:
            values = [dtype_map.get(col) for _, _, dtype_map in dataset_cols]
            non_null_values = {value for value in values if value is not None}
            if len(non_null_values) > 1:
                rows.append(tuple([col] + values))

        columns = [("column", "VARCHAR")] + [
            (safe_name, "VARCHAR") for _, safe_name, _ in dataset_cols
        ]
        return SchemaExplorer._rows_to_relation(rows, columns, order_by='"column"')

    @staticmethod
    def schema_signature(df: duckdb.DuckDBPyRelation):
        return tuple(sorted((col, str(dtype)) for col, dtype in zip(df.columns, df.types)))

    @staticmethod
    def basic_profile(df: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        selects: list[str] = []
        for col, dtype in zip(df.columns, df.types):
            quoted_col = SchemaExplorer._quote_ident(col)
            col_literal = col.replace("'", "''")
            dtype_literal = str(dtype).replace("'", "''")
            selects.append(
                f"""
                SELECT
                    '{col_literal}' AS column,
                    '{dtype_literal}' AS dtype,
                    CASE
                        WHEN COUNT(*) = 0 THEN 0.0
                        ELSE 100.0 * SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) / COUNT(*)
                    END AS null_pct,
                    COUNT(DISTINCT {quoted_col}) AS n_unique
                FROM src
                """
            )

        if not selects:
            conn = SchemaExplorer._get_connection()
            return conn.sql(
                "SELECT CAST(NULL AS VARCHAR) AS column, CAST(NULL AS VARCHAR) AS dtype, "
                "CAST(NULL AS DOUBLE) AS null_pct, CAST(NULL AS BIGINT) AS n_unique WHERE FALSE"
            )

        query = "\nUNION ALL\n".join(selects) + "\nORDER BY null_pct DESC"
        return df.query("src", query)
