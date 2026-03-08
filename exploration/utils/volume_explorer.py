import os
import re
import uuid
from typing import Any, Sequence, cast

import duckdb

SqlRow = tuple[Any, ...]


class VolumeExplorer:
    _conn: duckdb.DuckDBPyConnection | None = None
    _MONTH_KEY_PATTERN = re.compile(r"^(\d{4})-(\d{2})$")

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
    def _columns(df: duckdb.DuckDBPyRelation) -> list[str]:
        return [str(col) for col in cast(Sequence[Any], df.columns)]

    @staticmethod
    def _fetchone_required(relation: duckdb.DuckDBPyRelation) -> SqlRow:
        row = relation.fetchone()
        if row is None:
            raise ValueError("Expected one row from query but received none")
        return row

    @classmethod
    def _rows_to_relation(
        cls,
        rows: list[SqlRow],
        columns: list[tuple[str, str]],
        order_by: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        conn = cls._get_connection()
        table_name = f"tmp_volume_{uuid.uuid4().hex}"
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

    @classmethod
    def _parse_key(cls, key: str) -> tuple[int, int]:
        match = cls._MONTH_KEY_PATTERN.match(key)
        if not match:
            raise ValueError(
                f"Invalid key '{key}'. Expected format 'YYYY-MM' (example: '2020-04')."
            )
        year = int(match.group(1))
        month = int(match.group(2))
        if month < 1 or month > 12:
            raise ValueError(f"Invalid month in key '{key}'. Month must be 01-12.")
        return year, month

    @staticmethod
    def _detect_pickup_column(df: duckdb.DuckDBPyRelation) -> str:
        candidates = (
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "pickup_datetime",
        )
        columns = VolumeExplorer._columns(df)
        for col in candidates:
            if col in columns:
                return col
        raise ValueError(
            "Could not detect pickup datetime column. Pass a dataframe with one of: "
            "tpep_pickup_datetime, lpep_pickup_datetime, pickup_datetime."
        )

    @staticmethod
    def _row_count(df: duckdb.DuckDBPyRelation) -> int:
        row = VolumeExplorer._fetchone_required(df.aggregate("COUNT(*) AS row_count"))
        return int(row[0])

    @staticmethod
    def _null_percentage(df: duckdb.DuckDBPyRelation) -> tuple[int, int, float]:
        columns = VolumeExplorer._columns(df)
        if not columns:
            return 0, 0, 0.0

        null_expr = " + ".join(
            f"SUM(CASE WHEN {VolumeExplorer._quote_ident(col)} IS NULL THEN 1 ELSE 0 END)"
            for col in columns
        )
        row = VolumeExplorer._fetchone_required(
            df.query(
                "src",
                f"""
                SELECT
                    COUNT(*)::BIGINT AS row_count,
                    ({null_expr})::BIGINT AS null_cells
                FROM src
                """,
            )
        )
        row_count = int(row[0] or 0)
        null_cells = int(row[1] or 0)
        total_cells = row_count * len(columns)
        null_pct = (100.0 * null_cells / total_cells) if total_cells > 0 else 0.0
        return null_cells, total_cells, float(null_pct)

    @classmethod
    def _year_range_label(cls, dfs: dict[str, duckdb.DuckDBPyRelation]) -> str:
        if not dfs:
            return "empty"
        years = sorted({cls._parse_key(key)[0] for key in dfs.keys()})
        start_year = years[0]
        end_year = years[-1]
        return f"{start_year}" if start_year == end_year else f"{start_year}-{end_year}"

    @staticmethod
    def total_records_per_month(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        """
        Return total row count for each dataset key (YYYY-MM).
        """
        rows: list[SqlRow] = []
        for key, df in dfs.items():
            year, month = VolumeExplorer._parse_key(key)
            rows.append((key, year, month, VolumeExplorer._row_count(df)))

        columns = [
            ("dataset", "VARCHAR"),
            ("year", "INTEGER"),
            ("month", "INTEGER"),
            ("total_records", "BIGINT"),
        ]
        return VolumeExplorer._rows_to_relation(
            rows, columns, order_by='"year", "month"'
        )

    @staticmethod
    def average_records_per_day(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        """
        Return average records per active pickup day for each dataset key (YYYY-MM).
        """
        rows: list[SqlRow] = []
        for key, df in dfs.items():
            year, month = VolumeExplorer._parse_key(key)
            pickup_col = VolumeExplorer._detect_pickup_column(df)
            stats = VolumeExplorer._fetchone_required(
                df.query(
                    "src",
                    f"""
                    SELECT
                        COUNT(*) AS total_records,
                        COUNT(DISTINCT CAST({VolumeExplorer._quote_ident(pickup_col)} AS DATE)) AS active_days
                    FROM src
                    WHERE {VolumeExplorer._quote_ident(pickup_col)} IS NOT NULL
                    """,
                )
            )
            total_records = int(stats[0] or 0)
            active_days = int(stats[1] or 0)
            avg_per_day = float(total_records / active_days) if active_days > 0 else 0.0
            rows.append((key, year, month, total_records, active_days, avg_per_day))

        columns = [
            ("dataset", "VARCHAR"),
            ("year", "INTEGER"),
            ("month", "INTEGER"),
            ("total_records", "BIGINT"),
            ("active_days", "INTEGER"),
            ("avg_records_per_day", "DOUBLE"),
        ]
        return VolumeExplorer._rows_to_relation(
            rows, columns, order_by='"year", "month"'
        )

    @staticmethod
    def average_size_by_month_number(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> duckdb.DuckDBPyRelation:
        """
        Average rows for each month number across all years (e.g. all April datasets).
        """
        per_month = VolumeExplorer.total_records_per_month(dfs)
        return per_month.query(
            "m",
            """
            SELECT
                month,
                COUNT(*) AS datasets_count,
                AVG(total_records)::DOUBLE AS avg_records,
                MIN(total_records) AS min_records,
                MAX(total_records) AS max_records
            FROM m
            GROUP BY month
            ORDER BY month
            """,
        )

    @staticmethod
    def volume_overview(
        dfs: dict[str, duckdb.DuckDBPyRelation],
    ) -> dict[str, duckdb.DuckDBPyRelation]:
        """
        Return a full volume report:
        - per_dataset: size for each YYYY-MM dataset
        - overview: overall summary stats
        - avg_by_month_number: average size grouped by month number
        - total_records_per_month: total records per dataset
        - avg_records_per_day: average records per active day per dataset
        """
        per_dataset_rows: list[SqlRow] = []
        row_counts: list[int] = []
        null_pcts: list[float] = []
        for key, df in dfs.items():
            year, month = VolumeExplorer._parse_key(key)
            row_count = VolumeExplorer._row_count(df)
            column_count = len(VolumeExplorer._columns(df))
            cell_count = row_count * column_count
            null_cells, total_cells, null_pct = VolumeExplorer._null_percentage(df)
            per_dataset_rows.append(
                (
                    key,
                    year,
                    month,
                    row_count,
                    column_count,
                    cell_count,
                    null_cells,
                    total_cells,
                    null_pct,
                )
            )
            row_counts.append(row_count)
            null_pcts.append(null_pct)

        per_dataset = VolumeExplorer._rows_to_relation(
            per_dataset_rows,
            [
                ("dataset", "VARCHAR"),
                ("year", "INTEGER"),
                ("month", "INTEGER"),
                ("rows_count", "BIGINT"),
                ("columns_count", "INTEGER"),
                ("cells_count", "BIGINT"),
                ("null_cells", "BIGINT"),
                ("total_cells", "BIGINT"),
                ("null_pct", "DOUBLE"),
            ],
            order_by='"year", "month"',
        )

        if row_counts:
            total_rows = sum(row_counts)
            min_rows = min(row_counts)
            max_rows = max(row_counts)
            avg_rows = float(total_rows / len(row_counts))
        else:
            total_rows = 0
            min_rows = 0
            max_rows = 0
            avg_rows = 0.0
        avg_null_pct = float(sum(null_pcts) / len(null_pcts)) if null_pcts else 0.0

        overview = VolumeExplorer._rows_to_relation(
            [
                (
                    len(per_dataset_rows),
                    total_rows,
                    avg_rows,
                    min_rows,
                    max_rows,
                    avg_null_pct,
                )
            ],
            [
                ("datasets_count", "INTEGER"),
                ("total_records", "BIGINT"),
                ("avg_records_per_month", "DOUBLE"),
                ("min_records_month", "BIGINT"),
                ("max_records_month", "BIGINT"),
                ("avg_null_pct", "DOUBLE"),
            ],
        )

        return {
            "per_dataset": per_dataset,
            "overview": overview,
            "avg_by_month_number": VolumeExplorer.average_size_by_month_number(dfs),
            "total_records_per_month": VolumeExplorer.total_records_per_month(dfs),
            "avg_records_per_day": VolumeExplorer.average_records_per_day(dfs),
        }

    @staticmethod
    def save_report_excel(
        dfs: dict[str, duckdb.DuckDBPyRelation],
        output_dir: str = ".",
    ) -> str:
        """
        Save volume_overview report to Excel.
        File name is based on the year range found in dataset keys.
        """
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("pandas is required to export Excel reports.") from exc

        report = VolumeExplorer.volume_overview(dfs)
        year_range = VolumeExplorer._year_range_label(dfs)
        file_name = f"{year_range}.xlsx"
        file_path = os.path.join(output_dir, file_name)

        writer_ctx = cast(Any, pd.ExcelWriter(file_path))
        with writer_ctx as excel_writer:
            cast(Any, report["overview"].df()).to_excel(
                excel_writer, sheet_name="overview", index=False
            )
            cast(Any, report["per_dataset"].df()).to_excel(
                excel_writer, sheet_name="per_dataset", index=False
            )
            cast(Any, report["avg_by_month_number"].df()).to_excel(
                excel_writer, sheet_name="avg_by_month", index=False
            )
            cast(Any, report["total_records_per_month"].df()).to_excel(
                excel_writer, sheet_name="records_per_month", index=False
            )
            cast(Any, report["avg_records_per_day"].df()).to_excel(
                excel_writer, sheet_name="avg_records_day", index=False
            )

        return file_path
