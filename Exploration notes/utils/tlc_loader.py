import os
import random
import re
import time

import duckdb


class TLCLoader:

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    HTTP_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
    _conn: duckdb.DuckDBPyConnection | None = None

    @staticmethod
    def _quote_ident(value: str) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    @staticmethod
    def _safe_table_name(value: str) -> str:
        clean = re.sub(r"[^0-9A-Za-z_]", "_", value).strip("_")
        if not clean:
            clean = "tlc_data"
        if clean[0].isdigit():
            clean = f"_{clean}"
        return clean

    @classmethod
    def _get_connection(cls) -> duckdb.DuckDBPyConnection:
        if cls._conn is None:
            cls._conn = duckdb.connect()
            cpu_count = os.cpu_count() or 2
            threads = max(1, cpu_count - 1)
            cls._conn.execute(f"PRAGMA threads={threads}")
            # Best effort HTTP config for remote parquet on TLC CDN.
            for statement in (
                "INSTALL httpfs;",
                "LOAD httpfs;",
                f"SET custom_user_agent='{cls.HTTP_USER_AGENT}';",
                "SET http_retries=3;",
            ):
                try:
                    cls._conn.execute(statement)
                except duckdb.Error:
                    pass
        return cls._conn

    @staticmethod
    def load_taxi(
        service: str,
        year: int,
        month: int,
        max_retries: int = 5,
        retry_base_delay_seconds: float = 60.0,
        retry_jitter_seconds: float = 0.4,
        log_progress: bool = False,
        log_prefix: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """
        Load NYC TLC taxi data (yellow or green) for a specific month as DuckDB relation.
        """
        if service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow' or 'green'")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_base_delay_seconds < 0:
            raise ValueError("retry_base_delay_seconds must be >= 0")
        if retry_jitter_seconds < 0:
            raise ValueError("retry_jitter_seconds must be >= 0")

        url = f"{TLCLoader.BASE_URL}/{service}_tripdata_{year:04d}-{month:02d}.parquet"
        attempts = max_retries + 1
        last_error: duckdb.Error | None = None
        for attempt in range(1, attempts + 1):
            try:
                conn = TLCLoader._get_connection()
                table_name = TLCLoader._safe_table_name(
                    f"tlc_{service}_{year:04d}_{month:02d}"
                )
                quoted_table = TLCLoader._quote_ident(table_name)
                conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                conn.execute(
                    f"CREATE TEMP TABLE {quoted_table} AS "
                    f"SELECT * FROM read_parquet('{url}')"
                )
                relation = conn.sql(f"SELECT * FROM {quoted_table}")
                if log_progress and attempt > 1:
                    prefix = f"{log_prefix} " if log_prefix else ""
                    print(f"{prefix}loaded after retry {attempt}/{attempts}")
                return relation
            except duckdb.Error as exc:
                last_error = exc
                if attempt == attempts:
                    break
                backoff = retry_base_delay_seconds * (2 ** (attempt - 1))
                jitter = random.uniform(0.0, retry_jitter_seconds)
                wait_seconds = backoff + jitter
                if log_progress:
                    prefix = f"{log_prefix} " if log_prefix else ""
                    print(
                        f"{prefix}retry {attempt}/{attempts} failed, waiting "
                        f"{wait_seconds:.2f}s before next attempt"
                    )
                time.sleep(wait_seconds)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Unexpected error while loading taxi data")

    @staticmethod
    def multiple_month_load_taxi(
        service: str,
        years_range: tuple[int, int],
        months: list[int],
        delay_seconds: float = 0.25,
        max_retries: int = 5,
        retry_base_delay_seconds: float = 60.0,
        retry_jitter_seconds: float = 0.4,
        log_progress: bool = True,
    ) -> dict[str, duckdb.DuckDBPyRelation]:
        """
        Load NYC TLC taxi data for all year/month combinations in an inclusive year range.

        Returns:
            A dictionary where each key is formatted as 'YYYY-MM' and each value is
            the corresponding DuckDB relation.
        """
        if service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow' or 'green'")
        if len(years_range) != 2:
            raise ValueError("years_range must be a tuple like (start_year, end_year)")
        start_year, end_year = years_range
        if start_year > end_year:
            raise ValueError("years_range must have start_year <= end_year")
        if not months:
            raise ValueError("months must contain at least one month")
        if any(month < 1 or month > 12 for month in months):
            raise ValueError("months must contain values between 1 and 12")
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")

        dfs: dict[str, duckdb.DuckDBPyRelation] = {}
        total = (end_year - start_year + 1) * len(months)
        done = 0
        if log_progress:
            print(
                f"Starting load: service={service}, years={start_year}-{end_year}, "
                f"months={months}, total={total}"
            )
        for year in range(start_year, end_year + 1):
            for month in months:
                key = f"{year:04d}-{month:02d}"
                done += 1
                if log_progress:
                    print(f"[{done}/{total}] loading {service} {key}")
                dfs[key] = TLCLoader.load_taxi(
                    service,
                    year,
                    month,
                    max_retries=max_retries,
                    retry_base_delay_seconds=retry_base_delay_seconds,
                    retry_jitter_seconds=retry_jitter_seconds,
                    log_progress=log_progress,
                    log_prefix=f"[{done}/{total}] {service} {key}",
                )
                if log_progress:
                    print(f"[{done}/{total}] done {service} {key}")
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
        if log_progress:
            print(f"Completed load for service={service}, loaded={len(dfs)} dataset(s)")
        return dfs

    @staticmethod
    def load_zones() -> duckdb.DuckDBPyRelation:
        """
        Load TLC taxi zone lookup table as DuckDB relation.
        """
        conn = TLCLoader._get_connection()
        return conn.sql(f"SELECT * FROM read_csv_auto('{TLCLoader.ZONE_URL}')")
