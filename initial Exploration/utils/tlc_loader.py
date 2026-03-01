import os

import duckdb


class TLCLoader:

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
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
    def load_taxi(service: str, year: int, month: int) -> duckdb.DuckDBPyRelation:
        """
        Load NYC TLC taxi data (yellow or green) for a specific month as DuckDB relation.
        """
        if service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow' or 'green'")

        url = f"{TLCLoader.BASE_URL}/{service}_tripdata_{year:04d}-{month:02d}.parquet"
        conn = TLCLoader._get_connection()
        return conn.sql(f"SELECT * FROM read_parquet('{url}')")

    @staticmethod
    def load_zones() -> duckdb.DuckDBPyRelation:
        """
        Load TLC taxi zone lookup table as DuckDB relation.
        """
        conn = TLCLoader._get_connection()
        return conn.sql(f"SELECT * FROM read_csv_auto('{TLCLoader.ZONE_URL}')")
