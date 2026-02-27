import pandas as pd


class TLCLoader:

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    @staticmethod
    def load_taxi(service: str, year: int, month: int) -> pd.DataFrame:
        """
        Load NYC TLC taxi data (yellow or green) for a specific month.
        """
        if service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow' or 'green'")

        url = f"{TLCLoader.BASE_URL}/{service}_tripdata_{year:04d}-{month:02d}.parquet"
        return pd.read_parquet(url)

    @staticmethod
    def load_zones() -> pd.DataFrame:
        """
        Load TLC taxi zone lookup table.
        """
        return pd.read_csv(TLCLoader.ZONE_URL)
