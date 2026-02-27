import pandas as pd


class SchemaExplorer:

    @staticmethod
    def compare_columns(dfs: dict) -> pd.DataFrame:
        all_columns = set().union(*[set(df.columns) for df in dfs.values()])
        comparison = {}

        for name, df in dfs.items():
            comparison[name] = [col in df.columns for col in all_columns]

        return pd.DataFrame(comparison, index=sorted(all_columns))

    @staticmethod
    def compare_dtypes(dfs: dict) -> pd.DataFrame:
        all_columns = set().union(*[set(df.columns) for df in dfs.values()])
        rows = []

        for col in sorted(all_columns):
            row = {"column": col}
            for name, df in dfs.items():
                if col in df.columns:
                    row[name] = str(df[col].dtype)
                else:
                    row[name] = None
            rows.append(row)

        return pd.DataFrame(rows).set_index("column")

    @staticmethod
    def show_schema_differences(dfs: dict) -> pd.DataFrame:
        dtypes = SchemaExplorer.compare_dtypes(dfs)
        return dtypes[dtypes.nunique(axis=1) > 1]

    @staticmethod
    def schema_signature(df: pd.DataFrame):
        return tuple(sorted((col, str(dtype)) for col, dtype in df.dtypes.items()))

    @staticmethod
    def basic_profile(df: pd.DataFrame) -> pd.DataFrame:
        profile = []

        for col in df.columns:
            profile.append(
                {
                    "column": col,
                    "dtype": str(df[col].dtype),
                    "null_pct": df[col].isna().mean() * 100,
                    "n_unique": df[col].nunique(),
                }
            )

        return pd.DataFrame(profile).sort_values("null_pct", ascending=False)
