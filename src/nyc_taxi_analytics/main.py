import pandas as pd

DEFAULT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"


def load_data(url: str = DEFAULT_URL) -> pd.DataFrame:
    return pd.read_parquet(url)


def filter_by_hour(df: pd.DataFrame, start_hour: int, end_hour: int) -> pd.DataFrame:
    hour = df["tpep_pickup_datetime"].dt.hour
    return df[(hour >= start_hour) & (hour < end_hour)]


def get_key_statistics(df: pd.DataFrame) -> dict:
    return {
        "total_trips": len(df),
        "average_fare": df["fare_amount"].mean(),
        "average_distance": df["trip_distance"].mean(),
    }


def get_data_info(df: pd.DataFrame) -> dict:
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "column_count": len(df.columns),
    }