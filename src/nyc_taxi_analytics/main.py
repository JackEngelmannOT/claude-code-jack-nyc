import pandas as pd


def load_data(url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet") -> pd.DataFrame:
    return pd.read_parquet(url)


def get_key_statistics(df: pd.DataFrame) -> dict:
    return {
        "total_trips": len(df),
        "average_fare": df["fare_amount"].mean(),
        "average_distance": df["trip_distance"].mean(),
    }


if __name__ == "__main__":
    df = load_data()
    stats = get_key_statistics(df)
    print(f"Total Trips: {stats['total_trips']:,}")
    print(f"Average Fare: ${stats['average_fare']:.2f}")
    print(f"Average Distance: {stats['average_distance']:.2f} miles")