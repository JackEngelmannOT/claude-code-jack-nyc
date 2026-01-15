import pandas as pd

DEFAULT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def load_data(url: str = DEFAULT_URL) -> pd.DataFrame:
    return pd.read_parquet(url)


def filter_by_hour(df: pd.DataFrame, start_hour: int, end_hour: int) -> pd.DataFrame:
    hour = df["tpep_pickup_datetime"].dt.hour
    return df[(hour >= start_hour) & (hour < end_hour)]


def get_key_statistics(df: pd.DataFrame) -> dict:
    valid_distances = df[df["trip_distance"] > 0]["trip_distance"]
    return {
        "total_trips": len(df),
        "average_fare": df["fare_amount"].mean(),
        "average_distance": valid_distances.mean(),
    }


def get_data_info(df: pd.DataFrame) -> dict:
    return {
        "row_count": len(df),
        "columns": list(df.columns),
        "column_count": len(df.columns),
    }


def load_zone_lookup(url: str = ZONE_LOOKUP_URL) -> pd.DataFrame:
    return pd.read_csv(url)


def get_zone_info(zone_lookup: pd.DataFrame, location_id: int) -> dict | None:
    row = zone_lookup[zone_lookup["LocationID"] == location_id]
    if row.empty:
        return None
    row = row.iloc[0]
    return {
        "location_id": int(row["LocationID"]),
        "borough": row["Borough"],
        "zone": row["Zone"],
        "service_zone": row["service_zone"],
    }


def get_top_pickup_zones(
    df: pd.DataFrame, zone_lookup: pd.DataFrame, n: int = 10
) -> list[dict]:
    counts = df["PULocationID"].value_counts().head(n)
    results = []
    for location_id, count in counts.items():
        zone_info = get_zone_info(zone_lookup, int(location_id))
        zone_name = zone_info["zone"] if zone_info else f"Unknown ({location_id})"
        results.append({"zone": zone_name, "trip_count": int(count)})
    return results


def get_top_dropoff_zones(
    df: pd.DataFrame, zone_lookup: pd.DataFrame, n: int = 10
) -> list[dict]:
    counts = df["DOLocationID"].value_counts().head(n)
    results = []
    for location_id, count in counts.items():
        zone_info = get_zone_info(zone_lookup, int(location_id))
        zone_name = zone_info["zone"] if zone_info else f"Unknown ({location_id})"
        results.append({"zone": zone_name, "trip_count": int(count)})
    return results


PAYMENT_TYPES = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided",
}


def get_payment_breakdown(df: pd.DataFrame) -> list[dict]:
    counts = df["payment_type"].value_counts()
    total = len(df)
    results = []
    for payment_code, count in counts.items():
        label = PAYMENT_TYPES.get(int(payment_code), f"Unknown ({payment_code})")
        results.append({
            "payment_type": label,
            "count": int(count),
            "percentage": count / total * 100,
        })
    return results


def get_average_tip_percentage(df: pd.DataFrame) -> float:
    # Only credit card payments have recorded tips
    credit_card = df[df["payment_type"] == 1]
    # Filter out zero fares and outliers (tips > 100% of fare)
    valid = credit_card[
        (credit_card["fare_amount"] > 0)
        & (credit_card["tip_amount"] / credit_card["fare_amount"] <= 1)
    ]
    tip_pct = valid["tip_amount"] / valid["fare_amount"] * 100
    return tip_pct.mean()


def get_tip_percentage_by_borough(
    df: pd.DataFrame, zone_lookup: pd.DataFrame
) -> list[dict]:
    # Only credit card payments have recorded tips
    credit_card = df[df["payment_type"] == 1].copy()
    # Filter out zero fares and outliers (tips > 100% of fare)
    valid = credit_card[
        (credit_card["fare_amount"] > 0)
        & (credit_card["tip_amount"] / credit_card["fare_amount"] <= 1)
    ].copy()
    valid["tip_pct"] = valid["tip_amount"] / valid["fare_amount"] * 100

    # Join with zone lookup to get borough
    merged = valid.merge(
        zone_lookup[["LocationID", "Borough"]],
        left_on="PULocationID",
        right_on="LocationID",
        how="left",
    )

    # Group by borough and calculate average
    borough_tips = merged.groupby("Borough")["tip_pct"].mean()

    # Filter to only the 5 main boroughs and sort by tip percentage
    main_boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    results = []
    for borough in main_boroughs:
        if borough in borough_tips.index:
            results.append({"borough": borough, "tip_percentage": borough_tips[borough]})
    results.sort(key=lambda x: x["tip_percentage"], reverse=True)
    return results


def get_trips_by_hour(df: pd.DataFrame) -> list[dict]:
    hour = df["tpep_pickup_datetime"].dt.hour
    counts = hour.value_counts().sort_index()
    return [{"hour": h, "trip_count": int(counts.get(h, 0))} for h in range(24)]


DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def get_trips_by_day_of_week(df: pd.DataFrame) -> list[dict]:
    day = df["tpep_pickup_datetime"].dt.dayofweek
    counts = day.value_counts().sort_index()
    return [{"day": DAY_NAMES[d], "trip_count": int(counts.get(d, 0))} for d in range(7)]