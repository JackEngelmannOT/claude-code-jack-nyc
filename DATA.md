# NYC Taxi Trip Data Reference

## Data Source

NYC Taxi & Limousine Commission (TLC) provides free trip record data:

**Main page:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Data Files (Parquet Format)

| Type | URL Pattern | Description |
|------|-------------|-------------|
| Yellow Taxi | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` | Classic NYC yellow cabs |
| Green Taxi | `https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_YYYY-MM.parquet` | Boro taxis (outer boroughs) |
| For-Hire (FHV) | `https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_YYYY-MM.parquet` | Uber, Lyft, etc. |

**Example URLs:**
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
```

**Tip:** Start with one month of yellow taxi data (~3M rows, ~50MB parquet).

## Zone Lookup Table

Pickup/dropoff locations use zone IDs. Download the lookup table:

```
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

| LocationID | Borough | Zone | service_zone |
|------------|---------|------|--------------|
| 1 | EWR | Newark Airport | EWR |
| 132 | Queens | JFK Airport | Airports |
| 138 | Queens | LaGuardia Airport | Airports |
| 161 | Manhattan | Midtown Center | Yellow Zone |
| 237 | Manhattan | Upper East Side South | Yellow Zone |

## Yellow Taxi Schema

| Field | Type | Description |
|-------|------|-------------|
| `VendorID` | int | 1=CMT, 2=VeriFone |
| `tpep_pickup_datetime` | datetime | Pickup timestamp |
| `tpep_dropoff_datetime` | datetime | Dropoff timestamp |
| `passenger_count` | float | Number of passengers |
| `trip_distance` | float | Trip distance in miles |
| `RatecodeID` | float | Rate code (1=Standard, 2=JFK, 3=Newark, etc.) |
| `store_and_fwd_flag` | string | Y/N - was trip stored before sending |
| `PULocationID` | int | Pickup zone ID |
| `DOLocationID` | int | Dropoff zone ID |
| `payment_type` | int | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `fare_amount` | float | Base fare |
| `extra` | float | Rush hour/overnight surcharges |
| `mta_tax` | float | MTA tax ($0.50) |
| `tip_amount` | float | Tip (credit card only) |
| `tolls_amount` | float | Bridge/tunnel tolls |
| `improvement_surcharge` | float | $0.30 improvement surcharge |
| `total_amount` | float | Total charged |
| `congestion_surcharge` | float | Congestion pricing surcharge |
| `airport_fee` | float | Airport pickup fee |

## Sample Data Load (Python)

```python
import pandas as pd

# Load one month of yellow taxi data
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
df = pd.read_parquet(url)

print(f"Rows: {len(df):,}")
print(df.head())
```

## Sample Data Load (DuckDB)

```sql
-- DuckDB can query parquet directly from URL
SELECT
    COUNT(*) as trips,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_fare
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
WHERE trip_distance > 0;
```

## Interesting Analysis Ideas

1. **Busiest pickup zones** - Where do most trips start?
2. **Airport traffic patterns** - JFK vs LaGuardia vs Newark
3. **Tip analysis** - Average tip % by zone, time of day, trip distance
4. **Rush hour patterns** - Trip volume by hour of day
5. **Fare per mile** - How does it vary by borough?
6. **Trip duration** - Average trip time by route
7. **Payment trends** - Credit vs cash over time
8. **Weekend vs weekday** - How does demand differ?

## Data Size Reference

| Dataset | Rows/Month | File Size |
|---------|------------|-----------|
| Yellow Taxi | ~3 million | ~50 MB |
| Green Taxi | ~80,000 | ~2 MB |
| FHV | ~20 million | ~300 MB |

## Additional Resources

- [TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [Taxi Zone Shapefile](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip) (for mapping)
