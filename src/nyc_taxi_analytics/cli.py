import click

from nyc_taxi_analytics.main import (
    DEFAULT_URL,
    filter_by_hour,
    get_average_tip_percentage,
    get_data_info,
    get_key_statistics,
    get_payment_breakdown,
    get_tip_percentage_by_borough,
    get_top_dropoff_zones,
    get_top_pickup_zones,
    get_trips_by_day_of_week,
    get_trips_by_hour,
    get_zone_info,
    load_data,
    load_zone_lookup,
)


@click.command()
@click.option("--start-hour", type=int, default=None, help="Start hour for filtering (0-23)")
@click.option("--end-hour", type=int, default=None, help="End hour for filtering (0-23)")
@click.option("--url", default=DEFAULT_URL, help="Parquet data URL")
@click.option("--info", is_flag=True, help="Display dataset info (row count, columns)")
@click.option("--zone", type=int, default=None, help="Look up zone info by LocationID")
@click.option("--top-pickups", is_flag=True, help="Show top 10 busiest pickup zones")
@click.option("--top-dropoffs", is_flag=True, help="Show top 10 busiest dropoff zones")
@click.option("--payments", is_flag=True, help="Show payment type breakdown")
@click.option("--tip-avg", is_flag=True, help="Show average tip percentage")
@click.option("--tip-by-borough", is_flag=True, help="Show tip percentage by borough")
@click.option("--hourly-chart", is_flag=True, help="Show trips by hour chart")
@click.option("--daily-chart", is_flag=True, help="Show trips by day of week chart")
def cli(start_hour: int | None, end_hour: int | None, url: str, info: bool, zone: int | None, top_pickups: bool, top_dropoffs: bool, payments: bool, tip_avg: bool, tip_by_borough: bool, hourly_chart: bool, daily_chart: bool):
    """Display NYC taxi trip statistics."""
    if zone is not None:
        zones = load_zone_lookup()
        zone_info = get_zone_info(zones, zone)
        if zone_info:
            click.echo(f"Zone {zone}:")
            click.echo(f"  Zone: {zone_info['zone']}")
            click.echo(f"  Borough: {zone_info['borough']}")
            click.echo(f"  Service Zone: {zone_info['service_zone']}")
        else:
            click.echo(f"Zone {zone} not found")
        return

    df = load_data(url)

    if info:
        data_info = get_data_info(df)
        click.echo("Dataset Info:")
        click.echo(f"  Row Count: {data_info['row_count']:,}")
        click.echo(f"  Column Count: {data_info['column_count']}")
        click.echo(f"  Columns: {', '.join(data_info['columns'])}")
        return

    if top_pickups:
        zones = load_zone_lookup()
        top_zones = get_top_pickup_zones(df, zones)
        click.echo("Top 10 Pickup Zones:")
        for i, z in enumerate(top_zones, 1):
            click.echo(f"  {i}. {z['zone']}: {z['trip_count']:,} trips")
        return

    if top_dropoffs:
        zones = load_zone_lookup()
        top_zones = get_top_dropoff_zones(df, zones)
        click.echo("Top 10 Dropoff Zones:")
        for i, z in enumerate(top_zones, 1):
            click.echo(f"  {i}. {z['zone']}: {z['trip_count']:,} trips")
        return

    if payments:
        breakdown = get_payment_breakdown(df)
        click.echo("Payment Type Breakdown:")
        for p in breakdown:
            click.echo(f"  {p['payment_type']}: {p['count']:,} ({p['percentage']:.1f}%)")
        return

    if tip_avg:
        avg_tip = get_average_tip_percentage(df)
        click.echo(f"Average Tip Percentage: {avg_tip:.1f}%")
        click.echo("  (Credit card payments only, excluding tips > 100% of fare)")
        return

    if tip_by_borough:
        zones = load_zone_lookup()
        borough_tips = get_tip_percentage_by_borough(df, zones)
        click.echo("Tip Percentage by Borough:")
        max_tip = max(b["tip_percentage"] for b in borough_tips)
        for b in borough_tips:
            bar_len = int(b["tip_percentage"] / max_tip * 20)
            bar = "█" * bar_len
            click.echo(f"  {b['borough']:15} {bar} {b['tip_percentage']:.1f}%")
        return

    if hourly_chart:
        hourly = get_trips_by_hour(df)
        max_count = max(h["trip_count"] for h in hourly)
        click.echo("Trips by Hour of Day:")
        for h in hourly:
            bar_len = int(h["trip_count"] / max_count * 30)
            bar = "█" * bar_len
            click.echo(f"  {h['hour']:2d}:00 {bar} {h['trip_count']:,}")
        return

    if daily_chart:
        daily = get_trips_by_day_of_week(df)
        max_count = max(d["trip_count"] for d in daily)
        click.echo("Trips by Day of Week:")
        for d in daily:
            bar_len = int(d["trip_count"] / max_count * 30)
            bar = "█" * bar_len
            click.echo(f"  {d['day']} {bar} {d['trip_count']:,}")
        return

    if start_hour is not None and end_hour is not None:
        df = filter_by_hour(df, start_hour, end_hour)
        click.echo(f"Filtered ({start_hour}:00-{end_hour}:00):")
    else:
        click.echo("All trips:")

    stats = get_key_statistics(df)
    click.echo(f"  Total Trips: {stats['total_trips']:,}")
    click.echo(f"  Average Fare: ${stats['average_fare']:.2f}")
    click.echo(f"  Average Distance: {stats['average_distance']:.2f} miles")


if __name__ == "__main__":
    cli()
