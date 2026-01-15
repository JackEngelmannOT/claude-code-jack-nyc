import click

from nyc_taxi_analytics.main import (
    DEFAULT_URL,
    filter_by_hour,
    get_data_info,
    get_key_statistics,
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
def cli(start_hour: int | None, end_hour: int | None, url: str, info: bool, zone: int | None):
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
