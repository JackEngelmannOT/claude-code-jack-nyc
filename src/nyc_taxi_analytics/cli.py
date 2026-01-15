import click

from nyc_taxi_analytics.main import (
    DEFAULT_URL,
    filter_by_hour,
    get_key_statistics,
    load_data,
)


@click.command()
@click.option("--start-hour", type=int, default=None, help="Start hour for filtering (0-23)")
@click.option("--end-hour", type=int, default=None, help="End hour for filtering (0-23)")
@click.option("--url", default=DEFAULT_URL, help="Parquet data URL")
def cli(start_hour: int | None, end_hour: int | None, url: str):
    """Display NYC taxi trip statistics."""
    df = load_data(url)

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
