from sqlalchemy import create_engine, inspect, text, Connection

from pipeline import DB_FILE

import os

# expected row counts of all tables
COUNT_BUILDING_PERMITS = 366211
COUNT_COUNTRIES = 213
COUNT_HOUSING_PRICES = 32849
COUNT_INDICATORS = 212


def main():
    assert os.path.isfile(DB_FILE), "Database file does not exist"

    expected_tables = {"building_permits", "countries", "housing_prices", "indicators"}
    engine = create_engine("sqlite:///" + DB_FILE, echo=False)
    inspector = inspect(engine)
    present_tables = set(inspector.get_table_names())
    missing_tables = expected_tables - present_tables

    assert (
        len(missing_tables) <= 0
    ), "Following tables were expected but not found: {}.".format(
        ", ".join(missing_tables)
    )

    present_building_permits_cols = set(
        [c["name"] for c in inspector.get_columns("building_permits")]
    )
    expected_building_permits_cols = {
        "id",
        "country_code",
        "indicator_code",
        "year",
        "value",
    }
    missing_building_permits_cols = (
        expected_building_permits_cols - present_building_permits_cols
    )

    assert (
        len(missing_building_permits_cols) <= 0
    ), "In the `building_permits` table following columns were expected but not found: {}.".format(
        ", ".join(missing_building_permits_cols)
    )

    present_countries_cols = set(
        [c["name"] for c in inspector.get_columns("countries")]
    )
    expected_countries_cols = {"code", "name"}
    missing_countries_cols = expected_countries_cols - present_countries_cols

    assert (
        len(missing_countries_cols) <= 0
    ), "In the `countries` table following columns were expected but not found: {}.".format(
        ", ".join(missing_countries_cols)
    )

    present_indicators_cols = set(
        [c["name"] for c in inspector.get_columns("indicators")]
    )
    expected_indicators_cols = {"code", "name"}
    missing_indicators_cols = expected_indicators_cols - present_indicators_cols

    assert (
        len(missing_indicators_cols) <= 0
    ), "In the `indicators` table following columns were expected but not found: {}.".format(
        ", ".join(missing_indicators_cols)
    )

    present_housing_prices_cols = set(
        [c["name"] for c in inspector.get_columns("housing_prices")]
    )
    expected_housing_prices_cols = {
        "id",
        "country_code",
        "indicator_code",
        "year_quarter",
        "value",
    }
    missing_housing_prices_cols = (
        expected_housing_prices_cols - present_housing_prices_cols
    )

    assert (
        len(missing_housing_prices_cols) <= 0
    ), "In the `housing_prices` table following columns were expected but not found: {}.".format(
        ", ".join(missing_housing_prices_cols)
    )

    # connect to find our row counts
    with engine.connect() as conn:
        building_permits_count = get_row_count("building_permits", conn)
        countries_count = get_row_count("countries", conn)
        indicators_count = get_row_count("indicators", conn)
        housing_prices_count = get_row_count("housing_prices", conn)

    # assert each table contains expected amount of rows
    assert (
        building_permits_count == COUNT_BUILDING_PERMITS
    ), "{} rows found in the `building_permits` table but {} was expected.".format(
        building_permits_count, COUNT_BUILDING_PERMITS
    )
    assert (
        countries_count == COUNT_COUNTRIES
    ), "{} rows found in the `countries` table but {} was expected.".format(
        countries_count, COUNT_COUNTRIES
    )
    assert (
        indicators_count == COUNT_INDICATORS
    ), "{} rows found in the `indicators` table but {} was expected.".format(
        indicators_count, COUNT_INDICATORS
    )
    assert (
        housing_prices_count == COUNT_HOUSING_PRICES
    ), "{} rows found in the `housing_prices` table but {} was expected.".format(
        housing_prices_count, COUNT_HOUSING_PRICES
    )


def get_row_count(table_name: str, connection: Connection) -> int:
    return connection.scalar(text("SELECT COUNT(*) FROM " + table_name))


if __name__ == "__main__":
    main()
