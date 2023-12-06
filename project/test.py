from sqlalchemy import create_engine, inspect

from pipeline import DB_FILE

import os


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


if __name__ == "__main__":
    main()
