from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from sqlalchemy import (
    create_engine,
    Integer,
    String,
    Numeric,
    Engine,
)
from data_manipulators import PipelineBuilder, SchemaBuilder

import pandas as pd

URL_BUILDING_PERMITS_DS = "https://databank.worldbank.org/data/download/DB_CSV.zip"
URL_HOUSING_PRICES_DS = (
    "https://stats.oecd.org/FileView2.aspx?IDFile=3ccd4b5e-b4b6-4595-913e-93aea91550fe"
)

DB_FILE = "../data/db.sqlite"


def main():
    engine = create_engine("sqlite:///" + DB_FILE, echo=False)
    building_permits_pipeline(engine)
    housing_prices_pipeline(engine)


def building_permits_pipeline(engine: Engine):
    years = [str(y) for y in range(2004, 2021)]
    permit_mapping = {
        "Country Code": "country_code",
        "Country Name": "country_name",
        "Indicator Name": "indicator_name",
        "Indicator Code": "indicator_code",
    }
    columns = [*permit_mapping.keys(), *years]

    permits_builder = PipelineBuilder(
        read_zip_dataset(URL_BUILDING_PERMITS_DS, "DBData.csv")
    )
    permits_builder.whitelist_cols(columns).rename_cols(permit_mapping)

    indicators_mapping = {
        "indicator_code": "code",
        "indicator_name": "name",
    }
    indicators_builder = (
        permits_builder.copy()
        .whitelist_cols(list(indicators_mapping.keys()))
        .drop_duplicates()
        .rename_cols(indicators_mapping)
    )

    countries_mapping = {
        "country_code": "code",
        "country_name": "name",
    }

    countries_builder = (
        permits_builder.copy()
        .whitelist_cols(list(countries_mapping.keys()))
        .drop_duplicates()
        .rename_cols(countries_mapping)
    )

    indicators_schema = (SchemaBuilder("indicators")
                         .add_column("code", String(50), primary_key=True)
                         .add_column("name", String(200), nullable=True)
                         .build())

    countries_schema = (SchemaBuilder("countries")
                        .add_column("code", String(7), primary_key=True)
                        .add_column("name", String(50), nullable=False)
                        .build())

    permits_schema = (SchemaBuilder("building_permits")
                      .add_column("id", Integer, primary_key=True, autoincrement=True)
                      .add_column("country_code", String(7), nullable=False)
                      .add_column("indicator_code", String(50), nullable=False)
                      .add_column("year", Integer, nullable=False)
                      .add_column("value", Numeric, nullable=False)
                      .build())

    # import indicators
    indicators_builder.to_sqlite(indicators_schema, engine)

    # import countries
    countries_builder.to_sqlite(countries_schema, engine)

    # import building permit data by years
    permits_builder.drop_cols(["country_name", "indicator_name"])
    for year in years:
        other_years = years.copy()
        other_years.remove(year)

        (
            permits_builder.copy()
            .drop_cols(other_years)
            .rename_cols({year: "value"})
            .drop_empty()
            .set_constant("year", year)
            .to_sqlite(permits_schema, engine, year == years[0])
        )


def housing_prices_pipeline(engine: Engine):
    builder = PipelineBuilder(
        read_zip_dataset(URL_HOUSING_PRICES_DS, "HOUSE_PRICES-en.csv")
    )
    column_mapping = {
        "COU": "country_code",
        "IND": "indicator_code",
        "TIME": "year_quarter",
        "Value": "value",
    }

    indicators_schema = (SchemaBuilder("indicators")
                         .add_column("code", String(50), primary_key=True)
                         .add_column("name", String(200), nullable=True)
                         .build())

    (
        builder.copy()
        .whitelist_cols(["IND"])
        .rename_cols({"IND": "code"})
        .drop_duplicates()
        .to_sqlite(indicators_schema, engine, False)
    )

    schema = (SchemaBuilder("housing_prices")
              .add_column("id", Integer, primary_key=True)
              .add_column("country_code", String(7), nullable=False)
              .add_column("indicator_code", String(20), nullable=False)
              .add_column("year_quarter", String(7), nullable=False)
              .add_column("value", Numeric, nullable=False)
              .build())

    builder.whitelist_cols(list(column_mapping.keys())).rename_cols(
        column_mapping
    ).to_sqlite(schema, engine)


def read_zip_dataset(url: str, file_name: str) -> pd.DataFrame:
    resp = urlopen(url)
    myzip = ZipFile(BytesIO(resp.read()))
    virtual_file = myzip.open(file_name)

    dataframe = pd.read_csv(virtual_file)
    virtual_file.close()
    myzip.close()

    return dataframe


if __name__ == "__main__":
    main()
    print("Success")
