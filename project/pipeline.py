from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from sqlalchemy import create_engine
from sqlalchemy.types import *

import pandas as pd

URL_BUILDING_PERMITS_DS = "https://databank.worldbank.org/data/download/DB_CSV.zip"
URL_HOUSING_PRICES_DS = (
    "https://stats.oecd.org/FileView2.aspx?IDFile=3ccd4b5e-b4b6-4595-913e-93aea91550fe"
)

DB_FILE = "../data/db.sqlite"


def main():
    df_building_permits = read_zip_dataset(URL_BUILDING_PERMITS_DS, "DBData.csv")
    df_building_permits.drop("Unnamed: 21", axis=1, inplace=True)
    years = [str(year) for year in range(2004, 2021)]

    column_mapping = {
        "Country Code": "country_code",
        "Country Name": "country_name",
        "Indicator Name": "indicator_name",
        "Indicator Code": "indicator_code",
    }
    df_building_permits.rename(columns=column_mapping, inplace=True)

    # extract indicator names
    df_indicators = df_building_permits[["indicator_code", "indicator_name"]].copy()
    df_indicators.drop_duplicates(inplace=True, ignore_index=True)
    df_indicators.rename(
        columns={"indicator_code": "code", "indicator_name": "name"}, inplace=True
    )

    # extract countries and codes
    df_countries = df_building_permits[["country_code", "country_name"]].copy()
    df_countries.drop_duplicates(inplace=True, ignore_index=True)
    df_countries.rename(
        columns={"country_code": "code", "country_name": "name"}, inplace=True
    )

    # doing business DS
    df_building_permits.drop(["country_name", "indicator_name"], axis=1, inplace=True)

    df_housing_prices = read_zip_dataset(URL_HOUSING_PRICES_DS, "HOUSE_PRICES-en.csv")
    df_housing_prices.drop(
        ["Flag Codes", "PowerCode Code", "Reference Period Code"], axis=1, inplace=True
    )

    column_mapping = {
        "COU": "country_code",
        "IND": "indicator_code",
        "TIME": "year_quarter",
        "Value": "value",
    }
    df_housing_prices.rename(columns=column_mapping, inplace=True)

    engine = create_engine("sqlite:///" + DB_FILE, echo=False)
    sqlite_connection = engine.connect()

    # define schemas
    countries_schema = {
        "id": Integer,
        "code": String(7),
        "name": String(50),
    }
    indicators_schema = {
        "id": Integer,
        "code": String(50),
        "name": String(200),
    }
    permits_schema = {
        "id": Integer,
        "country_code": String(7),
        "indicator_code": String(50),
        "value": DECIMAL,
        "year": Integer,
    }
    housing_schema = {
        "id": Integer,
        "country_code": String(7),
        "indicator_code": String(20),
        "year_quarter": String(7),
        "value": DECIMAL,
    }

    # begin importing
    df_countries.to_sql(
        "countries", sqlite_connection, if_exists="replace", dtype=countries_schema
    )
    df_indicators.to_sql(
        "indicators", sqlite_connection, if_exists="replace", dtype=indicators_schema
    )

    # import building permit data by years
    for year in years:
        df_yearly_permits = df_building_permits.copy()
        other_years = years.copy()
        other_years.remove(year)
        df_yearly_permits.drop(other_years, axis=1, inplace=True)
        df_yearly_permits.rename(columns={year: "value"}, inplace=True)
        df_yearly_permits.dropna(inplace=True)
        df_yearly_permits["year"] = year
        if year == years[0]:
            df_yearly_permits.to_sql(
                "building_permits",
                sqlite_connection,
                if_exists="replace",
                dtype=permits_schema,
            )
        else:
            df_yearly_permits.to_sql(
                "building_permits",
                sqlite_connection,
                if_exists="append",
                dtype=permits_schema,
            )

    df_housing_prices.to_sql(
        "housing_prices", sqlite_connection, if_exists="replace", dtype=housing_schema
    )
    sqlite_connection.close()


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
