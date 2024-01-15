from pandas.core.frame import DataFrame
from zipfile import ZipFile
from sqlalchemy.types import *
from urllib.request import urlretrieve

import pandas as pd

URL_TEMPERATURES = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
DB_FILE = "temperatures.sqlite"
COLUMNS = [
    "Geraet",
    "Hersteller",
    "Model",
    "Monat",
    "Temperatur in °C (DWD)",
    "Batterietemperatur in °C",
    "Geraet aktiv",
]


def main():
    zip_file = download_zip(URL_TEMPERATURES)
    df_temperatures = parse_csv_file(zip_file)
    df_temperatures = transform_data(df_temperatures)
    df_temperatures = drop_invalid(df_temperatures)

    # define schema
    temperatures_schema = {
        "Geraet": Integer,
        "Hersteller": String,
        "Model": String,
        "Monat": Integer,
        "Temperatur": DECIMAL,
        "Batterietemperatur": DECIMAL,
        "Geraet aktiv": String,
    }

    df_temperatures.to_sql(
        "temperatures",
        "sqlite:///" + DB_FILE,
        if_exists="replace",
        dtype=temperatures_schema,
        index=False,
    )


def download_zip(url: str) -> ZipFile:
    return ZipFile(urlretrieve(url)[0])


def parse_csv_file(zip_file: ZipFile) -> DataFrame:
    with zip_file.open("data.csv") as data:
        return pd.read_csv(
            data,
            sep=";",
            decimal=",",
            usecols=COLUMNS,
            index_col=False,
        )


def transform_data(df: DataFrame) -> DataFrame:
    df = df.copy()

    df = rename(
        df,
        {
            "Temperatur in °C (DWD)": "Temperatur",
            "Batterietemperatur in °C": "Batterietemperatur",
        },
    )
    df = transform_to_fahrenheit(df, "Temperatur")
    df = transform_to_fahrenheit(df, "Batterietemperatur")

    return df


def transform_to_fahrenheit(df: DataFrame, column: str) -> DataFrame:
    df[column] = df[column].apply(lambda c: c * 9 / 5 + 32)
    return df


def rename(df: DataFrame, mapping: dict) -> DataFrame:
    df.rename(columns=mapping, inplace=True)
    return df


def drop_outside_of_range(
    df: DataFrame, column: str, low_bound: float, up_bound: float
) -> DataFrame:
    return df.drop(df[(df[column] < low_bound) | (df[column] > up_bound)].index)


def drop_under(df: DataFrame, column: str, threshold: float) -> DataFrame:
    return df.drop(df[df[column] < threshold].index)


def drop_invalid(df: DataFrame) -> DataFrame:
    df = df.dropna()
    df = drop_under(df, "Geraet", 1)
    df = drop_outside_of_range(df, "Monat", 1, 12)
    # discard temperatures bellow -100 C and above 100 C
    df = drop_outside_of_range(df, "Temperatur", -148, 212)
    # discard battery temperatures bellow -20 c and above 150 c
    df = drop_outside_of_range(df, "Batterietemperatur", -4, 302)

    return df


if __name__ == "__main__":
    main()
