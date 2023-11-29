from sqlalchemy import create_engine
from sqlalchemy.types import *
from pandas.core.frame import DataFrame

import pandas as pd


URL_TRAIN_STOPS = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
DB_FILE = "trainstops.sqlite"


def main():
    df_train_stops = pd.read_csv(URL_TRAIN_STOPS, delimiter=";")
    df_train_stops.drop("Status", axis=1, inplace=True)

    engine = create_engine("sqlite:///" + DB_FILE, echo=False)
    sqlite_connection = engine.connect()

    # define schemas
    train_stops_schema = {
        "EVA_NR": Integer,
        "DS100": String,
        "IFOPT": String,
        "NAME": String,
        "Verkehr": String(7),
        "Laenge": DECIMAL,
        "Breite": DECIMAL,
        "Betreiber_Name": String,
        "Betreiber_Nr": Integer,
    }

    df_train_stops = transform_data(df_train_stops)
    df_train_stops = drop_invalid(df_train_stops)

    df_train_stops.to_sql(
        "trainstops",
        sqlite_connection,
        if_exists="replace",
        dtype=train_stops_schema,
        index=False,
    )
    sqlite_connection.close()


def transform_data(df: DataFrame) -> DataFrame:
    df = df.copy()
    df = coord_to_float(df, "Laenge")
    df = coord_to_float(df, "Breite")
    return df


def coord_to_float(df: DataFrame, column: str) -> DataFrame:
    df = df.copy()
    df[column].replace(r",", ".", regex=True, inplace=True)
    df[column] = df[column].apply(pd.to_numeric, errors="coerce")
    df.dropna(subset=[column], inplace=True)

    return df


def drop_invalid(df: DataFrame) -> DataFrame:
    df = df.dropna()
    df = drop_whitelist(df, "Verkehr", ["FV", "RV", "nur DPN"])
    df = drop_outside_of_range(df, "Laenge", -90, 90)
    df = drop_outside_of_range(df, "Breite", -90, 90)
    df = drop_not_conforming_to_regex(df, "IFOPT", r"^[a-z]{2}:\d+:\d+(?::\d+)?$")

    return df


def drop_whitelist(df: DataFrame, column: str, permitted: list) -> DataFrame:
    return df[df[column].isin(permitted)].copy()


def drop_outside_of_range(
    df: DataFrame, column: str, low_bound: float, up_bound: float
) -> DataFrame:
    return df.drop(df[(df[column] < low_bound) | (df[column] > up_bound)].index)


def drop_not_conforming_to_regex(df: DataFrame, column: str, regex: str) -> DataFrame:
    return df.drop(df[~df[column].str.contains(regex)].index)


if __name__ == "__main__":
    main()
