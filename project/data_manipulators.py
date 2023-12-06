from pandas import DataFrame
from typing import Self
from sqlalchemy import Table
from sqlalchemy.engine import Engine


class PipelineBuilder:
    _data = None
    _immutable = True

    def __init__(self, data: DataFrame):
        super().__init__()
        self._data = data

    def get_columns(self) -> list[str]:
        return self._data.columns.tolist()

    def drop_cols(self, columns: list[str]) -> Self:
        existing = self.get_columns()
        missing = set(columns) - set(existing)
        if len(missing) > 0:
            raise ValueError(
                "Cannot drop columns, following columns are missing: {}.".format(
                    ", ".join(missing)
                )
            )

        self._data.drop(columns, axis=1, inplace=True)
        return self

    def whitelist_cols(self, columns: list[str]) -> Self:
        not_present = set(columns) - set(self.get_columns())
        if len(not_present) > 0:
            raise ValueError(
                "Cannot keep all columns. Following columns are not present: {}.".format(
                    ", ".join(not_present)
                )
            )

        to_drop = list(set(self.get_columns()) - set(columns))
        return self.drop_cols(to_drop)

    def rename_cols(self, mapping: dict[str, str]) -> Self:
        not_present = set(mapping.keys()) - set(self.get_columns())
        if len(not_present) > 0:
            raise ValueError(
                "Cannot rename columns. Following columns were not found: {}.".format(
                    ", ".join(not_present)
                )
            )

        self._data.rename(columns=mapping, inplace=True)
        return self

    def set_constant(self, column_name: str, value) -> Self:
        self._data[column_name] = value
        return self

    def drop_empty(self) -> Self:
        self._data.dropna(inplace=True)
        return self

    def drop_duplicates(self) -> Self:
        self._data.drop_duplicates(inplace=True)
        return self

    def copy(self) -> Self:
        return PipelineBuilder(self._data.copy())

    def to_sqlite(self, schema: Table, engine: Engine, replace: bool = True) -> Self:
        if replace:
            schema.drop(engine, True)
        schema.create(engine, True)

        schema_columns = set(schema.columns.keys())
        data_columns = set(self.get_columns())
        missing = data_columns - schema_columns

        if len(missing) > 0:
            raise ValueError(
                "Schema missing schema for following columns: {}.".format(
                    ", ".join(missing)
                )
            )

        self._data.to_sql(schema.name, engine, if_exists="append", index=False)
        return self
