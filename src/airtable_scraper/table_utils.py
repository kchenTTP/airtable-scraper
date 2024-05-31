from collections import Counter, defaultdict
from typing import Any

from ._logger_config import _get_logger

logger = _get_logger()


class ColumnTypeAnalyzer:
    """Helper class for analyzing Airtable columns. Pair with AirtableScraper class.

    Args:
        raw_rows_json (list | Any): AirtableScraper.raw_rows_json
        column_id_map (dict | Any): AirtableScraper.column_by_id

    Properties:
        available_airtable_types: Every different Airtable column type
        dtypes: Airtable column type and their datatypes
        dtype_counts: The number of columns for different datatypes

    Methods:
        analyze(): Analyzes a Airtable table, returns the Airtable column types available and their datatypes
        get_column_by_type(): Get all the columns of one Airtable column type
        get_info(): Prints table information pandas DataFrame.info style

    Example:
        analyzer = ColumnTypeAnalyzer(raw_rows_json=Airtable.raw_rows_json, column_id_map=Airtable.column_by_id)
        print(analyzer.dtypes)
    """

    def __init__(self, raw_rows_json: list | Any, column_id_map: dict | Any) -> None:
        self.__rows = raw_rows_json
        self.__col_id_map = column_id_map
        self.__col_def = self.__get_column_definition()
        self.__type_collection, self.__dtype_count = self.analyze()

    @property
    def available_airtable_types(self) -> set[str]:
        return self.__get_available_airtable_column_types()

    @property
    def dtype_counts(self) -> dict[Any, int]:
        return self.__dtype_count

    @property
    def dtypes(self) -> dict[Any, set[Any]]:
        return self.__type_collection

    def __get_column_definition(self) -> list[Any]:
        return list(self.__col_id_map.values())

    def __get_available_airtable_column_types(self) -> set[Any]:
        return set(self.dtypes.keys())

    def analyze(self) -> tuple[dict[Any, set[Any]], dict[Any, int]]:
        type_collection = defaultdict(set)

        for r in self.__rows:
            cur_row = r["cellValuesByColumnId"]
            for k, v in cur_row.items():
                type_collection[self.__col_id_map[k].type].add(type(v))

        type_list = [
            t for types in type_collection.values() for t in types
        ]  # flatten nested list, some columns have multiple dtypes
        type_counter = Counter(type_list).most_common()

        return dict(type_collection), dict(type_counter)

    def get_column_by_type(self, airtable_col_type: str) -> list[Any]:
        column_collection = []

        for r in self.__rows:
            cur_row = r["cellValuesByColumnId"]
            for k, v in cur_row.items():
                if self.__col_id_map[k].type == airtable_col_type:
                    column_collection.append(v)
        return column_collection

    def print_info(self) -> None:
        n_col_digits = len(
            str(len(self.__col_def))
        )  # get number of digits in number of columns ex. 10 columns -> 2 digits, 100 columns -> 3 digits
        max_col_name_width = max(
            len(c.name) for c in self.__col_def
        )  # get max length of column name
        max_air_type_width = max(
            len(c.type) for c in self.__col_def
        )  # get max length of airtable column types

        max_widths = {
            "idx": n_col_digits if n_col_digits > 3 else 3,
            "name": max_col_name_width,
            "type": max_air_type_width
            if max_air_type_width > len("Airtable Column Type")
            else len("Airtable Column Type"),
            "dtype": 5,
        }

        # get python datatypes for each airtable column type
        dtype_list = []
        for i in self.__col_def:
            type_collection = self.dtypes.get(i.type)
            d_names = [d.__name__ for d in type_collection]
            dtype_str = ", ".join(d_names)
            dtype_list.append(dtype_str)

        dtype_counts = [
            f"{k.__name__}({v})" for k, v in self.dtype_counts.items()
        ]  # format dictionary item to string
        dtype_str = ", ".join(dtype_counts)

        print(f"Total {len(self.__col_def)} columns:")
        print(
            f"{'#': ^{max_widths['idx']}}  {'Column': <{max_widths['name']}}  {'Airtable Column Type': <{max_widths['type']}}  {'Dtype': <{max_widths['dtype']}}"
        )
        print(
            f"{'-' * max_widths['idx']}  {'-' * max_widths['name']}  {'-' * max_widths['type']}  {'-' * max_widths['dtype']}"
        )

        for i, (col, dtype) in enumerate(zip(self.__col_def, dtype_list)):
            print(
                f"{i: ^{max_widths['idx']}}  {col.name: <{max_widths['name']}}  {col.type: <{max_widths['type']}}  {dtype: <{max_widths['dtype']}}"
            )

        print(f"dtypes: {dtype_str}")


def flatten_lookup_column_r(lst: list, type_options: Any, flat_list: list = []) -> list:
    """Flatten the list in a Airtable lookup column

    Args:
        lst (list): cell_value["valuesByForeignRowId"] casted to list
        type_options (dict): ColumnDefinition.typeOptions
        flat_list (list, optional): List to contain flattened list. Defaults to [].

    Returns:
        list: Flatten list
    """
    if not flat_list:
        flat_list = []
    for item in lst:
        if isinstance(item, dict):
            flat_list.append(item.get("foreignRowDisplayName"))
        elif isinstance(item, list):
            flat_list.extend(
                flatten_lookup_column_r(lst=item, type_options=type_options, flat_list=[])
            )
        else:
            try:
                flat_list.append(type_options[item])
            except Exception:
                flat_list.append(item)
    return flat_list
