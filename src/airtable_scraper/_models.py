import string
from typing import Any

from pydantic import BaseModel, create_model


class ColumnDefinition(BaseModel):
    """Model to hold information about Airtable column.

    Args:
        name (str): Column name
        type (str): Airtable column type (not python datatypes)
        resultType (str | None, optional): Resulting column type of the column for lookup, rollup, formula, ... columns. Defaults to "".
        typeOptions (dict[str, str] | None, optional): Options for select, multiSelect, ... columns. Defaults to "".
    """

    name: str
    type: str
    resultType: str | None = None
    typeOptions: dict[str, str] | None = None


def create_row_model(table_dtypes: dict[str, set], col_id_map: dict[str, ColumnDefinition]) -> Any:
    """Dynamically create Row model base on column name and datatypes within column.

    Args:
        table_dtypes (dict[str, set]): AirtableScraper.dtypes
        col_id_map (dict[str, ColumnDefinition]): AirtableScraper.col_by_id

    Returns:
        Any: eval of string for create_model()

    Example:
        Row = create_row_model(AirtableScraper.dtypes, AirtableScraper.column_by_id)
    """

    punct = string.punctuation.replace("_", "")
    field_parts = []
    for col_def in col_id_map.values():
        field_name = (
            col_def.name.strip().lower().translate(str.maketrans("", "", punct)).replace(" ", "_")
        )
        # get all data types and build string for create_model() function
        d_type_list = [d.__name__ for d in table_dtypes.get(col_def.type, [])] + ["Any"]
        dtype_str = " | ".join(d_type_list)
        field_str = f"{field_name}=({dtype_str}, None)"  # Pydantic create_model() field constraint (set init value to None)
        field_parts.append(field_str)

    field_params_str = ", ".join(field_parts)
    model_str = f"create_model('Row', {field_params_str})"  # function call string

    return eval(model_str)
