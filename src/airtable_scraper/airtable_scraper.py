import csv
import json
import re
import string
from collections import defaultdict

# from functools import cache, lru_cache
from io import StringIO
from pathlib import Path
from typing import Any, Literal, Self

import pandas as pd
import requests
from pydantic import create_model

from ._logger_config import _get_logger
from ._models import ColumnDefinition, create_row_model
from .table_utils import ColumnTypeAnalyzer, flatten_lookup_column_r
from .typing import cast_to_str, join_list_like, none_filter

logger = _get_logger()


class AirtableScraper:
    def __init__(self, url: str, timezone: str = "America/New_York") -> None:
        self._url = url
        self.timezone = timezone
        self.__s = requests.Session()
        self._headers = {
            "Accept": "*/*",
            "Accept-Language": "en-ZA,en;q=0.9",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "airtable.com",
            "Pragma": "no-cache",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        # initial page (no actual table data)
        self.__page = self._get(self._url, self._headers)
        self.__page.encoding = "utf-8-sig"
        self.__page = self.__page.text

        # send request to hidden api endpoint to get actual table data after parsing new header info
        self._new_url = self._get_new_url()
        self._headers = self._get_full_headers()
        self._data = self._get(self._new_url, self._headers).json()
        if self._data.get("msg") == "SUCCESS":
            self.__table = self._data.get("data").get("table")
            self.__status = "success"
        else:
            self.__table = {}
            self.__status = "failed"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        try:
            self.__s.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        if exc_type:
            raise exc_type(exc_value, exc_tb)

    def __del__(self) -> None:
        self.__s.close()

    @property
    def app_id(self) -> Any | str | None:
        """The application id of the airtable."""
        try:
            return self._get_app_id()
        except Exception as e:
            logger.error(f"Error getting app ID: {e}")
            return

    @property
    def column_definitions(self) -> list[Any] | None:
        """A list of column definitions."""
        try:
            return list(self._get_column_definition().values())
        except Exception as e:
            logger.error(f"Error getting column definition: {e}")
            return

    @property
    def column_by_id(self) -> dict[str, ColumnDefinition] | None:
        """Hashmap of column id to column definition."""
        try:
            return self._get_column_definition()
        except Exception as e:
            logger.error(f"Error getting column definition: {e}")
            return

    @property
    def data(self) -> list[Any] | None:
        """Data of the airtable in dictionary format."""
        try:
            return self._get_table_data()
        except Exception as e:
            logger.error(f"Error getting table data: {e}")
            return

    @property
    def dtypes(self) -> dict[str, set[type]] | None:
        """Returns the column type and datatype of each column."""
        try:
            return ColumnTypeAnalyzer(self.raw_rows_json, self.column_by_id).dtypes
        except Exception as e:
            logger.error(f"Error getting table datatypes: {e}")
            return

    @property
    def headers(self) -> dict[str, str]:
        """Headers used for the http get request."""
        return self._headers

    @headers.setter
    def headers(self, new_params: dict[str, str]) -> None:
        """Set new headers for http get request.

        Args:
            new_params (dict[str, str]): Header and it's values
        """
        if new_params != self._headers:
            self._headers = new_params

    @headers.deleter
    def headers(self) -> None:
        """Delete all headers for http get request."""
        self._headers = {}

    @property
    def raw_columns_json(self) -> list | Any | None:
        """Raw json data from http response that defines each column.

        Returns:
            list | Any | None: Column information
        """
        if not self.__table:
            logger.error("Table does not exist. Cannot find column data")
            return

        if "columns" not in self.__table.keys():
            logger.error("Table does not contain column data")
            return
        return self.__table.get("columns")

    @property
    def raw_rows_json(self) -> list | Any | None:
        """Raw json data from http response for each row.

        Returns:
            list | Any | None: Row data
        """
        if not self.__table:
            logger.error("Table does not exist. Cannot find row data")
            return

        if "rows" not in self.__table.keys():
            logger.error("Table does not contain row data")
            return
        return self.__table.get("rows")

    @property
    def request_id(self) -> str | Any:
        """The request id of your current http get request. Changes each time you send a new http request."""
        try:
            return self._get_request_id()
        except Exception as e:
            logger.error(f"Error getting request ID: {e}")
            return

    @property
    def share_id(self) -> Any | Literal[""]:
        """The share id of the airtable."""
        try:
            return self._get_share_id()
        except Exception as e:
            logger.error(f"Error getting share ID: {e}")
            return

    @property
    def status(self) -> str:
        """Success or fail of scraping (http) request."""
        return self.__status

    @property
    def table_id(self) -> Any | None:
        """The table id of the airtable."""
        try:
            return self._get_table_id()
        except Exception as e:
            logger.error(f"Error getting table ID: {e}")
            return

    @property
    def url(self) -> str:
        """The endpoint (url) of the airtable and the http get request."""
        return self._url

    @property
    def view_id(self) -> str | None:
        """The view id of the airtable."""
        try:
            return self._get_view_id()
        except Exception as e:
            logger.error(f"Error getting view ID: {e}")
            return

    def _get(self, url: str, headers: dict[str, str]) -> requests.Response:
        """Wrapper for requests.Session().get() method.

        Args:
            url (str): Endpoint (url)
            headers (dict[str, str]): Request headers in dictionary format

        Returns:
            requests.Response: Http response
        """
        return self.__s.get(url, headers=headers)

    def _get_new_url(self) -> str | Any:
        """Get the hidden api endpoint to actual airtable data.

        Returns:
            str | Any: Api endpoint. None if table not found in http response
        """
        url_with_params = re.search(r"urlWithParams:\s*\"(.*?)\"", self.__page).group(1)
        if not url_with_params:
            logger.error(f"Could not parse urlWithParams. Failed to scrape table: {self._url}")
            raise
        url_with_params = url_with_params.replace("u002F", "").replace("\\", "/")
        return "https://airtable.com" + url_with_params

    def __get_stringified_object_params(self) -> str | Any:
        """Get stringified object parameters.

        Returns:
            str | Any: stringified object parameters. None if not found in http response
        """
        url_with_params = re.search(r"urlWithParams:\s*\"(.*?)\"", self.__page).group(1)
        if not url_with_params:
            logger.error(
                f"Could not parse urlWithParams. Failed to download table as csv: {self._url}"
            )
            raise
        url_with_params = url_with_params.replace("u002F", "").replace("\\", "/")
        return url_with_params.split("/")[-1].split("?")[-1].replace("stringifiedObjectParams=", "")

    def _get_auth_data(self) -> Any:
        """Get authorization data in initial http response from url to include in the second http request to hidden api endpoint for actual data.

        Returns:
            Any: Auth headers in dictionary. None if not found
        """
        dirty_auth_json = re.search(r"var\s*headers\s*=\s*{(.*?)}", self.__page).group()
        if not dirty_auth_json:
            logger.error(f"Could not parse authorization data. Failed to scrape table: {self._url}")
            raise
        dirty_auth_json = dirty_auth_json.replace("var headers = ", "")
        return json.loads(dirty_auth_json)

    def _get_app_id(self) -> Any | str | None:
        """Get application id number from the response of the hidden api endpoint

        Returns:
            Any | str | None: Application id string. None if not found
        """
        if self.__table:
            return self.__table.get("applicationId")
        elif self._headers.get("x-airtable-application-id"):
            return self._headers.get("x-airtable-application-id")
        else:
            return re.search(r'\\"applicationId\\\"\s*:\s*\\*"([^"]+)\\"', self.__page).group(1)

    def __get_appBlanket_userInfoById(self) -> dict | None:
        """Get user id and name information from the response of the hidden api endpoint and return a dictionary

        Returns:
            dict | None: {Id: name} hashmap. None if not found
        """
        if self.__table:
            try:
                user_info_by_id: list = self.__table.get("appBlanket").get("userInfoById").values()
                return {
                    user["id"]: " ".join([user["firstName"], user["lastName"]])
                    for user in user_info_by_id
                }
            except Exception:
                logger.warning(
                    "Warning: No user in table. Ignore if there are no user column in the Airtable"
                )
                return
        return

    def _get_view_id(self) -> str:
        """Get airtable view id from either the response of hidden api endpoint or initial response.

        Returns:
            str: View id string. None if not found
        """
        if self.__table:
            view_id = self.__table.get("viewOrder")
            if isinstance(view_id, list):
                return view_id[0]
            else:
                return view_id
        else:
            view_id: str = re.search(r'"url":\s*"([^"]+)"', self.__page).group(1)
            if not view_id:
                logger.error("Could not pasrse view id")
                raise
            view_id_list: list[str] = view_id.split("\\u002F")
            return view_id_list[view_id_list.index("view") + 1]

    def _get_request_id(self) -> str | Any:
        """Get request id from initial http response. Different every time a new request is sent

        Returns:
            str | Any: Request id string. Empty string if not found
        """
        request_id = re.search(r'requestId:\s*"([^"]+)"', self.__page).group(1)
        if not request_id:
            logger.error("Could not parse request id. Empty string returned")
            return ""
        return request_id

    def _get_share_id(self) -> Any | Literal[""]:
        """Get airtable share id from initial http response.

        Returns:
            _type_: Share id string. Empty string if not found
        """
        share_id = re.search(r'"sharedViewId":\s*"([^"]+)"', self.__page).group(1)
        if not share_id:
            logger.error("Could not parse share id. Empty string returned")
            return ""
        return share_id

    def _get_table_id(self) -> Any | None:
        """Get airtable id from the response of hidden api endpoint or initial http response.

        Returns:
            Any | None: Airtable id string. None if not found
        """
        if self.__table:
            return self.__table.get("id")
        else:
            return re.search(r'"sharedViewTableId":\s*"([^"]+)"', self.__page).group(1)

    def _get_full_headers(self) -> dict[str, Any]:
        """Return headers used for sending get request to the hidden api endpoint.

        Returns:
            dict[str, Any]: Request headers
        """
        auth_json = self._get_auth_data()
        return {
            "Accept": "*/*",
            "Accept-Language": "en-ZA,en;q=0.9",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "airtable.com",
            "Pragma": "no-cache",
            "sec-ch-ua": '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "none",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "x-time-zone": self.timezone,
            "x-early-prefetch": auth_json["x-early-prefetch"],
            "x-user-locale": auth_json["x-user-locale"],
            "x-airtable-application-id": auth_json["x-airtable-application-id"],
            "x-airtable-page-load-id": auth_json["x-airtable-page-load-id"],
            "X-Requested-With": auth_json["X-Requested-With"],
            "x-airtable-inter-service-client": auth_json["x-airtable-inter-service-client"],
        }

    def _get_column_definition(self) -> dict[Any, Any]:
        """Parse column information from table data and return dictionary object of column id and information (ColumnDefinition object) key, value pairs.

        Returns:
            dict[Any, Any]: {id: column_definition} hashmap
        """
        if not self.__table:
            logger.error("Table don't exist. Cannot get column definitions")
            raise

        if "columns" not in self.__table.keys():
            logger.error("Table does not contain column data")
            raise

        columns = self.__table.get("columns")
        column_dict = {}

        for col in columns:
            options = None
            result_type = None

            if col["typeOptions"]:
                try:
                    options = {
                        choice["id"]: choice["name"]
                        for choice in col["typeOptions"]["choices"].values()
                    }
                except Exception:
                    pass
                result_type = col["typeOptions"].get("resultType")

            column_dict[col["id"]] = ColumnDefinition(
                name=col["name"],
                type=col["type"],
                resultType=result_type,
                typeOptions=options,
            )
        return column_dict

    def _get_table_data(self) -> list:
        """Parse row data and store into individual Row object. The way information is parsed depends on the airtable column type.

        Returns:
            list[Row]: List of all the rows in the airtable
        """
        user_by_id = self.__get_appBlanket_userInfoById()

        # hashmap for different data parsing strategies for different airtable column types. Resulting value should always be a string
        self.__type_handlers = {
            "autoNumber": lambda cell_val: cell_val,
            "barcode": lambda cell_val: cell_val.get("text"),
            "button": lambda cell_val: cell_val.get(
                "url"
            ),  # will always be a url, doesn't matter action
            "checkbox": lambda cell_val: "checked" if cell_val else None,
            "collaborator": lambda cell_val: user_by_id.get(cell_val),  # will container 1 user id
            "computation": lambda cell_val: user_by_id.get(
                cell_val
            ),  # collaborator object -> same as user, contain 1 user id
            "count": lambda cell_val: cell_val,  # count number of linked records -> int
            "date": lambda cell_val: f'"{cast_to_str(
                cell_val, sep=",", tz=self.timezone, date_only=True
            )}"',
            "foreignKey": lambda cell_val: f'"{",".join([f["foreignRowDisplayName"] for f in cell_val])}"',
            "formula": lambda cell_val: cast_to_str(
                cell_val, sep=",", tz=self.timezone, date_only=False
            ),  # variable output from formula, contains output value (not formula itself)
            "lookup": lambda cell_val: f'"{join_list_like(flatten_lookup_column_r(
                list(cell_val["valuesByForeignRowId"].values()),
                type_options=col_def.typeOptions,
                flat_list=[],
            ), ",")}"',  # variable output from looking up columns from other table
            "multilineText": lambda cell_val: f'"{cell_val}"',
            "multipleAttachment": lambda cell_val: f'"{",".join([f["filename"] for f in cell_val])}"',
            "multiSelect": lambda cell_val: f'"{",".join(
                [col_def.typeOptions.get(val) for val in cell_val]
            )}"',
            "number": lambda cell_val: cell_val,
            "phone": lambda cell_val: cell_val,
            "rating": lambda cell_val: cell_val,
            "richText": lambda cell_val: f'"{"".join(
                [section["insert"] for section in cell_val.get("documentValue")]
            ).strip()}"',
            "rollup": lambda cell_val: f'"{cast_to_str(cell_val, sep=",", tz=self.timezone, date_only=False)}"',  # datatype depends on linked field, not perfect -> might need data cleaning afterwards
            "select": lambda cell_val: col_def.typeOptions.get(cell_val),
            "text": lambda cell_val: cell_val,
        }
        punct = string.punctuation.replace("_", "")

        # dynamically create row model from column names and dtypes
        Row = create_row_model(self.dtypes, self.column_by_id)
        row_object_list = []  # to hold all the rows
        rows_list: list[dict] = [r["cellValuesByColumnId"] for r in self.raw_rows_json]
        row_items = [d.items() for d in rows_list]  # discard keys containing column ids

        # loop each row
        for row in row_items:
            row_vals = defaultdict(None)  # dict to map values of each row to variable

            # loop each column
            for col_id, cell_val in row:
                row_col_val = None
                col_def = self.column_by_id.get(col_id)
                col_name_key = (
                    col_def.name.strip()
                    .lower()
                    .translate(str.maketrans("", "", punct))
                    .replace(" ", "_")
                )  # create variable name to match field name created from create_row_model()

                # get lambda function that processes specific airtable col type
                handler = self.__type_handlers.get(col_def.type)

                if handler:
                    row_col_val = handler(cell_val)  # process data in each column
                else:
                    logger.warning(
                        f"Error: Table contains unknown Airtable column type: {col_def.type}"
                    )
                    row_col_val = cell_val

                row_vals[col_name_key] = row_col_val
            row_object_list.append(Row(**row_vals))  # instantiate Row object for each row in table
        return row_object_list

    def info(self) -> None:
        """
        Print a summary of the airtable.

        This method prints information about an airtable including the column index and name, column type, and datatypes.
        """
        try:
            return ColumnTypeAnalyzer(self.raw_rows_json, self.column_by_id).print_info()
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return

    def _get_download_csv_url(self, **kwargs) -> str:
        """Get the url for the download csv button.

        kwargs:
            {
            "viewId": self.view_id,
            "x-time-zone": self.timezone,
            "x-user-locale": self._get_auth_data()["x-user-locale"],
            "x-airtable-application-id": self._get_auth_data()["x-airtable-application-id"],
            "stringifiedObjectParams": self.__get_stringified_object_params(),
            }

        Returns:
            str: URL to download csv file
        """
        return f"https://airtable.com/v0.3/view/{kwargs['viewId']}/downloadCsv?x-time-zone={kwargs['x-time-zone']}&x-user-locale={kwargs['x-user-locale']}&x-airtable-application-id={kwargs['x-airtable-application-id']}&stringifiedObjectParams={kwargs['stringifiedObjectParams']}"

    def __get_csv_from_download_csv(self) -> str | None:
        """If the download csv button is available on an airtable, get the csv string from the url of the download csv button.

        Returns:
            str | None: Returns the csv string. Otherwise returns None if download csv button is not available.
        """
        auth_json = self._get_auth_data()
        str_object_params = self.__get_stringified_object_params()
        params = {
            "viewId": self.view_id,
            "x-time-zone": self.timezone,
            "x-user-locale": auth_json["x-user-locale"],
            "x-airtable-application-id": auth_json["x-airtable-application-id"],
            "stringifiedObjectParams": str_object_params,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html",
            "Accept-Language": "en",
        }

        resp = requests.get(self._get_download_csv_url(**params), headers=headers)

        if resp.status_code == 200:
            resp.encoding = "utf-8-sig"
            return resp.text

        logger.warning(f"Error {resp.status_code}: {json.loads(resp.content)}")
        return None

    def __write(self, data, path: str | Path) -> None:
        with open(path, "w") as f:
            f.write(data)

    def to_csv(self, path: str | Path | None = None) -> None | str:
        """Write object to comma-separated values (csv) file.

        Args:
            path (str | Path | None, optional): String or path object (implementing pathlib.Path). If None, the result is returned as a string. Defaults to None.

        Returns:
            None | str: If path is None, returns the resulting csv format as a string. Otherwise returns None.
        """
        data: str | None = self.__get_csv_from_download_csv()

        if not data:
            converted_rows = [row.model_dump() for row in self.data]
            stringified_rows = [
                ",".join(str(v) if v is not None else "" for v in row.values())
                for row in converted_rows
            ]
            body = "\n".join(stringified_rows)
            header = ",".join([col.name for col in self.column_by_id.values()]) + "\n"
            data = header + body

        if path:
            if not isinstance(path, Path):
                path = Path(path)
            assert (
                path.suffix.lower() == ".csv"
            ), "Error: No suffix or incorrect suffix provided. Please use .csv for file suffix"

            self.__write(data=data, path=path)
            return

        return data

    def to_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(StringIO(self.to_csv()))

    def to_dict(
        self, orient: str = "records", pass_to_json: bool = False
    ) -> list[dict[str, Any]] | dict[str, dict[Any, Any]] | None:
        """Convert the table data to a dictionary.

        Args:
            orient (str{'records', 'index'}, optional): determines the type of values of the dictionary. Defaults to "records".
            pass_to_json (bool, optional): Experimental. Use .to_json if you want to get a json file or string. Whether the dictionary will be passed to json.dumps function or not. Defaults to False.

        Returns:
            list[dict[str, Any]] | dict[str, dict[Any, Any]] | None: Returns a dictionary or list of dictionaries. The resulting transformation depends on the orient parameter
        """
        orient_options = {"records", "index"}

        assert (
            orient in orient_options
        ), f"Error: No orient option for {orient}. Please use one of the available options: {orient_options}"

        assert isinstance(
            pass_to_json, bool
        ), "Error: parameter pass_to_json requires boolean argument"

        rows = [row.model_dump() for row in self.data]
        header = [col.name for col in self.column_by_id.values()]

        for i, r in enumerate(rows):
            rows[i] = {k: none_filter(r[k]) for k in r}
            rows[i] = {header[j]: rows[i][val] for j, val in enumerate(rows[i])}

        if not pass_to_json:
            rows = [{k: v.replace('"', "") for k, v in item.items()} for item in rows]

        if orient == "records":
            return rows

        if orient == "index":
            return {f"row{i}": r for i, r in enumerate(rows, 1)}

    def to_json(self, path: str | Path | None = None) -> None | str:
        """Save to json file if filepath is provided, else returns a json string

        Args:
            path (str | Path | None, optional): Path to save json file. Defaults to None.

        Returns:
            None | str: If no path is provided, returns a json string
        """
        data: str | None = self.__get_csv_from_download_csv()

        if data:
            reader = csv.DictReader(StringIO(data))
            json_data = json.dumps(list(reader))
        else:
            rows_to_write = self.to_dict(pass_to_json=True)
            json_data = json.dumps(rows_to_write)
            json_data = json_data.replace('\\"', "").replace(r"\"", "")

        if path:
            if not isinstance(path, Path):
                path = Path(path)
            assert (
                path.suffix.lower() == ".json"
            ), "Error: No suffix or incorrect suffix provided. Please use .json for file suffix"

            self.__write(data=json_data, path=path)
            return

        return json_data
