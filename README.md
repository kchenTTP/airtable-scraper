# Airtable Scraper
<!-- [![Open In Colab](img-link)](colab-link) -->
<!-- [![PyPI](img-link)](pypi-link) -->

A package used to scrape any public airtable.


**Table of Contents**
- [Install](#install)
- [Usage](#usage)
  - [Table Data](#table-data)
  - [Save Airtable As ...](#save-airtable-as-)
  - [Metadata](#metadata)


## Install

```bash
pip install airtable_scraper
```

## Usage

After install, you can use this code for testing (replace url with your own airtable shared view url).

```python
from airtable_scraper import AirtableScraper

url = "https://airtable.com/app7rljHYfQ4tfGrq/shrJ2psoFssctoP5o"
table = AirtableScraper(url=url)

# check status code (ex. 200 = success)
print(table.status)
```

### Table Data

```python
print(table.data)
```

### Save Airtable As ...

#### CSV

- **String**: If no filepath is provided, returns a csv formatted string.

```python
csv_str = table.to_csv()
```

- **CSV File**: If a filepath is provide, save as a csv file. (Change "my_table.csv" to your own filename)

```python
table.to_csv("my_table.csv")
```

#### JSON

- **String**: If no filepath is provided, returns a json formatted string.

```python
json_str = table.to_json()
```
- **JSON File**: If a filepath is provide, save as a json file. (Change "my_table.json" to your own filename)

```python
table.to_json("my_table.json")
```

#### Python Dictionary

Converts data into python dictionary format.

- **`orient`**: Use `orient` parameter to change the type of values of the dictionary.
  - "records": list like [{column -> value}, … , {column -> value}]
  - "index": dict like {index -> {column -> value}}

```python
data_d = table.to_dict()
```

#### Pandas DataFrame

Save data to Pandas DataFrame.

```python
df = table.to_dataframe()
```


### Metadata

You can use these helper methods to checkout additional Airtable metadata or http request parameters.

#### Airtable Information

print a summary of the airtable, including the airtable column type and python datatype.

```python
table.info()
```

*Additional Airtable shared view information*

```python
print(table.view_id)
print(table.table_id)
print(table.app_id)
print(table.share_id)
```

#### HTTP Request Parameters & Responses

```python
print(table.headers)
print(table.request_id)
print(table.raw_rows_json)
print(table.raw_columns_json)
```
