# psycopg2_client

Psycopg2 helper function to run PostgreSQL query with #if support

## Usage

- Create query in queries directory

```python
qry_dic.update(
    {
        "read_schema": """
SELECT  table_schema, table_name
#if is_table
FROM    information_schema.tables
#else
FROM    information_schema.columns
#endif
WHERE   table_name ILIKE %(search_percent)s
"""
    }
)
```

- Call query with name and parameters

```python
with Psycopg2Client(db_settings=db_settings) as db_client:
    search = "stat"
    rows_table = db_client.read_rows(
        "read_schema", {"is_table": True, "search_percent": f"%{search}%"}
    )
    rows_column = db_client.read_rows(
        "read_schema", {"is_table": False, "search_percent": f"%{search}%"}
    )
    print("len(rows_table):", len(rows_table)) # len(rows_table): 51
    print("len(rows_column):", len(rows_column)) # len(rows_column): 621
```
