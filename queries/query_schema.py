"""query for schema"""

qry_dic = dict()

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
