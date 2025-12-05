# Psycopg2Client — Modern PostgreSQL Helper for Python

A lightweight, opinionated wrapper around **psycopg2** with built-in support for:

- Query dictionary management
- Conditional SQL (`#if` / `#elif` / `#endif`)
- Bilingual column aliases (`en|ko`)
- Simple transaction handling via context manager
- Safe parameter binding

> Successor-friendly alternative to raw psycopg2 with better developer experience.

## Installation

```bash
pip install psycopg2-binary
```

> Note: `Psycopg2Client` is a custom helper class (not on PyPI). See full source in repository.

## Quick Start

### 1. Define Queries

```python
qry_dic.update(
    {
        "upsert_user": """
WITH t AS (
    INSERT INTO t_user
        (
            user_id, user_name
        )
    VALUES
        (
            %(user_id)s, %(user_name)s
        )
    ON CONFLICT (user_id)
    DO UPDATE
    SET     user_name = %(user_name)s,
            update_time = NOW()
    RETURNING user_name
)
SELECT  user_name
FROM    t;
"""
    }
)
```

### 2. Configure Database Connection

```python
from psycopg2_client import Psycopg2Client
from psycopg2_client_settings import Psycopg2ClientSettings

db_settings = Psycopg2ClientSettings(
    host="127.0.0.1",
    port=5432,
    database="postgres",
    user="postgres",
    password="0000",
    minconn=3,
    maxconn=10,
    connect_timeout=5,
    use_en_ko_column_alias=True,
    use_conditional=True,
)
```

### 3. Basic Usage

```python
db = Psycopg2Client(db_settings=db_settings)

# Read single row
row = db.read_row("read_user_id_all", {})
print(row)  # RealDictRow({'user_id': 'gildong.hong'})

# Read all rows
rows = db.read_rows("read_user_id_all", {})
print(rows[:2])
```

## Create / Update / Delete Operations

### `update()` — Single CUD Statement

Returns affected row count:

```python
affected = db.update(
    "upsert_user",
    {"user_id": "gildong.hong", "user_name": "홍길동"}
)
print("Affected rows:", affected)  # 1
```

### Capture Output Parameters

```python
params_out = {"user_name": ""}
db.update(
    "upsert_user",
    {"user_id": "gildong.hong", "user_name": "홍길동"},
    params_out=params_out
)
print("Returned name:", params_out["user_name"])  # 홍길동
```

### `updates()` — Batch Execution

```python
batch = [
    ("upsert_user", {"user_id": "sunja.kim", "user_name": "김순자"}),
    ("upsert_user", {"user_id": "malja.kim", "user_name": "김말자"}),
]

results = db.updates(batch)
print("Batch results:", results)  # [1, 1]
```

## Transaction Support with `with`

Automatically commits on success, rolls back on exception:

```python
with Psycopg2Client(db_settings=db_settings) as db:
    new_id = "youngja.lee"
    db.update("upsert_user", {"user_id": new_id, "user_name": "이영자"})
    db.update("delete_user", {"user_id": new_id})  # Oops! Will rollback entire block
    print("This won't print if error occurs")
```

## Conditional SQL (`#if`, `#elif`, `#endif`)

Enabled when `use_conditional=True`

```python
qry_dic.update(
    {
        "read_user_search": """
SELECT  user_id, user_name, insert_time, update_time
FROM    t_user
WHERE   1 = 1
#if user_id
        AND user_id = %(user_id)s
#elif user_name
        AND user_name ILIKE %(user_name)s
#endif
"""
    }
)
"""
```

### Example: Search by `user_id`

```python
rows = db.read_rows(
    "read_user_search",
    {"user_id": "gildong.hong", "user_name": ""}
)
print([r["user_name"] for r in rows])
# ['홍길동']
```

### Example: Search by `user_name` (partial match)

```python
rows = db.read_rows(
    "read_user_search",
    {"user_id": "", "user_name": "%김%"}
)
print([r["user_name"] for r in rows])
# ['김순자', '김말자']
```

## Bilingual Column Aliases (English ↔ Korean)

Enabled when `use_en_ko_column_alias=True`

```python
qry_dic.update(
    {
        "read_user_alias": """
SELECT  user_id "Id|아이디", user_name "Name|이름"
FROM    t_user
WHERE   user_id = %(user_id)s
"""
    }
)
"""
```

### English mode (`en=True`)

```python
rows = db.read_rows("read_user_alias", {"user_id": "gildong.hong"}, en=True)
print(rows[0])
# {'Id': 'gildong.hong', 'Name': '홍길동'}
```

### Korean mode (`en=False` or omitted)

```python
rows = db.read_rows("read_user_alias", {"user_id": "gildong.hong"}, en=False)
print(rows[0])
# {'아이디': 'gildong.hong', '이름': '홍길동'}
```

## Safety & Security

### Q: Is conditional SQL safe from injection?

**A: Yes — completely safe.**

The `#if` preprocessor **only allows**:

- Parameter names (e.g. `user_id`)
- String literals (`'active'`, `"pending"`)
- Numbers and basic operators
- Whitespace and comments

Any attempt to inject raw SQL will raise a parsing error **before** execution.

```python
# This will RAISE an exception "ValueError: 'user_id;' not in ..." (not execute!)
"#if user_id; DROP TABLE t_user; --"
```

## Features Summary

| Feature                      | Supported | Notes                              |
| ---------------------------- | --------- | ---------------------------------- |
| Connection pooling           | Yes       | Via `minconn` / `maxconn`          |
| Named queries                | Yes       | Stored in dictionary               |
| Single-row / multi-row fetch | Yes       | `read_row()` / `read_rows()`       |
| Batch CUD operations         | Yes       | `updates()` returns list of counts |
| Transactions via `with`      | Yes       | Auto rollback on exception         |
| Conditional SQL              | Yes       | `#if` / `#elif` / `#endif`         |
| Bilingual column aliases     | Yes       | `"Name\|이름"` syntax              |
| SQL injection protection     | Yes       | Strict parsing in conditionals     |
| Output parameters            | Yes       | Via `params_out` dict              |

## License

MIT (or as defined in your project)

---

Made with ❤️ for cleaner, safer PostgreSQL code in Python.
