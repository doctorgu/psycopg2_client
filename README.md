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
pip install psycopg2-client
```

> Note: `psycopg2-client` is a custom helper class. See full source in repository.

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
    before_read_execute=lambda qry_type, params, qry_str, qry_with_value: print(
        f'READ_ROWS_START, QRY_TYPE: "{qry_type}"' f", QRY_WITH_VALUE: {qry_with_value}"
    ),
    after_read_execute=lambda qry_type, duration: print(
        f'READ_ROWS_END, QRY_TYPE: "{qry_type}"' f", DURATION: {duration}"
    ),
    before_update_execute=lambda qry_type, params, params_out, qry_str, qry_with_value: print(
        f'UPDATES_START, QRY_TYPE: "{qry_type}"' f", QRY_WITH_VALUE: {qry_with_value}"
    ),
    after_update_execute=lambda qry_type, row_count, params_out, duration: print(
        f'UPDATES_END, QRY_TYPE: "{qry_type}"' f", DURATION: {duration}"
    ),
)
```

### 3. Basic Usage

```python
from psycopg2_client import Psycopg2Client

db = Psycopg2Client(db_settings=db_settings)

# Read single row
row = db.read_row("read_user_id_all", {})
print(row)  # {'user_id': 'gildong.hong'}

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

## Partially return CSV

Read rows partially and return immediately to client to show progress in client

```python
# Flask
@app.route("/read-csv-partial")
def read_csv_partial():
    """read csv partial"""

    db_client = Psycopg2Client(db_settings=db_settings)
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

    return Response(
        db_client.read_csv_partial("read_csv_partial", {}),
        mimetype="text/csv",
        headers={
            # if FE and BE are on different origins,
            # server must expose the Content-Disposition header
            "Access-Control-Expose-Headers": "Content-Disposition",
            "Content-Disposition": f'attachment; filename="{filename}"',
            # Very important for progressive saving in many browsers
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",  # Important if using nginx
            "Transfer-Encoding": "chunked",
        },
    )

# Fast API
@router.get("/read-csv-partial-async")
async def read_csv_partial_async():
    """read csv partial async"""

    db_client = Psycopg2Client(db_settings=db_settings)
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

    return StreamingResponse(
        content=db_client.read_csv_partial_async("read_csv_partial", {}),
        media_type="text/csv",
        headers={
            # if FE and BE are on different origins,
            # server must expose the Content-Disposition header
            "Access-Control-Expose-Headers": "Content-Disposition",
            "Content-Disposition": f'attachment; filename="{filename}"',
            # Very important for progressive saving in many browsers
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",  # Important if using nginx
            "Transfer-Encoding": "chunked",
        },
    )
```

## Bilingual Column Aliases (English ↔ Korean)

Enabled when `use_en_ko_column_alias=True` and `en` not ommited

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

### Korean mode (`en=False`)

```python
rows = db.read_rows("read_user_alias", {"user_id": "gildong.hong"}, en=False)
print(rows[0])
# {'아이디': 'gildong.hong', '이름': '홍길동'}
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

## Logging support

- `before_read_execute` called before execute query for read
- `after_read_execute` called after execute query for read
- `before_update_execute` called before execute query for CUD
- `after_update_execute` called after execute query for CUD

Can be replaced `print` with `logger`

### Example: Use logger to write debug info

```python
import logging

def get_sql_logger(name="sql"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logging.basicConfig(
            filename="sql.log",
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)7s] %(message)s",
            encoding="utf-8"
        )
    return logger

logger = get_sql_logger()
db_settings.before_read_execute = lambda qry_type, params, qry_str, qry_with_value: logger.debug(
    f'READ_ROWS_START, QRY_TYPE: "{qry_type}"' f", QRY_WITH_VALUE: {qry_with_value}"
)
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

| Feature                       | Notes                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------- |
| Connection pooling            | Via `minconn` / `maxconn`                                                       |
| Named queries                 | Stored in dictionary                                                            |
| Single-row / multi-row fetch  | `read_row()` / `read_rows()`                                                    |
| Single / batch CUD operations | `update()` / `updates()`                                                        |
| Output parameters             | Via `params_out` dict                                                           |
| Transactions via `with`       | Auto rollback on exception                                                      |
| Partially return CSV          | `read_csv_partial` / `read_csv_partial_async`                                   |
| Bilingual column aliases      | `"Name\|이름"` syntax                                                           |
| Conditional SQL               | `#if` / `#elif` / `#endif`                                                      |
| Logging support               | Before and after execute to DB via `before...` and `after...` callable function |
| SQL injection protection      | Strict parsing in conditionals                                                  |

## License

MIT (or as defined in your project)

---

Made with ❤️ for cleaner, safer PostgreSQL code in Python.
