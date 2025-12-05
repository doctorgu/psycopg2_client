"""db_client_test"""

import os
import sys
import json
from dotenv import load_dotenv

# psycopg2_client
sys.path.append(__file__[0 : __file__.find("psycopg2_client") + len("psycopg2_client")])

# pylint: disable=wrong-import-position
from psycopg2_client import Psycopg2Client
from psycopg2_client_settings import Psycopg2ClientSettings
from db_client import DbClient

load_dotenv()

db_settings = Psycopg2ClientSettings(
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_DATABASE"),
    user=os.getenv("DB_USER"),
    minconn=3,
    maxconn=6,
    connect_timeout=3,
    use_en_ko_column_alias=True,
    use_conditional=True,
)


def create_tables():
    """create tables"""

    db_client = Psycopg2Client(db_settings=db_settings)
    db_client.update("create_tables", {})


def upsert_user():
    """upsert user"""

    db_client = Psycopg2Client(db_settings=db_settings)

    row_count = db_client.update(
        "upsert_user", {"user_id": "gildong.hong", "user_name": "홍길똥"}
    )

    # affected row count: 1
    print("affected row count:", row_count)


def upsert_user_params_out():
    """upsert user and get parameters"""

    db_client = Psycopg2Client(db_settings=db_settings)

    params_out = {"user_name": ""}
    db_client.update(
        "upsert_user", {"user_id": "gildong.hong", "user_name": "홍길동"}, params_out
    )

    # user_name after update: 홍길동
    print("user_name after update:", params_out["user_name"])


def upsert_user_list():
    """upsert user list (one transaction)"""

    db_client = Psycopg2Client(db_settings=db_settings)

    qry_list = [
        ("upsert_user", {"user_id": "sunja.kim", "user_name": "김순자"}),
        ("upsert_user", {"user_id": "malja.kim", "user_name": "김말자"}),
    ]
    row_counts = db_client.updates(qry_list)

    # [1, 1]
    print(row_counts)


def upsert_delete_user_with():
    """upsert user and delete in with (one transaction)"""

    with Psycopg2Client(db_settings=db_settings) as db_client:
        id_ = "youngja.lee"
        user_name = "이영자"
        db_client.update("upsert_user", {"user_id": id_, "user_name": user_name})

        row_count = db_client.update("delete_user", {"user_id": id_})

        # affected row count: 1
        print("affected row count:", row_count)


def read_user_one_row():
    """read first one row"""

    db_client = Psycopg2Client(db_settings=db_settings)

    row = db_client.read_row("read_user_id_all", {})

    # RealDictRow({'user_id': 'gildong.hong'})
    print(row)


def read_user_all_rows():
    """read all rows"""

    db_client = Psycopg2Client(db_settings=db_settings)

    rows = db_client.read_rows("read_user_id_all", {})

    # [
    #   RealDictRow({'user_id': 'gildong.hong'}),
    #   RealDictRow({'user_id': 'sunja.kim'}),
    #   RealDictRow({'user_id': 'malja.kim'})
    # ]
    print(rows)


def read_using_conditional():
    """read using conditional (#if #elif #endif)"""

    db_client = Psycopg2Client(db_settings=db_settings)

    # SELECT  user_id, user_name, insert_time, update_time
    # FROM    t_user
    # WHERE   1 = 1
    #         AND user_id = %(user_id)s
    rows = db_client.read_rows(
        "read_user_search", {"user_id": "gildong.hong", "user_name": ""}
    )
    # ['홍길동']
    print([row["user_name"] for row in rows])

    # SELECT  user_id, user_name, insert_time, update_time
    # FROM    t_user
    # WHERE   1 = 1
    #         AND user_name ILIKE %(user_name)s
    rows = db_client.read_rows("read_user_search", {"user_id": "", "user_name": "%김%"})
    # ['김순자', '김말자']
    print([row["user_name"] for row in rows])


def read_using_en_ko():
    """set column user_name by en variable"""

    db_client = Psycopg2Client(db_settings=db_settings)

    # SELECT  user_id "Id", user_name "Name"
    # FROM    t_user
    # WHERE   user_id = %(user_id)s
    rows = db_client.read_rows("read_user_alias", {"user_id": "gildong.hong"}, en=True)
    # [{"Id": "gildong.hong", "Name": "홍길동"}]
    print(json.dumps(rows, ensure_ascii=False))

    # SELECT  user_id "아이디", user_name "이름"
    # FROM    t_user
    # WHERE   user_id = %(user_id)s
    rows = db_client.read_rows("read_user_alias", {"user_id": "gildong.hong"}, en=False)
    # [{"아이디": "gildong.hong", "이름": "홍길동"}]
    print(json.dumps(rows, ensure_ascii=False))


def use_db_client():
    """use inherited class to not use db_settings every time"""

    db_client = DbClient()
    row = db_client.read_row("read_user_id_all", {})

    # RealDictRow({'user_id': 'gildong.hong'})
    print(row)


create_tables()
upsert_user()
upsert_user_params_out()
upsert_user_list()
upsert_delete_user_with()
read_user_one_row()
read_user_all_rows()
read_using_conditional()
read_using_en_ko()
use_db_client()
