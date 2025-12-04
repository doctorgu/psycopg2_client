"""db_client_test"""

import os
import sys
from dotenv import load_dotenv

# psycopg2_client
sys.path.append(__file__[0 : __file__.find("psycopg2_client") + len("psycopg2_client")])

# pylint: disable=wrong-import-position
from psycopg2_client import Psycopg2Client
from psycopg2_client_settings import Psycopg2ClientSettings

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


def read():
    """read"""

    with Psycopg2Client(db_settings=db_settings) as db_client:
        search = "stat"
        rows_table = db_client.read_rows(
            "read_schema", {"is_table": True, "search_percent": f"%{search}%"}
        )
        rows_column = db_client.read_rows(
            "read_schema", {"is_table": False, "search_percent": f"%{search}%"}
        )
        print("len(rows_table):", len(rows_table))
        print("len(rows_column):", len(rows_column))


read()
