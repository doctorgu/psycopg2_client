"""db_client_test"""

from psycopg2_client import Psycopg2Client
from psycopg2_client_settings import Psycopg2ClientSettings

db_settings = Psycopg2ClientSettings(
    password="0000",
    host="127.0.0.1",
    port=5432,
    database="postgres",
    user="postgres",
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
