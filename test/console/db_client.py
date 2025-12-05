"""psycopg2_client wrapper"""

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


class DbClient(Psycopg2Client):
    """db client"""

    def __init__(self):
        super().__init__(db_settings=db_settings)
