"""db_client_settings"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Psycopg2ClientSettings:
    """db client settings"""

    password: str
    host: str
    port: int
    database: str
    user: str

    minconn: int
    maxconn: int
    connect_timeout: int

    use_en_ko_column_alias: bool
    """SELECT file_name "File Name|파일명" """

    use_conditional: bool
    """
    #if target == 'korea'
        FROM tbl_korea
    #else
        FROM tbl_vietnam
    #endif
    """


# db_settings = Psycopg2ClientSettings(
#     password="0000",
#     host="127.0.0.1",
#     port=5432,
#     database="postgres",
#     user="postgres",
#     minconn=3,
#     maxconn=6,
#     connect_timeout=3,
#     use_en_ko_column_alias=True,
#     use_conditional=True,
# )
