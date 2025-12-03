"""database client"""

import csv
from datetime import datetime, date
import io
import atexit
import re
from typing import AsyncGenerator
import humps
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor, RealDictRow

from psycopg2_client_settings import Psycopg2ClientSettings
from psycopg2_client_util import get_conditional
from queries.query_all import qry_dic


class Psycopg2ClientPool:
    """database connection pool"""

    def __init__(self, db_settings_pool: Psycopg2ClientSettings):
        self.conn_pool = pool.ThreadedConnectionPool(
            minconn=db_settings_pool.minconn,
            maxconn=db_settings_pool.maxconn,
            connect_timeout=db_settings_pool.connect_timeout,
            host=db_settings_pool.host,
            port=db_settings_pool.port,
            database=db_settings_pool.database,
            user=db_settings_pool.user,
            password=db_settings_pool.password,
        )

        print(datetime.now(), self.__class__.__name__, self.__init__.__name__)

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the shared connection pool."""
        if self.conn_pool:
            self.conn_pool.closeall()
            self.conn_pool = None

            print(datetime.now(), self.__class__.__name__, self.__exit__.__name__)

    def getconn(self):
        """return conn_pool"""
        return self.conn_pool.getconn()

    def putconn(self, conn):
        """putconn"""
        self.conn_pool.putconn(conn)


db_pool: Psycopg2ClientPool = None


class Psycopg2Client:
    """database client"""

    # Class-level shared connection pool
    _conn_pool: pool.ThreadedConnectionPool = None

    def __init__(self, db_settings: Psycopg2ClientSettings):
        # pylint:disable=global-statement
        global db_pool

        self.conn = None
        self.cursor = None
        self.in_with_block = False
        self.db_settings = db_settings

        if not db_pool:
            db_pool = Psycopg2ClientPool(db_settings)
        Psycopg2Client._conn_pool = db_pool

    def __enter__(self):
        # Called when entering the 'with' block
        self.conn = Psycopg2Client._conn_pool.getconn()
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.in_with_block = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Called when exiting the 'with' block
        try:
            if exc_type is None:
                # No exception, commit the transaction
                self.conn.commit()
            else:
                # Exception occurred, rollback the transaction
                self.conn.rollback()
        finally:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.conn:
                self._conn_pool.putconn(self.conn)
                self.conn = None

            self.in_with_block = False

    def _serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)

    def _get_query_with_value(self, qry_str: str, params: dict, conn) -> str:
        """replace raw query to value filled query"""

        def escape_literal(value) -> str:
            ret = ""
            if isinstance(value, str):
                ret = "'" + value.replace("'", "''") + "'"
            elif isinstance(value, datetime):
                ret = f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'::TIMESTAMP"
            elif isinstance(value, list):
                ret = f"ARRAY{str(value)}"
            elif value is None:
                ret = "NULL"
            else:
                ret = str(value)
            return ret

        query_raw = sql.SQL(qry_str).as_string(conn)
        query_replaced = query_raw
        for key, value in params.items():
            find = f"%({key})s"
            if find in query_replaced:
                replace = escape_literal(value)
                query_replaced = query_replaced.replace(find, replace)
        # %% -> % : psycopg2
        # {{}} -> {} : python
        query_replaced = (
            query_replaced.replace("%%", "%").replace("{{", "{").replace("}}", "}")
        )

        return query_replaced

    def _replace_en_ko_column_alias(self, qry_str: str, en: bool) -> str:
        """ "
        return en part or ko part separated by '|' using en variable
        ex:
        tbl.obj_nm "File Name|파일명"
        ->
        tbl.obj_nm "File Name"
        """

        pattern = r'(?P<ws>\s)"(?P<en>[^"]+)\|(?P<ko>[^"]+)"'
        en_ko = "en" if en else "ko"
        repl = rf'\g<ws>"\g<{en_ko}>"'
        qry_str_new = re.sub(pattern, repl, qry_str, 0, re.MULTILINE | re.IGNORECASE)
        return qry_str_new

    def _normalize_qry_type_params_list(
        self,
        qry_type_params_list: list[tuple[str, dict]] | list[tuple[str, dict, dict]],
    ):
        """normalize all item from parameter of Psycopg2Client.updates"""

        for i, item in enumerate(qry_type_params_list):
            # append params_out if not exists
            if len(item) == 2:
                item = (item[0], item[1], {})

            qry_type, params, params_out = item
            if not isinstance(params, dict):
                params = vars(params)

            if params_out is None:
                params_out = {}
            if not isinstance(params_out, dict):
                params_out = vars(params_out)

            qry_type_params_list[i] = (qry_type, params, params_out)

    def read_rows(
        self,
        qry_type: str,
        params: dict,
        *,
        camelize: bool = False,
        en: bool = None,
        fetchone: bool = False,
    ) -> list[RealDictRow]:
        """Returns all rows

        Arguments:
            qry_type: Key of the Dictionary registered in the clients/queries folder
            params: Key, Value pairs to pass as parameters to the SQL query.
            camelize: If False (default), returns key (or field) names as database field names;
                    if True, converts key or field names to camelCase and return

        Returns:
            a List of Dictionaries;
        """

        def read_rows_by_param(
            qry_type: str,
            params: dict,
            *,
            camelize: bool = False,
            en: bool = None,
            fetchone: bool = False,
            cursor: any,
        ):
            if not isinstance(params, dict):
                params = vars(params)

            qry_str = qry_dic.get(qry_type)
            if not qry_str:
                raise KeyError(f"{qry_type} not exists")
            if self.db_settings.use_en_ko_column_alias and isinstance(en, bool):
                qry_str = self._replace_en_ko_column_alias(qry_str, en)
            if self.db_settings.use_conditional and "#if" in qry_str:
                qry_str = get_conditional(qry_str, params)

            rows: list[RealDictRow] = []
            cursor.execute(qry_str, params)

            if not fetchone:
                rows = cursor.fetchall()
            else:
                row = cursor.fetchone()
                if row:
                    rows.append(row)

            if not rows:
                return rows
            if camelize:
                rows = humps.camelize(rows)

            return rows

        rows: list[RealDictRow] = []
        if self.in_with_block:
            rows = read_rows_by_param(
                qry_type,
                params,
                camelize=camelize,
                en=en,
                fetchone=fetchone,
                cursor=self.cursor,
            )
        else:
            conn_pool = Psycopg2Client._conn_pool
            try:
                conn = conn_pool.getconn()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                rows = read_rows_by_param(
                    qry_type,
                    params,
                    camelize=camelize,
                    en=en,
                    fetchone=fetchone,
                    cursor=cursor,
                )
            finally:
                cursor.close()
                conn_pool.putconn(conn)

        return rows

    def read_row(
        self,
        qry_type: str,
        params: dict,
        *,
        camelize: bool = False,
        en: bool = None,
    ) -> RealDictRow | None:
        """call read_rows"""

        rows = self.read_rows(
            qry_type,
            params,
            camelize=camelize,
            en=en,
            fetchone=True,
        )
        if not rows:
            return None

        return rows[0]

    async def read_partial_to_csv(
        self,
        qry_type: str,
        params: dict,
        *,
        row_count_partial: int = 100,
        en: bool = None,
    ) -> AsyncGenerator[bytes, None]:
        """Return rows in batches of row_count_partial

        Arguments:
            qry_type: key of the Dictionary registered in the clients/queries folder
            params: key, value pairs to pass as parameters to the SQL query.
            row_count_partial: Number of rows to return at a time

        Returns:
            CSV format converted to UTF-8-BOM
        """

        async def read_partial_to_csv_by_param(
            qry_type: str,
            params: dict,
            *,
            row_count_partial: int = 100,
            en: bool = None,
            cursor: any,
        ) -> AsyncGenerator[bytes, None]:
            if not isinstance(params, dict):
                params = vars(params)

            cursor_name = "cur_partial"
            qry_str = f"DECLARE {cursor_name} CURSOR FOR {qry_dic.get(qry_type)}"
            if self.db_settings.use_en_ko_column_alias and isinstance(en, bool):
                qry_str = self._replace_en_ko_column_alias(qry_str, en)
            if self.db_settings.use_conditional and "#if" in qry_str:
                qry_str = get_conditional(qry_str, params)

            rows: list[RealDictRow] = []
            is_second = False

            cursor.execute(qry_str, params)
            while True:
                cursor.execute(f"FETCH {row_count_partial} FROM {cursor_name}", params)
                rows = cursor.fetchall()

                if not rows:
                    break

                csv_out = io.StringIO()
                csv_w = csv.DictWriter(csv_out, rows[0].keys())
                if not is_second:
                    csv_w.writeheader()

                csv_w.writerows(rows)

                is_second = True

                # without utf-8-sig, hangul will be broken.
                yield csv_out.getvalue().encode("utf-8-sig")

        if self.in_with_block:
            async for value in read_partial_to_csv_by_param(
                qry_type,
                params,
                row_count_partial=row_count_partial,
                en=en,
                cursor=self.cursor,
            ):
                yield value
        else:
            conn_pool = Psycopg2Client._conn_pool
            try:
                conn = conn_pool.getconn()
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                async for value in read_partial_to_csv_by_param(
                    qry_type,
                    params,
                    row_count_partial=row_count_partial,
                    en=en,
                    cursor=cursor,
                ):
                    yield value
            finally:
                cursor.close()
                conn_pool.putconn(conn)

    def updates(
        self,
        qry_type_params_list: list[tuple[str, dict]] | list[tuple[str, dict, dict]],
    ) -> list[int]:
        """Executes a list of SQL statements within a single transaction.
        If all SQL commands succeed, returns a list of the number of rows affected by each qry_type.
        If any command fails, an error is raised.

        Arguments:
            qry_type_params_list: A list of tuples, each containing the following two values:
                qry_type: key of the dictionary registered in the clients/queries folder
                params: key, value pairs to pass as parameters to the SQL query.

        Returns:
            A list of the number of rows affected
        """

        def updates_by_param(
            qry_type_params_list: list[tuple[str, dict]] | list[tuple[str, dict, dict]],
            cursor: any,
        ) -> list[int]:
            row_counts: list[int] = []
            qry_strs: list[str] = []

            self._normalize_qry_type_params_list(qry_type_params_list)

            for item in qry_type_params_list:
                qry_type, params, params_out = item

                qry_str = qry_dic.get(qry_type)
                if not qry_str:
                    raise KeyError(f"{qry_type} not exists")
                if self.db_settings.use_conditional and "#if" in qry_str:
                    qry_str = get_conditional(qry_str, params)

                cursor.execute(qry_str, params)
                row_count = cursor.rowcount

                if params_out:
                    row = cursor.fetchone()
                    for k, v in row.items():
                        if k in params_out:
                            params_out[k] = v

                row_counts.append(row_count)
                qry_strs.append(qry_str)

            return row_counts

        row_counts: list[int] = []
        if self.in_with_block:
            row_counts = updates_by_param(qry_type_params_list, self.cursor)
        else:
            conn_pool = Psycopg2Client._conn_pool
            try:
                with conn_pool.getconn() as conn:
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                    row_counts = updates_by_param(qry_type_params_list, cursor)
                    cursor.close()
            finally:
                conn_pool.putconn(conn)

        return row_counts

    def update(
        self,
        qry_type: str,
        params: dict,
        params_out: dict = None,
    ) -> int:
        """call updates"""

        row_counts = self.updates([(qry_type, params, params_out)])
        return row_counts[0] if row_counts else 0


@atexit.register
def close_all_connection():
    """call when python exits"""

    # pylint:disable=global-statement
    global db_pool

    if db_pool:
        db_pool = None
