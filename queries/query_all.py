"""query collection"""

from queries.query_schema import qry_dic as qry_schema

qry_all = [qry_schema]

qry_dic = {}
for qry_cur in qry_all:
    dup = qry_dic.keys() & qry_cur.keys()
    if dup:
        raise ValueError(
            f"duplicated keys: {dup} in {qry_dic.keys()} and {qry_cur.keys()}"
        )

    qry_dic |= qry_cur
