from uuid import uuid4

import pandas as pd


def iter_sql_chunks(sql, conn, params=None, chunksize=50000):
    if chunksize <= 0:
        raise ValueError("O tamanho do lote precisa ser maior que zero.")
    with conn.cursor(name=f"export_{uuid4().hex}") as cursor:
        cursor.itersize = chunksize
        cursor.execute(sql, params)
        yielded = False
        while True:
            rows = cursor.fetchmany(chunksize)
            columns = [column[0] for column in cursor.description]
            if not rows:
                if not yielded:
                    yield pd.DataFrame(columns=columns)
                break
            # Mantem a conversao de Decimal usada por read_sql_query.
            yield pd.DataFrame.from_records(rows, columns=columns, coerce_float=True)
            yielded = True


def read_sql_dataframe(sql, conn, params=None):
    return pd.concat(iter_sql_chunks(sql, conn, params), ignore_index=True)
