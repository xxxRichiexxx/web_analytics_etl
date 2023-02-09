import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt




external_bi_username = 'svc-pbi_ext_rw'
external_bi_password = quote('L0HlPe14qSNmSBIMocrG')
external_bi_host = 'dmz-vs01sql.ext.st.tech\INSTDMZ'
external_bi_db = 'sttt_data_marts'
external_bi_str = fr'mssql://{external_bi_username}:{external_bi_password}@{external_bi_host}/{external_bi_db}?driver=SQL Server'
external_bi_engine = sa.create_engine(external_bi_str)


dwh_login = 'ShveynikovAB'
dwh_host = 'vs-da-vertica'
dwh_port = '5433'
ps = quote('s@vy7hSA')
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_login}:{ps}@{dwh_host}:{dwh_port}/sttgaz'
)


def extract(table_name, query_part):
    return pd.read_sql_query(
        f"""SELECT *  FROM sttgaz.{table_name} LIMIT 10""" + query_part,
        dwh_engine,
    )


def transform(data):
    print(data)
    return data


def load(data, table_name, query_part):

    # pd.read_sql_query(
    #     f"""DELETE FROM {table_name}""" + query_part,
    #     external_bi_engine,
    # )

    data.to_sql(
        table_name,
        external_bi_engine,
        schema='dbo',
        if_exists='append',
        index=False,
    )        


def etl(table_name, **context):

    query_part = ''


    load(
        transform(
            extract(table_name, query_part)
        ),
        table_name,
        query_part
    )


etl('vw_optimatica_dealers')

data = pd.read_sql_query(
    """SELECT * FROM dbo.vw_optimatica_dealers""",
    external_bi_engine,
)
print(data)