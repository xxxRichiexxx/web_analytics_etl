import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import TaskInstance


external_bi_con = BaseHook.get_connection('ext_pbi')
external_bi_username = external_bi_con.login
external_bi_password = quote(external_bi_con.password)
external_bi_host = external_bi_con.host
external_bi_db = 'sttt_data_marts'
external_bi_str = fr'mssql://{external_bi_username}:{external_bi_password}@{external_bi_host}/{external_bi_db}?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
external_bi_engine = sa.create_engine(external_bi_str)

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


def extract(table_name, query_part):
    return pd.read_sql_query(
        f"""SELECT * FROM sttgaz.{table_name}""" + query_part,
        dwh_engine,
    )


def transform(data):
    return data


def load(data, table_name, query_part):

    pd.read_sql_query(
        f"""DELETE FROM {table_name}""" + query_part,
        external_bi_engine,
    )

    data.to_sql(
        table_name,
        external_bi_engine,
        schema='dbo',
        if_exists='append',
        index=False,
    )        


def etl(table_name, **context):
    if not context['prev_execution_date_success']:
        query_part = ''
    elif table_name in ('vw_optimatica_dealers',
                        'vw_total_dealers',
                        'vw_total_dealers_all'):
        query_part = f"""WHERE ym_s_date >= {context['execution_date'] - dt.timedelta(days=60)}"""
    elif table_name in ('vw_worklists_models_dealers',
                        'vw_worklists_models_dealers_all'):
        query_part = f"""WHERE "Дата события CRM" >= {context['execution_date'] - dt.timedelta(days=60)}"""

    load(
        transform(
            extract(table_name, query_part)
        ),
        table_name,
        query_part
    )


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'web_analytics_etl',
        default_args=default_args,
        description='Перемещение данных по веб-аналитике из хранилища во внешнюю BI-систему.',
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('ETL') as tg:

        tasks = []
        data_types = (
            'vw_optimatica_dealers',
            'vw_total_dealers',
            'vw_total_dealers_all',
            'vw_worklists_models_dealers',
            'vw_worklists_models_dealers_all',
        )
        for data_type in data_types:
            tasks.append(
                PythonOperator(
                    task_id=f'Перемещение_данных_{data_type}',
                    python_callable=etl,
                    op_kwargs={'table_name': data_type},
                )
            )

        tasks

    end = DummyOperator(task_id='Конец')

    start >> tg >> end
