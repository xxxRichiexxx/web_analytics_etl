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
    """Извлечение данных из источника."""
    with open(fr'/home/da/airflow/dags/web_analytics_etl/{table_name}.sql', 'r') as f:
        command = f.read() + query_part

    print(command)

    return pd.read_sql_query(
        command,
        dwh_engine,
    )


def transform(data):
    """Трансформация данных (если необходимо)."""
    return data


def load(data, table_name, query_part):
    """Загрузка данных в приемник."""

    external_bi_engine.execute(
        f"""DELETE FROM dbo.{table_name}""" + query_part,
    )
    print(data)

    data.to_sql(
        table_name,
        external_bi_engine,
        schema='dbo',
        if_exists='append',
        index=False,
    )        


def etl(table_name, **context):
    """Collable-объект. вызываемый оркестратором."""

    print('ДАТА ПРЕДЫДУЩЕГО УДАЧНОГО ЗАПУСКА', context['prev_execution_date_success'])
    execution_date = context['execution_date'].date()

    if not context['prev_execution_date_success']:
        query_part = ''
    elif table_name in ('vw_total_dealers',
                        'vw_total_dealers_all'):
        query_part = f""" WHERE ym_s_date >= '{execution_date - dt.timedelta(days=40)}'"""
    elif table_name in ('vw_worklists_models_dealers',
                        'vw_worklists_models_dealers_all'):
        query_part = f""" WHERE "Дата события CRM" >= '{execution_date - dt.timedelta(days=90)}'"""
    else:
        query_part = f""" WHERE date >= '{execution_date - dt.timedelta(days=450)}'"""

    load(
        transform(
            extract(table_name, query_part)
        ),
        table_name,
        query_part
    )

    data_in_sorce = pd.read_sql_query(
        f"""SELECT COUNT(*) FROM sttgaz.{table_name}""",
        dwh_engine,
    )

    data_in_ext_BI = pd.read_sql_query(
        f"""SELECT COUNT(*) FROM dbo.{table_name}""",
        external_bi_engine,
    )

    if data_in_sorce.values[0][0] != data_in_ext_BI.values[0][0]:
        raise Exception(f'Количество записей в источнике и приемнике не совпадает: {data_in_sorce} != {data_in_ext_BI}')      


#-------------- DAG -----------------
default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 2,
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
                    trigger_rule = 'all_done',
                )
            )

        tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> tasks[4]

    end = DummyOperator(task_id='Конец')

    start >> tg >> end
