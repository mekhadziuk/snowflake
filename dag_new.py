from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.models import Variable
import models_sn
import query_sql1


path_to_file = Variable.set('load_file', '/Users/mekhadiuk/Desktop/763K_plus_IOS_Apps_Info.csv')

default_args = {
    'start_date': datetime(2022, 9, 13, 1, 1),
    'schedule_interval': "@daily"
}


with DAG('snowflake', default_args=default_args) as dag:
    create_tables_and_streams = SnowflakeOperator(
        task_id='create_db_and_t',
        snowflake_conn_id='conn',
        sql=f"{query_sql1.query_1}"
    )

    insert_data = PythonOperator(
        task_id='insert_pd_to_snowflake',
        python_callable=models_sn.load_data,
        dag=dag
    )

    insert_from_raw_stream = SnowflakeOperator(
        task_id='insert_from_raw_stream',
        snowflake_conn_id='conn',
        sql=f'insert into stage_table select {query_sql1.columns} from raw_stream;'
    )

    insert_from_stage_stream = SnowflakeOperator(
        task_id='insert_from_stage_stream',
        snowflake_conn_id='conn',
        sql=f'insert into master_table select {query_sql1.columns} from stage_stream;'
    )

    create_tables_and_streams >> insert_data >> insert_from_raw_stream >> insert_from_stage_stream
