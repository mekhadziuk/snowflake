import pandas as pd
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def load_data():
    path = Variable.get('load_file')
    df = pd.read_csv(path)
    hook_connection = SnowflakeHook(snowflake_conn_id='snowflake_good')
    engine = hook_connection.get_sqlalchemy_engine()
    start = 0
    step = 10000
    try:
        connection = engine.connect().execution_options(autocommit=True)

        for i in range(df.shape[0] // step + 1):
            df.iloc[start:start+step, :].to_sql("raw_table", con=connection, if_exists='append', index=False)
            start += step
    finally:
        connection.close()
        engine.dispose()
