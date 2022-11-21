import os
import logging
import pendulum


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def load_data(
    output_file,
    table_name,
):
    import pyarrow.parquet as pq
    from sqlalchemy import create_engine

    postgresql_conn_string = Variable.get('postgres_conn_string_secret')
    engine = create_engine(postgresql_conn_string)

    trips = pq.read_table(output_file)
    trips = trips.to_pandas()
    trips.to_sql(
        name=table_name, con=engine, if_exists="replace", index=False, chunksize=10000
    )

    logging.info(f"Congrats! Data loaded to the {table_name} table.")


with DAG(
    "extract_load_parquet",
    schedule="@monthly",
    start_date=pendulum.datetime(2022, 1, 1),
    end_date=pendulum.datetime(2022, 1, 31),
):
    output_file = AIRFLOW_HOME + "/trips_{{ ds }}.parquet"
    URL = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
        + "{{ dag_run.logical_date.strftime('%Y-%m') }}.parquet"
    )

    extract_data_task = BashOperator(
        task_id="extract_data", bash_command=f"curl -o {output_file} {URL}"
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs=dict(
            output_file=output_file,
            table_name="trips_{{ dag_run.logical_date.strftime('%Y_%m') }}",
        ),
    )

    extract_data_task >> load_data_task