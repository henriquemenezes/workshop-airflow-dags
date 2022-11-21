import time
import requests
import logging
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email import EmailOperator


# Get email configured to send alert
email_to_alert = Variable.get('http_healthcheck_email_to_alert')

with DAG(
    'http_healthcheck',
    schedule=timedelta(minutes=2),
    start_date=pendulum.datetime(2022, 11, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'email': email_to_alert,
        'email_on_failure': True,
        'retries': 1,
    },
    description='HTTP healthcheck DAG',
    tags=['http', 'healthcheck'],
) as dag:
    def http_request(**kwargs):
        http_endpoint = Variable.get('http_healthcheck_endpoint')
        threshold = float(Variable.get('http_healthcheck_threshold'))

        logging.info(f"Starting healthcheck for '{http_endpoint}' endpoint with threshold {threshold}")

        start = time.time()
        response = requests.get(http_endpoint)
        end = time.time()
        elapsed = end - start

        logging.info(f"Request time elapsed: {elapsed} sec")

        ti = kwargs['ti']
        ti.xcom_push(key='endpoint', value=http_endpoint)
        ti.xcom_push(key='threshold', value=threshold)
        ti.xcom_push(key='elapsed', value=elapsed)
        ti.xcom_push(key='status_code', value=response.status_code)

        if response.status_code == 200 and elapsed <= threshold:
            return 'service_ok'
        elif response.status_code == 200 and elapsed > threshold:
            return 'service_degraded'
        else:
            return 'service_down'

    http_request_op = BranchPythonOperator(
        task_id='http_request',
        python_callable=http_request,
        execution_timeout=timedelta(seconds=30)
    )

    service_ok_op = DummyOperator(task_id='service_ok', dag=dag)

    service_degraded_op = EmailOperator(
        task_id='service_degraded',
        to=email_to_alert,
        subject="Service degraded - {{ ti.xcom_pull(task_ids='http_request', key='endpoint') }}",
        html_content="""<b>Date:</b> {{ ds }}<br>
        <b>Endpoint:</b> {{ ti.xcom_pull(task_ids='http_request', key='endpoint') }}<br>
        <b>Threshold:</b> {{ ti.xcom_pull(task_ids='http_request', key='threshold') }}<br>
        <b>Elapsed:</b> {{ ti.xcom_pull(task_ids='http_request', key='elapsed') }}<br>
        <b>Status Code:</b> {{ ti.xcom_pull(task_ids='http_request', key='status_code') }}<br>
        """)

    service_down_op = EmailOperator(
        task_id='service_down',
        to=email_to_alert,
        subject="Service is down - {{ ti.xcom_pull(task_ids='http_request', key='endpoint') }}",
        html_content="""<b>Date:</b> {{ ds }}<br>
        <b>Endpoint:</b> {{ ti.xcom_pull(task_ids='http_request', key='endpoint') }}<br>
        <b>Threshold:</b> {{ ti.xcom_pull(task_ids='http_request', key='threshold') }}<br>
        <b>Elapsed:</b> {{ ti.xcom_pull(task_ids='http_request', key='elapsed') }}<br>
        <b>Status Code:</b> {{ ti.xcom_pull(task_ids='http_request', key='status_code') }}<br>
        """)

    http_request_op >> [service_ok_op, service_degraded_op, service_down_op]