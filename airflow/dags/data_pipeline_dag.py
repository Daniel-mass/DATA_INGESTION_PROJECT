from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaAdminClient

default_args = {
    'owner': 'Daniel',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def _check_kafka_ready():
    admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
    print(admin.list_topics())

with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Phase-1: Kafka Producer + Consumer only',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 1),
    catchup=False,
) as dag:

    check_kafka = PythonOperator(
        task_id='check_kafka_availability',
        python_callable=_check_kafka_ready
    )

    start_producer = BashOperator(
        task_id='start_weather_producer',
        bash_command='python /opt/airflow/scripts/real_api_producer.py'
    )

    start_consumer = BashOperator(
        task_id='start_weather_consumer',
        bash_command='python /opt/airflow/scripts/real_api_consumer.py'
    )

    check_kafka >> start_producer >> start_consumer
