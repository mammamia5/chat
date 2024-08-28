from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
import time
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
        )

import pandas as pd
import os

# Kafka 데이터를 처리하고 Parquet 파일로 저장하는 함수
def save_kafka_to_parquet(ds_nodash):
#    from kafka import KafkaConsumer, KafkaProducer
#    from json import loads

    # 데이터를 쌓아둘 리스트
    messages = []

    # Kafka Consumer 설정
    consumer = KafkaConsumer(
        'mammamia10',
        bootstrap_servers=["ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092"],
        auto_offset_reset="earliest",
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=1000,
    )

    for m in consumer:
        data = m.value
        messages.append(str(data))

    if messages:
        df = pd.DataFrame(messages)
        file_path = f"/home/kimpass189/data/chat_data10/{ds_nodash}.parquet"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_parquet(file_path, index=False)
        print(f"Parquet 파일로 저장 완료: {file_path}")
        return True
    return True
def air_alarm():
        producer = KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        data = {
            'sender': '*공지*',  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
            'message': 'Airflow 작업이 완료되었습니다.',
            'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('mammamia10', value=data)
        producer.flush()
        return True
# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 26),
}

with DAG(
    'chat_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    #schedule="10 2 * * *",
    schedule_interval="@hourly",
    start_date=datetime(2024, 8, 26),
    #end_date=datetime(2016,1,1),
    catchup=False,
    tags=["kafka", "chat"],
#    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    # PythonOperator로 Kafka 데이터를 Parquet로 저장하는 작업 실행
    save_parquet_task = PythonOperator(
        task_id='save_parquet',
        python_callable=save_kafka_to_parquet
    )

    airflow_alarm = PythonOperator(
        task_id='air.alarm',
        python_callable=air_alarm,
        trigger_rule = 'all_success'
    )
    
    start = EmptyOperator(
        task_id='start'
    )
    end = EmptyOperator(
        task_id='end'
    )


    # flow
    start >> save_parquet_task >> airflow_alarm >> end
