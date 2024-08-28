from airflow import DAG
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps
#import pytz
import pendulum

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import (
        PythonOperator,
        )
#KST = pytz.timezone('Asia/Seoul')

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 26),
}

with DAG(
    'time_alarm',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='hello world DAG',
    schedule="20 9 * * *", # 매일 오전 9시 20분 한국 시간에 실행
    #schedule_interval="@hourly",
    #timezone=KST,
    #start_date=datetime(2024, 8, 26),
    start_date=pendulum.datetime(2024, 8, 26, tz="Asia/Seoul"),
    #end_date=datetime(2016,1,1),
    catchup=False,
    tags=["kafka", "chat"],
#    max_active_runs=1,  # 동시에 실행될 수 있는 최대 DAG 인스턴스 수
#    max_active_tasks=3,  # 동시에 실행될 수 있는 최대 태스크 수
) as dag:
    # PythonOperator로 Kafka 데이터를 Parquet로 저장하는 작업 실행

    def air_alarm():
        producer = KafkaProducer(                                                              bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        data = {
            'sender': '*공지*',  # 사용자 이름을 입력하고 시작하는 식으로 고칠까
            'message': '칸반 회의 시간입니다요.',
            'time': datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send('mammamia10', value=data)
        producer.flush()
        return True


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
    start >> airflow_alarm >> end
