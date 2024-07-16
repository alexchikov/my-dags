import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from notifiers.tg import TelegramNotifier
from utils.hello import print_hello
from utils.paths import Paths as pth
import yaml


default_args = {'retries': 5,
                'email_on_failure': False,
                'email_on_success': False,
                'depends_on_past': False,
                'retry_delay': datetime.timedelta(minutes=5),
                'owner': 'alexchikov'}
with open(os.environ['HOME']+pth.CONFIG_PATH) as file:
    config_file = yaml.safe_load(file)

with DAG(dag_id='test_dag',
         description='This is my test DAG',
         schedule='30 22 * * *',
         default_args=default_args,
         catchup=False,
         start_date=datetime.datetime(2024, 7, 15)) as dag:
    task1 = PythonOperator(task_id='python_func',
                           python_callable=print_hello,
                           on_success_callback=TelegramNotifier('Yeeah!',
                                                                config_file['TOKEN'],
                                                                config_file['CHAT_ID']),
                           on_failure_callback=TelegramNotifier(':(',
                                                                config_file['TOKEN'],
                                                                config_file['CHAT_ID']))

    task2 = BashOperator(task_id='some_bash',
                         bash_command='echo 1',
                         on_success_callback=TelegramNotifier('Yeeyy, success :)',
                                                              config_file['TOKEN'],
                                                              config_file['CHAT_ID']))

    task1 >> task2
