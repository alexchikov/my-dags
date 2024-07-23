from airflow import DAG
from airflow.operators.bash import BashOperator
from operators.simpleoperator import MySimpleOperator
from notifiers.tg import TelegramNotifier
from utils.paths import Paths as pth
import datetime
import yaml


default_args = {'owner': 'alexchikov',
                'retries': 1,
                'depends_on_past': False,
                'email_on_success': False,
                'email_on_failure': False}
with open(pth.CONFIG_PATH) as config_file:
    config = yaml.safe_load(config_file)


with DAG(dag_id='testing_simple_operator',
         description='This DAG runs simple custom operator',
         default_args=default_args,
         start_date=datetime.datetime(2024, 7, 23),
         schedule='0 12 15 * *',
         catchup=False) as dag:
    my_simple_operator = MySimpleOperator(task_id='my_operator',
                                          on_failure_callback=TelegramNotifier(message='dag failed',
                                                                               bot_token=config['TOKEN'],
                                                                               chat_id=config['CHAT_ID']),
                                          message='Hello, World!',
                                          name='simple_oper')

    another_simple_operator = BashOperator(task_id='bash_simple_task',
                                           bash_command='echo SUCCESS')

    my_simple_operator >> another_simple_operator


if __name__ == '__main__':
    dag.test()
