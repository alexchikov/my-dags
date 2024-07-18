from airflow.decorators import dag, task
from utils.paths import Paths as pth
from notifiers.tg import TelegramNotifier
from datetime import datetime, timedelta
import boto3
import yaml
import requests
import json
import logging

default_args = {'owner': 'alexchikov',
                'email_on_failure': False,
                'email_on_success': True,
                'retries': 3,
                'depends_on_past': False,
                'retry_delay': timedelta(minutes=5)}

with open(pth.CONFIG_PATH) as cnf_file:
    conf = yaml.safe_load(cnf_file)


@dag(dag_id='elt_cian_dag',
     description='This DAG runs ETL process for parsing data from CIAN',
     schedule='30 9,21 * * *',
     start_date=datetime(year=2024, month=7, day=17),
     default_args=default_args,
     catchup=False,
     on_success_callback=TelegramNotifier('dag succeed',
                                          conf['TOKEN'],
                                          conf['CHAT_ID']),
     on_failure_callback=TelegramNotifier('dag failed',
                                          conf['TOKEN'],
                                          conf['CHAT_ID']))
def elt_cian():
    logger = logging.getLogger(__name__)
    @task(multiple_outputs=True, show_return_value_in_logs=False)
    def get_orders(cookies: dict, headers: dict, json_data: dict) -> dict:
        response = requests.post(
            'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        logger.info('handle response from api')
        data = response.json()
        return data

    @task
    def save_to_s3(data: dict) -> None:
        s3 = boto3.client('s3',
                          aws_access_key_id=conf['my_key'],
                          aws_secret_access_key=conf['secret_access_key'])
        if isinstance(data, dict):
            filename = f'cian_{datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")}.json'
            s3.put_object(Body=json.dumps(data, ensure_ascii=False),
                          Bucket=conf['bucketname'],
                          Key='cian/'+filename)
            logger.info('put object into s3 bucket')
        else:
            raise BrokenPipeError('wrong data type input')

    save_to_s3(get_orders(conf['cookies'], conf['headers'], conf['json_data']))


elt_cian()
