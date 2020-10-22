from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import pendulum

local_tz = pendulum.timezone("Asia/Taipei")


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['jimmyyang886@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 10, 15 ,14, 0, tzinfo=local_tz),
}

dag = DAG('TWSE_Transactions_MySQL', description='Fetch TWSE to MySQL',
          default_args=default_args,
          schedule_interval='0 16 * * *',
          catchup=False,)
          #schedule_interval='@daily',)

TWSE_Fetch_OP = BashOperator(task_id='TWSE_FETCH_task', 
        bash_command='/home/spark/PycharmProjects/Stock_Price_API/FetchTWSEprice_daily.sh ', dag=dag)

TWSE_Import_OP = BashOperator(task_id='TWSE_import_task', 
        bash_command='/home/spark/PycharmProjects/Stock_Price_API/ImportTWSEprice_daily.sh ', dag=dag)

#proxymysql_OP = BashOperator(task_id='proxy_update_task', 
#        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get.sh ', dag=dag)

proxymysql_start_OP = BashOperator(task_id='proxy_update_addcron_task', 
        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get_addcron.sh ', dag=dag)

proxymysql_stop_OP = BashOperator(task_id='proxy_update_rmcron_task', 
        bash_command='/home/spark/PycharmProjects/Proxy2mySQL/proxy_get_rmcron.sh ', dag=dag,
        trigger_rule='none_skipped')


#[TWSE_Fetch_OP, proxymysql_OP, proxymysql_start_OP]
[TWSE_Fetch_OP, proxymysql_start_OP]

TWSE_Fetch_OP >> [TWSE_Import_OP, proxymysql_stop_OP] 

