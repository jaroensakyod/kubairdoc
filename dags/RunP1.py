import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pymysql.cursors
import pandas as pd
import requests


class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

# For PythonOperator

def get_data_from_db():
          connection = pymysql.connect(host=Config.MYSQL_HOST,
                                       port=Config.MYSQL_PORT,
                                       user=Config.MYSQL_USER,
                                       password=Config.MYSQL_PASSWORD,
                                       db=Config.MYSQL_DB,
                                       charset=Config.MYSQL_CHARSET,
                                       cursorclass=pymysql.cursors.DictCursor)

          with connection.cursor() as cursor:
                    sql = "SELECT * from bank_term_deposit_old"
                    cursor.execute(sql)
                    bank_term_deposit= cursor.fetchall()

          bank_term = pd.DataFrame(bank_term_deposit)

          bank_term.to_csv("/home/airflow/data/bank_term_deposit.csv", index=False)


def clear_db():
          table_df = pd.read_csv("/home/airflow/data/bank_term_deposit.csv")

          table_df['age'].fillna('41.6', inplace = True)
          table_df['balance'].fillna('1136.75', inplace = True)
          table_df['pdays'] = table_df['pdays'].astype(str)
          table_df['pdays']=table_df.apply(lambda x: x["pdays"].replace("-1","0"), axis=1)
          table_df['pdays'] = table_df['pdays'].astype(int)
          table_df['age'] = table_df['age'].astype(float)
          table_df['balance'] = table_df['balance'].astype(float)

          # save ไฟล์ CSV
          table_df.to_csv("/home/airflow/data/result.csv", index=False)



default_args = {
    'owner': 'datath',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bank_term_deposit',
    default_args=default_args,
    description='Pipeline for ETL bank_term_deposit data',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
          task_id="get_data_from_mysql",
          python_callable=get_data_from_db,
          dag=dag,
)

t2 = PythonOperator(
          task_id="clear_db",
          python_callable=clear_db,
          dag=dag,
)


t1 >> t2
